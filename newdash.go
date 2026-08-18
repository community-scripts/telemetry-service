package main

// A single aggregate for the consolidated dashboard at /new.
//
// Everything starts by collapsing rows to runs. An install writes several rows --
// installing, configuring, then a terminal status -- so counting rows answers
// "how many pings" while counting runs answers "how many installs". The existing
// pages mix the two, which is why the same figure differs between them. Here
// every number shares one unit, and the page says which.
//
// The vocabulary is the engine's own: error categories come from
// categorize_error in core/api/exitcodes.func, exit codes from the table beside
// it. Grouping by anything else would describe the dashboard rather than the
// thing being measured.

import (
	"context"
	"database/sql"
	"fmt"
)

// runsCTE collapses the filtered rows to one row per run.
//
// argMax(status, created) is the terminal status: whatever the run last
// reported. A run that stopped at "installing" never reached one, which makes it
// unfinished rather than failed -- a distinction the old pages do not draw.
const runsCTE = `
	SELECT
		if(execution_id = '', random_id, execution_id) AS run,
		argMax(status, created)                        AS final_status,
		argMax(exit_code, created)                     AS final_exit,
		argMax(error_category, created)                AS final_cat,
		argMax(error, created)                         AS final_err,
		max(created)                                   AS last_seen,
		any(nsapp)                                     AS nsapp,
		any(type)                                      AS type,
		any(method)                                    AS method,
		any(os_type)                                   AS os_type,
		any(os_version)                                AS os_version,
		any(pve_version)                               AS pve_version,
		any(%s)                                        AS plat,
		any(repo_source)                               AS repo_source,
		any(repo_slug)                                 AS repo_slug,
		any(ct_type)                                   AS ct_type,
		any(core_count)                                AS core_count,
		any(ram_size)                                  AS ram_size,
		any(disk_size)                                 AS disk_size,
		any(cpu_vendor)                                AS cpu_vendor,
		any(gpu_vendor)                                AS gpu_vendor,
		any(gpu_passthrough)                           AS gpu_passthrough,
		any(has_arm)                                   AS has_arm,
		max(install_duration)                          AS duration
	FROM telemetry_db.telemetry
	WHERE %s
	GROUP BY run`

// NewCount is a plain label/count, for breakdowns where a rate makes no sense.
type NewCount struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

// NewGroupStat is the shape used everywhere a rate is shown. It always carries
// its own denominators, so the page never has to display a percentage whose
// basis the reader cannot see.
type NewGroupStat struct {
	Label      string `json:"label"`
	Runs       int    `json:"runs"`
	Success    int    `json:"success"`
	Failed     int    `json:"failed"`
	Aborted    int    `json:"aborted"`
	Unfinished int    `json:"unfinished"`
}

// NewExitCode adds the engine's own description to a raw code.
type NewExitCode struct {
	Code  int    `json:"code"`
	Desc  string `json:"desc"`
	Count int    `json:"count"`
	Apps  int    `json:"apps"`
}

// NewErrorSignature groups failures by the exact message the engine captured.
type NewErrorSignature struct {
	Category string `json:"category"`
	ExitCode int    `json:"exit_code"`
	Message  string `json:"message"`
	Count    int    `json:"count"`
	Apps     int    `json:"apps"`
}

// NewRecentRun is one line of the install log.
type NewRecentRun struct {
	App      string `json:"app"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code"`
	Category string `json:"category"`
	Platform string `json:"platform"`
	OS       string `json:"os"`
	Repo     string `json:"repo"`
	Cores    int    `json:"cores"`
	RAM      int    `json:"ram"`
	Disk     int    `json:"disk"`
	Duration int    `json:"duration"`
	LastSeen string `json:"last_seen"`
	Error    string `json:"error"`
}

// NewDailyPoint is one day of the trend, on the same run basis as the rest.
type NewDailyPoint struct {
	Day        string `json:"day"`
	Runs       int    `json:"runs"`
	Success    int    `json:"success"`
	Failed     int    `json:"failed"`
	Aborted    int    `json:"aborted"`
	Unfinished int    `json:"unfinished"`
}

// NewDashData is the whole page in one response. One request, one render: the
// point of this page is that nothing needs a second one.
type NewDashData struct {
	Days     int    `json:"days"`
	Platform string `json:"platform"`
	Repo     string `json:"repo"`
	RepoSlug string `json:"repo_slug"`
	Type     string `json:"type"`

	// Headline, on runs.
	Runs       int `json:"runs"`
	Success    int `json:"success"`
	Failed     int `json:"failed"`
	Aborted    int `json:"aborted"`
	Unfinished int `json:"unfinished"`

	// Runs ever recorded, ignoring every filter. Labelled as such on the page,
	// because a number that ignores the controls above it is otherwise a trap.
	AllTime int `json:"all_time"`

	// Median, not mean: one stuck run reporting the 86400-second clamp drags an
	// average far enough to be useless.
	MedianDuration int `json:"median_duration"`

	// MinRuns is the floor below which a rate is shown but not colour-graded.
	// Sent so the page can say what it is instead of hiding the rule.
	MinRuns int `json:"min_runs"`

	Daily []NewDailyPoint `json:"daily"`

	// Everything that carries a rate uses the same shape.
	TopApps    []NewGroupStat `json:"top_apps"`
	WorstApps  []NewGroupStat `json:"worst_apps"`
	ByPlatform []NewGroupStat `json:"by_platform"`
	ByType     []NewGroupStat `json:"by_type"`
	ByRepo     []NewGroupStat `json:"by_repo"`
	ByOS       []NewGroupStat `json:"by_os"`
	ByHostVer  []NewGroupStat `json:"by_host_version"`
	ByRepoSlug []NewGroupStat `json:"by_repo_slug"`

	// Failure analysis.
	Categories []NewCount          `json:"categories"`
	ExitCodes  []NewExitCode       `json:"exit_codes"`
	Signatures []NewErrorSignature `json:"signatures"`

	// Advanced: what the container was actually given.
	ByPrivilege []NewCount `json:"by_privilege"`
	ByCores     []NewCount `json:"by_cores"`
	ByRAM       []NewCount `json:"by_ram"`
	ByDisk      []NewCount `json:"by_disk"`
	ByCPU       []NewCount `json:"by_cpu"`
	ByGPU       []NewCount `json:"by_gpu"`
	ByGPUPass   []NewCount `json:"by_gpu_passthrough"`
	ByArm       []NewCount `json:"by_arm"`
	ByMethod    []NewCount `json:"by_method"`

	Recent []NewRecentRun `json:"recent"`
}

// minRunsFor scales the floor with the window. A percentage over three runs is
// noise wearing a number's clothes, and on a one-day view almost everything has
// only a handful.
func minRunsFor(days int) int {
	switch {
	case days <= 1:
		return 5
	case days <= 7:
		return 10
	case days <= 30:
		return 20
	default:
		return 40
	}
}

func scanCounts(rows *sql.Rows, err error) ([]NewCount, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NewCount{}
	for rows.Next() {
		var c NewCount
		if err := rows.Scan(&c.Label, &c.Count); err != nil {
			return nil, err
		}
		if c.Label == "" {
			c.Label = "unknown"
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func scanGroupStats(rows *sql.Rows, err error) ([]NewGroupStat, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NewGroupStat{}
	for rows.Next() {
		var g NewGroupStat
		if err := rows.Scan(&g.Label, &g.Runs, &g.Success, &g.Failed,
			&g.Aborted, &g.Unfinished); err != nil {
			return nil, err
		}
		if g.Label == "" {
			g.Label = "unknown"
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// FetchNewDashboard builds the whole consolidated page in one pass.
func (ch *CHClient) FetchNewDashboard(
	ctx context.Context, days int, repoSource, repoSlug, platform, ctype string,
) (*NewDashData, error) {
	extras := []string{}
	if ctype != "" {
		// Allowlisted by the handler before it reaches here.
		extras = append(extras, fmt.Sprintf("type = '%s'", ctype))
	}
	where, args := chWhere(days, repoSource, repoSlug, platform, extras...)
	runs := fmt.Sprintf(runsCTE, platformExpr, where)

	d := &NewDashData{
		Days: days, Platform: platform, Repo: repoSource,
		RepoSlug: repoSlug, Type: ctype, MinRuns: minRunsFor(days),
	}

	// Headline.
	if err := ch.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			count(),
			countIf(final_status = 'success'),
			countIf(final_status = 'failed'),
			countIf(final_status = 'aborted'),
			countIf(final_status NOT IN ('success','failed','aborted'))
		FROM (%s)`, runs), args...,
	).Scan(&d.Runs, &d.Success, &d.Failed, &d.Aborted, &d.Unfinished); err != nil {
		return nil, fmt.Errorf("headline: %w", err)
	}

	// All time, deliberately unfiltered. uniq rather than uniqExact: it is a
	// scan of the whole table and approximate is good enough for a figure that
	// is explicitly labelled as an all-time total.
	_ = ch.db.QueryRowContext(ctx,
		`SELECT uniq(if(execution_id = '', random_id, execution_id)) FROM telemetry_db.telemetry`,
	).Scan(&d.AllTime)

	// Median over runs that actually finished.
	_ = ch.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT toUInt32(median(duration)) FROM (%s)
		WHERE final_status = 'success' AND duration > 0`, runs), args...,
	).Scan(&d.MedianDuration)

	// group runs a breakdown that carries its own denominators.
	//
	// The predicate goes in WHERE, not HAVING: these are per-run columns, not
	// aggregates, and ClickHouse rejects HAVING on an ungrouped column. That
	// mistake is why the first version of this page returned 500 for everything.
	group := func(target *[]NewGroupStat, expr, pred, order string, limit int) error {
		w := ""
		if pred != "" {
			w = " WHERE " + pred
		}
		q := fmt.Sprintf(`
			SELECT %s AS label, count() runs,
			       countIf(final_status = 'success')  success,
			       countIf(final_status = 'failed')   failed,
			       countIf(final_status = 'aborted')  aborted,
			       countIf(final_status NOT IN ('success','failed','aborted')) unfinished
			FROM (%s)%s
			GROUP BY label
			ORDER BY %s
			LIMIT %d`, expr, runs, w, order, limit)
		v, e := scanGroupStats(ch.db.QueryContext(ctx, q, args...))
		if e != nil {
			return e
		}
		*target = v
		return nil
	}

	simple := func(target *[]NewCount, expr, pred string, limit int) error {
		w := ""
		if pred != "" {
			w = " WHERE " + pred
		}
		q := fmt.Sprintf(
			"SELECT %s AS label, count() c FROM (%s)%s GROUP BY label ORDER BY c DESC LIMIT %d",
			expr, runs, w, limit)
		v, e := scanCounts(ch.db.QueryContext(ctx, q, args...))
		if e != nil {
			return e
		}
		*target = v
		return nil
	}

	osExpr := "if(os_type = '', 'unknown', concat(os_type, ' ', os_version))"

	groups := []struct {
		name   string
		target *[]NewGroupStat
		expr   string
		pred   string
		order  string
		limit  int
	}{
		{"top apps", &d.TopApps, "nsapp", "nsapp != ''", "runs DESC", 25},
		// Worst by rate, with the floor applied in SQL so the ranking is the
		// ranking -- not a top-25 that the page then has to filter again.
		{"worst apps", &d.WorstApps, "nsapp", "nsapp != ''",
			fmt.Sprintf("if(success + failed >= %d, failed / (success + failed), -1) DESC, failed DESC",
				d.MinRuns), 25},
		{"platform", &d.ByPlatform, "if(plat = '', 'unknown', plat)", "", "runs DESC", 6},
		{"type", &d.ByType, "if(type = '', 'unknown', type)", "", "runs DESC", 10},
		{"repo", &d.ByRepo, "if(repo_source = '', 'unknown', repo_source)", "", "runs DESC", 10},
		{"repo slug", &d.ByRepoSlug, "repo_slug", "repo_slug != ''", "runs DESC", 15},
		{"os", &d.ByOS, osExpr, "", "runs DESC", 15},
		{"host version", &d.ByHostVer, "pve_version", "pve_version != ''", "runs DESC", 15},
	}
	for _, g := range groups {
		if e := group(g.target, g.expr, g.pred, g.order, g.limit); e != nil {
			return nil, fmt.Errorf("%s: %w", g.name, e)
		}
	}

	counts := []struct {
		name   string
		target *[]NewCount
		expr   string
		pred   string
		limit  int
	}{
		// user_aborted is excluded: someone answering "no" is not a defect, and
		// including it makes it the largest bar on the chart.
		{"categories", &d.Categories, "final_cat",
			"final_status = 'failed' AND final_cat NOT IN ('', 'user_aborted')", 15},
		{"privilege", &d.ByPrivilege, "if(ct_type = 1, 'unprivileged', 'privileged')", "", 3},
		{"cores", &d.ByCores, "toString(core_count)", "core_count > 0", 12},
		{"ram", &d.ByRAM, "concat(toString(intDiv(ram_size, 1024)), ' GB')", "ram_size > 0", 12},
		{"disk", &d.ByDisk, "concat(toString(disk_size), ' GB')", "disk_size > 0", 12},
		{"cpu", &d.ByCPU, "cpu_vendor", "cpu_vendor != ''", 8},
		{"gpu", &d.ByGPU, "gpu_vendor", "gpu_vendor != ''", 8},
		{"gpu passthrough", &d.ByGPUPass, "gpu_passthrough", "gpu_passthrough != ''", 6},
		{"arm", &d.ByArm, "if(has_arm = 1, 'arm64', 'x86_64')", "", 3},
		{"method", &d.ByMethod, "method", "method != ''", 8},
	}
	for _, c := range counts {
		if e := simple(c.target, c.expr, c.pred, c.limit); e != nil {
			return nil, fmt.Errorf("%s: %w", c.name, e)
		}
	}

	// Exit codes, labelled with the engine's own descriptions.
	rows, e := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT final_exit, count() c, uniqExact(nsapp) apps
		FROM (%s)
		WHERE final_status = 'failed' AND final_exit != 0
		GROUP BY final_exit
		ORDER BY c DESC
		LIMIT 20`, runs), args...)
	if e != nil {
		return nil, fmt.Errorf("exit codes: %w", e)
	}
	for rows.Next() {
		var x NewExitCode
		if rows.Scan(&x.Code, &x.Count, &x.Apps) == nil {
			x.Desc = getExitCodeDescription(x.Code)
			d.ExitCodes = append(d.ExitCodes, x)
		}
	}
	rows.Close()

	// Signatures: the same failure text seen across runs. The closest thing to
	// "why", and the existing pages compute it without ever showing it.
	rows, e = ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT final_cat, final_exit, substring(final_err, 1, 200) msg,
		       count() c, uniqExact(nsapp) apps
		FROM (%s)
		WHERE final_status = 'failed' AND final_err != ''
		GROUP BY final_cat, final_exit, msg
		ORDER BY c DESC
		LIMIT 30`, runs), args...)
	if e != nil {
		return nil, fmt.Errorf("signatures: %w", e)
	}
	for rows.Next() {
		var s NewErrorSignature
		if rows.Scan(&s.Category, &s.ExitCode, &s.Message, &s.Count, &s.Apps) == nil {
			d.Signatures = append(d.Signatures, s)
		}
	}
	rows.Close()

	// Daily trend, on the same run basis as everything else.
	rows, e = ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT toString(toDate(last_seen)) day, count(),
		       countIf(final_status = 'success'),
		       countIf(final_status = 'failed'),
		       countIf(final_status = 'aborted'),
		       countIf(final_status NOT IN ('success','failed','aborted'))
		FROM (%s)
		GROUP BY day ORDER BY day`, runs), args...)
	if e != nil {
		return nil, fmt.Errorf("daily: %w", e)
	}
	for rows.Next() {
		var p NewDailyPoint
		if rows.Scan(&p.Day, &p.Runs, &p.Success, &p.Failed,
			&p.Aborted, &p.Unfinished) == nil {
			d.Daily = append(d.Daily, p)
		}
	}
	rows.Close()

	// The install log.
	rows, e = ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT nsapp, type, final_status, final_exit, final_cat, plat,
		       %s, repo_slug, core_count, ram_size, disk_size, duration,
		       formatDateTime(last_seen, '%%Y-%%m-%%d %%H:%%M'),
		       substring(final_err, 1, 200)
		FROM (%s)
		ORDER BY last_seen DESC
		LIMIT 200`, osExpr, runs), args...)
	if e != nil {
		return nil, fmt.Errorf("recent: %w", e)
	}
	for rows.Next() {
		var r NewRecentRun
		if rows.Scan(&r.App, &r.Type, &r.Status, &r.ExitCode, &r.Category,
			&r.Platform, &r.OS, &r.Repo, &r.Cores, &r.RAM, &r.Disk,
			&r.Duration, &r.LastSeen, &r.Error) == nil {
			d.Recent = append(d.Recent, r)
		}
	}
	rows.Close()

	return d, nil
}
