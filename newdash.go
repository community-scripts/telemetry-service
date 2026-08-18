package main

// A single aggregate for the consolidated dashboard at /new.
//
// Everything here starts by collapsing rows to runs. An install writes several
// rows -- installing, configuring, then a terminal status -- so counting rows
// answers "how many pings" while counting runs answers "how many installs". The
// existing pages mix the two, which is why the same figure differs between them.
// Here every number on the page shares one unit, and the page says which.
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

// NewCount is a label with a count, used for every breakdown on the page.
type NewCount struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

// NewOutcome is the headline: one run, one outcome.
type NewOutcome struct {
	Runs       int `json:"runs"`
	Success    int `json:"success"`
	Failed     int `json:"failed"`
	Aborted    int `json:"aborted"`
	Unfinished int `json:"unfinished"`
}

// NewFailingScript pairs a script with how often it failed, and out of how many.
type NewFailingScript struct {
	App    string `json:"app"`
	Runs   int    `json:"runs"`
	Failed int    `json:"failed"`
}

// NewErrorSignature groups failures by the exact message the engine captured.
type NewErrorSignature struct {
	Category string `json:"category"`
	ExitCode int    `json:"exit_code"`
	Message  string `json:"message"`
	Count    int    `json:"count"`
	Apps     int    `json:"apps"`
}

// NewRecentRun is one line of the run log.
type NewRecentRun struct {
	App      string `json:"app"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code"`
	Category string `json:"category"`
	Platform string `json:"platform"`
	OS       string `json:"os"`
	Duration int    `json:"duration"`
	LastSeen string `json:"last_seen"`
	Error    string `json:"error"`
}

// NewDailyPoint is one day of the trend.
type NewDailyPoint struct {
	Day     string `json:"day"`
	Runs    int    `json:"runs"`
	Success int    `json:"success"`
	Failed  int    `json:"failed"`
}

// NewDashData is the whole page in one response. One request, one render: the
// point of this page is that nothing needs a second one.
type NewDashData struct {
	Days     int    `json:"days"`
	Platform string `json:"platform"`
	Repo     string `json:"repo"`
	Type     string `json:"type"`

	Outcome NewOutcome `json:"outcome"`

	// Failure analysis, in the engine's own terms.
	Categories []NewCount          `json:"categories"`
	ExitCodes  []NewCount          `json:"exit_codes"`
	Failing    []NewFailingScript  `json:"failing"`
	Signatures []NewErrorSignature `json:"signatures"`

	// Platform split. Rows predating the platform column have it derived from
	// pve_version -- see platformExpr.
	ByPlatform []NewCount `json:"by_platform"`
	ByType     []NewCount `json:"by_type"`
	ByRepo     []NewCount `json:"by_repo"`

	// OS and host.
	ByOS         []NewCount `json:"by_os"`
	ByHostVer    []NewCount `json:"by_host_version"`
	SuccessByOS  []NewCount `json:"success_by_os"`
	FailuresByOS []NewCount `json:"failures_by_os"`

	// Advanced: what the container was actually given.
	ByPrivilege []NewCount `json:"by_privilege"`
	ByCores     []NewCount `json:"by_cores"`
	ByRAM       []NewCount `json:"by_ram"`
	ByDisk      []NewCount `json:"by_disk"`
	ByCPU       []NewCount `json:"by_cpu"`
	ByGPU       []NewCount `json:"by_gpu"`
	ByArm       []NewCount `json:"by_arm"`

	Daily  []NewDailyPoint `json:"daily"`
	Recent []NewRecentRun  `json:"recent"`

	// Median rather than mean: one stuck run reporting the 86400-second clamp
	// drags an average far enough to be useless.
	MedianDuration int `json:"median_duration"`
}

// scanCounts reads a two-column (label, count) result.
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
		Days: days, Platform: platform, Repo: repoSource, Type: ctype,
	}

	// Outcome. "unfinished" is a run whose last word was a progress status: it
	// never reported an ending, which is a different thing from failing.
	err := ch.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			count(),
			countIf(final_status = 'success'),
			countIf(final_status = 'failed'),
			countIf(final_status = 'aborted'),
			countIf(final_status NOT IN ('success','failed','aborted'))
		FROM (%s)`, runs), args...,
	).Scan(&d.Outcome.Runs, &d.Outcome.Success, &d.Outcome.Failed,
		&d.Outcome.Aborted, &d.Outcome.Unfinished)
	if err != nil {
		return nil, fmt.Errorf("outcome: %w", err)
	}

	simple := func(target *[]NewCount, expr, having string, limit int) error {
		h := ""
		if having != "" {
			h = " HAVING " + having
		}
		q := fmt.Sprintf(
			"SELECT %s AS label, count() c FROM (%s) GROUP BY label%s ORDER BY c DESC LIMIT %d",
			expr, runs, h, limit)
		v, e := scanCounts(ch.db.QueryContext(ctx, q, args...))
		if e != nil {
			return e
		}
		*target = v
		return nil
	}

	// user_aborted is excluded from the categories: someone answering "no" is
	// not a defect, and leaving it in makes it the largest bar on the chart.
	steps := []struct {
		name   string
		target *[]NewCount
		expr   string
		having string
		limit  int
	}{
		{"categories", &d.Categories, "final_cat",
			"final_status = 'failed' AND final_cat NOT IN ('', 'user_aborted')", 15},
		{"exit codes", &d.ExitCodes, "toString(final_exit)",
			"final_status = 'failed' AND final_exit != 0", 15},
		{"platform", &d.ByPlatform, "plat", "", 5},
		{"type", &d.ByType, "type", "", 10},
		{"repo", &d.ByRepo, "repo_source", "", 10},
		{"os", &d.ByOS, "concat(os_type, ' ', os_version)", "os_type != ''", 15},
		{"host version", &d.ByHostVer, "pve_version", "pve_version != ''", 15},
		{"failures by os", &d.FailuresByOS, "concat(os_type, ' ', os_version)",
			"final_status = 'failed' AND os_type != ''", 10},
		{"success by os", &d.SuccessByOS, "concat(os_type, ' ', os_version)",
			"final_status = 'success' AND os_type != ''", 10},
		{"privilege", &d.ByPrivilege,
			"if(ct_type = 1, 'unprivileged', 'privileged')", "", 3},
		{"cores", &d.ByCores, "toString(core_count)", "core_count > 0", 12},
		{"ram", &d.ByRAM, "concat(toString(intDiv(ram_size, 1024)), ' GB')", "ram_size > 0", 12},
		{"disk", &d.ByDisk, "concat(toString(disk_size), ' GB')", "disk_size > 0", 12},
		{"cpu", &d.ByCPU, "cpu_vendor", "cpu_vendor != ''", 8},
		{"gpu", &d.ByGPU, "gpu_vendor", "gpu_vendor != ''", 8},
		{"arm", &d.ByArm, "if(has_arm = 1, 'arm64', 'x86_64')", "", 3},
	}
	for _, s := range steps {
		if e := simple(s.target, s.expr, s.having, s.limit); e != nil {
			return nil, fmt.Errorf("%s: %w", s.name, e)
		}
	}

	// Scripts that fail, with their own denominator so a rate can be read
	// honestly rather than inferred from an absolute count.
	rows, e := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT nsapp, count() runs, countIf(final_status = 'failed') failed
		FROM (%s)
		WHERE nsapp != ''
		GROUP BY nsapp
		HAVING failed > 0
		ORDER BY failed DESC, runs DESC
		LIMIT 25`, runs), args...)
	if e != nil {
		return nil, fmt.Errorf("failing scripts: %w", e)
	}
	for rows.Next() {
		var f NewFailingScript
		if rows.Scan(&f.App, &f.Runs, &f.Failed) == nil {
			d.Failing = append(d.Failing, f)
		}
	}
	rows.Close()

	// Signatures: the same failure text seen across runs. This is the closest
	// thing to "why", and the existing pages compute it without showing it.
	rows, e = ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT final_cat, final_exit, substring(final_err, 1, 200) msg,
		       count() c, uniqExact(nsapp) apps
		FROM (%s)
		WHERE final_status = 'failed' AND final_err != ''
		GROUP BY final_cat, final_exit, msg
		ORDER BY c DESC
		LIMIT 25`, runs), args...)
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
		SELECT toString(toDate(last_seen)) d, count(),
		       countIf(final_status = 'success'), countIf(final_status = 'failed')
		FROM (%s)
		GROUP BY d ORDER BY d`, runs), args...)
	if e != nil {
		return nil, fmt.Errorf("daily: %w", e)
	}
	for rows.Next() {
		var p NewDailyPoint
		if rows.Scan(&p.Day, &p.Runs, &p.Success, &p.Failed) == nil {
			d.Daily = append(d.Daily, p)
		}
	}
	rows.Close()

	// Median install duration over runs that actually finished.
	_ = ch.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT toUInt32(median(duration)) FROM (%s)
		WHERE final_status = 'success' AND duration > 0`, runs), args...,
	).Scan(&d.MedianDuration)

	// Recent runs.
	rows, e = ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT nsapp, type, final_status, final_exit, final_cat, plat,
		       concat(os_type, ' ', os_version), duration,
		       formatDateTime(last_seen, '%%Y-%%m-%%d %%H:%%M'),
		       substring(final_err, 1, 160)
		FROM (%s)
		ORDER BY last_seen DESC
		LIMIT 60`, runs), args...)
	if e != nil {
		return nil, fmt.Errorf("recent: %w", e)
	}
	for rows.Next() {
		var r NewRecentRun
		if rows.Scan(&r.App, &r.Type, &r.Status, &r.ExitCode, &r.Category,
			&r.Platform, &r.OS, &r.Duration, &r.LastSeen, &r.Error) == nil {
			d.Recent = append(d.Recent, r)
		}
	}
	rows.Close()

	return d, nil
}
