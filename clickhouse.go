package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// CHClient wraps a ClickHouse database/sql connection for telemetry reads and writes.
type CHClient struct {
	db *sql.DB
}

// NewCHClient connects to ClickHouse using a DSN like
// "clickhouse://user:pass@host:9000/telemetry_db".
func NewCHClient(dsn string) (*CHClient, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.PingContext(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	log.Println("[CH] Connected to ClickHouse")
	ch := &CHClient{db: db}
	ch.migrate()
	return ch, nil
}

// migrate creates the database, tables, and materialized views if they don't exist.
func (ch *CHClient) migrate() {
	ctx := context.Background()
	stmts := []string{
		`CREATE DATABASE IF NOT EXISTS telemetry_db`,

		// ── Main telemetry table ──
		`CREATE TABLE IF NOT EXISTS telemetry_db.telemetry (
			id               String,
			nsapp            String,
			type             String,
			status           String,
			method           String,
			created          DateTime64(3),
			core_count       UInt8,
			ct_type          UInt8,
			disk_size        UInt32,
			ram_size         UInt32,
			exit_code        Int16,
			error            String,
			error_category   String,
			os_type          String,
			os_version       String,
			pve_version      String,
			random_id        String,
			execution_id     String,
			repo_source      String,
			cpu_vendor       String,
			cpu_model        String,
			gpu_vendor       String,
			gpu_model        String,
			gpu_passthrough  String,
			ram_speed        String,
			install_duration UInt32
		) ENGINE = MergeTree()
		ORDER BY (created, nsapp)
		PARTITION BY toYYYYMM(created)`,

		// ── Materialized view: daily stats per app ──
		// Pre-aggregates counts per (date, nsapp, type, status, repo_source).
		// Queries GROUP BY date/app become instant.
		`CREATE TABLE IF NOT EXISTS telemetry_db.mv_daily_stats (
			day              Date,
			nsapp            String,
			type             String,
			repo_source      String,
			total            UInt64,
			success          UInt64,
			failed           UInt64,
			aborted          UInt64,
			installing       UInt64
		) ENGINE = SummingMergeTree()
		ORDER BY (day, nsapp, type, repo_source)
		PARTITION BY toYYYYMM(day)`,

		`CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_db.mv_daily_stats_view
		TO telemetry_db.mv_daily_stats AS
		SELECT
			toDate(created) AS day,
			nsapp,
			type,
			repo_source,
			count()                        AS total,
			countIf(status='success')      AS success,
			countIf(status='failed')       AS failed,
			countIf(status='aborted')      AS aborted,
			countIf(status IN ('installing','validation','configuring')) AS installing
		FROM telemetry_db.telemetry
		GROUP BY day, nsapp, type, repo_source`,

		// ── Materialized view: daily OS distribution ──
		`CREATE TABLE IF NOT EXISTS telemetry_db.mv_daily_os (
			day         Date,
			repo_source String,
			os_type     String,
			cnt         UInt64
		) ENGINE = SummingMergeTree()
		ORDER BY (day, repo_source, os_type)
		PARTITION BY toYYYYMM(day)`,

		`CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_db.mv_daily_os_view
		TO telemetry_db.mv_daily_os AS
		SELECT
			toDate(created) AS day,
			repo_source,
			os_type,
			count() AS cnt
		FROM telemetry_db.telemetry
		WHERE os_type != '' AND status IN ('success','failed','aborted','unknown')
		GROUP BY day, repo_source, os_type`,

		// ── Materialized view: daily method distribution ──
		`CREATE TABLE IF NOT EXISTS telemetry_db.mv_daily_method (
			day         Date,
			repo_source String,
			method      String,
			cnt         UInt64
		) ENGINE = SummingMergeTree()
		ORDER BY (day, repo_source, method)
		PARTITION BY toYYYYMM(day)`,

		`CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_db.mv_daily_method_view
		TO telemetry_db.mv_daily_method AS
		SELECT
			toDate(created) AS day,
			repo_source,
			method,
			count() AS cnt
		FROM telemetry_db.telemetry
		WHERE method != '' AND status IN ('success','failed','aborted','unknown')
		GROUP BY day, repo_source, method`,

		// ── Materialized view: daily PVE version distribution ──
		`CREATE TABLE IF NOT EXISTS telemetry_db.mv_daily_pve (
			day         Date,
			repo_source String,
			pve_version String,
			cnt         UInt64
		) ENGINE = SummingMergeTree()
		ORDER BY (day, repo_source, pve_version)
		PARTITION BY toYYYYMM(day)`,

		`CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_db.mv_daily_pve_view
		TO telemetry_db.mv_daily_pve AS
		SELECT
			toDate(created) AS day,
			repo_source,
			pve_version,
			count() AS cnt
		FROM telemetry_db.telemetry
		WHERE pve_version != '' AND status IN ('success','failed','aborted','unknown')
		GROUP BY day, repo_source, pve_version`,

		// ── Materialized view: daily errors (excludes user_aborted) ──
		// Pre-aggregates real failures per (date, app, exit_code, error_category).
		// user_aborted (SIGHUP/SIGINT from closed terminals) is noise, not errors.
		`CREATE TABLE IF NOT EXISTS telemetry_db.mv_daily_errors (
			day            Date,
			nsapp          String,
			type           String,
			exit_code      Int16,
			error_category String,
			repo_source    String,
			cnt            UInt64
		) ENGINE = SummingMergeTree()
		ORDER BY (day, nsapp, exit_code, error_category, repo_source)
		PARTITION BY toYYYYMM(day)`,

		`CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_db.mv_daily_errors_view
		TO telemetry_db.mv_daily_errors AS
		SELECT
			toDate(created) AS day,
			nsapp,
			type,
			exit_code,
			error_category,
			repo_source,
			count() AS cnt
		FROM telemetry_db.telemetry
		WHERE status = 'failed'
		  AND error_category != 'user_aborted'
		  AND exit_code != 0
		GROUP BY day, nsapp, type, exit_code, error_category, repo_source`,
	}

	for _, s := range stmts {
		if _, err := ch.db.ExecContext(ctx, s); err != nil {
			log.Printf("[CH-MIGRATE] %v", err)
		}
	}

	// Backfill materialized views from existing data if they're empty
	var mvCount uint64
	_ = ch.db.QueryRowContext(ctx, "SELECT count() FROM telemetry_db.mv_daily_stats").Scan(&mvCount)
	if mvCount == 0 {
		var srcCount uint64
		_ = ch.db.QueryRowContext(ctx, "SELECT count() FROM telemetry_db.telemetry").Scan(&srcCount)
		if srcCount > 0 {
			log.Printf("[CH-MIGRATE] Backfilling materialized views from %d existing rows...", srcCount)
			backfills := []string{
				`INSERT INTO telemetry_db.mv_daily_stats
				SELECT toDate(created), nsapp, type, repo_source,
					count(), countIf(status='success'), countIf(status='failed'),
					countIf(status='aborted'),
					countIf(status IN ('installing','validation','configuring'))
				FROM telemetry_db.telemetry GROUP BY toDate(created), nsapp, type, repo_source`,

				`INSERT INTO telemetry_db.mv_daily_os
				SELECT toDate(created), repo_source, os_type, count()
				FROM telemetry_db.telemetry
				WHERE os_type != '' AND status IN ('success','failed','aborted','unknown')
				GROUP BY toDate(created), repo_source, os_type`,

				`INSERT INTO telemetry_db.mv_daily_method
				SELECT toDate(created), repo_source, method, count()
				FROM telemetry_db.telemetry
				WHERE method != '' AND status IN ('success','failed','aborted','unknown')
				GROUP BY toDate(created), repo_source, method`,

				`INSERT INTO telemetry_db.mv_daily_pve
				SELECT toDate(created), repo_source, pve_version, count()
				FROM telemetry_db.telemetry
				WHERE pve_version != '' AND status IN ('success','failed','aborted','unknown')
				GROUP BY toDate(created), repo_source, pve_version`,
			}
			for _, q := range backfills {
				if _, err := ch.db.ExecContext(ctx, q); err != nil {
					log.Printf("[CH-MIGRATE] backfill error: %v", err)
				}
			}
			log.Println("[CH-MIGRATE] Backfill complete")
		}
	}

	// Backfill mv_daily_errors separately (may be added after initial deploy)
	var errMVCount uint64
	_ = ch.db.QueryRowContext(ctx, "SELECT count() FROM telemetry_db.mv_daily_errors").Scan(&errMVCount)
	if errMVCount == 0 {
		var srcCount uint64
		_ = ch.db.QueryRowContext(ctx, "SELECT count() FROM telemetry_db.telemetry WHERE status='failed' AND error_category!='user_aborted' AND exit_code!=0").Scan(&srcCount)
		if srcCount > 0 {
			log.Printf("[CH-MIGRATE] Backfilling mv_daily_errors from %d error rows...", srcCount)
			_, err := ch.db.ExecContext(ctx, `INSERT INTO telemetry_db.mv_daily_errors
				SELECT toDate(created), nsapp, type, exit_code, error_category, repo_source, count()
				FROM telemetry_db.telemetry
				WHERE status = 'failed' AND error_category != 'user_aborted' AND exit_code != 0
				GROUP BY toDate(created), nsapp, type, exit_code, error_category, repo_source`)
			if err != nil {
				log.Printf("[CH-MIGRATE] mv_daily_errors backfill error: %v", err)
			} else {
				log.Println("[CH-MIGRATE] mv_daily_errors backfill complete")
			}
		}
	}
	log.Println("[CH-MIGRATE] Schema ready")
}

func (ch *CHClient) Close() error                   { return ch.db.Close() }
func (ch *CHClient) Ping(ctx context.Context) error { return ch.db.PingContext(ctx) }

func generateRecordID() string {
	b := make([]byte, 15)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ══════════════════════════════════════════════════════════════
//  WRITE OPERATIONS
// ══════════════════════════════════════════════════════════════

func (ch *CHClient) InsertTelemetry(ctx context.Context, p TelemetryOut) error {
	const q = `INSERT INTO telemetry_db.telemetry (
		id, nsapp, type, status, method, created,
		core_count, ct_type, disk_size, ram_size,
		exit_code, error, error_category,
		os_type, os_version, pve_version,
		random_id, execution_id, repo_source,
		cpu_vendor, cpu_model,
		gpu_vendor, gpu_model, gpu_passthrough,
		ram_speed, install_duration
	) VALUES (
		?, ?, ?, ?, ?, now64(3),
		?, ?, ?, ?,
		?, ?, ?,
		?, ?, ?,
		?, ?, ?,
		?, ?,
		?, ?, ?,
		?, ?
	)`
	_, err := ch.db.ExecContext(ctx, q,
		generateRecordID(), p.NSAPP, p.Type, p.Status, p.Method,
		uint8(p.CoreCount), uint8(p.CTType), uint32(p.DiskSize), uint32(p.RAMSize),
		int16(p.ExitCode), p.Error, p.ErrorCategory,
		p.OsType, p.OsVersion, p.PveVer,
		p.RandomID, p.ExecutionID, p.RepoSource,
		p.CPUVendor, p.CPUModel,
		p.GPUVendor, p.GPUModel, p.GPUPassthrough,
		p.RAMSpeed, uint32(p.InstallDuration),
	)
	return err
}

func (ch *CHClient) HasExecutionID(ctx context.Context, eid string) (bool, error) {
	var cnt uint64
	err := ch.db.QueryRowContext(ctx,
		"SELECT count() FROM telemetry_db.telemetry WHERE execution_id = ?", eid,
	).Scan(&cnt)
	return cnt > 0, err
}

// ══════════════════════════════════════════════════════════════
//  QUERY HELPERS
// ══════════════════════════════════════════════════════════════

func chSinceTime(days int) time.Time {
	if days == 1 {
		return time.Now().UTC().Truncate(24 * time.Hour)
	}
	return time.Now().UTC().AddDate(0, 0, -(days - 1)).Truncate(24 * time.Hour)
}

// chWhere builds a WHERE clause from days, repoSource, and extra predicates.
// Always starts with "1=1" so callers can freely AND-chain.
func chWhere(days int, repoSource string, extras ...string) (string, []interface{}) {
	parts := []string{"1=1"}
	var args []interface{}

	if days > 0 {
		parts = append(parts, "created >= ?")
		args = append(args, chSinceTime(days))
	}
	if repoSource != "" {
		parts = append(parts, "repo_source = ?")
		args = append(args, repoSource)
	}
	for _, e := range extras {
		parts = append(parts, e)
	}
	return strings.Join(parts, " AND "), args
}

// chSinceDate returns the date for filtering materialized views (Date column, not DateTime64).
func chSinceDate(days int) string {
	if days == 1 {
		return time.Now().UTC().Truncate(24 * time.Hour).Format("2006-01-02")
	}
	return time.Now().UTC().AddDate(0, 0, -(days - 1)).Truncate(24 * time.Hour).Format("2006-01-02")
}

// chMVWhere builds a WHERE clause for materialized view tables (uses "day" Date column).
func chMVWhere(days int, repoSource string) (string, []interface{}) {
	parts := []string{"1=1"}
	var args []interface{}
	if days > 0 {
		parts = append(parts, "day >= ?")
		args = append(args, chSinceDate(days))
	}
	if repoSource != "" {
		parts = append(parts, "repo_source = ?")
		args = append(args, repoSource)
	}
	return strings.Join(parts, " AND "), args
}

// scanRecords reads TelemetryRecord rows from a *sql.Rows.
func scanRecords(rows *sql.Rows) []TelemetryRecord {
	var out []TelemetryRecord
	for rows.Next() {
		var r TelemetryRecord
		var coreCount, ctType uint8
		var diskSize, ramSize, installDur uint32
		var exitCode int16
		err := rows.Scan(
			&r.NSAPP, &r.Type, &r.Status, &r.Method,
			&coreCount, &ctType, &diskSize, &ramSize,
			&exitCode, &r.Error, &r.ErrorCategory,
			&r.OsType, &r.OsVersion, &r.PveVer,
			&r.RandomID, &r.ExecutionID, &r.RepoSource,
			&r.CPUVendor, &r.CPUModel,
			&r.GPUVendor, &r.GPUModel, &r.GPUPassthrough,
			&r.RAMSpeed, &installDur,
			&r.Created,
		)
		if err != nil {
			log.Printf("[CH] row scan: %v", err)
			continue
		}
		r.CoreCount = int(coreCount)
		r.CTType = int(ctType)
		r.DiskSize = int(diskSize)
		r.RAMSize = int(ramSize)
		r.ExitCode = int(exitCode)
		r.InstallDuration = int(installDur)
		out = append(out, r)
	}
	return out
}

// recordSelectCols is the column list shared by all queries that return TelemetryRecord.
const recordSelectCols = `nsapp, type, status, method,
	core_count, ct_type, disk_size, ram_size,
	exit_code, error, error_category,
	os_type, os_version, pve_version,
	random_id, execution_id, repo_source,
	cpu_vendor, cpu_model,
	gpu_vendor, gpu_model, gpu_passthrough,
	ram_speed, install_duration,
	toString(created)`

// ══════════════════════════════════════════════════════════════
//  DASHBOARD DATA (SQL aggregation — replaces PB pagination)
// ══════════════════════════════════════════════════════════════

func (ch *CHClient) FetchDashboardData(ctx context.Context, days int, repoSource string) (*DashboardData, error) {
	data := &DashboardData{}
	mw, ma := chMVWhere(days, repoSource)
	tw, ta := chWhere(days, repoSource, "status IN ('success','failed','aborted','unknown')")

	// ── 1. Main counts (from materialized view) ──
	var total, sc, fc, ac uint64
	err := ch.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT sum(total), sum(success), sum(failed), sum(aborted)
		FROM telemetry_db.mv_daily_stats WHERE %s`, mw), ma...,
	).Scan(&total, &sc, &fc, &ac)
	if err != nil {
		return nil, fmt.Errorf("CH dashboard counts: %w", err)
	}
	data.TotalInstalls = int(total)
	data.SuccessCount = int(sc)
	data.FailedCount = int(fc)
	data.AbortedCount = int(ac)
	if sc+fc > 0 {
		data.SuccessRate = float64(sc) / float64(sc+fc) * 100
	}

	// Avg install duration (needs raw table — only non-zero durations)
	var avgDur float64
	_ = ch.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT if(countIf(install_duration>0)>0,
			toFloat64(sumIf(install_duration, install_duration>0))/countIf(install_duration>0), 0)
		FROM telemetry_db.telemetry WHERE %s`, tw), ta...,
	).Scan(&avgDur)
	data.AvgInstallDuration = avgDur

	// Total all-time (from MV, no filter)
	var tat uint64
	_ = ch.db.QueryRowContext(ctx,
		"SELECT sum(total) FROM telemetry_db.mv_daily_stats",
	).Scan(&tat)
	data.TotalAllTime = int(tat)
	data.SampleSize = data.TotalInstalls

	// ── 2. Installing count (needs raw table — execution_id subquery) ──
	var ic uint64
	_ = ch.db.QueryRowContext(ctx, `
		SELECT count() FROM telemetry_db.telemetry
		WHERE status IN ('installing','validation','configuring')
		  AND created >= now() - INTERVAL 1 DAY
		  AND (execution_id = '' OR execution_id NOT IN (
			SELECT execution_id FROM telemetry_db.telemetry
			WHERE status IN ('success','failed','aborted','unknown') AND execution_id != ''))
	`).Scan(&ic)
	data.InstallingCount = int(ic)

	// ── 3. Top apps (from MV) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT nsapp, sum(total) c FROM telemetry_db.mv_daily_stats WHERE %s AND nsapp!='' GROUP BY nsapp ORDER BY c DESC LIMIT 20", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var a AppCount
			var c uint64
			if rows.Scan(&a.App, &c) == nil {
				a.Count = int(c)
				data.TopApps = append(data.TopApps, a)
			}
		}
	}

	// ── 4. OS distribution (from MV) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT os_type, sum(cnt) c FROM telemetry_db.mv_daily_os WHERE %s AND os_type!='' GROUP BY os_type ORDER BY c DESC LIMIT 15", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var o OsCount
			var c uint64
			if rows.Scan(&o.Os, &c) == nil {
				o.Count = int(c)
				data.OsDistribution = append(data.OsDistribution, o)
			}
		}
	}

	// ── 5. Method stats (from MV) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT method, sum(cnt) c FROM telemetry_db.mv_daily_method WHERE %s AND method!='' GROUP BY method ORDER BY c DESC LIMIT 10", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var m MethodCount
			var c uint64
			if rows.Scan(&m.Method, &c) == nil {
				m.Count = int(c)
				data.MethodStats = append(data.MethodStats, m)
			}
		}
	}

	// ── 6. PVE versions (from MV) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT pve_version, sum(cnt) c FROM telemetry_db.mv_daily_pve WHERE %s AND pve_version!='' GROUP BY pve_version ORDER BY c DESC LIMIT 15", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var p PveCount
			var c uint64
			if rows.Scan(&p.Version, &c) == nil {
				p.Count = int(c)
				data.PveVersions = append(data.PveVersions, p)
			}
		}
	}

	// ── 7. Type stats (from MV) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT type, sum(total) c FROM telemetry_db.mv_daily_stats WHERE %s AND type!='' GROUP BY type ORDER BY c DESC LIMIT 10", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var t TypeCount
			var c uint64
			if rows.Scan(&t.Type, &c) == nil {
				t.Count = int(c)
				data.TypeStats = append(data.TypeStats, t)
			}
		}
	}

	// ── 8. Error analysis (needs raw table — text pattern matching, excludes user_aborted) ──
	fwErr, faErr := chWhere(days, repoSource, "status='failed'", "error!=''", "error_category!='user_aborted'")
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			multiIf(
				positionCaseInsensitive(error,'connection refused')>0,'connection refused',
				positionCaseInsensitive(error,'timeout')>0,'timeout',
				positionCaseInsensitive(error,'no space left')>0,'disk full',
				positionCaseInsensitive(error,'permission denied')>0,'permission denied',
				positionCaseInsensitive(error,'not found')>0,'not found',
				positionCaseInsensitive(error,'apt')>0,'apt error',
				positionCaseInsensitive(error,'dpkg')>0,'dpkg error',
				positionCaseInsensitive(error,'curl')>0,'network error',
				positionCaseInsensitive(error,'wget')>0,'network error',
				positionCaseInsensitive(error,'docker')>0,'docker error',
				positionCaseInsensitive(error,'systemctl')>0,'systemd error',
				substring(lower(error),1,40)
			) as pat,
			count() as cnt,
			uniqExact(nsapp) as ua,
			arrayStringConcat(arraySlice(groupUniqArray(nsapp),1,5),', ') as apps
		FROM telemetry_db.telemetry
		WHERE %s GROUP BY pat ORDER BY cnt DESC LIMIT 15`, fwErr), faErr...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var eg ErrorGroup
			var cnt, ua uint64
			if rows.Scan(&eg.Pattern, &cnt, &ua, &eg.Apps) == nil {
				eg.Count = int(cnt)
				eg.UniqueApps = int(ua)
				data.ErrorAnalysis = append(data.ErrorAnalysis, eg)
			}
		}
	}

	// ── 9. Failed apps with failure rates (from MV) ──
	minInstalls := 10
	switch {
	case days <= 1:
		minInstalls = 5
	case days <= 7:
		minInstalls = 15
	case days <= 30:
		minInstalls = 40
	default:
		minInstalls = 100
	}
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT nsapp, anyLast(type), sum(total) t, sum(failed) f
		FROM telemetry_db.mv_daily_stats
		WHERE %s AND nsapp!=''
		GROUP BY nsapp
		HAVING f > 0 AND t >= %d
		ORDER BY toFloat64(f)/t DESC LIMIT 50`, mw, minInstalls), ma...); err == nil {
		defer rows.Close()
		appTotal := make(map[string]int)
		appFailed := make(map[string]int)
		for rows.Next() {
			var nsapp, typ string
			var t, f uint64
			if rows.Scan(&nsapp, &typ, &t, &f) == nil {
				key := nsapp + "|" + typ
				appTotal[key] = int(t)
				appFailed[key] = int(f)
			}
		}
		data.FailedApps = buildFailedApps(appTotal, appFailed, 16, minInstalls)
	}

	// ── 10. Daily stats (from MV) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT toString(day) d, sum(success) s, sum(failed) f
		FROM telemetry_db.mv_daily_stats WHERE %s
		GROUP BY day ORDER BY day`, mw), ma...); err == nil {
		defer rows.Close()
		sMap := make(map[string]int)
		fMap := make(map[string]int)
		for rows.Next() {
			var d string
			var s, f uint64
			if rows.Scan(&d, &s, &f) == nil {
				sMap[d] = int(s)
				fMap[d] = int(f)
			}
		}
		actualDays := days
		if actualDays <= 0 {
			actualDays = 365
		}
		data.DailyStats = buildDailyStats(sMap, fMap, actualDays)
	}

	// ── 11. GPU stats (raw table — not materialized, low volume) ──
	gpuW, gpuA := chWhere(days, repoSource, "status IN ('success','failed','aborted','unknown')", "gpu_vendor!=''", "gpu_vendor!='unknown'")
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT gpu_vendor, gpu_passthrough, count() c FROM telemetry_db.telemetry WHERE %s GROUP BY gpu_vendor, gpu_passthrough ORDER BY c DESC", gpuW), gpuA...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var g GPUCount
			var c uint64
			if rows.Scan(&g.Vendor, &g.Passthrough, &c) == nil {
				g.Count = int(c)
				data.GPUStats = append(data.GPUStats, g)
			}
		}
	}

	// ── 12. Error categories (raw table — excludes user_aborted) ──
	catW, catA := chWhere(days, repoSource, "status='failed'", "error_category NOT IN ('','user_aborted')")
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT error_category, count() c FROM telemetry_db.telemetry WHERE %s GROUP BY error_category ORDER BY c DESC", catW), catA...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var e ErrorCatCount
			var c uint64
			if rows.Scan(&e.Category, &c) == nil {
				e.Count = int(c)
				data.ErrorCategories = append(data.ErrorCategories, e)
			}
		}
	}

	// ── 13. Top tools (from MV, type=pve) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT nsapp, sum(total) c FROM telemetry_db.mv_daily_stats WHERE %s AND type='pve' AND nsapp!='' GROUP BY nsapp ORDER BY c DESC LIMIT 15", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var t ToolCount
			var c uint64
			if rows.Scan(&t.Tool, &c) == nil {
				t.Count = int(c)
				data.TopTools = append(data.TopTools, t)
				data.TotalTools += int(c)
			}
		}
	}

	// ── 14. Top addons (from MV, type=addon) ──
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT nsapp, sum(total) c FROM telemetry_db.mv_daily_stats WHERE %s AND type='addon' AND nsapp!='' GROUP BY nsapp ORDER BY c DESC LIMIT 15", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var a AddonCount
			var c uint64
			if rows.Scan(&a.Addon, &c) == nil {
				a.Count = int(c)
				data.TopAddons = append(data.TopAddons, a)
				data.TotalAddons += int(c)
			}
		}
	}

	// ── 15. Recent records (raw table — needs full row data) ──
	recentW, recentA := chWhere(days, repoSource)
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT %s FROM telemetry_db.telemetry WHERE %s ORDER BY created DESC LIMIT 20",
		recordSelectCols, recentW), recentA...); err == nil {
		defer rows.Close()
		data.RecentRecords = scanRecords(rows)
	}

	return data, nil
}

// ══════════════════════════════════════════════════════════════
//  SCRIPT STATS (serves /api/scripts + frontend /api/stats)
// ══════════════════════════════════════════════════════════════

func (ch *CHClient) FetchScriptStats(ctx context.Context, days int, repoSource string, knownScripts map[string]ScriptInfo) (*ScriptAnalysisData, error) {
	mw, ma := chMVWhere(days, repoSource)

	rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT nsapp, anyLast(type) as typ,
			sum(total) as total,
			sum(success) as sc,
			sum(failed) as fc,
			sum(aborted) as ac
		FROM telemetry_db.mv_daily_stats WHERE %s
		GROUP BY nsapp ORDER BY total DESC`, mw), ma...)
	if err != nil {
		return nil, fmt.Errorf("CH script stats: %w", err)
	}
	defer rows.Close()

	data := &ScriptAnalysisData{}
	now := time.Now()
	seen := make(map[string]bool)

	for rows.Next() {
		var nsapp, typ string
		var total, sc, fc, ac uint64
		if err := rows.Scan(&nsapp, &typ, &total, &sc, &fc, &ac); err != nil {
			continue
		}
		// Filter to known scripts if provided
		if len(knownScripts) > 0 {
			if _, ok := knownScripts[nsapp]; !ok {
				continue
			}
		}
		seen[nsapp] = true

		// Override type from script_scripts metadata
		if knownScripts != nil {
			if info, ok := knownScripts[nsapp]; ok {
				typ = info.Type
			}
		}

		completed := sc + fc + ac
		rate := float64(0)
		if completed > 0 {
			rate = float64(sc) / float64(completed) * 100
		}
		daysOld := 0
		installsPerDay := float64(0)
		if knownScripts != nil {
			if info, ok := knownScripts[nsapp]; ok && !info.Created.IsZero() {
				daysOld = int(now.Sub(info.Created).Hours() / 24)
				if daysOld < 1 {
					daysOld = 1
				}
				installsPerDay = float64(total) / float64(daysOld)
			}
		}

		data.TopScripts = append(data.TopScripts, ScriptStat{
			App: nsapp, Type: typ,
			Total: int(total), Success: int(sc), Failed: int(fc), Aborted: int(ac),
			SuccessRate: rate, DaysOld: daysOld, InstallsPerDay: installsPerDay,
		})
		data.TotalInstalls += int(total)
	}

	// Add zero-usage known scripts (30d + alltime)
	if knownScripts != nil && (days == 0 || days >= 30) {
		for slug, info := range knownScripts {
			if !seen[slug] {
				daysOld := 0
				if !info.Created.IsZero() {
					daysOld = int(now.Sub(info.Created).Hours() / 24)
					if daysOld < 1 {
						daysOld = 1
					}
				}
				data.TopScripts = append(data.TopScripts, ScriptStat{
					App: slug, Type: info.Type, DaysOld: daysOld,
				})
			}
		}
	}
	data.TotalScripts = len(data.TopScripts)

	log.Printf("[CH] Script stats: %d scripts, %d total installs (days=%d)", data.TotalScripts, data.TotalInstalls, days)
	return data, nil
}

// ══════════════════════════════════════════════════════════════
//  ERROR ANALYSIS
// ══════════════════════════════════════════════════════════════

func (ch *CHClient) FetchErrorAnalysisData(ctx context.Context, days int, repoSource string) (*ErrorAnalysisData, error) {
	data := &ErrorAnalysisData{}

	mw, ma := chMVWhere(days, repoSource)

	// Total installs (from MV)
	var ti uint64
	_ = ch.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT sum(total) FROM telemetry_db.mv_daily_stats WHERE %s", mw), ma...,
	).Scan(&ti)
	data.TotalInstalls = int(ti)

	// Total real errors (from MV — excludes user_aborted)
	var te uint64
	_ = ch.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT sum(cnt) FROM telemetry_db.mv_daily_errors WHERE %s", mw), ma...,
	).Scan(&te)
	data.TotalErrors = int(te)

	if data.TotalInstalls > 0 {
		data.OverallFailRate = float64(data.TotalErrors) / float64(data.TotalInstalls) * 100
	}

	// Stuck installing
	var si uint64
	_ = ch.db.QueryRowContext(ctx, `
		SELECT count() FROM telemetry_db.telemetry
		WHERE status IN ('installing','validation','configuring')
		  AND created >= now() - INTERVAL 1 DAY
		  AND (execution_id = '' OR execution_id NOT IN (
			SELECT execution_id FROM telemetry_db.telemetry
			WHERE status IN ('success','failed','aborted','unknown') AND execution_id != ''))
	`).Scan(&si)
	data.StuckInstalling = int(si)

	// Exit code stats (from MV — excludes user_aborted)
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT exit_code, sum(cnt) c FROM telemetry_db.mv_daily_errors WHERE %s GROUP BY exit_code ORDER BY c DESC LIMIT 30", mw), ma...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var code int16
			var cnt uint64
			if rows.Scan(&code, &cnt) == nil {
				pct := float64(0)
				if data.TotalErrors > 0 {
					pct = float64(cnt) / float64(data.TotalErrors) * 100
				}
				data.ExitCodeStats = append(data.ExitCodeStats, ExitCodeStat{
					ExitCode: int(code), Count: int(cnt), Percentage: pct,
					Description: getExitCodeDescription(int(code)),
					Category:    getExitCodeCategory(int(code)),
				})
			}
		}
	}

	// Category stats (excludes user_aborted)
	cw, ca := chWhere(days, repoSource, "status='failed'", "error_category NOT IN ('','user_aborted')")
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT error_category, count() c,
			arrayStringConcat(arraySlice(groupUniqArray(nsapp),1,5),', ') apps
		FROM telemetry_db.telemetry WHERE %s
		GROUP BY error_category ORDER BY c DESC`, cw), ca...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var cs CategoryStat
			var cnt uint64
			if rows.Scan(&cs.Category, &cnt, &cs.TopApps) == nil {
				cs.Count = int(cnt)
				if data.TotalErrors > 0 {
					cs.Percentage = float64(cnt) / float64(data.TotalErrors) * 100
				}
				data.CategoryStats = append(data.CategoryStats, cs)
			}
		}
	}

	// App errors (excludes user_aborted)
	aw, aa := chWhere(days, repoSource, "status IN ('success','failed','aborted','unknown')")
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT nsapp, anyLast(type), count() t,
			countIf(status='failed' AND error_category!='user_aborted') f,
			countIf(status='aborted' AND error_category!='user_aborted') ab,
			topKIf(1)(exit_code, status='failed' AND error_category!='user_aborted' AND exit_code!=0) topec,
			anyIf(error, status='failed' AND error_category!='user_aborted' AND error!='') toperr,
			anyIf(error_category, status='failed' AND error_category NOT IN ('','uncategorized','user_aborted')) topcat
		FROM telemetry_db.telemetry WHERE %s AND nsapp!=''
		GROUP BY nsapp
		HAVING f+ab > 0
		ORDER BY f DESC LIMIT 50`, aw), aa...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var ae AppErrorDetail
			var t, f, ab uint64
			var topEC []int16
			if rows.Scan(&ae.App, &ae.Type, &t, &f, &ab, &topEC, &ae.TopError, &ae.TopCategory) == nil {
				ae.TotalCount = int(t)
				ae.FailedCount = int(f)
				ae.AbortedCount = int(ab)
				if t > 0 {
					ae.FailureRate = float64(f) / float64(t) * 100
				}
				if len(topEC) > 0 {
					ae.TopExitCode = int(topEC[0])
				}
				data.AppErrors = append(data.AppErrors, ae)
			}
		}
	}

	// Error timeline (from MV — excludes user_aborted)
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT toString(day) d, sum(cnt) f
		FROM telemetry_db.mv_daily_errors WHERE %s
		GROUP BY day ORDER BY day`, mw), ma...); err == nil {
		defer rows.Close()
		dailyF := make(map[string]int)
		for rows.Next() {
			var d string
			var f uint64
			if rows.Scan(&d, &f) == nil {
				dailyF[d] = int(f)
			}
		}
		actualDays := days
		if actualDays <= 0 {
			actualDays = 30
		}
		for i := actualDays - 1; i >= 0; i-- {
			date := time.Now().AddDate(0, 0, -i).Format("2006-01-02")
			data.ErrorTimeline = append(data.ErrorTimeline, ErrorTimelinePoint{
				Date:   date,
				Failed: dailyF[date],
			})
		}
	}

	// Recent errors (excludes user_aborted)
	rw, ra := chWhere(days, repoSource, "status='failed'", "error_category!='user_aborted'")
	if rows, err := ch.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT nsapp, type, status, exit_code, error, error_category,
			os_type, os_version, toString(created)
		FROM telemetry_db.telemetry WHERE %s
		ORDER BY created DESC LIMIT 100`, rw), ra...); err == nil {
		defer rows.Close()
		for rows.Next() {
			var er ErrorRecord
			var ec int16
			if rows.Scan(&er.NSAPP, &er.Type, &er.Status, &ec, &er.Error, &er.ErrorCategory,
				&er.OsType, &er.OsVersion, &er.Created) == nil {
				er.ExitCode = int(ec)
				data.RecentErrors = append(data.RecentErrors, er)
			}
		}
	}

	return data, nil
}

// ══════════════════════════════════════════════════════════════
//  PAGINATED RECORDS (/api/records)
// ══════════════════════════════════════════════════════════════

func (ch *CHClient) FetchRecordsPaginated(ctx context.Context, page, limit int,
	status, app, osType, typeFilter, sortField, repoSource string, days int,
) ([]TelemetryRecord, int, error) {
	parts := []string{"1=1"}
	var args []interface{}

	if days > 0 {
		parts = append(parts, "created >= ?")
		args = append(args, chSinceTime(days))
	}
	if status != "" {
		switch status {
		case "aborted":
			parts = append(parts, "(status='aborted' OR (status='failed' AND (exit_code IN (129,130) OR positionCaseInsensitive(error,'SIGINT')>0 OR positionCaseInsensitive(error,'SIGHUP')>0 OR positionCaseInsensitive(error,'Ctrl+C')>0 OR positionCaseInsensitive(error,'Ctrl-C')>0)))")
		case "failed":
			parts = append(parts, "(status='failed' AND exit_code NOT IN (129,130) AND positionCaseInsensitive(error,'SIGINT')=0 AND positionCaseInsensitive(error,'SIGHUP')=0 AND positionCaseInsensitive(error,'Ctrl+C')=0 AND positionCaseInsensitive(error,'Ctrl-C')=0)")
		default:
			parts = append(parts, "status = ?")
			args = append(args, status)
		}
	}
	if app != "" {
		parts = append(parts, "positionCaseInsensitive(nsapp, ?) > 0")
		args = append(args, app)
	}
	if osType != "" {
		parts = append(parts, "os_type = ?")
		args = append(args, osType)
	}
	if typeFilter != "" {
		parts = append(parts, "type = ?")
		args = append(args, typeFilter)
	}
	if repoSource != "" {
		parts = append(parts, "repo_source = ?")
		args = append(args, repoSource)
	}

	where := strings.Join(parts, " AND ")

	// Sort
	sort := "created DESC"
	allowed := map[string]string{
		"created": "created", "-created": "created DESC",
		"nsapp": "nsapp", "-nsapp": "nsapp DESC",
		"status": "status", "-status": "status DESC",
		"os_type": "os_type", "-os_type": "os_type DESC",
		"type": "type", "-type": "type DESC",
		"method": "method", "-method": "method DESC",
		"exit_code": "exit_code", "-exit_code": "exit_code DESC",
	}
	if s, ok := allowed[sortField]; ok {
		sort = s
	}

	// Total count
	var totalU uint64
	_ = ch.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM telemetry_db.telemetry WHERE %s", where), args...).Scan(&totalU)
	total := int(totalU)

	// Fetch page
	offset := (page - 1) * limit
	q := fmt.Sprintf("SELECT %s FROM telemetry_db.telemetry WHERE %s ORDER BY %s LIMIT %d OFFSET %d",
		recordSelectCols, where, sort, limit, offset)

	rows, err := ch.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("CH records: %w", err)
	}
	defer rows.Close()
	records := scanRecords(rows)
	return records, total, nil
}

// ══════════════════════════════════════════════════════════════
//  CLEANUP (stuck installing + retention)
// ══════════════════════════════════════════════════════════════

func (ch *CHClient) FindStuckInstallations(ctx context.Context, stuckHours int) ([]StuckRecord, error) {
	rows, err := ch.db.QueryContext(ctx, `
		SELECT id, nsapp, toString(created)
		FROM telemetry_db.telemetry
		WHERE status IN ('installing','configuring')
		  AND created < now() - INTERVAL ? HOUR
		  AND (execution_id = '' OR execution_id NOT IN (
			SELECT execution_id FROM telemetry_db.telemetry
			WHERE status IN ('success','failed','aborted','unknown') AND execution_id != ''))
		ORDER BY created LIMIT 500`, stuckHours)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StuckRecord
	for rows.Next() {
		var r StuckRecord
		if rows.Scan(&r.ID, &r.NSAPP, &r.Created) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (ch *CHClient) MarkRecordAsUnknown(ctx context.Context, record StuckRecord, stuckHours int) error {
	// Insert a terminal-status row for this record so stats count it properly.
	// Copy nsapp, type from the original row and set status=unknown.
	_, err := ch.db.ExecContext(ctx, `
		INSERT INTO telemetry_db.telemetry
			(id, nsapp, type, status, error, error_category, created, execution_id, random_id, repo_source)
		SELECT ?, nsapp, type, 'unknown', ?, 'timeout', now64(3), execution_id, random_id, repo_source
		FROM telemetry_db.telemetry WHERE id = ? LIMIT 1`,
		generateRecordID(),
		fmt.Sprintf("Installation timed out - no completion status received after %dh", stuckHours),
		record.ID,
	)
	return err
}

func (ch *CHClient) DeleteOldRecords(ctx context.Context, retentionDays int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	_, err := ch.db.ExecContext(ctx,
		"ALTER TABLE telemetry_db.telemetry DELETE WHERE created < ?", cutoff)
	return err
}

func (ch *CHClient) GetStuckCount(ctx context.Context, stuckHours int) (int, error) {
	var cnt uint64
	err := ch.db.QueryRowContext(ctx, `
		SELECT count() FROM telemetry_db.telemetry
		WHERE status IN ('installing','configuring')
		  AND created < now() - INTERVAL ? HOUR
		  AND (execution_id = '' OR execution_id NOT IN (
			SELECT execution_id FROM telemetry_db.telemetry
			WHERE status IN ('success','failed','aborted','unknown') AND execution_id != ''))`,
		stuckHours).Scan(&cnt)
	return int(cnt), err
}
