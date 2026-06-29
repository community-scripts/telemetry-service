-- =============================================================================
-- dedup_terminal_history.sql
--
-- ONE-TIME, MANUAL maintenance script for the ClickHouse telemetry store.
--
-- WHY THIS EXISTS
--   The raw table `telemetry_db.telemetry` is append-only. Before server-side
--   deduplication was added (see service.go: ExecIndex / HasTerminalExecutionID),
--   a single installation could append MANY rows for the same execution_id:
--     - multiple "installing" / "configuring" heartbeats, and
--     - multiple terminal events ("success" / "failed" / "aborted" / "unknown")
--       caused by overlapping traps (ERR + EXIT + signal traps) re-reporting.
--   Because every materialized view does count()/countIf() over ALL rows, this
--   inflated totals dramatically (e.g. "Total Created" 3364 for ProxmoxVE while
--   "All" showed 11277).
--
--   New ingest is already deduplicated at the service. This script repairs the
--   HISTORICAL rows and rebuilds the aggregate views so the dashboard matches.
--
-- WHAT IT DOES
--   1. Reports current duplication so you can see the impact before/after.
--   2. Builds a deduplicated copy of `telemetry`, keeping the EARLIEST row per
--      (execution_id, status). Rows with an empty execution_id (very old/partial
--      clients) are kept as-is because they cannot be safely deduplicated.
--   3. Atomically swaps the deduplicated copy in via EXCHANGE TABLES.
--   4. Truncates and rebuilds every aggregate table from the clean data.
--   5. (Optional) Backfills empty repo_source for legacy rows.
--
-- SAFETY
--   * Run during a quiet window. Step 3 (EXCHANGE) is atomic, but inserts that
--     arrive between step 2 and step 3 would land in the OLD table and be lost
--     in the swap. Briefly pausing the telemetry service is the safest option.
--   * Take a backup / snapshot first:  BACKUP TABLE telemetry_db.telemetry TO ...
--   * Execute statements ONE BLOCK AT A TIME and read the comments. This is NOT
--     run automatically by the service.
--
-- Requires: ClickHouse with `allow_experimental_*` defaults (EXCHANGE TABLES and
--           LIMIT n BY are stable features; no experimental flags needed).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 1 — Inspect duplication (read-only). Run this first.
-- -----------------------------------------------------------------------------

-- Overall: how many rows vs. distinct executions.
SELECT
    count()                                   AS total_rows,
    uniqExact(execution_id)                   AS distinct_executions,
    countIf(execution_id = '')                AS rows_without_execution_id,
    count() - uniqExactIf(execution_id, execution_id != '')
        - countIf(execution_id = '')          AS approx_duplicate_rows
FROM telemetry_db.telemetry;

-- Worst offenders: executions with the most duplicate rows.
SELECT
    execution_id,
    count()                AS rows,
    groupUniqArray(status) AS statuses
FROM telemetry_db.telemetry
WHERE execution_id != ''
GROUP BY execution_id
HAVING rows > 1
ORDER BY rows DESC
LIMIT 25;

-- How many terminal rows are duplicated per (execution_id, status).
SELECT
    sum(extra) AS surplus_terminal_rows
FROM (
    SELECT execution_id, status, count() - 1 AS extra
    FROM telemetry_db.telemetry
    WHERE execution_id != ''
      AND status IN ('success', 'failed', 'aborted', 'unknown')
    GROUP BY execution_id, status
    HAVING count() > 1
);


-- -----------------------------------------------------------------------------
-- STEP 2 — Build a deduplicated copy.
--   Keep the EARLIEST row per (execution_id, status). For empty execution_id we
--   keep everything (cannot dedupe reliably). `telemetry_dedup` inherits the
--   exact schema (column order, engine, partitioning) of `telemetry`.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS telemetry_db.telemetry_dedup;
CREATE TABLE telemetry_db.telemetry_dedup AS telemetry_db.telemetry;

-- Deduplicated rows (one earliest row per execution_id + status).
INSERT INTO telemetry_db.telemetry_dedup
SELECT *
FROM telemetry_db.telemetry
WHERE execution_id != ''
ORDER BY created ASC
LIMIT 1 BY execution_id, status;

-- Preserve legacy rows that have no execution_id (kept verbatim).
INSERT INTO telemetry_db.telemetry_dedup
SELECT *
FROM telemetry_db.telemetry
WHERE execution_id = '';

-- Sanity check the copy BEFORE swapping. Compare distinct executions: it should
-- be identical to STEP 1, while total rows should drop to (distinct execs +
-- legacy empty rows), at most one row per status per execution.
SELECT
    count()                 AS dedup_total_rows,
    uniqExact(execution_id) AS dedup_distinct_executions
FROM telemetry_db.telemetry_dedup;


-- -----------------------------------------------------------------------------
-- STEP 3 — Atomic swap. After this, `telemetry` holds the clean data and the
--   materialized views (which trigger by the name `telemetry`) keep working for
--   NEW inserts. Drop the leftover old table afterwards.
--   (Pause the telemetry service around this step to avoid losing in-flight rows.)
-- -----------------------------------------------------------------------------

EXCHANGE TABLES telemetry_db.telemetry AND telemetry_db.telemetry_dedup;
DROP TABLE IF EXISTS telemetry_db.telemetry_dedup;


-- -----------------------------------------------------------------------------
-- STEP 4 — Rebuild every aggregate table from the clean `telemetry`.
--   The INSERT ... SELECT bodies below are kept identical to the materialized
--   view definitions in clickhouse.go so the rebuilt aggregates match exactly.
-- -----------------------------------------------------------------------------

TRUNCATE TABLE telemetry_db.mv_daily_stats;
INSERT INTO telemetry_db.mv_daily_stats
SELECT
    toDate(created) AS day,
    nsapp,
    type,
    repo_source,
    count()                                                       AS total,
    countIf(status = 'success')                                   AS success,
    countIf(status = 'failed')                                    AS failed,
    countIf(status = 'aborted')                                   AS aborted,
    countIf(status IN ('installing', 'validation', 'configuring')) AS installing
FROM telemetry_db.telemetry
GROUP BY day, nsapp, type, repo_source;

TRUNCATE TABLE telemetry_db.mv_daily_os;
INSERT INTO telemetry_db.mv_daily_os
SELECT
    toDate(created) AS day,
    repo_source,
    os_type,
    count() AS cnt
FROM telemetry_db.telemetry
WHERE os_type != '' AND status IN ('success', 'failed', 'aborted', 'unknown')
GROUP BY day, repo_source, os_type;

TRUNCATE TABLE telemetry_db.mv_daily_method;
INSERT INTO telemetry_db.mv_daily_method
SELECT
    toDate(created) AS day,
    repo_source,
    method,
    count() AS cnt
FROM telemetry_db.telemetry
WHERE method != '' AND status IN ('success', 'failed', 'aborted', 'unknown')
GROUP BY day, repo_source, method;

TRUNCATE TABLE telemetry_db.mv_daily_pve;
INSERT INTO telemetry_db.mv_daily_pve
SELECT
    toDate(created) AS day,
    repo_source,
    pve_version,
    count() AS cnt
FROM telemetry_db.telemetry
WHERE pve_version != '' AND status IN ('success', 'failed', 'aborted', 'unknown')
GROUP BY day, repo_source, pve_version;

TRUNCATE TABLE telemetry_db.mv_daily_errors;
INSERT INTO telemetry_db.mv_daily_errors
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
GROUP BY day, nsapp, type, exit_code, error_category, repo_source;


-- -----------------------------------------------------------------------------
-- STEP 5 (OPTIONAL) — Backfill empty repo_source for legacy rows.
--   Older clients did not send repo_source. The vast majority of historical
--   traffic is community-scripts/ProxmoxVE, so default empty values to that.
--   Prefer deriving from repo_slug when present. Review before running.
--   NOTE: This is a mutation; re-run STEP 4 afterwards to refresh aggregates.
-- -----------------------------------------------------------------------------

-- Derive repo_source from repo_slug where we can.
-- ALTER TABLE telemetry_db.telemetry
-- UPDATE repo_source = multiIf(
--     repo_slug = 'community-scripts/ProxmoxVE',  'ProxmoxVE',
--     repo_slug = 'community-scripts/ProxmoxVED', 'ProxmoxVED',
--     repo_slug != '',                            'external',
--     repo_source)
-- WHERE repo_source = '' AND repo_slug != '';

-- Fallback: assume ProxmoxVE for anything still empty (legacy default).
-- ALTER TABLE telemetry_db.telemetry
-- UPDATE repo_source = 'ProxmoxVE'
-- WHERE repo_source = '';

-- After running STEP 5, re-run STEP 4 to rebuild the aggregates.


-- -----------------------------------------------------------------------------
-- STEP 6 — Verify. Totals should now line up with distinct executions.
-- -----------------------------------------------------------------------------

SELECT repo_source, sum(total) AS created
FROM telemetry_db.mv_daily_stats
GROUP BY repo_source
ORDER BY created DESC;

SELECT
    count()                 AS total_rows,
    uniqExact(execution_id) AS distinct_executions
FROM telemetry_db.telemetry;
