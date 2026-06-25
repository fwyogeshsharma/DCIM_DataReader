-- 010_metric_rollups.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Long-horizon rollups for `metrics` and `energy_metrics`. Raw rows live only
-- ~30m (retention.go / dcs.yaml), so reports and predictions read these
-- continuous aggregates instead. Raw tables, their schema, and their retention
-- policies are NOT touched here (raw retention is owned by dcs.yaml).
--
-- Tiered so only the 5m tier reads raw; 1h and 1d are built on the tier below
-- and therefore never depend on raw rows still existing:
--   5m  ← raw table       refresh 1 min    keep 7 days
--   1h  ← 5m rollup       refresh 5 min    keep 90 days
--   1d  ← 1h rollup       refresh 1 hour   keep 2 years
--
-- Each rollup stores: avg_value, min_value, max_value, last_value, sample_count.
-- Grouped by: bucket, device_id, metric_name, tag
--   (+ circuit, phase for energy — per-circuit / per-phase billing granularity).
--
-- device_type is NOT a grouping key: it lives on `devices`, never on the metric
-- tables, and a continuous aggregate cannot JOIN to carry it. It is exposed at
-- query time via the *_dt views at the end of this file.
--
-- Roll-up math across tiers:
--   - avg recombines as SUM(avg_value*sample_count)/SUM(sample_count); a plain
--     AVG(avg_value) is wrong when buckets hold unequal sample counts.
--   - last_value carries up via last(last_value, bucket) (newest raw reading).
--
-- New 5m policy uses start_offset = '20 minutes', so raw retention must be > 20m
-- (dcs.yaml raw_retention bumped 20m → 30m alongside this migration). Retention
-- on these new aggregates (7d / 90d / 2y) is fixed here and NOT reconciled by
-- retention.go (its targets list only covers metrics_5m / energy_metrics_5m).
--
-- Existing metrics_5m / energy_metrics_5m aggregates are left untouched.
-- No explicit transaction block: continuous aggregates cannot be created inside
-- one (matches the style of 005_energy_metrics.sql).
-- ─────────────────────────────────────────────────────────────────────────────

-- ═════════════════════════════════════════════════════════════════════════════
-- metrics rollups
-- ═════════════════════════════════════════════════════════════════════════════

-- ── 5-minute rollup (← raw metrics) ──────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_rollup_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', ts) AS bucket,
    device_id,
    metric_name,
    tag,
    avg(value)        AS avg_value,
    min(value)        AS min_value,
    max(value)        AS max_value,
    last(value, ts)   AS last_value,
    count(*)          AS sample_count
FROM metrics
GROUP BY bucket, device_id, metric_name, tag
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_rollup_5m',
    start_offset      => INTERVAL '20 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE);

SELECT add_retention_policy('metrics_rollup_5m', INTERVAL '7 days', if_not_exists => TRUE);

-- ── 1-hour rollup (← metrics_rollup_5m) ──────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_rollup_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    device_id,
    metric_name,
    tag,
    sum(avg_value * sample_count) / nullif(sum(sample_count), 0) AS avg_value,
    min(min_value)            AS min_value,
    max(max_value)            AS max_value,
    last(last_value, bucket)  AS last_value,
    sum(sample_count)         AS sample_count
FROM metrics_rollup_5m
GROUP BY time_bucket('1 hour', bucket), device_id, metric_name, tag
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_rollup_1h',
    start_offset      => INTERVAL '6 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);

SELECT add_retention_policy('metrics_rollup_1h', INTERVAL '90 days', if_not_exists => TRUE);

-- ── 1-day rollup (← metrics_rollup_1h) ───────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_rollup_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket) AS bucket,
    device_id,
    metric_name,
    tag,
    sum(avg_value * sample_count) / nullif(sum(sample_count), 0) AS avg_value,
    min(min_value)            AS min_value,
    max(max_value)            AS max_value,
    last(last_value, bucket)  AS last_value,
    sum(sample_count)         AS sample_count
FROM metrics_rollup_1h
GROUP BY time_bucket('1 day', bucket), device_id, metric_name, tag
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_rollup_1d',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE);

SELECT add_retention_policy('metrics_rollup_1d', INTERVAL '2 years', if_not_exists => TRUE);

-- ═════════════════════════════════════════════════════════════════════════════
-- energy_metrics rollups (keep circuit + phase as grouping keys)
-- ═════════════════════════════════════════════════════════════════════════════

-- ── 5-minute rollup (← raw energy_metrics) ───────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_rollup_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', ts) AS bucket,
    device_id,
    metric_name,
    tag,
    circuit,
    phase,
    avg(value)        AS avg_value,
    min(value)        AS min_value,
    max(value)        AS max_value,
    last(value, ts)   AS last_value,
    count(*)          AS sample_count
FROM energy_metrics
GROUP BY bucket, device_id, metric_name, tag, circuit, phase
WITH NO DATA;

SELECT add_continuous_aggregate_policy('energy_rollup_5m',
    start_offset      => INTERVAL '20 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE);

SELECT add_retention_policy('energy_rollup_5m', INTERVAL '7 days', if_not_exists => TRUE);

-- ── 1-hour rollup (← energy_rollup_5m) ───────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_rollup_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    device_id,
    metric_name,
    tag,
    circuit,
    phase,
    sum(avg_value * sample_count) / nullif(sum(sample_count), 0) AS avg_value,
    min(min_value)            AS min_value,
    max(max_value)            AS max_value,
    last(last_value, bucket)  AS last_value,
    sum(sample_count)         AS sample_count
FROM energy_rollup_5m
GROUP BY time_bucket('1 hour', bucket), device_id, metric_name, tag, circuit, phase
WITH NO DATA;

SELECT add_continuous_aggregate_policy('energy_rollup_1h',
    start_offset      => INTERVAL '6 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);

SELECT add_retention_policy('energy_rollup_1h', INTERVAL '90 days', if_not_exists => TRUE);

-- ── 1-day rollup (← energy_rollup_1h) ────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_rollup_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket) AS bucket,
    device_id,
    metric_name,
    tag,
    circuit,
    phase,
    sum(avg_value * sample_count) / nullif(sum(sample_count), 0) AS avg_value,
    min(min_value)            AS min_value,
    max(max_value)            AS max_value,
    last(last_value, bucket)  AS last_value,
    sum(sample_count)         AS sample_count
FROM energy_rollup_1h
GROUP BY time_bucket('1 day', bucket), device_id, metric_name, tag, circuit, phase
WITH NO DATA;

SELECT add_continuous_aggregate_policy('energy_rollup_1d',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE);

SELECT add_retention_policy('energy_rollup_1d', INTERVAL '2 years', if_not_exists => TRUE);

-- ═════════════════════════════════════════════════════════════════════════════
-- device_type convenience views
--   Plain (non-materialized) views — zero storage, always reflect current
--   device_type. Use these in reports when slicing by device_type.
-- ═════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW metrics_rollup_5m_dt AS
SELECT r.*, d.device_type FROM metrics_rollup_5m r JOIN devices d ON d.id = r.device_id;
CREATE OR REPLACE VIEW metrics_rollup_1h_dt AS
SELECT r.*, d.device_type FROM metrics_rollup_1h r JOIN devices d ON d.id = r.device_id;
CREATE OR REPLACE VIEW metrics_rollup_1d_dt AS
SELECT r.*, d.device_type FROM metrics_rollup_1d r JOIN devices d ON d.id = r.device_id;

CREATE OR REPLACE VIEW energy_rollup_5m_dt AS
SELECT r.*, d.device_type FROM energy_rollup_5m r JOIN devices d ON d.id = r.device_id;
CREATE OR REPLACE VIEW energy_rollup_1h_dt AS
SELECT r.*, d.device_type FROM energy_rollup_1h r JOIN devices d ON d.id = r.device_id;
CREATE OR REPLACE VIEW energy_rollup_1d_dt AS
SELECT r.*, d.device_type FROM energy_rollup_1d r JOIN devices d ON d.id = r.device_id;
