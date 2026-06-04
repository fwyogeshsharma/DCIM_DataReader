-- 002_energy_metrics.sql
-- Dedicated hypertable for BACnet/IP energy telemetry (Verdigris EV2 power
-- meters). Kept separate from the generic `metrics` table because energy data
-- is high-cardinality (per-circuit) and billing/trend-relevant (kWh accumulators
-- kept long), so it needs its own retention and index strategy. EDR emits these
-- with Kind="energy"; the ingest pipeline routes them here.

CREATE TABLE IF NOT EXISTS energy_metrics (
    device_id            UUID                NOT NULL,
    ts                   TIMESTAMPTZ         NOT NULL,
    metric_name          TEXT                NOT NULL,            -- e.g. energy.active_power_kw
    tag                  TEXT                NOT NULL DEFAULT '', -- raw secondary key (CktNN / PhA / H3)
    circuit              TEXT                NOT NULL DEFAULT '', -- CktNN for per-circuit rows, '' for panel
    phase                TEXT                NOT NULL DEFAULT '', -- PhA/PhB/PhC for phase rows, '' otherwise
    value                DOUBLE PRECISION    NOT NULL,
    attributes           JSONB,

    collector_agent      TEXT                NOT NULL DEFAULT 'EDR',
    collector_protocol   TEXT                NOT NULL DEFAULT 'BACNET'
);

SELECT create_hypertable('energy_metrics', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uix_energy_metrics
    ON energy_metrics (device_id, metric_name, tag, ts);

CREATE INDEX IF NOT EXISTS idx_energy_device   ON energy_metrics (device_id, metric_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_energy_name      ON energy_metrics (metric_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_energy_circuit   ON energy_metrics (device_id, circuit, ts DESC);

-- Energy is retained longer than generic metrics (kWh accumulators / trends).
SELECT add_retention_policy('energy_metrics', INTERVAL '365 days', if_not_exists => TRUE);

-- 5-minute rollup aggregate for dashboards / trend charts.
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_metrics_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', ts) AS bucket,
    device_id,
    metric_name,
    tag,
    circuit,
    phase,
    avg(value) AS avg_value,
    max(value) AS max_value,
    min(value) AS min_value,
    count(*)   AS samples
FROM energy_metrics
GROUP BY bucket, device_id, metric_name, tag, circuit, phase
WITH NO DATA;

SELECT add_continuous_aggregate_policy('energy_metrics_5m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);

SELECT add_retention_policy('energy_metrics_5m', INTERVAL '730 days', if_not_exists => TRUE);
