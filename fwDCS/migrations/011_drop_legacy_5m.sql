-- 011_drop_legacy_5m.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Two cleanups, both idempotent so this is safe to re-run:
--
-- 1. Drop the legacy 5-minute continuous aggregates `metrics_5m` (001_init.sql)
--    and `energy_metrics_5m` (005_energy_metrics.sql). They are fully superseded
--    by the tiered `metrics_rollup_5m` / `energy_rollup_5m` from 010 — same raw
--    source, same 5m bucket — so they were pure duplicate refresh work. CASCADE
--    also removes their refresh + retention policies. retention.go is repointed
--    to the *_rollup_5m views in the same change.
--
-- 2. Retune the surviving *_rollup_5m refresh from every 1 min → every 5 min.
--    A 5-minute bucket only finalizes every 5 minutes, so a 1-minute refresh
--    re-scanned the raw tables ~5x for no new data — the main source of the
--    periodic Postgres CPU spikes. remove-then-add makes the new cadence win
--    even though 010 already created the policy (add alone is a no-op then).
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Drop legacy duplicates.
DROP MATERIALIZED VIEW IF EXISTS metrics_5m CASCADE;
DROP MATERIALIZED VIEW IF EXISTS energy_metrics_5m CASCADE;

-- 2. Retune rollup_5m refresh cadence to 5 minutes.
SELECT remove_continuous_aggregate_policy('metrics_rollup_5m', if_exists => TRUE);
SELECT add_continuous_aggregate_policy('metrics_rollup_5m',
    start_offset      => INTERVAL '20 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);

SELECT remove_continuous_aggregate_policy('energy_rollup_5m', if_exists => TRUE);
SELECT add_continuous_aggregate_policy('energy_rollup_5m',
    start_offset      => INTERVAL '20 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);
