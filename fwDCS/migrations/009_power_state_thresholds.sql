-- 009_power_state_thresholds.sql
-- Two persistent stores for values that must NOT live in the time-series
-- metrics table (which expires under retention):
--
--   1. devices.power_state — chassis power is a fixed state, not a metric. EDR
--      polls it via Redfish and writes it here so the current value survives
--      retention and is read straight off the device row.
--
--   2. device_thresholds — per-device alert thresholds (HighCPU, HighMemory, …)
--      the user edits in the UI. They are config, not telemetry. EDR fetches
--      them from the simulator SNMP management plane (UDP 1161) and upserts here.

ALTER TABLE devices ADD COLUMN IF NOT EXISTS power_state SMALLINT; -- 1=On, 2=Off, NULL=unknown

CREATE TABLE IF NOT EXISTS device_thresholds (
    device_id   UUID         NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    rule        TEXT         NOT NULL,            -- HighCPU, HighMemory, HighTemperature, …
    value       INTEGER      NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, rule)
);

CREATE INDEX IF NOT EXISTS ix_device_thresholds_device ON device_thresholds (device_id);
