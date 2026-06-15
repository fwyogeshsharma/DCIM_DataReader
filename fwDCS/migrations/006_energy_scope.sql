-- 006_energy_scope.sql
-- Tag each EV2 energy reading with the meter's scope (it | cooling | facility),
-- derived by EDR from the meter name (RPP→it, COOL/CHILLER→cooling, FAC→facility).
-- Lets the UI compute PUE (facility/it) and DCiE (it/facility) by summing power
-- per scope per datacenter. Empty string = unknown/unclassified meter.

ALTER TABLE energy_metrics ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_energy_scope ON energy_metrics (scope, metric_name, ts DESC);
