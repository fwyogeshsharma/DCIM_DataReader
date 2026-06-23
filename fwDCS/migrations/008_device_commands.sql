-- 008_device_commands.sql
-- Downstream control plane: edits made in the DCIM UI flow back DOWN to the
-- simulated/real devices. The Aggregator reports user edits in its ingest
-- response (ui_device_changes); the forwarder turns each one into a row here.
-- EDR pulls pending rows over the admin REST API, applies them to the device
-- (SNMP SET on the mgmt IP, or Redfish for power), then acks the result.
--
-- This is intentionally a simple work queue, not time-series: low volume
-- (only fires on a human edit), and each row is a single field write.

CREATE TABLE IF NOT EXISTS device_commands (
    id          BIGSERIAL    PRIMARY KEY,
    org_id      TEXT         NOT NULL DEFAULT '',
    network_id  TEXT         NOT NULL DEFAULT '',
    device_ip   TEXT         NOT NULL,                 -- mgmt IP: the SNMP SET / Redfish target
    hostname    TEXT         NOT NULL DEFAULT '',      -- for logs/traceability only
    field       TEXT         NOT NULL,                 -- e.g. sys_location, name, country, power_state
    value       TEXT         NOT NULL DEFAULT '',      -- new value (stringified)
    status      TEXT         NOT NULL DEFAULT 'pending', -- pending | applied | failed
    attempts    INT          NOT NULL DEFAULT 0,
    last_error  TEXT         NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- EDR pulls oldest-pending-first; this index keeps that scan cheap.
CREATE INDEX IF NOT EXISTS ix_device_commands_pending
    ON device_commands (status, id)
    WHERE status = 'pending';

-- Idempotency: collapse repeated edits of the same field on the same device
-- while still pending, so a user dragging a slider doesn't queue 50 writes.
-- A new edit supersedes the value of an existing pending row for that field.
CREATE UNIQUE INDEX IF NOT EXISTS uix_device_commands_pending_field
    ON device_commands (device_ip, field)
    WHERE status = 'pending';
