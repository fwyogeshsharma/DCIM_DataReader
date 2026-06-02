-- 004_event_link_columns.sql
-- Link events live in the existing `events` table (no separate table) so they
-- flow to the UI through the normal events stream. A linkDown/linkUp that DCS
-- correlated to a topology_links row is stored as ONE enriched event carrying
-- both endpoints + the link id:
--
--   device_id     / source_hostname / src_port_name  → the device that sent it
--   dst_device_id / dst_hostname     / dst_port_name  → the peer (other end)
--   link_id                                            → the topology_links row
--
-- The peer's duplicate trap is suppressed (one event per real transition).
-- Non-link events leave all of these NULL. Correlation is authoritative — the
-- exact (device, interface) match against topology_links — never a guessed
-- neighbour. No FK (matches device_id, a plain nullable UUID).

-- Replaces the earlier link_events table approach; drop it if a previous dev
-- iteration created it. link_events was derived, so nothing is lost.
DROP TABLE IF EXISTS link_events;

ALTER TABLE events ADD COLUMN IF NOT EXISTS src_port_name TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS dst_device_id UUID;
ALTER TABLE events ADD COLUMN IF NOT EXISTS dst_hostname  TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS dst_port_name TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS link_id       UUID;

CREATE INDEX IF NOT EXISTS idx_events_link ON events (link_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_dst  ON events (dst_device_id, ts DESC);
