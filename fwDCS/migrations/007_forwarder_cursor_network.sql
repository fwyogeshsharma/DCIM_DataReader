-- 007_forwarder_cursor_network.sql
-- Devices now carry a per-country network_id (net-usa, net-in, …) and the
-- forwarder pushes each network separately. Cursors must therefore be keyed per
-- (table, network_id) — a single global cursor per table would let one network
-- advance the high-water mark past another network's unsent rows (data loss).
-- Repoint the forwarder_cursors primary key from (name) to (name, network_id).
-- Existing rows get network_id = '' (the pre-split default).

ALTER TABLE forwarder_cursors ADD COLUMN IF NOT EXISTS network_id TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'forwarder_cursors_pkey') THEN
        -- Only repoint while it is still the original single-column (name) PK.
        IF (SELECT array_length(conkey, 1) FROM pg_constraint WHERE conname = 'forwarder_cursors_pkey') = 1 THEN
            ALTER TABLE forwarder_cursors DROP CONSTRAINT forwarder_cursors_pkey;
            ALTER TABLE forwarder_cursors ADD PRIMARY KEY (name, network_id);
        END IF;
    ELSE
        ALTER TABLE forwarder_cursors ADD PRIMARY KEY (name, network_id);
    END IF;
END $$;
