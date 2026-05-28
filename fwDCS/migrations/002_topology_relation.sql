-- 002: parent-child hierarchy on topology_links
--
-- Adds a directed relation flag to each topology edge so the datacenter fabric
-- tree (edge_router → firewall → load_balancer → core → spine → leaf → server)
-- can be reconstructed from the existing edge rows — no separate hierarchy
-- table needed.
--
-- Semantics (dst relative to src):
--   'uplink'   — dst_device is the PARENT of src_device (src points up the tree)
--   'downlink' — dst_device is the CHILD  of src_device
--   'peer'     — same tier (spine↔spine / MLAG) or non-fabric device; no parent
--
-- Idempotent: safe to re-run.

ALTER TABLE topology_links
    ADD COLUMN IF NOT EXISTS relation TEXT NOT NULL DEFAULT 'peer';

CREATE INDEX IF NOT EXISTS idx_links_relation ON topology_links(relation);

-- ────────────────────────────────────────────────────────────────────────────
-- View: topology_tree  (UI-ready hierarchy)
-- ────────────────────────────────────────────────────────────────────────────
-- Flattens the topology_links edge list (with BFS-computed `relation`) into one
-- row per device, already paired with its parent + depth + root flag. The UI /
-- aggregator does a single SELECT — no BFS, no edge orientation, no dedup.
-- Derives live from topology_links + devices; stores nothing. Idempotent.

CREATE OR REPLACE VIEW topology_tree AS
WITH RECURSIVE
-- One parent per child, deduped from both LLDP directions of the tree edge.
parent_of AS (
    SELECT DISTINCT ON (child_id) child_id, parent_id
    FROM (
        SELECT src_device_id AS child_id, dst_device_id AS parent_id
        FROM topology_links WHERE relation = 'uplink'
        UNION ALL
        SELECT dst_device_id AS child_id, src_device_id AS parent_id
        FROM topology_links WHERE relation = 'downlink'
    ) e
    ORDER BY child_id, parent_id
),
tree AS (
    -- Roots = devices that are nobody's child (BFS roots + isolated devices).
    SELECT d.id AS device_id, NULL::uuid AS parent_id, 0 AS depth
    FROM devices d
    WHERE NOT EXISTS (SELECT 1 FROM parent_of po WHERE po.child_id = d.id)
    UNION ALL
    -- Walk down: each child inherits depth+1 from its parent.
    SELECT po.child_id, po.parent_id, t.depth + 1
    FROM parent_of po
    JOIN tree t ON po.parent_id = t.device_id
    WHERE t.depth < 100          -- cycle guard against dirty relation data
)
SELECT
    d.org_id,
    d.datacenter_id,
    d.floor_id,
    d.network_id,
    d.group_id,
    t.device_id,
    d.hostname,
    d.device_type,
    d.mgmt_ip,
    d.prod_ip,
    t.parent_id              AS parent_device_id,
    p.hostname               AS parent_hostname,
    t.depth,
    (t.parent_id IS NULL)    AS is_root
FROM tree t
JOIN devices d ON d.id = t.device_id
LEFT JOIN devices p ON p.id = t.parent_id;
