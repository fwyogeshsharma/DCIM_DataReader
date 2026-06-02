-- 003: switch fabric-role classification (core / spine / leaf / access / distribution).
--
-- The coarse `device_type` column (router|switch|server|...) cannot distinguish
-- the fabric tier — every CORE/SPINE/LEAF box reports device_type='switch'. This
-- migration adds an inferred fabric ROLE, computed by DCS after each topology
-- recompute from three signals already in the DB: hostname (sysName), model /
-- sys_description, and the LLDP-neighbor profile (topology_links + neighbor
-- device_type). See internal/store/classifier.go.
--
-- Orthogonal to device_type — device_type stays the physical kind, device_role
-- is the fabric position. Idempotent.

ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS device_role     TEXT,     -- core|spine|leaf|access|distribution|edge_router|firewall|load_balancer|server|oob_switch|ups|pdu|floor_pdu|sensor|unclassified
    ADD COLUMN IF NOT EXISTS role_confidence REAL,     -- 0.0–1.0 (score[role] / sum(scores))
    ADD COLUMN IF NOT EXISTS role_source     TEXT,     -- inferred (>=0.6) | suggested (0.4–0.6) | unclassified (<0.4) | override
    ADD COLUMN IF NOT EXISTS role_overridden BOOLEAN NOT NULL DEFAULT FALSE; -- admin-set role; classifier never touches these rows

CREATE INDEX IF NOT EXISTS idx_devices_role ON devices(device_role);

-- ────────────────────────────────────────────────────────────────────────────
-- View: topology_view  — add both endpoints' fabric role (appended at end so
-- CREATE OR REPLACE keeps the existing column order intact).
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW topology_view AS
SELECT
    tl.id,
    tl.layer,
    tl.protocol,
    tl.is_active,
    tl.link_speed_mbps,
    tl.link_type,
    tl.updated_at,

    sd.hostname                  AS src_hostname,
    sd.device_type               AS src_device_type,
    sd.mgmt_ip                   AS src_mgmt_ip,
    sd.prod_ip                   AS src_prod_ip,
    sd.snmp_enabled              AS src_snmp_enabled,
    sd.gnmi_enabled              AS src_gnmi_enabled,
    sd.collector_agent           AS src_collector_agent,
    COALESCE(si.interface_name, tl.src_port_name)         AS src_port_name,
    COALESCE(si.speed_mbps,     tl.link_speed_mbps)       AS src_port_speed_mbps,
    si.operational_status                                  AS src_port_operational_status,
    si.admin_status                                        AS src_port_admin_status,

    dd.hostname                  AS dst_hostname,
    dd.device_type               AS dst_device_type,
    dd.mgmt_ip                   AS dst_mgmt_ip,
    dd.prod_ip                   AS dst_prod_ip,
    dd.snmp_enabled              AS dst_snmp_enabled,
    dd.gnmi_enabled              AS dst_gnmi_enabled,
    dd.collector_agent           AS dst_collector_agent,
    COALESCE(di.interface_name, tl.dst_port_name)         AS dst_port_name,
    COALESCE(di.speed_mbps,     tl.link_speed_mbps)       AS dst_port_speed_mbps,
    di.operational_status                                  AS dst_port_operational_status,
    di.admin_status                                        AS dst_port_admin_status,

    -- fabric role (migration 003) — appended last
    sd.device_role               AS src_device_role,
    dd.device_role               AS dst_device_role

FROM       topology_links tl
JOIN       devices    sd ON sd.id = tl.src_device_id
JOIN       devices    dd ON dd.id = tl.dst_device_id
LEFT JOIN  interfaces si ON si.id = tl.src_interface_id
LEFT JOIN  interfaces di ON di.id = tl.dst_interface_id;

-- ────────────────────────────────────────────────────────────────────────────
-- View: topology_tree — add the device's fabric role + confidence (appended last).
-- Lets the UI lay out tiers (core→spine→leaf→server) without a second query.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW topology_tree AS
WITH RECURSIVE
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
    SELECT d.id AS device_id, NULL::uuid AS parent_id, 0 AS depth
    FROM devices d
    WHERE NOT EXISTS (SELECT 1 FROM parent_of po WHERE po.child_id = d.id)
    UNION ALL
    SELECT po.child_id, po.parent_id, t.depth + 1
    FROM parent_of po
    JOIN tree t ON po.parent_id = t.device_id
    WHERE t.depth < 100
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
    (t.parent_id IS NULL)    AS is_root,
    -- fabric role (migration 003) — appended last
    d.device_role,
    d.role_confidence,
    d.role_source
FROM tree t
JOIN devices d ON d.id = t.device_id
LEFT JOIN devices p ON p.id = t.parent_id;
