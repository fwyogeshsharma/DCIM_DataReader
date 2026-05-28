-- fwDCIM unified schema (v1.0)
-- Single-file, dev-phase drop-and-recreate. Idempotent — re-running against a
-- matching schema is a no-op.
--
-- Industry-standard naming conventions used throughout. There is no generic
-- `ip_address` or `meta` column — every field has a clear, single semantic
-- meaning.
--
-- Multi-collector model: every device and every metric carries a
-- `collector_agent` flag so consumers can distinguish data sources:
--   collector_agent = 'IDR'  — Internal Data Reader (host agent on the device)
--   collector_agent = 'EDR'  — External Data Reader (remote SNMP/gNMI poller)
--
-- Every metric also carries `collector_protocol`:
--   AGENT_LOCAL | SNMP | GNMI | TRAP

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ────────────────────────────────────────────────────────────────────────────
-- Table: devices
-- ────────────────────────────────────────────────────────────────────────────
-- One row per managed device. Inserted by DCS on first sight (either a sys.*
-- packet from EDR or an os_info packet from IDR).

CREATE TABLE IF NOT EXISTS devices (
    id                    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Resource hierarchy (multi-tenant scoping)
    org_id                TEXT         NOT NULL,
    datacenter_id         TEXT         NOT NULL,
    floor_id              TEXT         NOT NULL,
    network_id            TEXT         NOT NULL,
    group_id              TEXT         NOT NULL,

    -- Identity
    hostname              TEXT         NOT NULL,
    device_type           TEXT         NOT NULL,           -- router|switch|firewall|load_balancer|server|ups|pdu|floor_pdu|sensor
    vendor                TEXT,
    model_name            TEXT,
    os_name               TEXT,
    os_version            TEXT,
    sys_oid               TEXT,                            -- SNMPv2 sysObjectID
    sys_description       TEXT,                            -- SNMPv2 sysDescr
    sys_location          TEXT,                            -- SNMPv2 sysLocation

    -- IP addresses (no generic ip_address — every IP has explicit role)
    mgmt_ip               INET,                            -- Always set when known. How operators/EDR reach the device (192.168.x in sim).
    prod_ip               INET,                            -- Main "production" IP (router/switch data-plane / loopback, server primary NIC; 10.x in sim).
    loopback_ip           INET,                            -- Router/switch loopback (advertised in IGP/iBGP).
    oob_ip                INET,                            -- Out-of-band management network IP (separate from primary mgmt).

    -- Capability flags
    snmp_enabled          BOOLEAN      NOT NULL DEFAULT FALSE,
    gnmi_enabled          BOOLEAN      NOT NULL DEFAULT FALSE,
    snmp_port             INTEGER      NOT NULL DEFAULT 161,
    snmp_version          SMALLINT     NOT NULL DEFAULT 2,
    gnmi_port             INTEGER      NOT NULL DEFAULT 57400,

    -- Collector boundary — which agent is responsible for this device
    collector_agent       TEXT         NOT NULL DEFAULT 'EDR',  -- IDR (host agent) | EDR (remote poller)

    -- Physical placement (optional, populated by inventory imports)
    country               TEXT,
    datacenter_city       TEXT,
    datacenter            TEXT,
    room                  TEXT,
    rack_row              SMALLINT,
    rack_num              SMALLINT,
    rack_unit             SMALLINT,

    -- Power dependencies (graph edges for power-chain topology)
    power_draw_w          SMALLINT,
    power_source_id       UUID         REFERENCES devices(id) ON DELETE SET NULL,
    ups_backup_id         UUID         REFERENCES devices(id) ON DELETE SET NULL,

    -- Reachability
    is_reachable          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen_at          TIMESTAMPTZ
);

-- Per-tenant unique hostname
CREATE UNIQUE INDEX IF NOT EXISTS uix_devices_identity
    ON devices (org_id, datacenter_id, floor_id, network_id, group_id, hostname);

CREATE INDEX IF NOT EXISTS idx_devices_org_dc       ON devices(org_id, datacenter_id);
CREATE INDEX IF NOT EXISTS idx_devices_type         ON devices(device_type);
CREATE INDEX IF NOT EXISTS idx_devices_mgmt         ON devices(mgmt_ip);
CREATE INDEX IF NOT EXISTS idx_devices_prod         ON devices(prod_ip);
CREATE INDEX IF NOT EXISTS idx_devices_collector    ON devices(collector_agent);

-- ────────────────────────────────────────────────────────────────────────────
-- Table: interfaces
-- ────────────────────────────────────────────────────────────────────────────
-- Per-device interface inventory. One row per physical/logical port.
-- Refreshed on every EDR/IDR poll cycle.

CREATE TABLE IF NOT EXISTS interfaces (
    id                       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id                UUID         NOT NULL REFERENCES devices(id) ON DELETE CASCADE,

    interface_name           TEXT         NOT NULL,           -- ifName, e.g. "GigabitEthernet0/1"
    interface_index          INTEGER,                         -- IF-MIB ifIndex (NULL for gNMI-only devices)
    interface_description    TEXT,                            -- ifDescr / ifAlias text
    interface_type           TEXT,                            -- ethernet | loopback | vlan | lag | mgmt | other
    interface_mac_address    MACADDR,                         -- ifPhysAddress

    speed_mbps               INTEGER,                         -- ifHighSpeed
    admin_status             SMALLINT     NOT NULL DEFAULT 1, -- 1=up 2=down 3=testing
    operational_status       SMALLINT     NOT NULL DEFAULT 1, -- 1=up 2=down 3=testing 4=unknown ...

    access_vlan_id           INTEGER,                         -- access VLAN if applicable
    mtu_bytes                INTEGER,                         -- ifMtu

    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),

    UNIQUE (device_id, interface_name)
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_iface_device_idx
    ON interfaces (device_id, interface_index)
    WHERE interface_index IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_iface_device   ON interfaces(device_id);
CREATE INDEX IF NOT EXISTS idx_iface_op       ON interfaces(device_id, operational_status);

-- ────────────────────────────────────────────────────────────────────────────
-- Table: interface_addresses
-- ────────────────────────────────────────────────────────────────────────────
-- Multi-IP support: one row per IP per interface. Replaces the old single
-- ipv4_address column. Supports IPv4, IPv6, VRFs, primary/secondary roles.

CREATE TABLE IF NOT EXISTS interface_addresses (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    interface_id    UUID         NOT NULL REFERENCES interfaces(id) ON DELETE CASCADE,

    address         INET         NOT NULL,                  -- e.g. 10.50.0.4/24
    address_family  TEXT         NOT NULL DEFAULT 'ipv4',   -- ipv4 | ipv6
    is_primary      BOOLEAN      NOT NULL DEFAULT TRUE,
    vrf             TEXT,                                   -- optional, for L3VPN / multi-tenant routers

    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    UNIQUE (interface_id, address)
);

CREATE INDEX IF NOT EXISTS idx_iface_addr_iface   ON interface_addresses(interface_id);
CREATE INDEX IF NOT EXISTS idx_iface_addr_addr    ON interface_addresses(address);

-- ────────────────────────────────────────────────────────────────────────────
-- Table: metrics  (TimescaleDB hypertable)
-- ────────────────────────────────────────────────────────────────────────────
-- All time-series numeric telemetry. Industry-standard naming:
--
-- INTERFACE METRICS  (tag = interface index as string; interface_id FK is set)
--   interface.bytes_received           interface.bytes_sent
--   interface.bytes_received_hc        interface.bytes_sent_hc           (64-bit, 10G+)
--   interface.packets_received_unicast interface.packets_sent_unicast
--   interface.errors_received          interface.errors_sent
--   interface.discards_received        interface.discards_sent
--   interface.admin_status             interface.operational_status
--   interface.speed_mbps
--
-- SYSTEM METRICS  (tag = '')
--   system.uptime_centiseconds         system.cpu_utilization_percent
--   system.memory_total_bytes          system.memory_used_bytes
--   system.memory_free_bytes
--   system.chassis_temperature_c       system.cpu_temperature_c
--   system.inlet_temperature_c
--
-- SERVER METRICS  (HR-MIB + UCD; tag = '' or cpu/disk index)
--   server.cpu_per_core_percent        server.cpu_user_percent
--   server.cpu_system_percent          server.cpu_idle_percent
--   server.memory_total_kb             server.memory_used_kb
--   server.memory_cached_kb            server.memory_buffer_kb
--   server.storage_size_kb             server.storage_used_kb
--   server.storage_available_kb
--
-- ENVIRONMENT METRICS  (sensors / PDU / UPS; tag = sensor index)
--   environment.temperature_c          environment.humidity_percent
--   environment.dew_point_c            environment.airflow_cfm
--   environment.ups_battery_status     environment.ups_charge_percent
--   environment.ups_battery_voltage_v  environment.ups_input_voltage_v
--   environment.ups_output_voltage_v   environment.ups_output_current_a
--   environment.ups_output_load_percent

CREATE TABLE IF NOT EXISTS metrics (
    device_id            UUID                NOT NULL,
    ts                   TIMESTAMPTZ         NOT NULL,
    metric_name          TEXT                NOT NULL,           -- e.g. interface.bytes_received
    tag                  TEXT                NOT NULL DEFAULT '', -- index/secondary key (ifIndex, core#, sensor#, etc.)
    value                DOUBLE PRECISION    NOT NULL,
    attributes           JSONB,                                  -- per-metric extras (interface name, sensor type, etc.)

    -- Collector provenance
    collector_agent      TEXT                NOT NULL DEFAULT 'EDR',           -- IDR | EDR
    collector_protocol   TEXT                NOT NULL DEFAULT 'SNMP',          -- AGENT_LOCAL | SNMP | GNMI | TRAP

    -- Cross-table joins
    interface_id         UUID                REFERENCES interfaces(id) ON DELETE SET NULL
);

SELECT create_hypertable('metrics', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uix_metrics
    ON metrics (device_id, metric_name, tag, ts);

CREATE INDEX IF NOT EXISTS idx_metrics_device     ON metrics (device_id, metric_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_name       ON metrics (metric_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_iface      ON metrics (interface_id, metric_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_collector  ON metrics (collector_agent, ts DESC);

SELECT add_retention_policy('metrics', INTERVAL '30 days', if_not_exists => TRUE);

-- 5-minute rollup aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', ts) AS bucket,
    device_id,
    metric_name,
    tag,
    avg(value)   AS avg_value,
    max(value)   AS max_value,
    min(value)   AS min_value,
    count(*)     AS samples
FROM metrics
GROUP BY bucket, device_id, metric_name, tag
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_5m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);

SELECT add_retention_policy('metrics_5m', INTERVAL '6 months', if_not_exists => TRUE);

-- ────────────────────────────────────────────────────────────────────────────
-- Table: topology_links
-- ────────────────────────────────────────────────────────────────────────────
-- One row per directed L2/L3 edge between two devices. Discovered from LLDP /
-- CDP / gNMI OpenConfig LLDP. Each physical cable normally produces TWO rows
-- (one per side advertising LLDP). The view layer deduplicates when an
-- undirected graph is needed.
--
-- layer:    network | management | power
-- protocol: lldp    | cdp        | gnmi-lldp | manual | power-feed

CREATE TABLE IF NOT EXISTS topology_links (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    layer               TEXT         NOT NULL,

    src_device_id       UUID         NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    src_port_name       TEXT         NOT NULL DEFAULT '',
    src_interface_id    UUID         REFERENCES interfaces(id) ON DELETE SET NULL,

    dst_device_id       UUID         NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    dst_port_name       TEXT         NOT NULL DEFAULT '',
    dst_interface_id    UUID         REFERENCES interfaces(id) ON DELETE SET NULL,

    link_speed_mbps     INTEGER,
    link_type           TEXT,                                   -- ethernet | fiber | power-c13 | power-c19 ...
    protocol            TEXT         NOT NULL DEFAULT 'lldp',
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,    -- FALSE = link not seen in last poll
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),

    UNIQUE (layer, src_device_id, src_port_name, dst_device_id)
);

CREATE INDEX IF NOT EXISTS idx_links_layer       ON topology_links(layer);
CREATE INDEX IF NOT EXISTS idx_links_src         ON topology_links(src_device_id);
CREATE INDEX IF NOT EXISTS idx_links_dst         ON topology_links(dst_device_id);
CREATE INDEX IF NOT EXISTS idx_links_src_iface   ON topology_links(src_interface_id);
CREATE INDEX IF NOT EXISTS idx_links_dst_iface   ON topology_links(dst_interface_id);

-- ────────────────────────────────────────────────────────────────────────────
-- Table: device_state
-- ────────────────────────────────────────────────────────────────────────────
-- Current operational state blobs per device per category. UPSERT only —
-- no history. Categories: lldp | mac_table | vlans | bgp | ospf | routing | power_chain.

CREATE TABLE IF NOT EXISTS device_state (
    device_id    UUID         NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    category     TEXT         NOT NULL,
    data         JSONB        NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),

    PRIMARY KEY (device_id, category)
);

CREATE INDEX IF NOT EXISTS idx_state_category ON device_state(category);
CREATE INDEX IF NOT EXISTS idx_state_data     ON device_state USING GIN(data);

-- ────────────────────────────────────────────────────────────────────────────
-- Table: events  (TimescaleDB hypertable)
-- ────────────────────────────────────────────────────────────────────────────
-- SNMP traps, alarms, state-change events. Append-only.

CREATE TABLE IF NOT EXISTS events (
    id              UUID         NOT NULL DEFAULT gen_random_uuid(),
    device_id       UUID,                                        -- resolved via mgmt_ip lookup; NULL when device unknown
    source_hostname TEXT,                                        -- canonical hostname carried alongside (or instead of) device_id when known
    ts              TIMESTAMPTZ  NOT NULL,
    kind            TEXT         NOT NULL,                       -- trap | alarm | event
    event_name      TEXT         NOT NULL,
    severity        TEXT         NOT NULL DEFAULT 'informational', -- informational | minor | major | critical
    trap_oid        TEXT,
    source_ip       INET,
    event_payload   JSONB,                                       -- vendor-specific event data
    collector_agent TEXT         NOT NULL DEFAULT 'EDR'
);

SELECT create_hypertable('events', 'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE);

CREATE INDEX IF NOT EXISTS idx_events_device   ON events (device_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_severity
    ON events (severity, ts DESC)
    WHERE severity IN ('major', 'critical');
CREATE INDEX IF NOT EXISTS idx_events_name     ON events (event_name, ts DESC);

SELECT add_retention_policy('events', INTERVAL '1 year', if_not_exists => TRUE);

-- ────────────────────────────────────────────────────────────────────────────
-- View: topology_view  (UI-ready)
-- ────────────────────────────────────────────────────────────────────────────
-- Resolves UUIDs to hostnames, IPs, port names. Includes both endpoint
-- capability flags so the UI can render SNMP-only vs dual-managed differently.

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
    di.admin_status                                        AS dst_port_admin_status

FROM       topology_links tl
JOIN       devices    sd ON sd.id = tl.src_device_id
JOIN       devices    dd ON dd.id = tl.dst_device_id
LEFT JOIN  interfaces si ON si.id = tl.src_interface_id
LEFT JOIN  interfaces di ON di.id = tl.dst_interface_id;

-- ────────────────────────────────────────────────────────────────────────────
-- Table: forwarder_cursors
-- ────────────────────────────────────────────────────────────────────────────
-- Persistent high-water marks for the DCS→Aggregator incremental push loop.
-- One row per logical cursor (devices | metrics | topology | events).
-- The forwarder queries rows with ts/updated_at > cursor, pushes them, then
-- advances the cursor to the max timestamp seen in that batch.

CREATE TABLE IF NOT EXISTS forwarder_cursors (
    name        TEXT         PRIMARY KEY,
    cursor      TIMESTAMPTZ  NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ────────────────────────────────────────────────────────────────────────────
-- View: device_inventory  (UI device list)
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW device_inventory AS
SELECT
    d.id,
    d.hostname,
    d.device_type,
    d.vendor,
    d.os_name,
    d.os_version,
    d.mgmt_ip,
    d.prod_ip,
    d.loopback_ip,
    d.oob_ip,
    d.snmp_enabled,
    d.gnmi_enabled,
    d.collector_agent,
    d.is_reachable,
    d.last_seen_at,
    d.org_id,
    d.datacenter_id,
    d.floor_id,
    d.network_id,
    d.group_id,
    COUNT(i.id)::INTEGER                                            AS interface_count,
    COUNT(i.id) FILTER (WHERE i.operational_status = 1)::INTEGER    AS interfaces_up,
    COUNT(i.id) FILTER (WHERE i.operational_status != 1)::INTEGER   AS interfaces_down
FROM       devices d
LEFT JOIN  interfaces i ON i.device_id = d.id
GROUP BY   d.id;
