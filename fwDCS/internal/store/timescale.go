// Package store handles all database writes for DCS.
//
// Schema reference: see migrations/001_init.sql. Industry-standard naming
// throughout (no generic `meta`, no overloaded `ip_address`). Every metric
// carries a collector_agent (IDR|EDR) + collector_protocol tag.
package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "github.com/faberwork/fwdcs/proto/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/faberwork/fwdcs/migrations"
)

// DB wraps the PostgreSQL/TimescaleDB connection pool.
type DB struct {
	pool *pgxpool.Pool
}

// New creates a DB, runs schema migrations, and returns a ready store.
func New(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	db := &DB{pool: pool}
	if err := db.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return db, nil
}

// migrate applies every embedded migration that hasn't run yet, in filename
// order, recording each in the schema_migrations ledger so it runs once per DB.
// Existing databases created before the ledger existed simply re-run the
// idempotent 001 baseline (a no-op) and then pick up newer migrations.
func (db *DB) migrate(ctx context.Context) error {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT        PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("migrate: ledger: %w", err)
	}

	migs, err := migrations.All()
	if err != nil {
		return fmt.Errorf("migrate: load: %w", err)
	}

	for _, m := range migs {
		var applied bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, m.Version,
		).Scan(&applied); err != nil {
			return fmt.Errorf("migrate: check %s: %w", m.Version, err)
		}
		if applied {
			continue
		}
		// PgConn().Exec runs the simple-query protocol so a file may contain
		// multiple statements.
		mrr := conn.Conn().PgConn().Exec(ctx, m.SQL)
		if _, err := mrr.ReadAll(); err != nil {
			_ = mrr.Close()
			return fmt.Errorf("migrate: apply %s: %w", m.Version, err)
		}
		if err := mrr.Close(); err != nil {
			return fmt.Errorf("migrate: close %s: %w", m.Version, err)
		}
		if _, err := conn.Exec(ctx,
			`INSERT INTO schema_migrations(version) VALUES($1)`, m.Version,
		); err != nil {
			return fmt.Errorf("migrate: record %s: %w", m.Version, err)
		}
	}
	return nil
}

// Close releases the connection pool.
func (db *DB) Close() { db.pool.Close() }

// Ping verifies the connection pool can reach Postgres. Used by the /readyz endpoint.
func (db *DB) Ping(ctx context.Context) error { return db.pool.Ping(ctx) }

// ─── device registration ─────────────────────────────────────────────────────

// UpsertDevice inserts or updates a device row from a TelemetryPacket.
//
// Industry-standard IP roles:
//
//	mgmt_ip      — operator-facing mgmt IP (always set when known; 192.168.x in sim)
//	prod_ip      — production / data-plane IP (router/switch loopback OR server primary NIC; 10.x in sim)
//	loopback_ip  — explicit router/switch loopback if known
//	oob_ip       — out-of-band mgmt network IP
//
// Capability flags accumulate via OR so a dual-managed device keeps both
// snmp_enabled and gnmi_enabled after either collector writes.
//
// collector_agent comes from pkt.Meta["collector_agent"] (IDR|EDR) or is
// derived from pkt.ReaderId prefix ("idr-..." → IDR, "edr-..." → EDR).
func (db *DB) UpsertDevice(ctx context.Context, pkt *v1.TelemetryPacket) error {
	hostname := pkt.SourceId
	if v, ok := pkt.Meta["hostname"]; ok && v != "" {
		hostname = v
	}
	deviceType := pkt.Meta["device_type"]
	if deviceType == "" {
		deviceType = "server"
	}

	collectorAgent := pkt.Meta["collector_agent"]
	if collectorAgent == "" {
		collectorAgent = collectorAgentFromReaderID(pkt.ReaderId)
	}
	collectorProtocol := pkt.Meta["collector_protocol"]

	gnmiEnabled := collectorProtocol == "GNMI" || pkt.Meta["gnmi_enabled"] == "true"
	snmpEnabled := collectorProtocol == "SNMP" || pkt.Meta["snmp_enabled"] == "true"

	// Physical location — parse rack integers (0 → NULL in DB).
	rackRow := nilIfZeroInt(atoi(pkt.Meta["rack_row"]))
	rackNum := nilIfZeroInt(atoi(pkt.Meta["rack_num"]))
	rackUnit := nilIfZeroInt(atoi(pkt.Meta["rack_unit"]))

	_, err := db.pool.Exec(ctx, `
		INSERT INTO devices (
			org_id, datacenter_id, floor_id, network_id, group_id,
			hostname, device_type, vendor, model_name, os_name, os_version, sys_description,
			mgmt_ip, prod_ip, loopback_ip, oob_ip,
			snmp_enabled, gnmi_enabled, collector_agent,
			country, datacenter_city, datacenter, room, rack_row, rack_num, rack_unit,
			last_seen_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
		          NULLIF($13,'')::INET, NULLIF($14,'')::INET, NULLIF($15,'')::INET, NULLIF($16,'')::INET,
		          $17,$18,$19,
		          NULLIF($20,''), NULLIF($21,''), NULLIF($22,''), NULLIF($23,''),
		          $24::SMALLINT, $25::SMALLINT, $26::SMALLINT,
		          now(), now())
		ON CONFLICT (org_id, datacenter_id, floor_id, network_id, group_id, hostname)
		DO UPDATE SET
			last_seen_at    = now(),
			updated_at      = now(),
			os_name         = COALESCE(NULLIF(EXCLUDED.os_name, ''),         devices.os_name),
			os_version      = COALESCE(NULLIF(EXCLUDED.os_version, ''),      devices.os_version),
			sys_description = COALESCE(NULLIF(EXCLUDED.sys_description, ''), devices.sys_description),
			model_name      = COALESCE(NULLIF(EXCLUDED.model_name, ''),      devices.model_name),
			vendor          = COALESCE(NULLIF(EXCLUDED.vendor, ''),          devices.vendor),
			mgmt_ip         = COALESCE(EXCLUDED.mgmt_ip,                     devices.mgmt_ip),
			prod_ip         = COALESCE(EXCLUDED.prod_ip,                     devices.prod_ip),
			loopback_ip     = COALESCE(EXCLUDED.loopback_ip,                 devices.loopback_ip),
			oob_ip          = COALESCE(EXCLUDED.oob_ip,                      devices.oob_ip),
			country         = COALESCE(EXCLUDED.country,                     devices.country),
			datacenter_city = COALESCE(EXCLUDED.datacenter_city, devices.datacenter_city),
			datacenter      = COALESCE(EXCLUDED.datacenter,                  devices.datacenter),
			room            = COALESCE(EXCLUDED.room,                        devices.room),
			rack_row        = COALESCE(EXCLUDED.rack_row,                    devices.rack_row),
			rack_num        = COALESCE(EXCLUDED.rack_num,                    devices.rack_num),
			rack_unit       = COALESCE(EXCLUDED.rack_unit,                   devices.rack_unit),
			device_type     = EXCLUDED.device_type,
			collector_agent = EXCLUDED.collector_agent,
			snmp_enabled    = EXCLUDED.snmp_enabled OR devices.snmp_enabled,
			gnmi_enabled    = EXCLUDED.gnmi_enabled OR devices.gnmi_enabled,
			is_reachable    = TRUE`,
		pkt.OrgId, pkt.DatacenterId, pkt.FloorId, pkt.NetworkId, pkt.GroupId,
		hostname,
		deviceType,
		pkt.Meta["vendor"],
		pkt.Meta["model_name"],
		pkt.Meta["os_name"],
		pkt.Meta["platform_version"],
		pkt.Meta["sys_description"],
		pkt.Meta["mgmt_ip"],
		pkt.Meta["prod_ip"],
		pkt.Meta["loopback_ip"],
		pkt.Meta["oob_ip"],
		snmpEnabled,
		gnmiEnabled,
		collectorAgent,
		pkt.Meta["country"],
		pkt.Meta["datacenter_city"],
		pkt.Meta["datacenter"],
		pkt.Meta["room"],
		rackRow,
		rackNum,
		rackUnit,
	)
	return err
}

// collectorAgentFromReaderID infers IDR|EDR from the reader_id prefix.
func collectorAgentFromReaderID(readerID string) string {
	if strings.HasPrefix(readerID, "idr-") {
		return "IDR"
	}
	if strings.HasPrefix(readerID, "edr-") {
		return "EDR"
	}
	return "EDR"
}

// ─── interface inventory ─────────────────────────────────────────────────────

// UpsertInterface ensures an interface row exists for (deviceID, interfaceName
// OR interfaceIndex) and returns its UUID. Handles BOTH unique constraints on
// the interfaces table:
//   - UNIQUE (device_id, interface_name)
//   - UNIQUE (device_id, interface_index) WHERE interface_index IS NOT NULL
//
// Two protocols (SNMP, gNMI, LLDP, IDR) writing the same physical interface
// with different naming conventions (e.g. SNMP "if1" vs gNMI "Ethernet 1")
// would otherwise hit the second constraint and the upsert would silently
// fail with SQLSTATE 23505. We resolve this by selecting any existing row
// matching either key first, then UPDATEing or INSERTing.
func (db *DB) UpsertInterface(ctx context.Context, deviceID, interfaceName string, interfaceIndex, speedMbps, operationalStatus, adminStatus int) (string, error) {
	if deviceID == "" || interfaceName == "" {
		return "", nil
	}
	idxArg := interface{}(nil)
	if interfaceIndex > 0 {
		idxArg = interfaceIndex
	}
	speedArg := interface{}(nil)
	if speedMbps > 0 {
		speedArg = speedMbps
	}
	var id string
	err := db.pool.QueryRow(ctx, `
		WITH existing AS (
			SELECT id FROM interfaces
			WHERE device_id = $1
			  AND ( interface_name = $2
			        OR (interface_index IS NOT NULL AND interface_index = $3::INTEGER) )
			LIMIT 1
		),
		upd AS (
			UPDATE interfaces SET
				interface_name     = $2,
				interface_index    = COALESCE($3::INTEGER, interface_index),
				speed_mbps         = COALESCE($4::INTEGER, speed_mbps),
				operational_status = CASE WHEN $5::INTEGER > 0 THEN $5::INTEGER ELSE operational_status END,
				admin_status       = CASE WHEN $6::INTEGER > 0 THEN $6::INTEGER ELSE admin_status       END,
				updated_at         = now()
			WHERE id = (SELECT id FROM existing)
			RETURNING id
		),
		ins AS (
			INSERT INTO interfaces (
				device_id, interface_index, interface_name,
				speed_mbps, operational_status, admin_status, updated_at
			)
			SELECT $1, $3::INTEGER, $2, $4::INTEGER,
				CASE WHEN $5::INTEGER > 0 THEN $5::INTEGER ELSE 1 END,
				CASE WHEN $6::INTEGER > 0 THEN $6::INTEGER ELSE 1 END,
				now()
			WHERE NOT EXISTS (SELECT 1 FROM existing)
			RETURNING id
		)
		SELECT id FROM upd
		UNION ALL
		SELECT id FROM ins`,
		deviceID, interfaceName, idxArg, speedArg, operationalStatus, adminStatus,
	).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

// InterfaceIDByDeviceAndName looks up interfaces.id by (device_id, interface_name).
func (db *DB) InterfaceIDByDeviceAndName(ctx context.Context, deviceID, interfaceName string) (string, error) {
	if deviceID == "" || interfaceName == "" {
		return "", nil
	}
	var id string
	err := db.pool.QueryRow(ctx, `
		SELECT id FROM interfaces WHERE device_id = $1 AND interface_name = $2 LIMIT 1`,
		deviceID, interfaceName,
	).Scan(&id)
	if err != nil {
		return "", nil
	}
	return id, nil
}

// InterfaceIDByDeviceAndIndex looks up interfaces.id by (device_id, interface_index).
// Fallback for topology + interface_addresses paths where the name format
// differs between collection protocols (SNMP ifDescr vs gNMI OpenConfig vs
// LLDP port-id) but ifIndex is consistent.
func (db *DB) InterfaceIDByDeviceAndIndex(ctx context.Context, deviceID string, interfaceIndex int) (string, error) {
	if deviceID == "" || interfaceIndex <= 0 {
		return "", nil
	}
	var id string
	err := db.pool.QueryRow(ctx, `
		SELECT id FROM interfaces WHERE device_id = $1 AND interface_index = $2 LIMIT 1`,
		deviceID, interfaceIndex,
	).Scan(&id)
	if err != nil {
		return "", nil
	}
	return id, nil
}

// UpsertInterfaceAddress writes one IP address for an interface. Idempotent
// via the unique (interface_id, address) constraint.
func (db *DB) UpsertInterfaceAddress(ctx context.Context, interfaceID, address, addressFamily string, isPrimary bool) error {
	if interfaceID == "" || address == "" {
		return nil
	}
	family := addressFamily
	if family == "" {
		family = "ipv4"
	}
	_, err := db.pool.Exec(ctx, `
		INSERT INTO interface_addresses (interface_id, address, address_family, is_primary, updated_at)
		VALUES ($1, $2::INET, $3, $4, now())
		ON CONFLICT (interface_id, address)
		DO UPDATE SET
			address_family = EXCLUDED.address_family,
			is_primary     = EXCLUDED.is_primary,
			updated_at     = now()`,
		interfaceID, address, family, isPrimary,
	)
	return err
}

// ─── metrics ─────────────────────────────────────────────────────────────────

// MetricRow is one buffered metric row ready for COPY into the metrics
// hypertable. Column names match the canonical schema in 001_init.sql.
type MetricRow struct {
	DeviceID          string
	TS                time.Time
	MetricName        string // e.g. interface.bytes_received
	Tag               string // index/secondary key
	Value             float64
	Attributes        string // pre-encoded JSONB (metric-specific extras)
	CollectorAgent    string // IDR | EDR
	CollectorProtocol string // AGENT_LOCAL | SNMP | GNMI | TRAP
	InterfaceID       string // empty → NULL
}

// AttributesJSON is the canonical JSONB encoder for the metrics.attributes column.
func AttributesJSON(m map[string]string) string { return mapToJSON(m) }

// WriteMetricsBatch inserts rows via a single COPY through a staging table,
// then upserts into the metrics hypertable.
func (db *DB) WriteMetricsBatch(ctx context.Context, rows []MetricRow) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("metrics batch: acquire: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("metrics batch: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE _metrics_stage (
			device_id          UUID,
			ts                 TIMESTAMPTZ,
			metric_name        TEXT,
			tag                TEXT,
			value              DOUBLE PRECISION,
			attributes         JSONB,
			collector_agent    TEXT,
			collector_protocol TEXT,
			interface_id       UUID
		) ON COMMIT DROP`)
	if err != nil {
		return fmt.Errorf("metrics batch: stage create: %w", err)
	}

	src := pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
		r := rows[i]
		var ifaceID any
		if r.InterfaceID != "" {
			ifaceID = r.InterfaceID
		}
		attrs := r.Attributes
		if attrs == "" {
			attrs = "{}"
		}
		agent := r.CollectorAgent
		if agent == "" {
			agent = "EDR"
		}
		proto := r.CollectorProtocol
		if proto == "" {
			proto = "SNMP"
		}
		return []any{r.DeviceID, r.TS, r.MetricName, r.Tag, r.Value, attrs, agent, proto, ifaceID}, nil
	})
	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"_metrics_stage"},
		[]string{"device_id", "ts", "metric_name", "tag", "value", "attributes", "collector_agent", "collector_protocol", "interface_id"},
		src); err != nil {
		return fmt.Errorf("metrics batch: copy: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO metrics (device_id, ts, metric_name, tag, value, attributes, collector_agent, collector_protocol, interface_id)
		SELECT device_id, ts, metric_name, tag, value, attributes, collector_agent, collector_protocol, interface_id
		FROM _metrics_stage
		ON CONFLICT (device_id, metric_name, tag, ts) DO NOTHING`); err != nil {
		return fmt.Errorf("metrics batch: upsert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("metrics batch: commit: %w", err)
	}
	return nil
}

// ─── events ──────────────────────────────────────────────────────────────────

// WriteEvent inserts a trap or alarm event. sourceHostname is stored in the
// dedicated column when known — it gives operators a human-readable identity
// even if device_id couldn't be resolved (unknown device, pre-discovery trap,
// etc.).
func (db *DB) WriteEvent(ctx context.Context, deviceID, sourceHostname string, pkt *v1.TelemetryPacket) error {
	ts := time.Unix(0, pkt.TimestampNs).UTC()
	agent := pkt.Meta["collector_agent"]
	if agent == "" {
		agent = collectorAgentFromReaderID(pkt.ReaderId)
	}
	_, err := db.pool.Exec(ctx, `
		INSERT INTO events (device_id, source_hostname, ts, kind, event_name, severity, trap_oid, source_ip, event_payload, collector_agent)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8,'')::INET, $9, $10)`,
		nilIfEmpty(deviceID),
		nilIfEmpty(sourceHostname),
		ts,
		pkt.Kind,
		pkt.Name,
		pkt.Severity,
		nilIfEmpty(pkt.Meta["trap_oid"]),
		pkt.Meta["source_ip"],
		mapToJSON(pkt.Meta),
		agent,
	)
	return err
}

// ─── device state ─────────────────────────────────────────────────────────────

func (db *DB) UpsertDeviceState(ctx context.Context, deviceID, category, jsonData string) error {
	_, err := db.pool.Exec(ctx, `
		INSERT INTO device_state (device_id, category, data, updated_at)
		VALUES ($1, $2, $3::jsonb, now())
		ON CONFLICT (device_id, category)
		DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
		deviceID, category, jsonData,
	)
	return err
}

// ─── topology ────────────────────────────────────────────────────────────────

// TopologyLink is one directed edge in the network/power graph.
type TopologyLink struct {
	Layer          string
	SrcDeviceID    string
	SrcPortName    string
	SrcInterfaceID string
	DstDeviceID    string
	DstPortName    string
	DstInterfaceID string
	LinkSpeedMbps  *int
	LinkType       string
	Protocol       string // lldp|cdp|gnmi-lldp|manual|power-feed
	Relation       string // set by RecomputeHierarchy (BFS): uplink|downlink|peer
}

// UpsertLinks batch-upserts topology edges.
func (db *DB) UpsertLinks(ctx context.Context, links []TopologyLink) error {
	for _, l := range links {
		proto := l.Protocol
		if proto == "" {
			proto = "lldp"
		}
		relation := l.Relation
		if relation == "" {
			relation = "peer"
		}
		_, err := db.pool.Exec(ctx, `
			INSERT INTO topology_links
				(layer, src_device_id, src_port_name, dst_device_id, dst_port_name,
				 src_interface_id, dst_interface_id,
				 link_speed_mbps, link_type, protocol, relation, is_active, updated_at)
			VALUES ($1, $2::uuid, $3, $4::uuid, $5, $6, $7, $8, $9, $10, $11, TRUE, now())
			ON CONFLICT (layer, src_device_id, src_port_name, dst_device_id)
			DO UPDATE SET
				dst_port_name     = EXCLUDED.dst_port_name,
				src_interface_id  = COALESCE(EXCLUDED.src_interface_id, topology_links.src_interface_id),
				dst_interface_id  = COALESCE(EXCLUDED.dst_interface_id, topology_links.dst_interface_id),
				link_speed_mbps   = COALESCE(EXCLUDED.link_speed_mbps,   topology_links.link_speed_mbps),
				link_type         = COALESCE(EXCLUDED.link_type,         topology_links.link_type),
				protocol          = EXCLUDED.protocol,
				-- relation is owned by RecomputeHierarchy (BFS); preserve it
				-- across LLDP re-polls so the spanning-tree classification sticks.
				is_active         = TRUE,
				updated_at        = now()`,
			l.Layer, l.SrcDeviceID, l.SrcPortName, l.DstDeviceID, l.DstPortName,
			nilIfEmpty(l.SrcInterfaceID), nilIfEmpty(l.DstInterfaceID),
			l.LinkSpeedMbps, nilIfEmpty(l.LinkType),
			proto, relation,
		)
		if err != nil {
			return fmt.Errorf("store: upsert link %s→%s: %w", l.SrcDeviceID, l.DstDeviceID, err)
		}
	}
	return nil
}

// WriteTopologyLink resolves LLDP neighbor hostnames to device UUIDs,
// resolves interface UUIDs for both endpoints, and upserts the link row.
func (db *DB) WriteTopologyLink(ctx context.Context, pkt *v1.TelemetryPacket) error {
	srcID, _ := db.DeviceIDBySource(ctx,
		pkt.OrgId, pkt.DatacenterId, pkt.FloorId,
		pkt.NetworkId, pkt.GroupId, pkt.SourceId)
	if srcID == "" {
		return nil
	}
	remSysName := pkt.Meta["remote_sys_name"]
	if remSysName == "" {
		return nil
	}
	// Destination may be in a different datacenter than the source (cross-DC
	// LLDP links). DeviceIDByOrgAndHostname searches by org/network/group only —
	// no datacenter_id filter — so it resolves correctly for multi-DC topologies.
	dstID, _ := db.DeviceIDByOrgAndHostname(ctx,
		pkt.OrgId, pkt.NetworkId, pkt.GroupId, remSysName)
	if dstID == "" {
		return nil
	}

	layer := pkt.Meta["layer"]
	if layer == "" {
		layer = "network"
	}

	var speedMbps *int
	if s := pkt.Meta["link_speed_mbps"]; s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			speedMbps = &v
		}
	}

	// Resolve interface UUIDs with name-first, ifIndex fallback. Different
	// protocols stamp different names (SNMP ifDescr "if1", gNMI OpenConfig
	// "Ethernet 1", LLDP port-id "Eth1") but ifIndex is consistent.
	srcIfaceID, _ := db.InterfaceIDByDeviceAndName(ctx, srcID, pkt.Tag)
	if srcIfaceID == "" {
		if idx, err := strconv.Atoi(pkt.Meta["local_port_index"]); err == nil && idx > 0 {
			srcIfaceID, _ = db.InterfaceIDByDeviceAndIndex(ctx, srcID, idx)
		}
	}
	dstIfaceID, _ := db.InterfaceIDByDeviceAndName(ctx, dstID, pkt.Meta["remote_port_id"])
	if dstIfaceID == "" {
		if idx, err := strconv.Atoi(pkt.Meta["remote_port_index"]); err == nil && idx > 0 {
			dstIfaceID, _ = db.InterfaceIDByDeviceAndIndex(ctx, dstID, idx)
		}
	}

	return db.UpsertLinks(ctx, []TopologyLink{{
		Layer:          layer,
		SrcDeviceID:    srcID,
		SrcPortName:    pkt.Tag,
		SrcInterfaceID: srcIfaceID,
		DstDeviceID:    dstID,
		DstPortName:    pkt.Meta["remote_port_id"],
		DstInterfaceID: dstIfaceID,
		LinkSpeedMbps:  speedMbps,
		Protocol:       "lldp",
		// Relation is owned by RecomputeHierarchy (BFS); new edges default to
		// 'peer' and are reclassified there. Not set per-edge here.
	}})
}

// ─── device lookup ────────────────────────────────────────────────────────────

// DeviceIDBySource returns the device UUID for a given source_id within a tenant hierarchy.
func (db *DB) DeviceIDBySource(ctx context.Context,
	orgID, dcID, floorID, netID, grpID, sourceID string) (string, error) {

	var id string
	err := db.pool.QueryRow(ctx, `
		SELECT id FROM devices
		WHERE org_id=$1 AND datacenter_id=$2 AND floor_id=$3
		  AND network_id=$4 AND group_id=$5
		  AND hostname=$6
		LIMIT 1`,
		orgID, dcID, floorID, netID, grpID, sourceID,
	).Scan(&id)
	if err != nil {
		return "", nil
	}
	return id, nil
}

// DeviceIDByOrgAndHostname returns the device UUID by (org, network, group,
// hostname) WITHOUT filtering by datacenter_id or floor_id. Used by
// WriteTopologyLink when resolving the remote (LLDP) neighbor — the neighbor
// may live in a different datacenter than the source device, so filtering by
// pkt.DatacenterId would return nothing for cross-DC links.
func (db *DB) DeviceIDByOrgAndHostname(ctx context.Context,
	orgID, netID, grpID, hostname string) (string, error) {

	var id string
	err := db.pool.QueryRow(ctx, `
		SELECT id FROM devices
		WHERE org_id=$1 AND network_id=$2 AND group_id=$3
		  AND hostname=$4
		LIMIT 1`,
		orgID, netID, grpID, hostname,
	).Scan(&id)
	if err != nil {
		return "", nil
	}
	return id, nil
}

// DeviceIDByIP returns the device UUID for an IP. Matches mgmt_ip first
// (operator-facing address — the most reliable identifier for trap-sourced
// IPs), then falls back to prod_ip / loopback_ip / oob_ip.
//
// Used by the trap path when the trap's UDP source is loopback (sim mode) but
// the community string carries the real device IP.
func (db *DB) DeviceIDByIP(ctx context.Context,
	orgID, dcID, floorID, netID, grpID, ip string) (string, error) {

	id, _, _ := db.DeviceByIP(ctx, orgID, dcID, floorID, netID, grpID, ip)
	return id, nil
}

// DeviceByIP returns (id, hostname, true) for the device whose IP matches one
// of mgmt_ip / prod_ip / loopback_ip / oob_ip. Resolution order favours
// mgmt_ip — the canonical operator-facing IP — so trap sources that carry the
// management address (e.g. sim trap community) resolve first.
func (db *DB) DeviceByIP(ctx context.Context,
	orgID, dcID, floorID, netID, grpID, ip string) (string, string, bool) {

	if ip == "" {
		return "", "", false
	}
	var id, hostname string
	err := db.pool.QueryRow(ctx, `
		SELECT id, hostname
		FROM   devices
		WHERE  org_id=$1 AND datacenter_id=$2 AND floor_id=$3
		  AND  network_id=$4 AND group_id=$5
		  AND  ($6::INET IN (mgmt_ip, prod_ip, loopback_ip, oob_ip))
		ORDER BY ($6::INET = mgmt_ip)     DESC,
		         ($6::INET = prod_ip)     DESC,
		         ($6::INET = loopback_ip) DESC,
		         ($6::INET = oob_ip)      DESC
		LIMIT 1`,
		orgID, dcID, floorID, netID, grpID, ip,
	).Scan(&id, &hostname)
	if err != nil {
		return "", "", false
	}
	return id, hostname, true
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func mapToJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b := make([]byte, 0, 64)
	b = append(b, '{')
	first := true
	for k, v := range m {
		if !first {
			b = append(b, ',')
		}
		first = false
		b = append(b, '"')
		b = append(b, jsonEscape(k)...)
		b = append(b, '"', ':', '"')
		b = append(b, jsonEscape(v)...)
		b = append(b, '"')
	}
	b = append(b, '}')
	return string(b)
}

func jsonEscape(s string) []byte {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		default:
			b = append(b, c)
		}
	}
	return b
}

// MapWithout returns a copy of m with the given keys removed.
// Exported so ingest can strip transport keys before persisting attributes.
func MapWithout(m map[string]string, keys ...string) map[string]string {
	if len(m) == 0 {
		return m
	}
	out := make(map[string]string, len(m))
	skip := make(map[string]bool, len(keys))
	for _, k := range keys {
		skip[k] = true
	}
	for k, v := range m {
		if !skip[k] {
			out[k] = v
		}
	}
	return out
}

func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// atoi converts a string to int; returns 0 on empty string or parse error.
func atoi(s string) int {
	if s == "" {
		return 0
	}
	v, _ := strconv.Atoi(s)
	return v
}

// nilIfZeroInt returns nil when v == 0 (maps to SQL NULL for SMALLINT columns),
// otherwise returns the value as interface{}.
func nilIfZeroInt(v int) interface{} {
	if v == 0 {
		return nil
	}
	return v
}
