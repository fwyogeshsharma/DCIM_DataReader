// Package store handles all database writes for DCS.
//
// Schema reference: see migrations/001_init.sql. Industry-standard naming
// throughout (no generic `meta`, no overloaded `ip_address`). Every metric
// carries a collector_agent (IDR|EDR) + collector_protocol tag.
package store

import (
	"context"
	"encoding/json"
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

// Pool exposes the underlying connection pool for integration test harnesses
// (e.g. cmd/linktest) that need ad-hoc read queries. Not used by the pipeline.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

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

	// Rename-safe identity: a device is the same box as long as its mgmt_ip is
	// the same, regardless of hostname. So when we already have a row for this
	// mgmt_ip, UPDATE it in place (including the hostname) — this makes renaming a
	// device in the simulator update the existing row instead of inserting a
	// duplicate. We fall through to the INSERT path only when mgmt_ip is new or
	// empty (first sight). hostname's unique index is untouched: updating one
	// row's hostname to a new, non-colliding value never trips it.
	mgmtIP := pkt.Meta["mgmt_ip"]
	if mgmtIP != "" {
		// Capture current identity BEFORE the update so we can record a rename in
		// the events table (audit trail). Cheap single-row lookup, upsert path only.
		var oldID, oldHostname string
		_ = db.pool.QueryRow(ctx,
			`SELECT id::text, hostname FROM devices WHERE org_id=$1 AND mgmt_ip=$2::INET`,
			pkt.OrgId, mgmtIP).Scan(&oldID, &oldHostname)

		tag, err := db.pool.Exec(ctx, `
			UPDATE devices SET
				hostname        = $7,
				device_type     = $8,
				vendor          = COALESCE(NULLIF($9,''),  vendor),
				model_name      = COALESCE(NULLIF($10,''), model_name),
				os_name         = COALESCE(NULLIF($11,''), os_name),
				os_version      = COALESCE(NULLIF($12,''), os_version),
				sys_description = COALESCE(NULLIF($13,''), sys_description),
				prod_ip         = COALESCE(NULLIF($14,'')::INET, prod_ip),
				loopback_ip     = COALESCE(NULLIF($15,'')::INET, loopback_ip),
				oob_ip          = COALESCE(NULLIF($16,'')::INET, oob_ip),
				snmp_enabled    = $17 OR snmp_enabled,
				gnmi_enabled    = $18 OR gnmi_enabled,
				collector_agent = $19,
				country         = COALESCE(NULLIF($20,''), country),
				datacenter_city = COALESCE(NULLIF($21,''), datacenter_city),
				datacenter      = COALESCE(NULLIF($22,''), datacenter),
				room            = COALESCE(NULLIF($23,''), room),
				rack_row        = COALESCE($24::SMALLINT, rack_row),
				rack_num        = COALESCE($25::SMALLINT, rack_num),
				rack_unit       = COALESCE($26::SMALLINT, rack_unit),
				is_reachable    = TRUE,
				last_seen_at    = now(),
				updated_at      = now(),
				datacenter_id   = COALESCE(NULLIF($2,''), datacenter_id),
				floor_id        = COALESCE(NULLIF($3,''), floor_id),
				network_id      = COALESCE(NULLIF($4,''), network_id),
				group_id        = COALESCE(NULLIF($5,''), group_id)
			WHERE org_id=$1 AND mgmt_ip = $6::INET`,
			pkt.OrgId, pkt.DatacenterId, pkt.FloorId, pkt.NetworkId, pkt.GroupId,
			mgmtIP, hostname, deviceType,
			pkt.Meta["vendor"], pkt.Meta["model_name"], pkt.Meta["os_name"],
			pkt.Meta["platform_version"], pkt.Meta["sys_description"],
			pkt.Meta["prod_ip"], pkt.Meta["loopback_ip"], pkt.Meta["oob_ip"],
			snmpEnabled, gnmiEnabled, collectorAgent,
			pkt.Meta["country"], pkt.Meta["datacenter_city"], pkt.Meta["datacenter"], pkt.Meta["room"],
			rackRow, rackNum, rackUnit,
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() > 0 {
			// Record a hostname change (rename) as a device_change event so it
			// surfaces in the UI events stream. Only for an existing device whose
			// name actually changed; best-effort — never fails the upsert.
			if oldID != "" && oldHostname != "" && hostname != "" && oldHostname != hostname {
				db.recordHostnameChange(ctx, oldID, oldHostname, hostname, mgmtIP, collectorAgent)
			}
			return nil
		}
		// no existing row for this mgmt_ip → first sight; fall through to INSERT.
	}

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

// recordHostnameChange writes a device_change event capturing a device rename
// (old → new hostname) into the events table, so the change appears in the UI
// events stream alongside traps/alarms — an audit trail of identity changes.
// Best-effort: a failure here must never break the device upsert.
func (db *DB) recordHostnameChange(ctx context.Context, deviceID, oldName, newName, mgmtIP, agent string) {
	payload, _ := json.Marshal(map[string]string{
		"field": "hostname",
		"old":   oldName,
		"new":   newName,
	})
	if _, err := db.pool.Exec(ctx, `
		INSERT INTO events (device_id, source_hostname, ts, kind, event_name, severity,
		                    source_ip, event_payload, collector_agent)
		VALUES ($1::uuid, $2, now(), 'device_change', 'hostname_changed', 'informational',
		        NULLIF($3,'')::INET, $4, $5)`,
		deviceID, newName, mgmtIP, string(payload), agent); err != nil {
		// No logger on DB; swallow — the rename itself already persisted.
		_ = err
	}
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

// ─── energy metrics ───────────────────────────────────────────────────────────

// EnergyRow is one BACnet/IP energy reading destined for the energy_metrics
// hypertable. circuit/phase are first-class columns derived from the packet tag.
type EnergyRow struct {
	DeviceID          string
	TS                time.Time
	MetricName        string // e.g. energy.active_power_kw
	Tag               string // raw secondary key (CktNN / PhA / H3)
	Circuit           string // CktNN for per-circuit rows; "" for panel
	Phase             string // PhA/PhB/PhC for phase rows; "" otherwise
	Value             float64
	Scope             string // it|cooling|facility — meter classification for PUE/DCiE; "" if unknown
	Attributes        string // pre-encoded JSONB
	CollectorAgent    string
	CollectorProtocol string
}

// WriteEnergyBatch inserts energy rows via a COPY through a staging table, then
// upserts into the energy_metrics hypertable. Mirrors WriteMetricsBatch.
func (db *DB) WriteEnergyBatch(ctx context.Context, rows []EnergyRow) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("energy batch: acquire: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("energy batch: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE _energy_stage (
			device_id          UUID,
			ts                 TIMESTAMPTZ,
			metric_name        TEXT,
			tag                TEXT,
			circuit            TEXT,
			phase              TEXT,
			value              DOUBLE PRECISION,
			scope              TEXT,
			attributes         JSONB,
			collector_agent    TEXT,
			collector_protocol TEXT
		) ON COMMIT DROP`)
	if err != nil {
		return fmt.Errorf("energy batch: stage create: %w", err)
	}

	src := pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
		r := rows[i]
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
			proto = "BACNET"
		}
		return []any{r.DeviceID, r.TS, r.MetricName, r.Tag, r.Circuit, r.Phase, r.Value, r.Scope, attrs, agent, proto}, nil
	})
	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"_energy_stage"},
		[]string{"device_id", "ts", "metric_name", "tag", "circuit", "phase", "value", "scope", "attributes", "collector_agent", "collector_protocol"},
		src); err != nil {
		return fmt.Errorf("energy batch: copy: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO energy_metrics (device_id, ts, metric_name, tag, circuit, phase, value, scope, attributes, collector_agent, collector_protocol)
		SELECT device_id, ts, metric_name, tag, circuit, phase, value, scope, attributes, collector_agent, collector_protocol
		FROM _energy_stage
		ON CONFLICT (device_id, metric_name, tag, ts) DO NOTHING`); err != nil {
		return fmt.Errorf("energy batch: upsert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("energy batch: commit: %w", err)
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
	// Link correlation columns (peer endpoint + link id) are populated by
	// handleLinkTrap via pkt.Meta for a linkDown/linkUp that resolved to a
	// topology_links row; they are empty (→ NULL) for every other event. This lets
	// the UI render one "A:portA <-> B:portB DOWN" entry from the events stream
	// without a separate table.
	_, err := db.pool.Exec(ctx, `
		INSERT INTO events (device_id, source_hostname, src_port_name,
		                    dst_device_id, dst_hostname, dst_port_name, link_id,
		                    ts, kind, event_name, severity, trap_oid, source_ip, event_payload, collector_agent)
		VALUES ($1, $2, NULLIF($3,''),
		        NULLIF($4,'')::UUID, NULLIF($5,''), NULLIF($6,''), NULLIF($7,'')::UUID,
		        $8, $9, $10, $11, $12, NULLIF($13,'')::INET, $14, $15)`,
		nilIfEmpty(deviceID),
		nilIfEmpty(sourceHostname),
		pkt.Meta["src_port_name"],
		pkt.Meta["dst_device_id"],
		pkt.Meta["dst_hostname"],
		pkt.Meta["dst_port_name"],
		pkt.Meta["link_id"],
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

// LinkRow is one topology_links row resolved verbatim — its two endpoints are
// the AUTHORITATIVE devices for the link. Src/Dst follow the row's own column
// order (NOT trap-relative); no neighbor is ever inferred.
type LinkRow struct {
	LinkID      string
	Layer       string
	SrcDeviceID string
	SrcHostname string
	SrcPort     string
	DstDeviceID string
	DstHostname string
	DstPort     string
}

// CorrelateLinkByPort finds the EXACT topology_links row that owns the interface
// named in a link trap, for the device that sent it. topology_links is the single
// source of truth: a (device, port) pair belongs to exactly one network link, so
// the match is unique.
//
// PORT NUMBERING — the critical detail. topology_links.src_/dst_port_name store
// the 0-based interface index (the simulator/EDR JSON edge port). A trap, however,
// carries the IF-MIB ifIndex which is 1-based: EDR's loader documents
// "src_iface/dst_iface are 0-based; +1 = SNMP ifIndex". So the trap's raw ifIndex
// is ONE MORE than the stored port_name. Matching the raw ifIndex against
// port_name lands on the NEIGHBOURING link (the FW2-instead-of-FW1 bug). We must
// match the 0-based port, derived as:
//  1. the trailing digits of ifName  (e.g. "eth3" -> "3") — explicit identity, or
//  2. ifIndex - 1                     (1-based ifIndex back to the 0-based port).
//
// The raw ifIndex is never used as a key.
//
// There is deliberately NO fallback: we never pick "the device's only other
// link", never scan for another active edge, never guess a neighbour. If no row
// matches, found=false and the caller records nothing (the raw trap still lands
// in `events`).
func (db *DB) CorrelateLinkByPort(ctx context.Context, deviceID string, ifIndex int, ifName string) (LinkRow, bool, error) {
	var r LinkRow
	match := func(port string) (bool, error) {
		if port == "" {
			return false, nil
		}
		err := db.pool.QueryRow(ctx, `
			SELECT id::text, layer,
			       src_device_id::text, dst_device_id::text,
			       src_port_name, dst_port_name
			FROM topology_links
			WHERE layer='network' AND (
			      (src_device_id=$1::uuid AND src_port_name=$2)
			   OR (dst_device_id=$1::uuid AND dst_port_name=$2))
			LIMIT 1`,
			deviceID, port).Scan(&r.LinkID, &r.Layer, &r.SrcDeviceID, &r.DstDeviceID, &r.SrcPort, &r.DstPort)
		if err != nil {
			if err == pgx.ErrNoRows {
				return false, nil
			}
			return false, err
		}
		return r.LinkID != "", nil
	}

	// Build 0-based port candidates in priority order; raw ifIndex is excluded.
	for _, port := range portCandidates(ifIndex, ifName) {
		ok, err := match(port)
		if err != nil {
			return LinkRow{}, false, err
		}
		if ok {
			_ = db.pool.QueryRow(ctx, `SELECT hostname FROM devices WHERE id=$1::uuid`, r.SrcDeviceID).Scan(&r.SrcHostname)
			_ = db.pool.QueryRow(ctx, `SELECT hostname FROM devices WHERE id=$1::uuid`, r.DstDeviceID).Scan(&r.DstHostname)
			return r, true, nil
		}
	}
	return LinkRow{}, false, nil
}

// portCandidates returns the 0-based port_name keys to try for a link trap, in
// priority order, de-duplicated. ifName's trailing digits first ("eth3" -> "3"),
// then ifIndex-1 (1-based SNMP ifIndex back to the 0-based port). The raw ifIndex
// is intentionally NOT a candidate — it is off by one from port_name.
func portCandidates(ifIndex int, ifName string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(s string) {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	add(trailingDigits(ifName))
	if ifIndex >= 1 {
		add(strconv.Itoa(ifIndex - 1))
	}
	return out
}

// trailingDigits returns the trailing run of digits in s ("eth3" -> "3",
// "Gi0/12" -> "12", "eth" -> ""). Used to recover the 0-based port index from an
// interface name.
func trailingDigits(s string) string {
	i := len(s)
	for i > 0 && s[i-1] >= '0' && s[i-1] <= '9' {
		i--
	}
	return s[i:]
}

// SetLinkActive flips topology_links.is_active for the WHOLE physical link — BOTH
// directed rows of the device pair, so an A→B / B→A pair never goes half-stale.
// Only the two devices in `row` are used; no neighbour is inferred.
//
// changed reports whether is_active actually transitioned (false = already in the
// desired state, e.g. the peer's duplicate trap or a same-state repeat). The
// caller uses that to write exactly ONE enriched link event per real transition.
func (db *DB) SetLinkActive(ctx context.Context, row LinkRow, down bool) (changed bool, err error) {
	layer := row.Layer
	if layer == "" {
		layer = "network"
	}
	desired := !down
	tag, e := db.pool.Exec(ctx, `
		UPDATE topology_links SET is_active=$3, updated_at=now()
		WHERE layer=$4 AND is_active <> $3 AND (
		      (src_device_id=$1::uuid AND dst_device_id=$2::uuid)
		   OR (src_device_id=$2::uuid AND dst_device_id=$1::uuid))`,
		row.SrcDeviceID, row.DstDeviceID, desired, layer)
	if e != nil {
		return false, e
	}
	return tag.RowsAffected() > 0, nil
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
				-- across re-polls so the spanning-tree classification sticks.
				-- is_active is owned by the link-state machine (linkDown/linkUp traps,
				-- LinkStateChange) — NEVER reset it here, or a periodic topology
				-- re-emit would silently flip a broken link back to UP.
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
	// Resolve the SOURCE device. Prefer the stable mgmt_ip (rename-proof) over the
	// hostname: the polling device carries its own mgmt_ip in Meta, and IP is
	// unique within a tenant, so dc/floor are left blank. Hostname (SourceId) is
	// the fallback for any packet without mgmt_ip.
	srcID := ""
	if mip := pkt.Meta["mgmt_ip"]; mip != "" {
		srcID, _ = db.DeviceIDByIP(ctx, pkt.OrgId, "", "", pkt.NetworkId, pkt.GroupId, mip)
	}
	if srcID == "" {
		srcID, _ = db.DeviceIDBySource(ctx,
			pkt.OrgId, pkt.DatacenterId, pkt.FloorId,
			pkt.NetworkId, pkt.GroupId, pkt.SourceId)
	}
	if srcID == "" {
		return nil
	}

	// Resolve the DESTINATION (LLDP neighbor). Prefer the stable mgmt_ip carried in
	// remote_mgmt_ip (from lldpRemChassisId) — this is what fixes both the missing
	// links and rename-breaks-correlation: name-only resolution silently dropped a
	// link whenever the advertised neighbor name didn't exactly match devices.hostname
	// (rename, shard-sync lag, etc.). Hostname is the fallback. dc/floor blank so
	// cross-DC links resolve.
	dstID := ""
	if rmip := pkt.Meta["remote_mgmt_ip"]; rmip != "" {
		dstID, _ = db.DeviceIDByIP(ctx, pkt.OrgId, "", "", pkt.NetworkId, pkt.GroupId, rmip)
	}
	if dstID == "" {
		if remSysName := pkt.Meta["remote_sys_name"]; remSysName != "" {
			dstID, _ = db.DeviceIDByOrgAndHostname(ctx,
				pkt.OrgId, pkt.NetworkId, pkt.GroupId, remSysName)
		}
	}
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
// dc/floor are applied ONLY when non-empty. A trap packet carries no per-device
// scope today — the trap receiver uses the global identity, whose
// datacenter_id/floor_id are blank (per-device DC/floor ride only on polled
// metrics from the topology). The old hard filter `datacenter_id=$2 AND
// floor_id=$3` matched zero devices against blank values, so events.device_id
// and source_hostname stayed NULL. The conditional below resolves by
// org+network+group+IP when scope is absent, and tightens to the exact DC/floor
// the moment a future trap (or simulator) supplies them — no assumption either
// way. IPs are unique within a tenant, so this stays correct in both modes.
func (db *DB) DeviceByIP(ctx context.Context,
	orgID, dcID, floorID, netID, grpID, ip string) (string, string, bool) {

	if ip == "" {
		return "", "", false
	}
	var id, hostname string
	err := db.pool.QueryRow(ctx, `
		SELECT id, hostname
		FROM   devices
		WHERE  org_id=$1 AND network_id=$4 AND group_id=$5
		  AND  ($2='' OR datacenter_id=$2)
		  AND  ($3='' OR floor_id=$3)
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
