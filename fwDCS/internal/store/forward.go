package store

// forward.go — read-side query methods used exclusively by the DCS→Aggregator
// forwarder. All methods are cursor-based (since time.Time) so the forwarder
// can page through large result sets without missing or double-sending rows.

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// ─── Result types ─────────────────────────────────────────────────────────────

// FwdDevice is a device row enriched for the Aggregator payload. Carries the
// full set of columns the Aggregator devices table stores so location/IP/OS
// fields are populated there, not just hostname+mgmt_ip.
type FwdDevice struct {
	ID             string
	DatacenterID   string // payload scoping key
	FloorID        string // payload scoping key
	Hostname       string
	DeviceType     string
	Vendor         string
	ModelName      string
	OSName         string
	OSVersion      string
	SysOID         string
	SysDescr       string
	SysLocation    string
	MgmtIP         string // empty string when not set
	ProdIP         string
	LoopbackIP     string
	OOBIP          string
	SNMPEnabled    bool
	GNMIEnabled    bool
	SNMPPort       int
	SNMPVersion    int
	GNMIPort       int
	CollectorAgent string
	IsReachable    bool
	Country        string
	DatacenterCity string
	Datacenter     string
	Room           string
	RackRow        *int
	RackNum        *int
	RackUnit       *int
	PowerDrawW     *int
	PowerState     *int     // 1=On, 2=Off — migration 009 (nil when unknown)
	DeviceRole     string   // fabric role (core/spine/leaf/...) — migration 003
	RoleConfidence *float64 // 0.0–1.0, nil when never classified
	RoleSource     string   // inferred|suggested|unclassified|override
	RoleOverridden bool     // admin-set role; classifier skips these — migration 003
	UpdatedAt      time.Time
}

// fwdDeviceCols is the shared SELECT list for all device queries so every code
// path returns an identically-shaped row (scanned by scanFwdDevice).
const fwdDeviceCols = `
	id, datacenter_id, floor_id, hostname, device_type,
	COALESCE(vendor,''), COALESCE(model_name,''), COALESCE(os_name,''),
	COALESCE(os_version,''), COALESCE(sys_oid,''), COALESCE(sys_description,''),
	COALESCE(sys_location,''),
	COALESCE(mgmt_ip::text,''), COALESCE(prod_ip::text,''),
	COALESCE(loopback_ip::text,''), COALESCE(oob_ip::text,''),
	snmp_enabled, gnmi_enabled, snmp_port, snmp_version, gnmi_port, collector_agent,
	is_reachable,
	COALESCE(country,''), COALESCE(datacenter_city,''),
	COALESCE(datacenter,''), COALESCE(room,''),
	rack_row, rack_num, rack_unit, power_draw_w, power_state,
	COALESCE(device_role,''), role_confidence, COALESCE(role_source,''), role_overridden,
	updated_at`

func scanFwdDevice(rows pgx.Rows) (FwdDevice, error) {
	var d FwdDevice
	err := rows.Scan(&d.ID, &d.DatacenterID, &d.FloorID, &d.Hostname, &d.DeviceType,
		&d.Vendor, &d.ModelName, &d.OSName, &d.OSVersion, &d.SysOID, &d.SysDescr,
		&d.SysLocation,
		&d.MgmtIP, &d.ProdIP, &d.LoopbackIP, &d.OOBIP,
		&d.SNMPEnabled, &d.GNMIEnabled, &d.SNMPPort, &d.SNMPVersion, &d.GNMIPort, &d.CollectorAgent,
		&d.IsReachable,
		&d.Country, &d.DatacenterCity, &d.Datacenter, &d.Room,
		&d.RackRow, &d.RackNum, &d.RackUnit, &d.PowerDrawW, &d.PowerState,
		&d.DeviceRole, &d.RoleConfidence, &d.RoleSource, &d.RoleOverridden,
		&d.UpdatedAt)
	return d, err
}

// FwdInterface is one interface row for a device.
type FwdInterface struct {
	ID                string
	DeviceID          string
	InterfaceName     string
	InterfaceIndex    *int
	Description       string
	Type              string
	MACAddress        string
	SpeedMbps         *int // nil when unknown
	AdminStatus       int
	OperationalStatus int
	AccessVlanID      *int
	MTUBytes          *int
}

// FwdAddress is one IP address for an interface.
type FwdAddress struct {
	ID            string
	InterfaceID   string
	Address       string // CIDR notation, e.g. "10.0.0.1/24"
	AddressFamily string
	IsPrimary     bool
	VRF           string
}

// FwdMetric is one metric sample destined for the Aggregator payload.
type FwdMetric struct {
	DeviceID          string
	InterfaceID       string // empty if not an interface metric
	MetricName        string
	Value             float64
	Tag               string
	Attributes        string // raw JSONB text ('{}' when none)
	CollectorAgent    string
	CollectorProtocol string
	InterfaceName     string // empty if not an interface metric
	TS                time.Time
}

// FwdTopologyLink is one directed edge resolved to hostnames. Carries the
// source device's datacenter/floor so the forwarder can group links into the
// correct per-scope payload (Aggregator requires both endpoints in the same
// (datacenter_id, floor_id) payload).
type FwdTopologyLink struct {
	ID              string
	SrcDatacenterID string
	SrcFloorID      string
	Layer           string
	SrcDeviceID     string
	SrcInterfaceID  string // empty when unresolved
	SrcHostname     string
	SrcPortName     string
	DstDeviceID     string
	DstInterfaceID  string // empty when unresolved
	DstHostname     string
	DstPortName     string
	LinkSpeedMbps   *int // nil when unknown
	LinkType        string
	Protocol        string
	Relation        string // uplink (dst=parent) | downlink (dst=child) | peer
	IsActive        bool
	UpdatedAt       time.Time
}

// FwdEvent is one event row resolved to source hostname. Carries the device's
// datacenter/floor for per-scope payload grouping (empty when device_id NULL).
type FwdEvent struct {
	ID             string
	DeviceID       string // source device — empty when device_id NULL
	DatacenterID   string
	FloorID        string
	Hostname       string // source hostname
	Kind           string
	EventName      string
	Severity       string
	TrapOID        string
	SourceIP       string // empty string when not set
	CollectorAgent string
	// Link-event enrichment — set only for a correlated linkDown/linkUp; empty
	// otherwise. Lets the UI render one "src:port <-> dst:port" entry.
	SrcPortName string
	DstDeviceID string
	DstHostname string
	DstPortName string
	LinkID      string
	TS          time.Time
	Payload     string // raw JSONB text — already valid JSON
}

// ─── Device queries ───────────────────────────────────────────────────────────

// DevicesUpdatedSince returns devices whose updated_at is strictly after since,
// ordered by updated_at ASC so the caller can advance the cursor to the last row.
// Scoped to the given tenant hierarchy.
func (db *DB) DevicesUpdatedSince(ctx context.Context,
	orgID, netID, grpID string,
	since time.Time, limit int) ([]FwdDevice, error) {

	rows, err := db.pool.Query(ctx, `
		SELECT `+fwdDeviceCols+`
		FROM devices
		WHERE org_id=$1
		  AND network_id=$2 AND group_id=$3
		  AND updated_at > $4
		ORDER BY updated_at ASC
		LIMIT $5`,
		orgID, netID, grpID, since, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FwdDevice
	for rows.Next() {
		d, err := scanFwdDevice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DevicesByIDs fetches full device rows for the given UUIDs. Used to hydrate
// devices that only appear in the metrics delta (no device row change).
func (db *DB) DevicesByIDs(ctx context.Context, ids []string) ([]FwdDevice, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := db.pool.Query(ctx, `
		SELECT `+fwdDeviceCols+`
		FROM devices
		WHERE id = ANY($1::uuid[])`,
		ids,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdDevice
	for rows.Next() {
		d, err := scanFwdDevice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DevicesByHostnames fetches device rows by hostname within a tenant. Used by
// the forwarder to pull topology-link endpoint devices that aren't already in
// the change delta, so each link's src+dst hostnames are present in the same
// payload (the Aggregator requires both endpoints in one payload).
func (db *DB) DevicesByHostnames(ctx context.Context,
	orgID, netID, grpID string, hostnames []string) ([]FwdDevice, error) {
	if len(hostnames) == 0 {
		return nil, nil
	}
	rows, err := db.pool.Query(ctx, `
		SELECT `+fwdDeviceCols+`
		FROM devices
		WHERE org_id=$1 AND network_id=$2 AND group_id=$3
		  AND hostname = ANY($4::text[])`,
		orgID, netID, grpID, hostnames,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdDevice
	for rows.Next() {
		d, err := scanFwdDevice(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ─── Interface queries ────────────────────────────────────────────────────────

// InterfacesByDeviceIDs fetches all interface rows for the given device UUIDs.
// Returns ALL interfaces (not just recently updated) so the Aggregator gets a
// complete snapshot of each device's port inventory.
func (db *DB) InterfacesByDeviceIDs(ctx context.Context, deviceIDs []string) ([]FwdInterface, error) {
	if len(deviceIDs) == 0 {
		return nil, nil
	}
	rows, err := db.pool.Query(ctx, `
		SELECT id, device_id, interface_name, interface_index,
		       COALESCE(interface_description,''), COALESCE(interface_type,''),
		       COALESCE(interface_mac_address::text,''),
		       speed_mbps, admin_status, operational_status,
		       access_vlan_id, mtu_bytes
		FROM interfaces
		WHERE device_id = ANY($1::uuid[])`,
		deviceIDs,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdInterface
	for rows.Next() {
		var i FwdInterface
		if err := rows.Scan(&i.ID, &i.DeviceID, &i.InterfaceName, &i.InterfaceIndex,
			&i.Description, &i.Type, &i.MACAddress,
			&i.SpeedMbps, &i.AdminStatus, &i.OperationalStatus,
			&i.AccessVlanID, &i.MTUBytes); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// AddressesByInterfaceIDs fetches all IP addresses for the given interface UUIDs.
func (db *DB) AddressesByInterfaceIDs(ctx context.Context, ifaceIDs []string) ([]FwdAddress, error) {
	if len(ifaceIDs) == 0 {
		return nil, nil
	}
	rows, err := db.pool.Query(ctx, `
		SELECT id, interface_id, address::text, address_family, is_primary, COALESCE(vrf,'')
		FROM interface_addresses
		WHERE interface_id = ANY($1::uuid[])`,
		ifaceIDs,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdAddress
	for rows.Next() {
		var a FwdAddress
		if err := rows.Scan(&a.ID, &a.InterfaceID, &a.Address, &a.AddressFamily, &a.IsPrimary, &a.VRF); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ─── Metrics query ────────────────────────────────────────────────────────────

// MetricsSince returns metric samples with ts > since, scoped to the tenant
// hierarchy. The JOIN to devices ensures we only pull metrics for devices in
// this org/datacenter. interface_name is resolved via the interface_id FK.
func (db *DB) MetricsSince(ctx context.Context,
	orgID, netID, grpID string,
	since time.Time, limit int) ([]FwdMetric, error) {

	rows, err := db.pool.Query(ctx, `
		SELECT m.device_id, COALESCE(m.interface_id::text, ''),
		       m.metric_name, m.value, m.tag,
		       COALESCE(m.attributes::text, '{}'),
		       m.collector_agent, m.collector_protocol,
		       COALESCE(i.interface_name, ''),
		       m.ts
		FROM metrics m
		JOIN devices d ON d.id = m.device_id
		LEFT JOIN interfaces i ON i.id = m.interface_id
		WHERE d.org_id=$1
		  AND d.network_id=$2 AND d.group_id=$3
		  AND m.ts > $4
		ORDER BY m.ts ASC
		LIMIT $5`,
		orgID, netID, grpID, since, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdMetric
	for rows.Next() {
		var m FwdMetric
		if err := rows.Scan(&m.DeviceID, &m.InterfaceID,
			&m.MetricName, &m.Value, &m.Tag,
			&m.Attributes, &m.CollectorAgent, &m.CollectorProtocol,
			&m.InterfaceName, &m.TS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// MetricsAt returns ALL metric samples at exactly ts for the tenant — used by
// the forwarder only when a single timestamp holds more rows than the paged
// limit (so the page can't be trimmed to a clean ts boundary without losing
// rows). A timestamp is the smallest cursor unit, so its whole group must ship
// together. No LIMIT: the group is one instant's data and is forwarded atomically.
func (db *DB) MetricsAt(ctx context.Context,
	orgID, netID, grpID string, ts time.Time) ([]FwdMetric, error) {

	rows, err := db.pool.Query(ctx, `
		SELECT m.device_id, COALESCE(m.interface_id::text, ''),
		       m.metric_name, m.value, m.tag,
		       COALESCE(m.attributes::text, '{}'),
		       m.collector_agent, m.collector_protocol,
		       COALESCE(i.interface_name, ''),
		       m.ts
		FROM metrics m
		JOIN devices d ON d.id = m.device_id
		LEFT JOIN interfaces i ON i.id = m.interface_id
		WHERE d.org_id=$1
		  AND d.network_id=$2 AND d.group_id=$3
		  AND m.ts = $4
		ORDER BY m.ts ASC`,
		orgID, netID, grpID, ts,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdMetric
	for rows.Next() {
		var m FwdMetric
		if err := rows.Scan(&m.DeviceID, &m.InterfaceID,
			&m.MetricName, &m.Value, &m.Tag,
			&m.Attributes, &m.CollectorAgent, &m.CollectorProtocol,
			&m.InterfaceName, &m.TS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// FwdEnergy is one BACnet energy reading forwarded to the Aggregator. circuit/
// phase are first-class so the Aggregator stores them in its own columns.
type FwdEnergy struct {
	DeviceID          string
	MetricName        string
	Value             float64
	Tag               string
	Circuit           string
	Phase             string
	Scope             string // it|cooling|facility — for PUE/DCiE
	Attributes        string // raw JSONB text ('{}' when none)
	CollectorAgent    string
	CollectorProtocol string
	TS                time.Time
}

// EnergySince returns energy_metrics rows with ts > since for the tenant.
func (db *DB) EnergySince(ctx context.Context,
	orgID, netID, grpID string,
	since time.Time, limit int) ([]FwdEnergy, error) {

	rows, err := db.pool.Query(ctx, `
		SELECT e.device_id, e.metric_name, e.value, e.tag, e.circuit, e.phase,
		       COALESCE(e.scope, ''),
		       COALESCE(e.attributes::text, '{}'),
		       e.collector_agent, e.collector_protocol, e.ts
		FROM energy_metrics e
		JOIN devices d ON d.id = e.device_id
		WHERE d.org_id=$1 AND d.network_id=$2 AND d.group_id=$3
		  AND e.ts > $4
		ORDER BY e.ts ASC
		LIMIT $5`,
		orgID, netID, grpID, since, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdEnergy
	for rows.Next() {
		var e FwdEnergy
		if err := rows.Scan(&e.DeviceID, &e.MetricName, &e.Value, &e.Tag,
			&e.Circuit, &e.Phase, &e.Scope, &e.Attributes,
			&e.CollectorAgent, &e.CollectorProtocol, &e.TS); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EnergyAt returns ALL energy_metrics rows at exactly ts for the tenant — the
// energy analogue of MetricsAt, used only for an oversized single-ts group.
func (db *DB) EnergyAt(ctx context.Context,
	orgID, netID, grpID string, ts time.Time) ([]FwdEnergy, error) {

	rows, err := db.pool.Query(ctx, `
		SELECT e.device_id, e.metric_name, e.value, e.tag, e.circuit, e.phase,
		       COALESCE(e.scope, ''),
		       COALESCE(e.attributes::text, '{}'),
		       e.collector_agent, e.collector_protocol, e.ts
		FROM energy_metrics e
		JOIN devices d ON d.id = e.device_id
		WHERE d.org_id=$1 AND d.network_id=$2 AND d.group_id=$3
		  AND e.ts = $4
		ORDER BY e.ts ASC`,
		orgID, netID, grpID, ts,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdEnergy
	for rows.Next() {
		var e FwdEnergy
		if err := rows.Scan(&e.DeviceID, &e.MetricName, &e.Value, &e.Tag,
			&e.Circuit, &e.Phase, &e.Scope, &e.Attributes,
			&e.CollectorAgent, &e.CollectorProtocol, &e.TS); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ─── Topology query ───────────────────────────────────────────────────────────

// TopologyLinksSince returns topology edges with updated_at > since, joining
// devices to resolve src/dst UUIDs to hostnames for the Aggregator payload.
func (db *DB) TopologyLinksSince(ctx context.Context,
	orgID, netID, grpID string,
	since time.Time, limit int) ([]FwdTopologyLink, error) {

	rows, err := db.pool.Query(ctx, `
		SELECT tl.id, sd.datacenter_id, sd.floor_id,
		       tl.layer,
		       tl.src_device_id, COALESCE(tl.src_interface_id::text,''),
		       sd.hostname, tl.src_port_name,
		       tl.dst_device_id, COALESCE(tl.dst_interface_id::text,''),
		       dd.hostname, tl.dst_port_name,
		       tl.link_speed_mbps, COALESCE(tl.link_type,''), tl.protocol, tl.relation, tl.is_active, tl.updated_at
		FROM topology_links tl
		JOIN devices sd ON sd.id = tl.src_device_id
		JOIN devices dd ON dd.id = tl.dst_device_id
		WHERE sd.org_id=$1
		  AND sd.network_id=$2 AND sd.group_id=$3
		  AND tl.updated_at > $4
		ORDER BY tl.updated_at ASC
		LIMIT $5`,
		orgID, netID, grpID, since, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdTopologyLink
	for rows.Next() {
		var tl FwdTopologyLink
		if err := rows.Scan(&tl.ID, &tl.SrcDatacenterID, &tl.SrcFloorID,
			&tl.Layer,
			&tl.SrcDeviceID, &tl.SrcInterfaceID, &tl.SrcHostname, &tl.SrcPortName,
			&tl.DstDeviceID, &tl.DstInterfaceID, &tl.DstHostname, &tl.DstPortName,
			&tl.LinkSpeedMbps, &tl.LinkType, &tl.Protocol, &tl.Relation, &tl.IsActive, &tl.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, tl)
	}
	return out, rows.Err()
}

// ─── Events query ─────────────────────────────────────────────────────────────

// EventsSince returns events with ts > since, resolved to a forwardable device.
// A trap often arrives with device_id IS NULL (it fired before the device was
// discovered, or carried no resolvable agent address). The forwarder groups
// every payload by (datacenter_id, floor_id), so an event with no owning device
// has no scope and was being silently dropped. Here we recover the owning device
// — and therefore the scope — for NULL-device events by matching, in order:
//  1. device_id  (already set)
//  2. source_hostname → devices.hostname  (trap names the device)
//  3. source_ip       → any of the device's IP columns
//
// Events that still resolve to no device are omitted: without a scope they can
// never be placed in a per-scope payload anyway.
func (db *DB) EventsSince(ctx context.Context,
	orgID, netID, grpID string,
	since time.Time, limit int) ([]FwdEvent, error) {

	rows, err := db.pool.Query(ctx, `
		SELECT e.id,
		       COALESCE(e.device_id::text, dh.id::text, dip.id::text, ''),
		       COALESCE(d.datacenter_id, dh.datacenter_id, dip.datacenter_id, ''),
		       COALESCE(d.floor_id, dh.floor_id, dip.floor_id, ''),
		       COALESCE(e.source_hostname, d.hostname, dh.hostname, dip.hostname, ''),
		       e.kind, e.event_name, e.severity,
		       COALESCE(e.trap_oid, ''),
		       COALESCE(e.source_ip::text, ''),
		       e.collector_agent,
		       COALESCE(e.src_port_name, ''),
		       COALESCE(e.dst_device_id::text, ''),
		       COALESCE(e.dst_hostname, ''),
		       COALESCE(e.dst_port_name, ''),
		       COALESCE(e.link_id::text, ''),
		       e.ts,
		       COALESCE(e.event_payload::text, '{}')
		FROM events e
		LEFT JOIN devices d ON d.id = e.device_id
		-- resolve by trap-reported hostname when device_id is unset
		LEFT JOIN LATERAL (
		    SELECT x.id, x.datacenter_id, x.floor_id, x.hostname
		    FROM devices x
		    WHERE e.device_id IS NULL
		      AND e.source_hostname IS NOT NULL AND e.source_hostname <> ''
		      AND x.hostname = e.source_hostname
		      AND x.org_id=$1 AND x.network_id=$2 AND x.group_id=$3
		    LIMIT 1
		) dh ON TRUE
		-- else resolve by source IP against any of the device's IP columns
		LEFT JOIN LATERAL (
		    SELECT x.id, x.datacenter_id, x.floor_id, x.hostname
		    FROM devices x
		    WHERE e.device_id IS NULL AND dh.id IS NULL
		      AND e.source_ip IS NOT NULL
		      AND x.org_id=$1 AND x.network_id=$2 AND x.group_id=$3
		      AND e.source_ip IN (x.mgmt_ip, x.prod_ip, x.oob_ip, x.loopback_ip)
		    LIMIT 1
		) dip ON TRUE
		WHERE e.ts > $4
		  AND (
		      (d.org_id=$1 AND d.network_id=$2 AND d.group_id=$3)
		      OR dh.id IS NOT NULL
		      OR dip.id IS NOT NULL
		  )
		ORDER BY e.ts ASC
		LIMIT $5`,
		orgID, netID, grpID, since, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FwdEvent
	for rows.Next() {
		var ev FwdEvent
		if err := rows.Scan(&ev.ID, &ev.DeviceID,
			&ev.DatacenterID, &ev.FloorID,
			&ev.Hostname, &ev.Kind, &ev.EventName, &ev.Severity,
			&ev.TrapOID, &ev.SourceIP, &ev.CollectorAgent,
			&ev.SrcPortName, &ev.DstDeviceID, &ev.DstHostname, &ev.DstPortName, &ev.LinkID,
			&ev.TS, &ev.Payload); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// ─── Cursor management ────────────────────────────────────────────────────────

// GetForwarderCursor returns the last successfully pushed timestamp for the
// named cursor. Returns the zero time.Time when no cursor exists yet (causes
// the forwarder to pull all historical rows on first run).
func (db *DB) GetForwarderCursor(ctx context.Context, name, netID string) (time.Time, error) {
	var t time.Time
	err := db.pool.QueryRow(ctx, `
		SELECT cursor FROM forwarder_cursors WHERE name=$1 AND network_id=$2`, name, netID,
	).Scan(&t)
	if err != nil {
		// pgx returns pgx.ErrNoRows when there's no matching row — treat as
		// "never pushed before" and return zero time so the forwarder pulls
		// everything from the beginning.
		return time.Time{}, nil //nolint:nilerr
	}
	return t, nil
}

// SetForwarderCursor upserts the cursor for the given (name, network_id). Cursors
// are per-network so one network never advances another network's high-water mark.
func (db *DB) SetForwarderCursor(ctx context.Context, name, netID string, t time.Time) error {
	_, err := db.pool.Exec(ctx, `
		INSERT INTO forwarder_cursors (name, network_id, cursor, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (name, network_id) DO UPDATE
		  SET cursor     = EXCLUDED.cursor,
		      updated_at = now()`,
		name, netID, t,
	)
	return err
}

// DistinctNetworks returns every network_id present in the org's devices, so the
// topology runner and forwarder can operate once per network (devices carry a
// per-country network_id). Empty when no devices exist yet.
func (db *DB) DistinctNetworks(ctx context.Context, orgID string) ([]string, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT DISTINCT network_id FROM devices WHERE org_id=$1 ORDER BY network_id`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nets []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		nets = append(nets, n)
	}
	return nets, rows.Err()
}
