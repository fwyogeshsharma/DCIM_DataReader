// Package forwarder implements the DCS→Aggregator incremental push loop.
//
// Design:
//   - Runs in a background goroutine; wakes on a configurable interval.
//   - Four independent cursors (devices, metrics, topology, events) stored in
//     the forwarder_cursors table so a DCS restart resumes exactly where it
//     left off without replaying data.
//   - Each push cycle:
//     1. Load cursors from DB.
//     2. Query each table for rows newer than the cursor (ORDER BY ts/updated_at ASC, LIMIT N).
//     3. Build a single Aggregator JSON payload: devices nested with their
//     interfaces, addresses, and recent metrics; topology_links resolved
//     to hostnames; events with source hostname.
//     4. POST to the Aggregator endpoint with the required headers.
//     5. On success advance each cursor to the latest timestamp in that batch.
//     On HTTP 5xx or network error retry up to 3 times with backoff before
//     giving up (the next tick will retry the same batch).
//   - If all four tables return zero rows, the push is skipped to avoid empty
//     requests.
package forwarder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwdcs/internal/store"
	"github.com/faberwork/fwdcs/pkg/config"
)

// ─── Aggregator payload structs ───────────────────────────────────────────────

type aggPayload struct {
	OrgID         string        `json:"org_id"`
	DatacenterID  string        `json:"datacenter_id"`
	FloorID       string        `json:"floor_id"`
	NetworkID     string        `json:"network_id"`
	GroupID       string        `json:"group_id"`
	Devices       []aggDevice   `json:"devices"`
	TopologyLinks []aggTopoLink `json:"topology_links"`
	Events        []aggEvent    `json:"events"`
}

type aggDevice struct {
	ID             string            `json:"id"`
	Hostname       string            `json:"hostname"`
	DeviceType     string            `json:"device_type"`
	Vendor         string            `json:"vendor,omitempty"`
	ModelName      string            `json:"model_name,omitempty"`
	OSName         string            `json:"os_name,omitempty"`
	OSVersion      string            `json:"os_version,omitempty"`
	SysOID         string            `json:"sys_oid,omitempty"`
	SysDescription string            `json:"sys_description,omitempty"`
	SysLocation    string            `json:"sys_location,omitempty"`
	MgmtIP         string            `json:"mgmt_ip,omitempty"`
	ProdIP         string            `json:"prod_ip,omitempty"`
	LoopbackIP     string            `json:"loopback_ip,omitempty"`
	OOBIP          string            `json:"oob_ip,omitempty"`
	SNMPEnabled    bool              `json:"snmp_enabled"`
	GNMIEnabled    bool              `json:"gnmi_enabled"`
	SNMPPort       int               `json:"snmp_port,omitempty"`
	SNMPVersion    int               `json:"snmp_version,omitempty"`
	GNMIPort       int               `json:"gnmi_port,omitempty"`
	CollectorAgent string            `json:"collector_agent,omitempty"`
	IsReachable    bool              `json:"is_reachable"`
	Country        string            `json:"country,omitempty"`
	DatacenterCity string            `json:"datacenter_city,omitempty"`
	Datacenter     string            `json:"datacenter,omitempty"`
	Room           string            `json:"room,omitempty"`
	RackRow        *int              `json:"rack_row,omitempty"`
	RackNum        *int              `json:"rack_num,omitempty"`
	RackUnit       *int              `json:"rack_unit,omitempty"`
	PowerDrawW     *int              `json:"power_draw_w,omitempty"`
	DeviceRole     string            `json:"device_role,omitempty"`
	RoleConfidence *float64          `json:"role_confidence,omitempty"`
	RoleSource     string            `json:"role_source,omitempty"`
	Interfaces     []aggInterface    `json:"interfaces,omitempty"`
	Metrics        []aggMetric       `json:"metrics,omitempty"`
	EnergyMetrics  []aggEnergyMetric `json:"energy_metrics,omitempty"`
}

type aggInterface struct {
	ID                string       `json:"id"`
	DeviceID          string       `json:"device_id"`
	InterfaceName     string       `json:"interface_name"`
	InterfaceIndex    *int         `json:"interface_index,omitempty"`
	Description       string       `json:"interface_description,omitempty"`
	Type              string       `json:"interface_type,omitempty"`
	MACAddress        string       `json:"interface_mac_address,omitempty"`
	SpeedMbps         *int         `json:"speed_mbps,omitempty"`
	AdminStatus       int          `json:"admin_status"`
	OperationalStatus int          `json:"operational_status"`
	AccessVlanID      *int         `json:"access_vlan_id,omitempty"`
	MTUBytes          *int         `json:"mtu_bytes,omitempty"`
	Addresses         []aggAddress `json:"addresses,omitempty"`
}

type aggAddress struct {
	ID            string `json:"id"`
	InterfaceID   string `json:"interface_id"`
	Address       string `json:"address"`
	AddressFamily string `json:"address_family,omitempty"`
	IsPrimary     bool   `json:"is_primary"`
	VRF           string `json:"vrf,omitempty"`
}

type aggMetric struct {
	DeviceID          string          `json:"device_id"`
	InterfaceID       string          `json:"interface_id,omitempty"`
	MetricName        string          `json:"metric_name"`
	Value             float64         `json:"value"`
	Tag               string          `json:"tag,omitempty"`
	Attributes        json.RawMessage `json:"attributes,omitempty"`
	CollectorAgent    string          `json:"collector_agent,omitempty"`
	CollectorProtocol string          `json:"collector_protocol,omitempty"`
	InterfaceName     string          `json:"interface_name,omitempty"`
	TS                string          `json:"ts,omitempty"` // RFC3339
}

type aggEnergyMetric struct {
	DeviceID          string          `json:"device_id"`
	MetricName        string          `json:"metric_name"`
	Value             float64         `json:"value"`
	Tag               string          `json:"tag,omitempty"`
	Circuit           string          `json:"circuit,omitempty"`
	Phase             string          `json:"phase,omitempty"`
	Scope             string          `json:"scope,omitempty"` // it|cooling|facility — PUE/DCiE
	Attributes        json.RawMessage `json:"attributes,omitempty"`
	CollectorAgent    string          `json:"collector_agent,omitempty"`
	CollectorProtocol string          `json:"collector_protocol,omitempty"`
	TS                string          `json:"ts,omitempty"` // RFC3339
}

type aggTopoLink struct {
	ID             string `json:"id"`
	Layer          string `json:"layer"`
	SrcDeviceID    string `json:"src_device_id"`
	SrcInterfaceID string `json:"src_interface_id,omitempty"`
	SrcHostname    string `json:"src_hostname"`
	SrcPortName    string `json:"src_port_name,omitempty"`
	DstDeviceID    string `json:"dst_device_id"`
	DstInterfaceID string `json:"dst_interface_id,omitempty"`
	DstHostname    string `json:"dst_hostname"`
	DstPortName    string `json:"dst_port_name,omitempty"`
	LinkSpeedMbps  *int   `json:"link_speed_mbps,omitempty"`
	LinkType       string `json:"link_type,omitempty"`
	Protocol       string `json:"protocol,omitempty"`
	Relation       string `json:"relation,omitempty"` // uplink (dst=parent) | downlink (dst=child) | peer
	IsActive       bool   `json:"is_active"`
}

type aggEvent struct {
	ID             string          `json:"id"`
	DeviceID       string          `json:"device_id,omitempty"` // source device
	Hostname       string          `json:"hostname"`            // source hostname
	Kind           string          `json:"kind"`
	EventName      string          `json:"event_name"`
	Severity       string          `json:"severity"`
	TrapOID        string          `json:"trap_oid,omitempty"`
	SourceIP       string          `json:"source_ip,omitempty"`
	CollectorAgent string          `json:"collector_agent,omitempty"`
	SrcPortName    string          `json:"src_port_name,omitempty"` // link events: sender port
	DstDeviceID    string          `json:"dst_device_id,omitempty"` // link events: peer device
	DstHostname    string          `json:"dst_hostname,omitempty"`  // link events: peer hostname
	DstPortName    string          `json:"dst_port_name,omitempty"` // link events: peer port
	LinkID         string          `json:"link_id,omitempty"`       // link events: topology_links id
	TS             string          `json:"ts"`                      // RFC3339
	Payload        json.RawMessage `json:"payload,omitempty"`
}

// aggResponse is the Aggregator's success envelope.
type aggResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message,omitempty"`
	Ingested struct {
		Devices       int `json:"devices"`
		TopologyLinks int `json:"topology_links"`
		Events        int `json:"events"`
	} `json:"ingested"`
}

// ─── Forwarder ────────────────────────────────────────────────────────────────

// Forwarder pushes new data from DCS to the Aggregator on a configurable tick.
type Forwarder struct {
	db     *store.DB
	cfg    config.AggregatorConfig
	log    *zap.Logger
	client *http.Client
}

// New constructs a Forwarder. Call Run in a goroutine to start it.
func New(db *store.DB, cfg config.AggregatorConfig, log *zap.Logger) *Forwarder {
	return &Forwarder{
		db:  db,
		cfg: cfg,
		log: log,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Run loops until ctx is cancelled, pushing incremental batches to the
// Aggregator on each tick.
func (f *Forwarder) Run(ctx context.Context) {
	interval := time.Duration(f.cfg.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 5 * time.Second
	}

	// Short initial delay — let the ingest pipeline populate the DB before
	// the first push so we don't send an empty payload on startup.
	select {
	case <-time.After(15 * time.Second):
	case <-ctx.Done():
		return
	}

	f.log.Info("aggregator forwarder started",
		zap.String("endpoint", f.cfg.Endpoint),
		zap.Duration("interval", interval))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := f.push(ctx); err != nil {
				f.log.Warn("aggregator forwarder push failed", zap.Error(err))
			}
		}
	}
}

// push executes one full push cycle: query → build payload → POST → advance cursors.
func (f *Forwarder) push(ctx context.Context) error {
	batchLimit := f.cfg.BatchLimit
	if batchLimit <= 0 {
		batchLimit = 1000
	}
	metricLimit := batchLimit * 50 // metrics volume >> device volume

	// ── 1. Load cursors ──────────────────────────────────────────────────────
	devCursor, _ := f.db.GetForwarderCursor(ctx, "devices")
	metricsCursor, _ := f.db.GetForwarderCursor(ctx, "metrics")
	energyCursor, _ := f.db.GetForwarderCursor(ctx, "energy")
	topoCursor, _ := f.db.GetForwarderCursor(ctx, "topology")
	eventsCursor, _ := f.db.GetForwarderCursor(ctx, "events")

	// ── 2. Query data ─────────────────────────────────────────────────────────
	changedDevices, err := f.db.DevicesUpdatedSince(ctx,
		f.cfg.OrgID, f.cfg.NetworkID, f.cfg.GroupID,
		devCursor, batchLimit)
	if err != nil {
		return fmt.Errorf("devices query: %w", err)
	}

	recentMetrics, err := f.db.MetricsSince(ctx,
		f.cfg.OrgID, f.cfg.NetworkID, f.cfg.GroupID,
		metricsCursor, metricLimit)
	if err != nil {
		return fmt.Errorf("metrics query: %w", err)
	}

	recentEnergy, err := f.db.EnergySince(ctx,
		f.cfg.OrgID, f.cfg.NetworkID, f.cfg.GroupID,
		energyCursor, metricLimit)
	if err != nil {
		return fmt.Errorf("energy query: %w", err)
	}

	topoLinks, err := f.db.TopologyLinksSince(ctx,
		f.cfg.OrgID, f.cfg.NetworkID, f.cfg.GroupID,
		topoCursor, batchLimit)
	if err != nil {
		return fmt.Errorf("topology query: %w", err)
	}

	events, err := f.db.EventsSince(ctx,
		f.cfg.OrgID, f.cfg.NetworkID, f.cfg.GroupID,
		eventsCursor, batchLimit)
	if err != nil {
		return fmt.Errorf("events query: %w", err)
	}

	// ── 3. Group metrics by device_id; hydrate extra devices ─────────────────
	metricsByDevice := make(map[string][]store.FwdMetric, len(recentMetrics))
	for _, m := range recentMetrics {
		metricsByDevice[m.DeviceID] = append(metricsByDevice[m.DeviceID], m)
	}
	energyByDevice := make(map[string][]store.FwdEnergy, len(recentEnergy))
	for _, e := range recentEnergy {
		energyByDevice[e.DeviceID] = append(energyByDevice[e.DeviceID], e)
	}

	// Devices that only appear in the metrics/energy delta (device row not
	// updated) need to be fetched separately so we can include them in the payload.
	changedIDs := make(map[string]bool, len(changedDevices))
	for _, d := range changedDevices {
		changedIDs[d.ID] = true
	}
	extraSeen := make(map[string]bool)
	var extraIDs []string
	addExtra := func(devID string) {
		if !changedIDs[devID] && !extraSeen[devID] {
			extraSeen[devID] = true
			extraIDs = append(extraIDs, devID)
		}
	}
	for devID := range metricsByDevice {
		addExtra(devID)
	}
	for devID := range energyByDevice {
		addExtra(devID)
	}
	extraDevices, err := f.db.DevicesByIDs(ctx, extraIDs)
	if err != nil {
		return fmt.Errorf("extra devices query: %w", err)
	}

	allDevices := append(changedDevices, extraDevices...)

	// Skip entirely when nothing changed.
	if len(allDevices) == 0 && len(topoLinks) == 0 && len(events) == 0 {
		return nil
	}

	// ── 3b. Hydrate topology-link endpoint devices ───────────────────────────
	// The Aggregator drops any topology link whose src/dst hostname is not also
	// present as a device in the SAME payload. At steady state the device delta
	// is empty while topology links keep flowing, so without this the links are
	// silently rejected (the "topology in DCS but 0 at Aggregator" symptom).
	// Pull every link endpoint device (idempotent upsert at the Aggregator) so
	// each same-scope link travels with both its devices.
	seen := make(map[string]bool, len(allDevices))
	for _, d := range allDevices {
		seen[d.ID] = true
	}
	if len(topoLinks) > 0 {
		hostSet := make(map[string]struct{}, len(topoLinks)*2)
		for _, tl := range topoLinks {
			if tl.SrcHostname != "" {
				hostSet[tl.SrcHostname] = struct{}{}
			}
			if tl.DstHostname != "" {
				hostSet[tl.DstHostname] = struct{}{}
			}
		}
		hostnames := make([]string, 0, len(hostSet))
		for h := range hostSet {
			hostnames = append(hostnames, h)
		}
		linkDevices, err := f.db.DevicesByHostnames(ctx,
			f.cfg.OrgID, f.cfg.NetworkID, f.cfg.GroupID, hostnames)
		if err != nil {
			return fmt.Errorf("link-endpoint devices query: %w", err)
		}
		for _, d := range linkDevices {
			if !seen[d.ID] {
				seen[d.ID] = true
				allDevices = append(allDevices, d)
			}
		}
	}

	// ── 4. Fetch interfaces + addresses for all devices in scope ─────────────
	allDeviceIDs := make([]string, 0, len(allDevices))
	for _, d := range allDevices {
		allDeviceIDs = append(allDeviceIDs, d.ID)
	}

	ifaces, err := f.db.InterfacesByDeviceIDs(ctx, allDeviceIDs)
	if err != nil {
		return fmt.Errorf("interfaces query: %w", err)
	}

	ifacesByDevice := make(map[string][]store.FwdInterface, len(allDevices))
	ifaceIDs := make([]string, 0, len(ifaces))
	for _, i := range ifaces {
		ifacesByDevice[i.DeviceID] = append(ifacesByDevice[i.DeviceID], i)
		ifaceIDs = append(ifaceIDs, i.ID)
	}

	addrs, err := f.db.AddressesByInterfaceIDs(ctx, ifaceIDs)
	if err != nil {
		return fmt.Errorf("addresses query: %w", err)
	}
	addrsByIface := make(map[string][]store.FwdAddress, len(ifaceIDs))
	for _, a := range addrs {
		addrsByIface[a.InterfaceID] = append(addrsByIface[a.InterfaceID], a)
	}

	// ── 5. Build per-scope payloads ───────────────────────────────────────────
	// The Aggregator requires one (datacenter_id, floor_id) scope per payload
	// and upserts devices by org+datacenter+floor+network+group+hostname. Our
	// devices carry per-device dc/floor, so we group everything by that scope
	// and emit one payload per group.
	payloads := f.buildPayloads(
		allDevices, ifacesByDevice, addrsByIface,
		metricsByDevice, energyByDevice, topoLinks, events,
	)
	if len(payloads) == 0 {
		return nil
	}

	// ── 6. POST each scope in parallel. Scopes are disjoint (different
	// datacenter/floor → different devices), so concurrent ingests don't conflict.
	// Each scope payload is split into size-bounded chunks (chunkPayload) so no
	// single POST body exceeds the Aggregator's request-size limit (the cause of
	// the repeated HTTP 413 stall: a 4xx is non-retryable, so cursors never
	// advanced and every tick re-sent the same oversized batch). Chunks within a
	// scope are POSTed SEQUENTIALLY — all device chunks first, then the trailing
	// topology+events chunk — so link endpoints are already committed and the
	// Aggregator resolves them via its hostname DB lookup. If ANY chunk fails we
	// record the error and advance no cursors, so the next tick retries the whole
	// batch — upserts are idempotent. ─────────────────────────────────────────
	var (
		mu       sync.Mutex
		firstErr error
		totDev   int
		totTopo  int
		totEv    int
		wg       sync.WaitGroup
	)
	for _, payload := range payloads {
		chunks := f.chunkPayload(payload)
		if len(chunks) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, chunk := range chunks {
				resp, err := f.postWithRetry(ctx, chunk)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return // stop this scope; cursors won't advance, batch retries next tick
				}
				mu.Lock()
				totDev += resp.Ingested.Devices
				totTopo += resp.Ingested.TopologyLinks
				totEv += resp.Ingested.Events
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	f.log.Info("aggregator forwarder push ok",
		zap.Int("scopes", len(payloads)),
		zap.Int("devices", totDev),
		zap.Int("topology_links", totTopo),
		zap.Int("events", totEv))

	// ── 7. Advance cursors (only when the batch was non-empty) ────────────────
	if len(changedDevices) > 0 {
		_ = f.db.SetForwarderCursor(ctx, "devices", changedDevices[len(changedDevices)-1].UpdatedAt)
	}
	if len(recentMetrics) > 0 {
		_ = f.db.SetForwarderCursor(ctx, "metrics", recentMetrics[len(recentMetrics)-1].TS)
	}
	if len(recentEnergy) > 0 {
		_ = f.db.SetForwarderCursor(ctx, "energy", recentEnergy[len(recentEnergy)-1].TS)
	}
	if len(topoLinks) > 0 {
		_ = f.db.SetForwarderCursor(ctx, "topology", topoLinks[len(topoLinks)-1].UpdatedAt)
	}
	if len(events) > 0 {
		_ = f.db.SetForwarderCursor(ctx, "events", events[len(events)-1].TS)
	}

	return nil
}

// scopeKey identifies one (datacenter_id, floor_id) Aggregator payload scope.
type scopeKey struct{ dc, floor string }

// scopeFallback is the last-resort value for an empty datacenter_id/floor_id
// when no default is configured. The Aggregator requires both keys non-empty,
// so coalescing guarantees every row is forwarded rather than wedging the push.
const scopeFallback = "unknown"

// dcOrDefault / floorOrDefault coalesce an empty scope key to the configured
// default (falling back to scopeFallback) so no device or event is ever shipped
// with an empty datacenter_id/floor_id.
func (f *Forwarder) dcOrDefault(dc string) string {
	if dc != "" {
		return dc
	}
	if f.cfg.DefaultDatacenterID != "" {
		return f.cfg.DefaultDatacenterID
	}
	return scopeFallback
}

func (f *Forwarder) floorOrDefault(floor string) string {
	if floor != "" {
		return floor
	}
	if f.cfg.DefaultFloorID != "" {
		return f.cfg.DefaultFloorID
	}
	return scopeFallback
}

// buildPayloads groups all query results by (datacenter_id, floor_id) and
// returns one aggPayload per scope. The Aggregator upserts devices by
// org+datacenter+floor+network+group+hostname and requires topology link
// endpoints to share the payload, so each scope is shipped independently.
func (f *Forwarder) buildPayloads(
	devices []store.FwdDevice,
	ifacesByDevice map[string][]store.FwdInterface,
	addrsByIface map[string][]store.FwdAddress,
	metricsByDevice map[string][]store.FwdMetric,
	energyByDevice map[string][]store.FwdEnergy,
	topoLinks []store.FwdTopologyLink,
	events []store.FwdEvent,
) []*aggPayload {

	groups := make(map[scopeKey]*aggPayload)
	// hostname → scope, so topology links can be placed only when both endpoints
	// resolve to the same (dc,floor) payload.
	hostScope := make(map[string]scopeKey, len(devices))
	for _, d := range devices {
		hostScope[d.Hostname] = scopeKey{f.dcOrDefault(d.DatacenterID), f.floorOrDefault(d.FloorID)}
	}
	getGroup := func(dc, floor string) *aggPayload {
		k := scopeKey{dc, floor}
		p := groups[k]
		if p == nil {
			p = &aggPayload{
				OrgID:         f.cfg.OrgID,
				DatacenterID:  dc,
				FloorID:       floor,
				NetworkID:     f.cfg.NetworkID,
				GroupID:       f.cfg.GroupID,
				Devices:       make([]aggDevice, 0),
				TopologyLinks: make([]aggTopoLink, 0),
				Events:        make([]aggEvent, 0),
			}
			groups[k] = p
		}
		return p
	}

	// Devices (with nested interfaces, addresses, metrics) → scope by dc/floor.
	for _, d := range devices {
		ad := aggDevice{
			ID:             d.ID,
			Hostname:       d.Hostname,
			DeviceType:     d.DeviceType,
			Vendor:         d.Vendor,
			ModelName:      d.ModelName,
			OSName:         d.OSName,
			OSVersion:      d.OSVersion,
			SysOID:         d.SysOID,
			SysDescription: d.SysDescr,
			SysLocation:    d.SysLocation,
			MgmtIP:         d.MgmtIP,
			ProdIP:         d.ProdIP,
			LoopbackIP:     d.LoopbackIP,
			OOBIP:          d.OOBIP,
			SNMPEnabled:    d.SNMPEnabled,
			GNMIEnabled:    d.GNMIEnabled,
			SNMPPort:       d.SNMPPort,
			SNMPVersion:    d.SNMPVersion,
			GNMIPort:       d.GNMIPort,
			CollectorAgent: d.CollectorAgent,
			IsReachable:    d.IsReachable,
			Country:        d.Country,
			DatacenterCity: d.DatacenterCity,
			Datacenter:     d.Datacenter,
			Room:           d.Room,
			RackRow:        d.RackRow,
			RackNum:        d.RackNum,
			RackUnit:       d.RackUnit,
			PowerDrawW:     d.PowerDrawW,
			DeviceRole:     d.DeviceRole,
			RoleConfidence: d.RoleConfidence,
			RoleSource:     d.RoleSource,
			Interfaces:     make([]aggInterface, 0),
			Metrics:        make([]aggMetric, 0),
			EnergyMetrics:  make([]aggEnergyMetric, 0),
		}
		for _, iface := range ifacesByDevice[d.ID] {
			ai := aggInterface{
				ID:                iface.ID,
				DeviceID:          iface.DeviceID,
				InterfaceName:     iface.InterfaceName,
				InterfaceIndex:    iface.InterfaceIndex,
				Description:       iface.Description,
				Type:              iface.Type,
				MACAddress:        iface.MACAddress,
				SpeedMbps:         iface.SpeedMbps,
				AdminStatus:       iface.AdminStatus,
				OperationalStatus: iface.OperationalStatus,
				AccessVlanID:      iface.AccessVlanID,
				MTUBytes:          iface.MTUBytes,
				Addresses:         make([]aggAddress, 0),
			}
			for _, addr := range addrsByIface[iface.ID] {
				ai.Addresses = append(ai.Addresses, aggAddress{
					ID:            addr.ID,
					InterfaceID:   addr.InterfaceID,
					Address:       addr.Address,
					AddressFamily: addr.AddressFamily,
					IsPrimary:     addr.IsPrimary,
					VRF:           addr.VRF,
				})
			}
			ad.Interfaces = append(ad.Interfaces, ai)
		}
		for _, m := range metricsByDevice[d.ID] {
			am := aggMetric{
				DeviceID:          m.DeviceID,
				InterfaceID:       m.InterfaceID,
				MetricName:        m.MetricName,
				Value:             m.Value,
				Tag:               m.Tag,
				CollectorAgent:    m.CollectorAgent,
				CollectorProtocol: m.CollectorProtocol,
				InterfaceName:     m.InterfaceName,
				TS:                m.TS.UTC().Format(time.RFC3339),
			}
			if m.Attributes != "" && m.Attributes != "{}" {
				am.Attributes = json.RawMessage(m.Attributes)
			}
			ad.Metrics = append(ad.Metrics, am)
		}
		for _, e := range energyByDevice[d.ID] {
			ae := aggEnergyMetric{
				DeviceID:          e.DeviceID,
				MetricName:        e.MetricName,
				Value:             e.Value,
				Tag:               e.Tag,
				Circuit:           e.Circuit,
				Phase:             e.Phase,
				Scope:             e.Scope,
				CollectorAgent:    e.CollectorAgent,
				CollectorProtocol: e.CollectorProtocol,
				TS:                e.TS.UTC().Format(time.RFC3339),
			}
			if e.Attributes != "" && e.Attributes != "{}" {
				ae.Attributes = json.RawMessage(e.Attributes)
			}
			ad.EnergyMetrics = append(ad.EnergyMetrics, ae)
		}
		g := getGroup(f.dcOrDefault(d.DatacenterID), f.floorOrDefault(d.FloorID))
		g.Devices = append(g.Devices, ad)
	}

	// Topology links → only when BOTH endpoints resolve to the SAME scope and
	// are present in this payload. The Aggregator requires both hostnames in one
	// (dc,floor) payload and drops links otherwise, so cross-scope links
	// (endpoints on different floors) cannot be represented and are skipped.
	skippedCrossScope := 0
	for _, tl := range topoLinks {
		srcScope, okS := hostScope[tl.SrcHostname]
		dstScope, okD := hostScope[tl.DstHostname]
		if !okS || !okD || srcScope != dstScope {
			skippedCrossScope++
			continue
		}
		g := groups[srcScope] // exists: src device was added to it above
		if g == nil {
			skippedCrossScope++
			continue
		}
		g.TopologyLinks = append(g.TopologyLinks, aggTopoLink{
			ID:             tl.ID,
			Layer:          tl.Layer,
			SrcDeviceID:    tl.SrcDeviceID,
			SrcInterfaceID: tl.SrcInterfaceID,
			SrcHostname:    tl.SrcHostname,
			SrcPortName:    tl.SrcPortName,
			DstDeviceID:    tl.DstDeviceID,
			DstInterfaceID: tl.DstInterfaceID,
			DstHostname:    tl.DstHostname,
			DstPortName:    tl.DstPortName,
			LinkSpeedMbps:  tl.LinkSpeedMbps,
			LinkType:       tl.LinkType,
			Protocol:       tl.Protocol,
			Relation:       tl.Relation,
			IsActive:       tl.IsActive,
		})
	}
	if skippedCrossScope > 0 {
		f.log.Debug("forwarder: skipped cross-scope topology links (endpoints in different dc/floor)",
			zap.Int("skipped", skippedCrossScope))
	}

	// Events → scope by device's dc/floor. Empty dc/floor (e.g. device_id NULL
	// or a device with no scope) coalesces to the configured default so the
	// event is still forwarded rather than dropped.
	for _, ev := range events {
		ae := aggEvent{
			ID:             ev.ID,
			DeviceID:       ev.DeviceID,
			Hostname:       ev.Hostname,
			Kind:           ev.Kind,
			EventName:      ev.EventName,
			Severity:       ev.Severity,
			TrapOID:        ev.TrapOID,
			SourceIP:       ev.SourceIP,
			CollectorAgent: ev.CollectorAgent,
			SrcPortName:    ev.SrcPortName,
			DstDeviceID:    ev.DstDeviceID,
			DstHostname:    ev.DstHostname,
			DstPortName:    ev.DstPortName,
			LinkID:         ev.LinkID,
			TS:             ev.TS.UTC().Format(time.RFC3339),
		}
		if ev.Payload != "" && ev.Payload != "{}" {
			ae.Payload = json.RawMessage(ev.Payload)
		}
		g := getGroup(f.dcOrDefault(ev.DatacenterID), f.floorOrDefault(ev.FloorID))
		g.Events = append(g.Events, ae)
	}

	out := make([]*aggPayload, 0, len(groups))
	for _, p := range groups {
		out = append(out, p)
	}
	return out
}

// maxPostBytes bounds each POST body by MEASURED serialized JSON size. Set well
// under 1MB because the Aggregator sits behind a proxy/ingress whose body limit
// (~1MB) is far below express's 10MB — count-based bounds (N devices) can't
// guarantee byte size, so a 50-device chunk still tripped HTTP 413. 512KB clears
// a 1MB proxy with wide margin and is still a large batch.
const maxPostBytes = 512 * 1024

// chunkPayload splits one scope payload into byte-bounded sub-payloads, each
// carrying the same scope identifiers. Devices (with their nested interfaces,
// addresses, and metrics) are packed by MEASURED serialized size until adding
// the next device would exceed maxPostBytes — but a single device always fits in
// one chunk even if it alone exceeds the budget (a device can't be split; it
// ships alone and is logged). Topology links and events ride in trailing
// chunk(s) emitted AFTER all device chunks, so by the time the Aggregator
// ingests links every endpoint device is already committed and resolvable by
// hostname; events fall back to their forwarded device_id.
func (f *Forwarder) chunkPayload(p *aggPayload) []*aggPayload {
	base := func() *aggPayload {
		return &aggPayload{
			OrgID:         p.OrgID,
			DatacenterID:  p.DatacenterID,
			FloorID:       p.FloorID,
			NetworkID:     p.NetworkID,
			GroupID:       p.GroupID,
			Devices:       make([]aggDevice, 0),
			TopologyLinks: make([]aggTopoLink, 0),
			Events:        make([]aggEvent, 0),
		}
	}
	// Fixed envelope overhead (scope ids + empty arrays). Approximate; the 512KB
	// budget already leaves ample headroom for it.
	const envelope = 512

	deviceBudget := maxPostBytes - envelope

	var chunks []*aggPayload
	cur := base()
	curBytes := envelope
	for _, d := range p.Devices {
		db := jsonLen(d)
		// A device whose own serialized size exceeds the budget (high-port switch
		// with a large metric backlog) can't ride a shared chunk. Split it across
		// its nested arrays into several sub-devices, each ≤ budget, and emit each
		// as its own chunk. The Aggregator upserts the device by mgmt_ip/hostname
		// and INSERTs metrics ON CONFLICT DO NOTHING, so re-sending the device's
		// scalar identity with a slice of its metrics is idempotent and safe — no
		// 413, no lost data.
		if db > deviceBudget {
			if len(cur.Devices) > 0 {
				chunks = append(chunks, cur)
				cur = base()
				curBytes = envelope
			}
			for _, sd := range splitDeviceByBytes(d, deviceBudget) {
				c := base()
				c.Devices = append(c.Devices, sd)
				chunks = append(chunks, c)
			}
			continue
		}
		if len(cur.Devices) > 0 && curBytes+db > maxPostBytes {
			chunks = append(chunks, cur)
			cur = base()
			curBytes = envelope
		}
		cur.Devices = append(cur.Devices, d)
		curBytes += db
	}
	if len(cur.Devices) > 0 {
		chunks = append(chunks, cur)
	}

	// Trailing topology + events, split into byte-bounded chunks of their own and
	// sent last so link endpoints are already committed.
	for _, tl := range p.TopologyLinks {
		if len(chunks) == 0 || lastTailFull(chunks, jsonLen(tl), true) {
			chunks = append(chunks, base())
		}
		c := chunks[len(chunks)-1]
		c.TopologyLinks = append(c.TopologyLinks, tl)
	}
	for _, ev := range p.Events {
		if len(chunks) == 0 || lastTailFull(chunks, jsonLen(ev), false) {
			chunks = append(chunks, base())
		}
		c := chunks[len(chunks)-1]
		c.Events = append(c.Events, ev)
	}

	return chunks
}

// jsonLen returns the serialized byte length of v (0 on marshal error, which
// then just packs conservatively).
// splitDeviceByBytes breaks one oversized device into several sub-devices, each
// serializing to ≤ budget. Every sub-device repeats the device's scalar fields
// (cheap — a few hundred bytes) but carries only a slice of the nested arrays:
// interfaces (with their addresses), then energy metrics, then metrics, packed by
// measured size. Because the Aggregator upserts the device idempotently and
// appends metrics ON CONFLICT DO NOTHING, the duplicated scalars are harmless and
// each nested row is sent exactly once. A single nested item larger than budget
// still ships alone (unavoidable, and not realistic for one metric/interface).
func splitDeviceByBytes(d aggDevice, budget int) []aggDevice {
	skel := d
	skel.Interfaces, skel.Metrics, skel.EnergyMetrics = nil, nil, nil
	base := jsonLen(skel)

	// Room for nested items per chunk, after the repeated scalar skeleton. Guard
	// against a pathologically large scalar skeleton so we always make progress.
	room := budget - base - 256
	if room < 4096 {
		room = 4096
	}

	var chunks []aggDevice
	cur := skel
	curLen := 0
	flush := func() {
		chunks = append(chunks, cur)
		cur = skel
		curLen = 0
	}
	add := func(itemLen int, place func(*aggDevice)) {
		if curLen > 0 && curLen+itemLen > room {
			flush()
		}
		place(&cur)
		curLen += itemLen
	}

	for _, it := range d.Interfaces {
		it := it
		add(jsonLen(it), func(c *aggDevice) { c.Interfaces = append(c.Interfaces, it) })
	}
	for _, e := range d.EnergyMetrics {
		e := e
		add(jsonLen(e), func(c *aggDevice) { c.EnergyMetrics = append(c.EnergyMetrics, e) })
	}
	for _, m := range d.Metrics {
		m := m
		add(jsonLen(m), func(c *aggDevice) { c.Metrics = append(c.Metrics, m) })
	}
	flush() // trailing items (or the bare skeleton if the device had no nested rows)
	return chunks
}

func jsonLen(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

// lastTailFull reports whether the trailing chunk can't take addBytes more of a
// topology link (isLink) or event without exceeding the byte budget, OR isn't a
// pure tail chunk (still holds devices — links/events must not piggyback on a
// device chunk, which is POSTed before endpoints commit).
func lastTailFull(chunks []*aggPayload, addBytes int, isLink bool) bool {
	c := chunks[len(chunks)-1]
	if len(c.Devices) > 0 {
		return true // last chunk is a device chunk — force a fresh tail chunk
	}
	cur := 512 // envelope
	for _, l := range c.TopologyLinks {
		cur += jsonLen(l)
	}
	for _, e := range c.Events {
		cur += jsonLen(e)
	}
	hasContent := len(c.TopologyLinks) > 0 || len(c.Events) > 0
	return hasContent && cur+addBytes > maxPostBytes
}

// postWithRetry POSTs the payload to the Aggregator endpoint. Retries on
// network errors and HTTP 5xx. Does not retry on 4xx (caller error).
// Retry delays: 0s, 2s, 5s (3 attempts total).
func (f *Forwarder) postWithRetry(ctx context.Context, payload *aggPayload) (*aggResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	delays := []time.Duration{0, 2 * time.Second, 5 * time.Second}
	var lastErr error

	for attempt, delay := range delays {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.cfg.Endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Ingest-Key", f.cfg.IngestKey)

		resp, err := f.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: POST: %w", attempt+1, err)
			f.log.Warn("aggregator POST network error",
				zap.Int("attempt", attempt+1),
				zap.Error(err))
			continue
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("attempt %d: HTTP %d: %s",
				attempt+1, resp.StatusCode, truncate(string(respBody), 256))
			f.log.Warn("aggregator returned error status",
				zap.Int("attempt", attempt+1),
				zap.Int("status", resp.StatusCode),
				zap.String("body", truncate(string(respBody), 256)))
			// Don't retry on 4xx — the payload is malformed or auth failed.
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return nil, lastErr
			}
			continue
		}

		var ar aggResponse
		if err := json.Unmarshal(respBody, &ar); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		if !ar.Success {
			return nil, fmt.Errorf("aggregator rejected: %s", ar.Message)
		}
		return &ar, nil
	}

	return nil, fmt.Errorf("all retries exhausted: %w", lastErr)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
