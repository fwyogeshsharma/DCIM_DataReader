package ingest

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwdcs/internal/store"
	"github.com/faberwork/fwdcs/pkg/config"
	v1 "github.com/faberwork/fwdcs/proto/v1"
)

// Pipeline is the async ingest pipeline.
//
// Throughput design:
//   - gRPC handler hands a batch to Submit(); the call returns instantly.
//   - N workers pull batches from a buffered channel.
//   - Each worker buffers metric rows and flushes via pgx.CopyFrom every
//     BufferRows or FlushIntervalMs — whichever comes first.
//   - Per-worker device + interface UUID cache keeps lookups in-memory after
//     first sight, avoiding round-trips to Postgres on the hot path.
//   - Topology and event packets are written one-at-a-time (low volume).
type Pipeline struct {
	db         *store.DB
	dedup      *Deduper
	cfg        config.IngestConfig
	log        *zap.Logger
	ch         chan []*v1.TelemetryPacket
	wg         sync.WaitGroup
	devCache   *LRU // hostname → device UUID
	ifCache    *LRU // device_id|if_name → interface UUID
	queueDepth atomicUint32
}

// NewPipeline creates a Pipeline. Start must be called before Submit.
func NewPipeline(db *store.DB, dedup *Deduper, cfg config.IngestConfig, log *zap.Logger) *Pipeline {
	ttl := time.Duration(cfg.CacheTTLSeconds) * time.Second
	if cfg.CacheTTLSeconds <= 0 {
		ttl = 60 * time.Second
	}
	return &Pipeline{
		db:       db,
		dedup:    dedup,
		cfg:      cfg,
		log:      log,
		ch:       make(chan []*v1.TelemetryPacket, cfg.ChannelSize),
		devCache: NewLRUWithTTL(10000, ttl),
		ifCache:  NewLRUWithTTL(100000, ttl),
	}
}

// FlushCaches drops all cached device and interface UUIDs. Call after an
// external DB truncate/restore so writes recover without waiting for TTL
// expiry. Returns (deviceEntriesDropped, interfaceEntriesDropped).
func (p *Pipeline) FlushCaches() (int, int) {
	d := p.devCache.Flush()
	i := p.ifCache.Flush()
	p.log.Info("ingest caches flushed", zap.Int("devices", d), zap.Int("interfaces", i))
	return d, i
}

// Start launches Workers goroutines that drain the batch channel.
func (p *Pipeline) Start(ctx context.Context) {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			p.workerLoop(ctx, id)
		}(i)
	}
	p.log.Info("ingest pipeline started",
		zap.Int("workers", p.cfg.Workers),
		zap.Int("buffer_rows", p.cfg.BufferRows),
		zap.Int("flush_ms", p.cfg.FlushIntervalMs),
		zap.Int("channel_size", p.cfg.ChannelSize))
}

// Wait blocks until all workers have exited.
func (p *Pipeline) Wait() { p.wg.Wait() }

// Submit hands a batch to a worker. Returns (accepted, rejected_due_to_full).
func (p *Pipeline) Submit(batch []*v1.TelemetryPacket) (uint32, uint32) {
	select {
	case p.ch <- batch:
		p.queueDepth.Add(1)
		return uint32(len(batch)), 0
	default:
		// Channel full — reject (backpressure). EDR will requeue and retry.
		return 0, uint32(len(batch))
	}
}

// QueueDepth returns the number of pending batches.
func (p *Pipeline) QueueDepth() uint32 { return p.queueDepth.Load() }

// ─── worker ──────────────────────────────────────────────────────────────────

func (p *Pipeline) workerLoop(ctx context.Context, id int) {
	rows := make([]store.MetricRow, 0, p.cfg.BufferRows)
	energyRows := make([]store.EnergyRow, 0, p.cfg.BufferRows)
	flushInterval := time.Duration(p.cfg.FlushIntervalMs) * time.Millisecond
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(rows) > 0 {
			if err := p.db.WriteMetricsBatch(context.Background(), rows); err != nil {
				p.log.Warn("metrics batch insert failed",
					zap.Int("worker", id), zap.Int("rows", len(rows)), zap.Error(err))
			}
			rows = rows[:0]
		}
		if len(energyRows) > 0 {
			if err := p.db.WriteEnergyBatch(context.Background(), energyRows); err != nil {
				p.log.Warn("energy batch insert failed",
					zap.Int("worker", id), zap.Int("rows", len(energyRows)), zap.Error(err))
			}
			energyRows = energyRows[:0]
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case batch := <-p.ch:
			p.queueDepth.Add(^uint32(0)) // -1
			for _, pkt := range batch {
				// BACnet energy telemetry routes to its own hypertable.
				if pkt.Kind == "energy" {
					if er, ok := p.processEnergy(ctx, pkt); ok {
						energyRows = append(energyRows, er)
						if len(energyRows) >= p.cfg.BufferRows {
							flush()
						}
					}
					continue
				}
				row, ok := p.process(ctx, pkt)
				if !ok {
					continue
				}
				rows = append(rows, row)
				if len(rows) >= p.cfg.BufferRows {
					flush()
				}
			}
		case <-ticker.C:
			flush()
		}
	}
}

// processEnergy turns a Kind=="energy" packet into an EnergyRow. circuit/phase
// are derived from the packet tag (CktNN → circuit, PhA/PhB/PhC → phase).
func (p *Pipeline) processEnergy(ctx context.Context, pkt *v1.TelemetryPacket) (store.EnergyRow, bool) {
	if pkt.Id == "" || pkt.SourceId == "" || pkt.Name == "" {
		return store.EnergyRow{}, false
	}
	Normalize(pkt)
	deviceID := p.lookupOrRegister(ctx, pkt)
	if deviceID == "" {
		return store.EnergyRow{}, false
	}

	var circuit, phase string
	switch {
	case strings.HasPrefix(pkt.Tag, "Ckt"):
		circuit = pkt.Tag
	case pkt.Tag == "PhA" || pkt.Tag == "PhB" || pkt.Tag == "PhC":
		phase = pkt.Tag
	}

	agent := pkt.Meta["collector_agent"]
	if agent == "" {
		agent = "EDR"
	}
	proto := pkt.Meta["collector_protocol"]
	if proto == "" {
		proto = "BACNET"
	}
	attrs := store.MapWithout(pkt.Meta,
		"hostname", "device_type", "vendor",
		"mgmt_ip", "prod_ip", "loopback_ip", "oob_ip",
		"collector_agent", "collector_protocol",
		"snmp_enabled", "gnmi_enabled")

	return store.EnergyRow{
		DeviceID:          deviceID,
		TS:                time.Unix(0, pkt.TimestampNs).UTC(),
		MetricName:        pkt.Name,
		Tag:               pkt.Tag,
		Circuit:           circuit,
		Phase:             phase,
		Value:             pkt.Value,
		Attributes:        store.AttributesJSON(attrs),
		CollectorAgent:    agent,
		CollectorProtocol: proto,
	}, true
}

// process handles one packet. Returns (row, true) if the packet became a
// COPY-able metric row; returns (_, false) for topology/event/dropped packets
// (which are handled synchronously inside process or skipped).
func (p *Pipeline) process(ctx context.Context, pkt *v1.TelemetryPacket) (store.MetricRow, bool) {
	if pkt.Id == "" || pkt.SourceId == "" || pkt.Name == "" {
		return store.MetricRow{}, false
	}

	// Dedup: only for kinds where we want it (events/traps). Skipping metrics
	// is safe because the metrics table has a unique index on (device_id, name,
	// tag, ts) and writes use ON CONFLICT DO NOTHING.
	wantDedup := false
	if pkt.Kind == "trap" || pkt.Kind == "event" || pkt.Kind == "alarm" {
		wantDedup = p.cfg.DedupEvents
	} else if pkt.Kind == "metric" {
		wantDedup = p.cfg.DedupMetrics
	}
	if wantDedup && p.dedup != nil {
		dup, err := p.dedup.IsDuplicate(ctx, pkt.Id, pkt.SourceId, pkt.Nonce)
		if err == nil && dup {
			return store.MetricRow{}, false
		}
	}

	Normalize(pkt)

	// Route non-metric packets out of the COPY path.
	switch pkt.Kind {
	case "topology":
		if err := p.db.WriteTopologyLink(ctx, pkt); err != nil {
			p.log.Debug("topology write failed", zap.Error(err))
		}
		return store.MetricRow{}, false
	case "interface_address":
		p.writeInterfaceAddress(ctx, pkt)
		return store.MetricRow{}, false
	case "device_state":
		// Chassis power state (and future fixed device states). Stored on the
		// device row, NOT in metrics — a fixed state that must survive retention.
		p.writeDeviceState(ctx, pkt)
		return store.MetricRow{}, false
	case "threshold":
		// Per-device alert threshold fetched from the SNMP management plane.
		p.writeThreshold(ctx, pkt)
		return store.MetricRow{}, false
	case "trap", "alarm", "event":
		// Traps in sim mode have SourceId set to the device IP (from the trap
		// community string). lookupOrRegister tries hostname lookup first;
		// then we fall back to mgmt_ip-prioritised IP lookup against
		// (mgmt_ip / prod_ip / loopback_ip / oob_ip). We also capture the
		// canonical hostname so events.source_hostname is populated even
		// when the device can be resolved by IP only.
		deviceID := p.lookupOrRegister(ctx, pkt)
		sourceHostname := pkt.Meta["hostname"]
		if deviceID == "" {
			candidates := []string{pkt.SourceId, pkt.Meta["source_ip"], pkt.Meta["device_ip"], pkt.Meta["mgmt_ip"]}
			for _, ip := range candidates {
				if ip == "" {
					continue
				}
				if id, hn, ok := p.db.DeviceByIP(ctx,
					pkt.OrgId, pkt.DatacenterId, pkt.FloorId,
					pkt.NetworkId, pkt.GroupId, ip); ok {
					deviceID = id
					if sourceHostname == "" {
						sourceHostname = hn
					}
					break
				}
			}
		}
		// Link traps (linkDown/linkUp): correlate to the EXACT topology_links row,
		// flip is_active for the whole link, and write ONE enriched event per real
		// transition (the peer's duplicate trap is suppressed). If handleLinkTrap
		// took ownership we're done; otherwise (not a link trap, or no topology
		// match) fall through and store the trap as a plain event so nothing is lost.
		if deviceID != "" && (pkt.Name == "linkDown" || pkt.Name == "linkUp") {
			if p.handleLinkTrap(ctx, deviceID, sourceHostname, pkt) {
				return store.MetricRow{}, false
			}
		}
		if err := p.db.WriteEvent(ctx, deviceID, sourceHostname, pkt); err != nil {
			p.log.Debug("event write failed", zap.Error(err))
		}
		return store.MetricRow{}, false
	}

	// Metric — resolve device ID (registering on first sight).
	deviceID := p.lookupOrRegister(ctx, pkt)
	if deviceID == "" {
		return store.MetricRow{}, false
	}

	// For interface.* metrics, resolve / upsert interface_id.
	var ifaceID string
	if strings.HasPrefix(pkt.Name, "interface.") && pkt.Tag != "" {
		ifaceID = p.lookupOrUpsertInterface(ctx, deviceID, pkt)
	}

	agent := pkt.Meta["collector_agent"]
	if agent == "" {
		if strings.HasPrefix(pkt.ReaderId, "idr-") {
			agent = "IDR"
		} else {
			agent = "EDR"
		}
	}
	proto := pkt.Meta["collector_protocol"]
	if proto == "" {
		proto = "SNMP"
	}

	// Strip transport-layer keys (already columnized) before persisting as attributes.
	attrs := store.MapWithout(pkt.Meta,
		"hostname", "device_type", "vendor",
		"mgmt_ip", "prod_ip", "loopback_ip", "oob_ip",
		"collector_agent", "collector_protocol",
		"snmp_enabled", "gnmi_enabled",
		"os_name", "platform", "platform_version", "kernel_version", "architecture",
		"sys_description")

	return store.MetricRow{
		DeviceID:          deviceID,
		TS:                time.Unix(0, pkt.TimestampNs).UTC(),
		MetricName:        pkt.Name,
		Tag:               pkt.Tag,
		Value:             pkt.Value,
		Attributes:        store.AttributesJSON(attrs),
		CollectorAgent:    agent,
		CollectorProtocol: proto,
		InterfaceID:       ifaceID,
	}, true
}

// writeInterfaceAddress handles a Kind=="interface_address" packet emitted by
// EDR's background enrichment. Resolves the interface UUID by (device_id,
// ifindex) and upserts into interface_addresses. Silently skipped if the
// interface row doesn't exist yet — EDR will re-emit on next enrichment cycle.
func (p *Pipeline) writeInterfaceAddress(ctx context.Context, pkt *v1.TelemetryPacket) {
	deviceID := p.lookupOrRegister(ctx, pkt)
	if deviceID == "" {
		return
	}
	idx, _ := strconv.Atoi(pkt.Meta["interface_index"])
	if idx <= 0 {
		return
	}
	ifaceID, _ := p.db.InterfaceIDByDeviceAndIndex(ctx, deviceID, idx)
	if ifaceID == "" {
		// Interface row not built yet (the MEDIUM walk hasn't reached this device —
		// common under a serial/one-shot walk, where the enrichment pass outruns
		// interface creation). CREATE it from the ifIndex now so the address is not
		// lost; the MEDIUM walk later fills in the real name/speed/status, matched
		// by ifIndex (so no duplicate). This decouples interface_addresses from
		// MEDIUM-walk timing — both tables populate regardless of order.
		ifaceID, _ = p.db.UpsertInterface(ctx, deviceID, "if"+strconv.Itoa(idx), idx, 0, 0, 0)
	}
	if ifaceID == "" {
		return
	}
	addr := pkt.Meta["address"]
	family := pkt.Meta["address_family"]
	if family == "" {
		family = "ipv4"
	}
	if err := p.db.UpsertInterfaceAddress(ctx, ifaceID, addr, family, true); err != nil {
		p.log.Debug("interface_address upsert failed",
			zap.String("device_id", deviceID),
			zap.Int("ifindex", idx),
			zap.String("address", addr),
			zap.Error(err))
	}
}

// resolveDeviceID finds an existing device for a non-metric packet (device_state,
// threshold). These never register a device — they only annotate one already
// known from the metric path. Tries source/hostname first, then mgmt_ip-style IP
// candidates (matching the trap resolver).
func (p *Pipeline) resolveDeviceID(ctx context.Context, pkt *v1.TelemetryPacket) string {
	if id := p.lookupOrRegister(ctx, pkt); id != "" {
		return id
	}
	candidates := []string{pkt.SourceId, pkt.Meta["mgmt_ip"], pkt.Meta["device_ip"], pkt.Meta["source_ip"]}
	for _, ip := range candidates {
		if ip == "" {
			continue
		}
		if id, _, ok := p.db.DeviceByIP(ctx,
			pkt.OrgId, pkt.DatacenterId, pkt.FloorId,
			pkt.NetworkId, pkt.GroupId, ip); ok {
			return id
		}
	}
	return ""
}

// writeDeviceState persists a fixed device state (currently power_state) on the
// device row. Skips silently if the device isn't known yet — EDR re-emits.
func (p *Pipeline) writeDeviceState(ctx context.Context, pkt *v1.TelemetryPacket) {
	deviceID := p.resolveDeviceID(ctx, pkt)
	if deviceID == "" {
		return
	}
	if pkt.Name == "power_state" {
		if err := p.db.SetDevicePowerState(ctx, deviceID, int(pkt.Value)); err != nil {
			p.log.Debug("power_state write failed", zap.String("device_id", deviceID), zap.Error(err))
		}
	}
}

// writeThreshold upserts one per-device alert threshold. rule = packet name,
// value = packet value (integer). Skips if the device isn't known yet.
func (p *Pipeline) writeThreshold(ctx context.Context, pkt *v1.TelemetryPacket) {
	deviceID := p.resolveDeviceID(ctx, pkt)
	if deviceID == "" {
		return
	}
	if err := p.db.UpsertThreshold(ctx, deviceID, pkt.Name, int(pkt.Value)); err != nil {
		p.log.Debug("threshold write failed",
			zap.String("device_id", deviceID), zap.String("rule", pkt.Name), zap.Error(err))
	}
}

// lookupOrRegister returns the device UUID, registering the device on first
// sight or when a sys.uptime_cs packet arrives (which refreshes os fields).
func (p *Pipeline) lookupOrRegister(ctx context.Context, pkt *v1.TelemetryPacket) string {
	key := devKey(pkt)
	if v, ok := p.devCache.Get(key); ok {
		// Still need to refresh on sys.uptime_cs to keep os fields current.
		if pkt.Name == "system.uptime_centiseconds" {
			_ = p.db.UpsertDevice(ctx, pkt)
		}
		return v
	}
	// Try the DB first — device may exist but not be cached yet.
	if id, _ := p.db.DeviceIDBySource(ctx, pkt.OrgId, pkt.DatacenterId, pkt.FloorId, pkt.NetworkId, pkt.GroupId, pkt.SourceId); id != "" {
		p.devCache.Put(key, id)
		if pkt.Name == "system.uptime_centiseconds" {
			_ = p.db.UpsertDevice(ctx, pkt)
		}
		return id
	}
	// First sight — only metric packets carry enough meta to register, and
	// we want device registration to be triggered by sys.uptime_cs (which is
	// emitted every poll cycle, so missing it once is fine).
	if pkt.Kind != "metric" {
		return ""
	}
	if err := p.db.UpsertDevice(ctx, pkt); err != nil {
		p.log.Debug("upsert device failed", zap.String("hostname", pkt.SourceId), zap.Error(err))
		return ""
	}
	id, _ := p.db.DeviceIDBySource(ctx, pkt.OrgId, pkt.DatacenterId, pkt.FloorId, pkt.NetworkId, pkt.GroupId, pkt.SourceId)
	if id != "" {
		p.devCache.Put(key, id)
	}
	return id
}

// handleLinkTrap correlates a linkDown/linkUp trap to its EXACT topology_links row
// (single source of truth — never a guessed neighbour), flips is_active for the
// whole physical link, and writes ONE enriched event per real transition: the
// event carries the peer endpoint + link_id in dedicated columns so the UI can
// render a single "src:port <-> dst:port DOWN" entry. The peer's duplicate trap
// (and same-state repeats) cause no transition and are suppressed.
//
// Returns true when it took ownership (wrote the enriched event, or intentionally
// suppressed the duplicate). Returns false when the trap couldn't be correlated to
// a link — the caller then stores it as a plain event so nothing is lost. We never
// fall back to another link of this device.
func (p *Pipeline) handleLinkTrap(ctx context.Context, deviceID, sourceHostname string, pkt *v1.TelemetryPacket) bool {
	down := pkt.Name == "linkDown"

	// Interface identity from the trap: IF-MIB ifIndex varbind + ifName.
	ifIndex := -1
	if v := pkt.Meta["vb.1.3.6.1.2.1.2.2.1.1.1"]; v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			ifIndex = n
		}
	}
	ifName := pkt.Meta["vb.1.3.6.1.2.1.2.2.1.2.1"]

	row, found, err := p.db.CorrelateLinkByPort(ctx, deviceID, ifIndex, ifName)
	if err != nil {
		p.log.Debug("link correlation failed", zap.Error(err))
		return false // let the plain event through
	}
	if !found {
		p.log.Warn("link trap: no exact topology_links match; storing plain event",
			zap.String("trap_device_id", deviceID),
			zap.Int("if_index", ifIndex),
			zap.String("if_name", ifName))
		return false
	}

	changed, err := p.db.SetLinkActive(ctx, row, down)
	if err != nil {
		p.log.Debug("set link active failed", zap.Error(err))
		return false // fall back to a plain event rather than lose the trap
	}
	if !changed {
		// Peer's duplicate trap or a same-state repeat → one event per transition.
		return true
	}

	// Canonical orientation: always present the link in the topology row's FIXED
	// direction (row.Src → row.Dst), regardless of which endpoint's trap arrived
	// first. The simulator fires the trap from BOTH ends of a link; whichever won
	// the dedup above would otherwise become "source", flipping src/dst between
	// linkDown and linkUp for the same link_id. Writing the row's canonical
	// direction keeps the orientation stable across down/up. The device that
	// actually reported is preserved in meta (reported_by + udp_src/community/
	// source_ip) so the reporter isn't lost.
	pkt.Meta["reported_by"] = sourceHostname
	pkt.Meta["src_port_name"] = row.SrcPort
	pkt.Meta["dst_device_id"] = row.DstDeviceID
	pkt.Meta["dst_hostname"] = row.DstHostname
	pkt.Meta["dst_port_name"] = row.DstPort
	pkt.Meta["link_id"] = row.LinkID
	if err := p.db.WriteEvent(ctx, row.SrcDeviceID, row.SrcHostname, pkt); err != nil {
		p.log.Debug("link event write failed", zap.Error(err))
	}
	p.log.Info("link event",
		zap.String("src_hostname", row.SrcHostname),
		zap.String("dst_hostname", row.DstHostname),
		zap.String("reported_by", sourceHostname),
		zap.Bool("down", down))
	return true
}

func (p *Pipeline) lookupOrUpsertInterface(ctx context.Context, deviceID string, pkt *v1.TelemetryPacket) string {
	ifName := pkt.Meta["interface_name"]
	if ifName == "" {
		ifName = pkt.Tag
	}
	key := deviceID + "|" + ifName
	if v, ok := p.ifCache.Get(key); ok {
		// Skip a DB roundtrip unless this packet carries a column that should
		// update the interface (speed/oper/admin). For pure counter updates
		// (bytes_received/sent, etc.), the cached UUID is enough.
		switch pkt.Name {
		case "interface.speed_mbps", "interface.operational_status", "interface.admin_status":
			ifIdx, _ := strconv.Atoi(pkt.Meta["interface_index"])
			var spd, op, adm int
			switch pkt.Name {
			case "interface.speed_mbps":
				spd = int(pkt.Value)
			case "interface.operational_status":
				op = int(pkt.Value)
			case "interface.admin_status":
				adm = int(pkt.Value)
			}
			_, _ = p.db.UpsertInterface(ctx, deviceID, ifName, ifIdx, spd, op, adm)
		}
		return v
	}
	ifIdx, _ := strconv.Atoi(pkt.Meta["interface_index"])
	var spd, op, adm int
	switch pkt.Name {
	case "interface.speed_mbps":
		spd = int(pkt.Value)
	case "interface.operational_status":
		op = int(pkt.Value)
	case "interface.admin_status":
		adm = int(pkt.Value)
	}
	id, err := p.db.UpsertInterface(ctx, deviceID, ifName, ifIdx, spd, op, adm)
	if err != nil || id == "" {
		return ""
	}
	p.ifCache.Put(key, id)
	return id
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func devKey(pkt *v1.TelemetryPacket) string {
	return pkt.OrgId + "|" + pkt.DatacenterId + "|" + pkt.FloorId + "|" + pkt.NetworkId + "|" + pkt.GroupId + "|" + pkt.SourceId
}

// atomicUint32 is a tiny wrapper to avoid pulling in sync/atomic everywhere.
type atomicUint32 struct{ v uint32 }

func (a *atomicUint32) Load() uint32 {
	return atomicLoadUint32(&a.v)
}
func (a *atomicUint32) Add(delta uint32) uint32 {
	return atomicAddUint32(&a.v, delta)
}
