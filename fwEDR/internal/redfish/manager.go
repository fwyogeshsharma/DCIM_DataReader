package redfish

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

type basePacket struct {
	orgID, dcID, floorID, netID, grpID, readerID string
}

// binding pairs a server target with its BMC address and cached resource paths.
type binding struct {
	t    *target.Target
	base basePacket
	ip   string // BMC address (mgmt IP, falls back to IP)

	// Cached Redfish resource paths (device-derived; discovered once, then reused
	// until a request fails — at which point rediscovery is forced).
	sysPath, chassisPath, mgrPath string
	discovered                    bool
}

// Manager polls each server's BMC over Redfish on a fixed interval. Mirrors the
// BACnet manager: built over the matching targets, then Run blocks until ctx is
// cancelled, fanning out polls bounded by MaxConcurrent.
type Manager struct {
	cfg      config.RedfishConfig
	identity config.IdentityConfig
	signer   *packet.Signer
	log      *zap.Logger

	client   *Client
	bindings []*binding
	profile  *Profile // OS-usage field paths (default = simulator Oem.Simulator)
}

// NewManager builds a Manager over the server targets (device_type=server with a
// mgmt IP — the BMC endpoint).
func NewManager(
	targets []*target.Target,
	cfg config.RedfishConfig,
	identity config.IdentityConfig,
	signer *packet.Signer,
	log *zap.Logger,
) *Manager {
	// Load the OS-usage field profile once; a bad file degrades to the built-in
	// simulator default so Redfish collection never stops on a config typo.
	profile, err := LoadProfile(cfg.ProfilePath)
	if err != nil {
		log.Warn("redfish profile load failed — using built-in simulator default",
			zap.String("path", cfg.ProfilePath), zap.Error(err))
		profile = DefaultProfile()
	}
	log.Info("redfish profile loaded", zap.String("name", profile.Name),
		zap.String("path", cfg.ProfilePath))
	m := &Manager{
		cfg:      cfg,
		identity: identity,
		signer:   signer,
		log:      log,
		client:   NewClient(cfg),
		profile:  profile,
	}
	for _, t := range targets {
		if t.DeviceType != "server" {
			continue
		}
		ip := t.MgmtIP
		if ip == "" {
			ip = t.IP
		}
		if ip == "" {
			continue
		}
		m.bindings = append(m.bindings, &binding{t: t, base: m.baseFor(t), ip: ip})
	}
	return m
}

// Count returns the number of server BMCs this manager will poll.
func (m *Manager) Count() int { return len(m.bindings) }

func (m *Manager) baseFor(t *target.Target) basePacket {
	dc := m.identity.DatacenterID
	if t.DatacenterID != "" {
		dc = t.DatacenterID
	}
	fl := m.identity.FloorID
	if t.FloorID != "" {
		fl = t.FloorID
	}
	return basePacket{
		orgID:    m.identity.OrgID,
		dcID:     dc,
		floorID:  fl,
		netID:    t.NetworkID(m.identity.NetworkID),
		grpID:    m.identity.GroupID,
		readerID: m.identity.ReaderID,
	}
}

// Run starts the Redfish poll loop and blocks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context, out chan<- *v1.TelemetryPacket) {
	if len(m.bindings) == 0 {
		return
	}
	m.log.Info("redfish: starting",
		zap.Int("servers", len(m.bindings)),
		zap.Int("port", m.cfg.Port),
		zap.String("scheme", m.client.scheme),
		zap.Int("poll_interval_ms", m.cfg.PollIntervalMs))

	interval := time.Duration(m.cfg.PollIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	m.pollAll(ctx, out) // immediate first pass
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pollAll(ctx, out)
		}
	}
}

// pollAll polls every server BMC concurrently, bounded by MaxConcurrent so a
// large fleet never opens more than a handful of HTTP connections at once.
func (m *Manager) pollAll(ctx context.Context, out chan<- *v1.TelemetryPacket) {
	sem := make(chan struct{}, m.cfg.MaxConcurrent)
	var wg sync.WaitGroup
	for _, b := range m.bindings {
		wg.Add(1)
		go func(b *binding) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			m.pollOne(ctx, b, out)
		}(b)
	}
	wg.Wait()
}

// pollOne reads the BMC resources for one server and emits metric packets. A BMC
// answers even while the chassis is Off (standby power), so this keeps producing
// chassis_power_state + decayed sensor values regardless of OS state.
func (m *Manager) pollOne(ctx context.Context, b *binding, out chan<- *v1.TelemetryPacket) {
	if !b.discovered {
		sys, chassis, mgr, err := m.client.discover(ctx, b.ip)
		if err != nil {
			m.log.Debug("redfish: discovery failed", zap.String("bmc", b.ip), zap.Error(err))
			return
		}
		b.sysPath, b.chassisPath, b.mgrPath, b.discovered = sys, chassis, mgr, true
	}

	var pkts []*v1.TelemetryPacket

	// ComputerSystem: chassis power state + OS usage. Read generically so the
	// OS-usage field locations come from the profile (Oem.Simulator on the sim;
	// retargetable for real BMCs) rather than hardcoded struct tags.
	var sys map[string]any
	if err := m.client.get(ctx, b.ip, b.sysPath, &sys); err != nil {
		m.log.Debug("redfish: system read failed", zap.String("bmc", b.ip), zap.Error(err))
		b.discovered = false // force rediscovery next tick (ids may have changed)
		return
	}
	powerState := 1.0 // 1 = On (matches the BMC-SNMP chassis power encoding)
	if strings.EqualFold(stringAtPath(sys, m.profile.PowerStatePath), "Off") {
		powerState = 2.0
	}
	// Power is a fixed state, not telemetry — emit it as device_state so DCS
	// stores it on the device row (persistent) instead of the metrics table
	// (which expires under retention).
	pkts = append(pkts, m.deviceState(b, "power_state", powerState))
	for _, f := range m.profile.OSUsage {
		pkts = appendIf(pkts, m.metricP(b, f.Metric, "", floatAtPath(sys, f.Path)))
	}

	// Thermal: temperatures + fans (counts dynamic — iterate the arrays).
	if b.chassisPath != "" {
		var th thermal
		if err := m.client.get(ctx, b.ip, b.chassisPath+"/Thermal", &th); err == nil {
			for _, t := range th.Temperatures {
				if t.ReadingCelsius != nil {
					pkts = append(pkts, m.metric(b, "bmc.temp_c", t.Name, *t.ReadingCelsius))
				}
			}
			for _, f := range th.Fans {
				if f.Reading != nil {
					pkts = append(pkts, m.metric(b, "bmc.fan_rpm", f.Name, *f.Reading))
				}
			}
		}
		// Power: total draw + per-PSU output (PSU count dynamic).
		var pw power
		if err := m.client.get(ctx, b.ip, b.chassisPath+"/Power", &pw); err == nil {
			for _, pc := range pw.PowerControl {
				if pc.PowerConsumedWatts != nil {
					pkts = append(pkts, m.metric(b, "bmc.power_draw_w", "", *pc.PowerConsumedWatts))
					break
				}
			}
			for _, ps := range pw.PowerSupplies {
				if ps.LastPowerOutputWatts != nil {
					pkts = append(pkts, m.metric(b, "bmc.psu_output_w", ps.Name, *ps.LastPowerOutputWatts))
				}
			}
		}
	}

	// Manager: BMC firmware/model/vendor — string inventory carried in attributes
	// on a single info metric (metric values are numeric).
	if b.mgrPath != "" {
		var mgr managerDoc
		if err := m.client.get(ctx, b.ip, b.mgrPath, &mgr); err == nil {
			meta := m.meta(b)
			meta["bmc_firmware"] = mgr.FirmwareVersion
			meta["bmc_model"] = mgr.Model
			meta["bmc_vendor"] = mgr.Manufacturer
			pkts = append(pkts, m.metricMeta(b, "bmc.info", "", powerState, meta))
		}
	}

	for _, p := range pkts {
		select {
		case out <- p:
		case <-ctx.Done():
			return
		}
	}
}

// ─── packet helpers (signed exactly like snmp.Collector.newMetric) ───────────

func (m *Manager) meta(b *binding) map[string]string {
	a := b.t.Asset()
	return map[string]string{
		"hostname":           b.t.SourceID(),
		"mgmt_ip":            b.t.MgmtIP,
		"prod_ip":            b.t.ProdIP,
		"device_type":        b.t.DeviceType,
		"vendor":             b.t.Vendor,
		"model_name":         a.ModelName,
		"country":            a.Country,
		"datacenter":         a.DatacenterName,
		"datacenter_city":    a.DatacenterCity,
		"room":               a.Room,
		"collector_agent":    "EDR",
		"collector_protocol": "Redfish",
	}
}

func (m *Manager) metric(b *binding, name, tag string, value float64) *v1.TelemetryPacket {
	return m.metricMeta(b, name, tag, value, m.meta(b))
}

// deviceState builds a Kind="device_state" packet (e.g. power_state). The
// signature covers id/src/ts/name/tag/value/nonce — not Kind — so overriding
// Kind after the build is safe.
func (m *Manager) deviceState(b *binding, name string, value float64) *v1.TelemetryPacket {
	p := m.metricMeta(b, name, "", value, m.meta(b))
	p.Kind = "device_state"
	return p
}

// metricP builds a metric from a nullable reading, returning nil when absent so
// the caller skips it (e.g. a sensor the BMC zeroes/omits while powered off).
func (m *Manager) metricP(b *binding, name, tag string, value *float64) *v1.TelemetryPacket {
	if value == nil {
		return nil
	}
	return m.metric(b, name, tag, *value)
}

func (m *Manager) metricMeta(b *binding, name, tag string, value float64, meta map[string]string) *v1.TelemetryPacket {
	now := time.Now().UnixNano()
	id := packet.NewID()
	nonce := m.signer.NextNonce()
	src := b.t.SourceID()
	canonical := packet.CanonicalBytes(id, src, now, name, tag, value, nonce)
	sig := m.signer.Sign(canonical)
	return &v1.TelemetryPacket{
		Id:           id,
		OrgId:        b.base.orgID,
		DatacenterId: b.base.dcID,
		FloorId:      b.base.floorID,
		NetworkId:    b.base.netID,
		GroupId:      b.base.grpID,
		SourceType:   "device",
		SourceId:     src,
		ReaderId:     b.base.readerID,
		TimestampNs:  now,
		Name:         name,
		Tag:          tag,
		Value:        value,
		Meta:         meta,
		Kind:         "metric",
		Signature:    sig,
		Nonce:        nonce,
	}
}

func appendIf(pkts []*v1.TelemetryPacket, p *v1.TelemetryPacket) []*v1.TelemetryPacket {
	if p == nil {
		return pkts
	}
	return append(pkts, p)
}
