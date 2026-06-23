// Package threshold fetches per-device alert thresholds from the simulator's
// SNMP management plane (UDP 1161, community = device IP) and emits them as
// kind="threshold" telemetry packets. DCS stores them in device_thresholds.
//
// Read-only and slow-cadence: thresholds are config, not telemetry, and rarely
// change. This is the up-path companion to the command-apply down-path.
package threshold

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

// thresholdOID maps a rule name to its writable/readable OID on the management
// plane (enterprise 1.3.6.1.4.1.99999.3.x), per SNMP_ARCHITECTURE.md §2.2 and
// core/snmp_set_agent.py. All Integer. Airflow values are served ×10.
var thresholdOIDs = []struct {
	rule string
	oid  string
}{
	{"HighCPU", "1.3.6.1.4.1.99999.3.1.0"},
	{"HighCPUSustained", "1.3.6.1.4.1.99999.3.2.0"},
	{"CPUNormal", "1.3.6.1.4.1.99999.3.3.0"},
	{"HighMemory", "1.3.6.1.4.1.99999.3.4.0"},
	{"HighTemperature", "1.3.6.1.4.1.99999.3.6.0"},
	{"RackFailureMin", "1.3.6.1.4.1.99999.3.9.0"},
	{"SensorAmbientTempHigh", "1.3.6.1.4.1.99999.3.10.0"},
	{"SensorAmbientTempCritical", "1.3.6.1.4.1.99999.3.11.0"},
	{"SensorAmbientTempNormal", "1.3.6.1.4.1.99999.3.12.0"},
	{"SensorHighHumidity", "1.3.6.1.4.1.99999.3.13.0"},
	{"SensorCriticalHumidity", "1.3.6.1.4.1.99999.3.14.0"},
	{"SensorLowHumidity", "1.3.6.1.4.1.99999.3.15.0"},
	{"SensorHumidityNormalLow", "1.3.6.1.4.1.99999.3.16.1.0"},
	{"SensorHumidityNormalHigh", "1.3.6.1.4.1.99999.3.16.2.0"},
	{"SensorHighDewPoint", "1.3.6.1.4.1.99999.3.17.0"},
	{"SensorDewPointNormal", "1.3.6.1.4.1.99999.3.18.0"},
	{"SensorHighAirflow", "1.3.6.1.4.1.99999.3.19.0"},
	{"SensorLowAirflow", "1.3.6.1.4.1.99999.3.20.0"},
	{"SensorAirflowNormalLow", "1.3.6.1.4.1.99999.3.21.1.0"},
	{"SensorAirflowNormalHigh", "1.3.6.1.4.1.99999.3.21.2.0"},
}

// assetOIDs maps the writable asset/location fields (enterprise 99999.4.x on the
// mgmt agent) to the PatchAsset field name. Read each poll so a value changed on
// the device (via SNMP SET / UI edit) becomes authoritative over the seed JSON —
// and survives an EDR restart (JSON only seeds; the device is the source).
var assetOIDs = []struct {
	field string
	oid   string
}{
	{"country", "1.3.6.1.4.1.99999.4.1.0"},
	{"datacenter_city", "1.3.6.1.4.1.99999.4.2.0"},
	{"datacenter", "1.3.6.1.4.1.99999.4.3.0"},
	{"floor", "1.3.6.1.4.1.99999.4.4.0"},
	{"room", "1.3.6.1.4.1.99999.4.5.0"},
	{"rack_row", "1.3.6.1.4.1.99999.4.6.0"},
	{"rack_num", "1.3.6.1.4.1.99999.4.7.0"},
	{"rack_unit", "1.3.6.1.4.1.99999.4.8.0"},
	{"model", "1.3.6.1.4.1.99999.4.9.0"},
}

var oidToAsset = func() map[string]string {
	m := make(map[string]string, len(assetOIDs))
	for _, a := range assetOIDs {
		m[a.oid] = a.field
	}
	return m
}()

// oidToRule is the reverse lookup, built once.
var oidToRule = func() map[string]string {
	m := make(map[string]string, len(thresholdOIDs))
	for _, t := range thresholdOIDs {
		m[t.oid] = t.rule
	}
	return m
}()

// Poller GETs threshold OIDs from each target's mgmt plane on a fixed interval.
type Poller struct {
	cfg      config.ThresholdSyncConfig
	identity config.IdentityConfig
	signer   *packet.Signer
	log      *zap.Logger
	targets  []*target.Target
}

// New builds a Poller over all targets that have a management IP.
func New(targets []*target.Target, cfg config.ThresholdSyncConfig, identity config.IdentityConfig, signer *packet.Signer, log *zap.Logger) *Poller {
	var ts []*target.Target
	for _, t := range targets {
		if t.MgmtIP != "" || t.IP != "" {
			ts = append(ts, t)
		}
	}
	return &Poller{cfg: cfg, identity: identity, signer: signer, log: log, targets: ts}
}

// Count returns the number of targets that will be polled.
func (p *Poller) Count() int { return len(p.targets) }

// Run polls all targets until ctx is cancelled.
func (p *Poller) Run(ctx context.Context, out chan<- *v1.TelemetryPacket) {
	if len(p.targets) == 0 {
		return
	}
	p.log.Info("threshold-sync: starting",
		zap.Int("targets", len(p.targets)),
		zap.Int("port", p.cfg.Port),
		zap.Int("poll_interval_ms", p.cfg.PollIntervalMs))

	interval := time.Duration(p.cfg.PollIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	p.pollAll(ctx, out)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollAll(ctx, out)
		}
	}
}

func (p *Poller) pollAll(ctx context.Context, out chan<- *v1.TelemetryPacket) {
	for _, t := range p.targets {
		if ctx.Err() != nil {
			return
		}
		p.pollOne(ctx, t, out)
	}
}

// pollOne GETs every threshold OID from one target's mgmt agent and emits a
// threshold packet per integer reading.
func (p *Poller) pollOne(ctx context.Context, t *target.Target, out chan<- *v1.TelemetryPacket) {
	deviceIP := t.MgmtIP
	if deviceIP == "" {
		deviceIP = t.IP
	}
	host := p.cfg.Agent
	if host == "" {
		host = deviceIP
	}
	community := p.cfg.Community
	if community == "" {
		community = deviceIP
	}

	// Share the process-wide SNMP socket cap so this aux poller can't bypass the
	// main poller's max_concurrent protection and trigger Windows WSAENOBUFS.
	release, ok := snmp.AcquireSocket(ctx)
	if !ok {
		return
	}
	defer release()

	g := &gosnmp.GoSNMP{
		Target:    host,
		Port:      uint16(p.cfg.Port),
		Transport: "udp",
		Community: community,
		Version:   gosnmp.Version2c,
		Timeout:   time.Duration(p.cfg.TimeoutMs) * time.Millisecond,
		Retries:   p.cfg.Retries,
		MaxOids:   gosnmp.MaxOids,
	}
	if err := g.Connect(); err != nil {
		p.log.Debug("threshold-sync: connect failed", zap.String("host", host), zap.Error(err))
		return
	}
	defer g.Conn.Close()

	oids := make([]string, len(thresholdOIDs))
	for i, t := range thresholdOIDs {
		oids[i] = t.oid
	}
	res, err := g.Get(oids)
	if err != nil {
		p.log.Debug("threshold-sync: get failed", zap.String("host", host), zap.Error(err))
		return
	}

	for _, vb := range res.Variables {
		if vb.Type == gosnmp.NoSuchObject || vb.Type == gosnmp.NoSuchInstance || vb.Value == nil {
			continue
		}
		// gosnmp returns OIDs with a leading dot (".1.3.6..."); map keys have none.
		rule := oidToRule[strings.TrimPrefix(vb.Name, ".")]
		if rule == "" {
			continue
		}
		val := gosnmp.ToBigInt(vb.Value).Int64()
		select {
		case out <- p.packet(t, rule, float64(val)):
		case <-ctx.Done():
			return
		}
	}

	// Asset/location fields: read the device's current values and patch the
	// in-memory target so the main poll forwards the device truth (durable across
	// restart). NoSuchObject (real devices without the enterprise OID) → skip,
	// keeping the seed JSON value.
	ares, err := g.Get(func() []string {
		o := make([]string, len(assetOIDs))
		for i, a := range assetOIDs {
			o[i] = a.oid
		}
		return o
	}())
	if err != nil {
		return
	}
	for _, vb := range ares.Variables {
		if vb.Type == gosnmp.NoSuchObject || vb.Type == gosnmp.NoSuchInstance || vb.Value == nil {
			continue
		}
		field := oidToAsset[strings.TrimPrefix(vb.Name, ".")]
		if field == "" {
			continue
		}
		var sval string
		switch v := vb.Value.(type) {
		case []byte:
			sval = string(v)
		case string:
			sval = v
		default:
			sval = strconv.FormatInt(gosnmp.ToBigInt(vb.Value).Int64(), 10)
		}
		t.PatchAsset(field, sval)
	}
}

func (p *Poller) packet(t *target.Target, rule string, value float64) *v1.TelemetryPacket {
	now := time.Now().UnixNano()
	id := packet.NewID()
	nonce := p.signer.NextNonce()
	src := t.SourceID()
	canonical := packet.CanonicalBytes(id, src, now, rule, "", value, nonce)
	sig := p.signer.Sign(canonical)
	return &v1.TelemetryPacket{
		Id:           id,
		OrgId:        p.identity.OrgID,
		DatacenterId: dcID(t, p.identity),
		FloorId:      floorID(t, p.identity),
		NetworkId:    t.NetworkID(p.identity.NetworkID),
		GroupId:      p.identity.GroupID,
		SourceType:   "device",
		SourceId:     src,
		ReaderId:     p.identity.ReaderID,
		TimestampNs:  now,
		Name:         rule,
		Value:        value,
		Meta: map[string]string{
			"hostname":           src,
			"mgmt_ip":            t.MgmtIP,
			"device_type":        t.DeviceType,
			"collector_agent":    "EDR",
			"collector_protocol": "SNMP",
		},
		Kind:      "threshold",
		Signature: sig,
		Nonce:     nonce,
	}
}

func dcID(t *target.Target, id config.IdentityConfig) string {
	if t.DatacenterID != "" {
		return t.DatacenterID
	}
	return id.DatacenterID
}

func floorID(t *target.Target, id config.IdentityConfig) string {
	if t.FloorID != "" {
		return t.FloorID
	}
	return id.FloorID
}
