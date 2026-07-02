package snmp

import (
	"strings"

	"github.com/gosnmp/gosnmp"

	v1 "github.com/faberwork/fwedr/proto/v1"
)

// pollplan.go implements the walk-once → GET model for SNMP telemetry. A device's
// PollPlan is the explicit list of leaf OIDs to fetch on each poll, with how to
// emit each value. It is built ONCE (from the profile + a discovery walk) and then
// polled with cheap GETs on per-class cadences — instead of re-walking whole tables
// every cycle. See docs/SNMP_POLL_REDESIGN.md.
//
// Phase A (this file) covers the FIXED-OID device types — pdu / floor_pdu /
// generator / ups — whose OIDs need no index discovery. Index-discovered classes
// (interface counters, HR storage, sensor tables) are added in a later phase.

// PollClass groups OIDs by cadence so different metric kinds poll at different
// intervals (cpu/mem slower than interface traffic, etc.).
type PollClass string

const (
	ClassLiveness    PollClass = "liveness"
	ClassInterface   PollClass = "interface"
	ClassServer      PollClass = "server"
	ClassEnvironment PollClass = "environment"
	ClassPower       PollClass = "power"
)

// PollOID is one leaf OID to GET, plus how to turn its value into a metric.
type PollOID struct {
	OID   string  `json:"oid"`
	Name  string  `json:"name"`  // metric_name
	Tag   string  `json:"tag"`   // metric tag ("" if scalar)
	Scale float64 `json:"scale"` // divisor undoing fixed-point encoding (1 = none)
	// SensorIndex, when set, is added to meta["sensor_index"] (temperature heatmap).
	SensorIndex string `json:"sensor_index,omitempty"`
	// Meta carries extra per-metric meta keys (e.g. interface_index/interface_name).
	Meta map[string]string `json:"meta,omitempty"`
}

// StorageRow is a discovered HOST-RESOURCES storage row. Size/used are re-GET per
// poll; alloc units + descr/type are constant (captured at discovery) so
// server.storage_{size,used,available}_kb can be computed from two GETs.
type StorageRow struct {
	Index    string  `json:"index"`
	Descr    string  `json:"descr"`
	TypeName string  `json:"type_name"`
	Alloc    float64 `json:"alloc"`
	SizeOID  string  `json:"size_oid"`
	UsedOID  string  `json:"used_oid"`
}

// PollGroup is a set of OIDs fetched together at one cadence. Special routes the
// group to a computed collector ("" = generic GET; "hrstorage" = server storage).
type PollGroup struct {
	Class       PollClass    `json:"class"`
	OIDs        []PollOID    `json:"oids"`
	Special     string       `json:"special,omitempty"`
	StorageRows []StorageRow `json:"storage_rows,omitempty"`
}

// PollPlan is a device's full set of poll groups, built at discovery.
type PollPlan struct {
	DeviceType string      `json:"device_type"`
	WalkedAt   int64       `json:"walked_at"` // unix nano; freshness for the cache
	Groups     []PollGroup `json:"groups"`
}

// Empty reports whether the plan has nothing to poll.
func (p *PollPlan) Empty() bool {
	for _, g := range p.Groups {
		if len(g.OIDs) > 0 || len(g.StorageRows) > 0 {
			return false
		}
	}
	return true
}

// ─── plan builder (fixed-OID device types) ───────────────────────────────────

// BuildStaticPlan builds the poll plan for a device from FIXED OIDs only — the
// enterprise power scalars (from the profile) and the standard UPS-MIB group.
// These need no index discovery. Returns nil for device types not handled here.
func BuildStaticPlan(deviceType string, profile *Profile, walkedAt int64) *PollPlan {
	if profile == nil {
		profile = DefaultProfile()
	}
	p := &PollPlan{DeviceType: deviceType, WalkedAt: walkedAt}
	switch deviceType {
	case "pdu", "floor_pdu":
		p.add(ClassPower, scalarsToPollOIDs(profile.PDUBase, profile.PDUScalars))
	case "generator":
		p.add(ClassPower, scalarsToPollOIDs(profile.GeneratorBase, profile.GeneratorScalars))
	case "ups":
		p.add(ClassPower, scalarsToPollOIDs(profile.UPSEntBase, profile.UPSEntScalars))
		p.add(ClassPower, upsMibPollOIDs())
	default:
		return nil
	}
	return p
}

func (p *PollPlan) add(class PollClass, oids []PollOID) {
	if len(oids) == 0 {
		return
	}
	p.Groups = append(p.Groups, PollGroup{Class: class, OIDs: oids})
}

// scalarsToPollOIDs expands a profile scalar table (base.col.0) into poll OIDs.
func scalarsToPollOIDs(base string, scalars []scalarMetric) []PollOID {
	out := make([]PollOID, 0, len(scalars))
	for _, s := range scalars {
		scale := s.scale
		if scale == 0 {
			scale = 1
		}
		out = append(out, PollOID{OID: base + "." + s.col + ".0", Name: s.name, Tag: s.tag, Scale: scale})
	}
	return out
}

// upsMibPollOIDs returns the standard UPS-MIB battery + input/output group as fixed
// OIDs (single line, index .1 for the table entries) — the same OIDs collectUPS
// reads, expressed for GET. Mirrors collectUPS's metric names/scaling.
func upsMibPollOIDs() []PollOID {
	return []PollOID{
		{OID: OIDUpsBatteryStatus, Name: "environment.ups_battery_status", Scale: 1},
		{OID: OIDUpsSecondsOnBattery, Name: "environment.ups_seconds_on_battery", Scale: 1},
		{OID: OIDUpsEstimatedMinutesRemain, Name: "environment.ups_minutes_remaining", Scale: 1},
		{OID: OIDUpsEstimatedChargeRemaining, Name: "environment.ups_charge_percent", Scale: 1},
		{OID: OIDUpsBatteryVoltage, Name: "environment.ups_battery_voltage_v", Scale: 1},
		{OID: OIDUpsBatteryTemperature, Name: "environment.temperature_c", Tag: "BATTERY", Scale: 1, SensorIndex: "battery"},
		// Input/output tables — single line, index 1.
		{OID: OIDUpsInputVoltage + ".1", Name: "environment.ups_input_voltage_v", Tag: "1", Scale: 1},
		{OID: OIDUpsOutputVoltage + ".1", Name: "environment.ups_output_voltage_v", Tag: "1", Scale: 1},
		{OID: OIDUpsOutputCurrent + ".1", Name: "environment.ups_output_current_a", Tag: "1", Scale: 1},
		{OID: OIDUpsOutputLoad + ".1", Name: "environment.ups_output_load_percent", Tag: "1", Scale: 1},
	}
}

// ─── plan-based collection (GET, not walk) ────────────────────────────────────

// CollectGroup GETs every OID in a poll group (chunked to fit the PDU) and emits
// one metric per readable varbind. Unanswered columns (NoSuchInstance/Object) are
// skipped so a partial agent never fails the group. This is the steady-state poll
// under the walk-once model — no table walks.
func (c *Collector) CollectGroup(g PollGroup) ([]*v1.TelemetryPacket, error) {
	if g.Special == "hrstorage" {
		return c.collectStorageGroup(g), nil
	}
	if len(g.OIDs) == 0 {
		return nil, nil
	}
	byOID := make(map[string]PollOID, len(g.OIDs))
	oids := make([]string, 0, len(g.OIDs))
	for _, o := range g.OIDs {
		byOID[o.OID] = o
		oids = append(oids, o.OID)
	}

	chunk := gosnmp.MaxOids
	if chunk <= 0 {
		chunk = 60
	}
	var pkts []*v1.TelemetryPacket
	for start := 0; start < len(oids); start += chunk {
		end := start + chunk
		if end > len(oids) {
			end = len(oids)
		}
		pkt, err := c.client.Get(oids[start:end])
		if err != nil {
			return nil, err
		}
		for _, vb := range pkt.Variables {
			switch vb.Type {
			case gosnmp.NoSuchObject, gosnmp.NoSuchInstance, gosnmp.EndOfMibView, gosnmp.Null:
				continue
			}
			o, ok := byOID[strings.TrimPrefix(vb.Name, ".")]
			if !ok {
				continue
			}
			scale := o.Scale
			if scale == 0 {
				scale = 1
			}
			meta := c.hostMeta()
			if o.SensorIndex != "" {
				meta["sensor_index"] = o.SensorIndex
			}
			for k, v := range o.Meta {
				meta[k] = v
			}
			pkts = append(pkts, c.newMetric(o.Name, o.Tag, ToFloat64(vb)/scale, meta))
		}
	}
	return pkts, nil
}
