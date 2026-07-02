package snmp

import (
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"

	v1 "github.com/faberwork/fwedr/proto/v1"
)

// pollplan_discovery.go builds the index-discovered part of a device's poll plan
// (Phase B). It WALKS the tables ONCE to learn the concrete instances, then the
// plan is polled with cheap GETs (see CollectGroup). Scope: server + sensor —
// SNMP-only device types that were stale under the one-shot model. Switch/router
// interface telemetry comes from gNMI, so it is intentionally not planned here.

// DiscoverPlan walks a device's tables once and returns its full poll plan:
// fixed-OID groups (BuildStaticPlan) plus index-discovered groups for server and
// sensor. includeUCD adds the UCD CPU/RAM scalars (server) when enabled.
func (c *Collector) DiscoverPlan(includeUCD bool) *PollPlan {
	dt := c.target.DeviceType
	now := time.Now().UnixNano()
	plan := BuildStaticPlan(dt, c.profile, now)
	if plan == nil {
		plan = &PollPlan{DeviceType: dt, WalkedAt: now}
	}
	switch dt {
	case "server":
		plan.addGroup(c.discoverServerCPU())
		plan.addGroup(c.discoverServerStorage())
		plan.addGroup(c.discoverServerTemp())
		if includeUCD {
			plan.add(ClassServer, c.ucdPollOIDs())
		}
	case "sensor":
		plan.add(ClassEnvironment, c.discoverSensors())
	}
	return plan
}

func (p *PollPlan) addGroup(g PollGroup) {
	if len(g.OIDs) == 0 && len(g.StorageRows) == 0 {
		return
	}
	p.Groups = append(p.Groups, g)
}

// ─── server ───────────────────────────────────────────────────────────────────

// discoverServerCPU walks hrProcessorLoad → one poll OID per logical CPU.
func (c *Collector) discoverServerCPU() PollGroup {
	var oids []PollOID
	pdus, err := c.client.Walk(c.profile.HrProcessorLoad)
	if err == nil {
		for _, p := range pdus {
			idx := LastOIDComponent(p.Name)
			oids = append(oids, PollOID{
				OID:  c.profile.HrProcessorLoad + "." + idx,
				Name: "server.cpu_per_core_percent", Tag: idx, Scale: 1,
			})
		}
	}
	return PollGroup{Class: ClassServer, OIDs: oids}
}

// discoverServerStorage walks the storage table once, capturing per-row descr,
// type and alloc units. Only size+used are re-GET per poll (see collectStorageGroup).
func (c *Collector) discoverServerStorage() PollGroup {
	typeMap := walkIndexed(c.client, c.profile.HrStorageType)
	descrMap := walkIndexedStr(c.client, c.profile.HrStorageDescr)
	allocMap := walkIndexed(c.client, c.profile.HrStorageAllocUnits)
	sizePDUs, _ := c.client.Walk(c.profile.HrStorageSize)

	var rows []StorageRow
	for _, p := range sizePDUs {
		idx := LastOIDComponent(p.Name)
		descr := descrMap[idx]
		if descr == "" {
			descr = idx
		}
		rows = append(rows, StorageRow{
			Index:    idx,
			Descr:    descr,
			TypeName: hrStorageTypeName(int(typeMap[idx])),
			Alloc:    allocMap[idx],
			SizeOID:  c.profile.HrStorageSize + "." + idx,
			UsedOID:  c.profile.HrStorageUsed + "." + idx,
		})
	}
	return PollGroup{Class: ClassServer, Special: "hrstorage", StorageRows: rows}
}

// collectStorageGroup GETs size+used for each discovered row (one/few PDUs) and
// computes size/used/available KB using the cached alloc units. No table walk.
func (c *Collector) collectStorageGroup(g PollGroup) []*v1.TelemetryPacket {
	if len(g.StorageRows) == 0 {
		return nil
	}
	oids := make([]string, 0, len(g.StorageRows)*2)
	for _, r := range g.StorageRows {
		oids = append(oids, r.SizeOID, r.UsedOID)
	}
	vals := c.getFloats(oids)

	var pkts []*v1.TelemetryPacket
	for _, r := range g.StorageRows {
		sizeU, okS := vals[r.SizeOID]
		usedU, okU := vals[r.UsedOID]
		if !okS || !okU {
			continue
		}
		sizeKB := sizeU * r.Alloc / 1024
		usedKB := usedU * r.Alloc / 1024
		avail := sizeKB - usedKB
		meta := c.hostMeta()
		meta["storage_type"] = r.TypeName
		meta["storage_descr"] = r.Descr
		pkts = append(pkts,
			c.newMetric("server.storage_size_kb", r.Descr, sizeKB, meta),
			c.newMetric("server.storage_used_kb", r.Descr, usedKB, meta),
			c.newMetric("server.storage_available_kb", r.Descr, avail, meta),
		)
	}
	return pkts
}

// getFloats GETs a set of scalar OIDs (chunked) and returns readable numeric
// values by OID (NoSuch/Null skipped).
func (c *Collector) getFloats(oids []string) map[string]float64 {
	out := make(map[string]float64, len(oids))
	chunk := gosnmp.MaxOids
	if chunk <= 0 {
		chunk = 60
	}
	for start := 0; start < len(oids); start += chunk {
		end := start + chunk
		if end > len(oids) {
			end = len(oids)
		}
		pkt, err := c.client.Get(oids[start:end])
		if err != nil {
			continue
		}
		for _, vb := range pkt.Variables {
			switch vb.Type {
			case gosnmp.NoSuchObject, gosnmp.NoSuchInstance, gosnmp.EndOfMibView, gosnmp.Null:
				continue
			}
			out[strings.TrimPrefix(vb.Name, ".")] = ToFloat64(vb)
		}
	}
	return out
}

// discoverServerTemp walks ENTITY-SENSOR-MIB → one temp poll OID per index
// (×10 °C), tagged CHASSIS/CPU per the existing convention.
func (c *Collector) discoverServerTemp() PollGroup {
	var oids []PollOID
	pdus, err := c.client.Walk(OIDEntPhySensorValue)
	if err == nil {
		for _, p := range pdus {
			idx := LastOIDComponent(p.Name)
			oids = append(oids, PollOID{
				OID:  OIDEntPhySensorValue + "." + idx,
				Name: "environment.temperature_c", Tag: entSensorTag(idx),
				Scale: 10, SensorIndex: idx,
			})
		}
	}
	return PollGroup{Class: ClassEnvironment, OIDs: oids}
}

// ucdPollOIDs returns the fixed UCD CPU/RAM scalars (from the profile).
func (c *Collector) ucdPollOIDs() []PollOID {
	return []PollOID{
		{OID: c.profile.UcdCpuUser, Name: "server.cpu_user_percent", Scale: 1},
		{OID: c.profile.UcdCpuSystem, Name: "server.cpu_system_percent", Scale: 1},
		{OID: c.profile.UcdCpuIdle, Name: "server.cpu_idle_percent", Scale: 1},
		{OID: c.profile.UcdMemTotal, Name: "server.memory_total_kb", Scale: 1},
		{OID: c.profile.UcdMemAvail, Name: "server.memory_available_kb", Scale: 1},
		{OID: c.profile.UcdMemCached, Name: "server.memory_cached_kb", Scale: 1},
		{OID: c.profile.UcdMemBuffer, Name: "server.memory_buffer_kb", Scale: 1},
	}
}

// ─── sensor ─────────────────────────────────────────────────────────────────

// discoverSensors walks the vendor sensor table once (Raritan/Vertiv/APC) and
// builds env poll OIDs. Mirrors collectSensors' vendor gating + scaling.
func (c *Collector) discoverSensors() []PollOID {
	switch c.target.Vendor {
	case "raritan":
		return c.discoverRaritan()
	case "vertiv":
		return c.discoverVertiv()
	case "apc":
		return c.discoverAPC()
	default:
		if o := c.discoverRaritan(); len(o) > 0 {
			return o
		}
		return c.discoverVertiv()
	}
}

func (c *Collector) discoverRaritan() []PollOID {
	typeMap := walkIndexedInt(c.client, c.profile.RaritanSensorType)
	valuePDUs, err := c.client.Walk(c.profile.RaritanSensorValue)
	if err != nil {
		return nil
	}
	var oids []PollOID
	for _, p := range valuePDUs {
		idx := LastOIDComponent(p.Name)
		var name string
		switch typeMap[idx] {
		case c.profile.RaritanTypeTemp:
			name = "environment.temperature_c"
		case c.profile.RaritanTypeHumidity:
			name = "environment.humidity_percent"
		default:
			continue
		}
		oids = append(oids, PollOID{
			OID:  c.profile.RaritanSensorValue + "." + idx,
			Name: name, Tag: idx, Scale: 10, SensorIndex: idx,
		})
	}
	return oids
}

func (c *Collector) discoverVertiv() []PollOID {
	var oids []PollOID
	add := func(base, name string) {
		pdus, err := c.client.Walk(base)
		if err != nil {
			return
		}
		for _, p := range pdus {
			idx := LastOIDComponent(p.Name)
			oids = append(oids, PollOID{OID: base + "." + idx, Name: name, Tag: idx, Scale: 10, SensorIndex: idx})
		}
	}
	add(c.profile.VertivTempValue, "environment.temperature_c")
	add(c.profile.VertivHumValue, "environment.humidity_percent")
	add(c.profile.VertivDewValue, "environment.dew_point_c")
	return oids
}

func (c *Collector) discoverAPC() []PollOID {
	labelMap := walkIndexedStr(c.client, c.profile.APCSensorLabel)
	valuePDUs, err := c.client.Walk(c.profile.APCSensorValue)
	if err != nil {
		return nil
	}
	var oids []PollOID
	for _, p := range valuePDUs {
		idx := LastOIDComponent(p.Name)
		var name string
		scale := 10.0
		switch apcSensorKind(labelMap[idx], idx) {
		case "temperature":
			name = "environment.temperature_c"
		case "humidity":
			name, scale = "environment.humidity_percent", 1 // APC humidity is whole percent
		case "airflow":
			name = "environment.airflow_cfm"
		default:
			continue
		}
		oids = append(oids, PollOID{OID: c.profile.APCSensorValue + "." + idx, Name: name, Tag: idx, Scale: scale, SensorIndex: idx})
	}
	return oids
}
