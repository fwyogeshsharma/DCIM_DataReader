package snmp

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

// Collector polls one target device and returns TelemetryPackets.
type Collector struct {
	target *target.Target
	client *Client
	base   basePacket
	signer *packet.Signer

	// mgmtPort, when > 0, is the simulator SET management agent port (e.g. 1161).
	// When set, collectSystem reads sysName from it (live device name) so renames
	// reflect immediately instead of waiting on the lazily-patched 161 .snmprec.
	// 0 in production — real devices have no such agent.
	mgmtPort      int
	mgmtTimeoutMs int

	// log is used for diagnostic debug lines (e.g. a sensor walk that returns
	// PDUs but produces zero metric packets — the signature of an OID mismatch).
	// May be nil; all uses must guard.
	log *zap.Logger
}

type basePacket struct {
	orgID, dcID, floorID, netID, grpID, readerID string
}

// NewCollector creates a Collector. The caller must Close() when done.
// mgmtPort/mgmtTimeoutMs configure the optional live-sysName read against the
// simulator SET agent (0 = disabled / production).
func NewCollector(
	t *target.Target,
	client *Client,
	orgID, dcID, floorID, netID, grpID, readerID string,
	signer *packet.Signer,
	mgmtPort, mgmtTimeoutMs int,
	log *zap.Logger,
) *Collector {
	return &Collector{
		target:        t,
		client:        client,
		base:          basePacket{orgID, dcID, floorID, netID, grpID, readerID},
		signer:        signer,
		mgmtPort:      mgmtPort,
		mgmtTimeoutMs: mgmtTimeoutMs,
		log:           log,
	}
}

// Tier selects which class of SNMP work to perform on a single Collect() call.
//
// Industry-standard tiered polling: split work so the cheap stuff happens
// often (keeps liveness up to date) and the expensive walks happen rarely
// (keeps load on the SNMP agent low). For SNMPSim this is the difference
// between wedging the asyncio loop and steady-state operation.
type Tier int

const (
	TierFast         Tier = iota // sys.uptime only — 1 Get per device
	TierMedium                   // interface state (admin/oper/speed) — 3 walks per device
	TierSlow                     // interface counters, server HR/UCD, UPS, sensors
	TierTopology                 // LLDP neighbor walks — separate tier, fired once after discovery + periodic refresh
	TierEnvironment              // environment sensors ONLY (temperature/humidity) for sensor/PDU devices — light, for the heatmap without the heavy SLOW counter walk
	TierServerHealth             // server CPU/RAM via UCD-SNMP scalars ONLY — light (a few Gets, no HOST-RESOURCES walks), independent of the heavy SLOW tier

	// NumTiers is the count of Tier values; keep tier-indexed arrays sized to it.
	NumTiers = int(TierServerHealth) + 1
)

// Collect runs the MIB walks for the given tier and returns metric packets.
func (c *Collector) Collect(tier Tier) ([]*v1.TelemetryPacket, error) {
	var pkts []*v1.TelemetryPacket

	switch tier {
	case TierFast:
		// Primary liveness walk. Propagate its error so a timed-out / dead
		// device trips the circuit breaker instead of silently logging
		// "packets:0" and recording a false success.
		sysPkts, err := c.collectSystem()
		if err != nil {
			return nil, err
		}
		pkts = append(pkts, sysPkts...)
	case TierMedium:
		ifPkts, err := c.collectInterfacesStatus()
		if err != nil {
			return nil, err
		}
		pkts = append(pkts, ifPkts...)
		// Interface IP addresses (ipAddrTable) — folded in here so it reuses the
		// pooled persistent socket instead of the old standalone enrichment pass,
		// which opened a fresh socket per device and got starved by the single
		// responder (interface_addresses stayed empty). Best-effort: never fails the
		// tier.
		pkts = append(pkts, c.collectInterfaceAddresses()...)
	case TierSlow:
		ifPkts, err := c.collectInterfacesCounters()
		if err != nil {
			return nil, err
		}
		pkts = append(pkts, ifPkts...)
		switch c.target.DeviceType {
		case "server":
			if p, err := c.collectServerHR(); err == nil {
				pkts = append(pkts, p...)
			}
			if p, err := c.collectUCD(); err == nil {
				pkts = append(pkts, p...)
			}
		case "ups":
			if p, err := c.collectUPS(); err == nil {
				pkts = append(pkts, p...)
			}
		case "sensor":
			if p, err := c.collectSensors(); err == nil {
				pkts = append(pkts, p...)
			}
		}
	case TierEnvironment:
		// Light, heatmap-focused tier: ONLY the environment sensor walk, and only
		// for devices that carry sensors. No interface counters / HR / UCD — so it
		// adds minimal SNMP load. Errors are non-fatal (returns what it has).
		switch c.target.DeviceType {
		case "sensor":
			if p, err := c.collectSensors(); err == nil {
				pkts = append(pkts, p...)
			}
		case "pdu", "floor_pdu":
			// Enterprise PDU scalars (load/voltage/current/power/energy/freq +
			// temperature + humidity). The simulator serves these only on the
			// 99999.5 tree — the vendor sensor walks return nothing for PDUs.
			if p, err := c.collectPDU(); err == nil {
				pkts = append(pkts, p...)
			}
		case "generator":
			if p, err := c.collectGenerator(); err == nil {
				pkts = append(pkts, p...)
			}
		case "server":
			// Servers carry chassis/CPU temp in ENTITY-SENSOR-MIB. The heavy
			// HR/UCD walks live in the SLOW tier (often disabled), so collect the
			// light temp here so the heatmap covers servers regardless.
			if p, err := c.collectServerTemp(); err == nil {
				pkts = append(pkts, p...)
			}
		case "ups":
			if p, err := c.collectUPSTemp(); err == nil {
				pkts = append(pkts, p...)
			}
			if p, err := c.collectUPSEnterprise(); err == nil {
				pkts = append(pkts, p...)
			}
		}
	case TierServerHealth:
		// Light server CPU/RAM tier: UCD-SNMP scalars ONLY (cpu user/system/idle,
		// mem total/avail/cached/buffer) — a handful of Gets, no HOST-RESOURCES
		// walks. Lets us collect server CPU/RAM without enabling the heavy SLOW
		// tier. Best-effort, non-fatal; no-op for non-server device types.
		if c.target.DeviceType == "server" {
			if p, err := c.collectUCD(); err == nil {
				pkts = append(pkts, p...)
			}
		}
	case TierTopology:
		switch c.target.DeviceType {
		case "router", "switch", "firewall", "load_balancer":
			// Production fabric: these advertise production-layer neighbors in
			// LLDP. collectLLDP tags them layer="network".
			if p, err := c.collectLLDP(); err == nil {
				pkts = append(pkts, p...)
			}
		case "oob_switch":
			// Management plane: the simulator advertises the device↔OOB
			// (management-layer) links ONLY in each OOB switch's LLDP table —
			// production devices keep their OOB port off their LLDP. Without
			// walking OOB switches the management links are never discovered and
			// every link lands as "network". collectLLDP tags these "management"
			// (DeviceType == "oob_switch" branch in the layer classifier).
			if p, err := c.collectLLDP(); err == nil {
				pkts = append(pkts, p...)
			}
		}
	}

	return pkts, nil
}

// ─── system ──────────────────────────────────────────────────────────────────

func (c *Collector) collectSystem() ([]*v1.TelemetryPacket, error) {
	pkt, err := c.client.Get([]string{OIDSysUpTime, OIDSysName, OIDSysDescr})
	if err != nil {
		return nil, err
	}
	// Adopt the live sysName as the hostname FIRST, so the uptime packet built
	// below already carries the current name. A device renamed at the source
	// (e.g. in the simulator) thus propagates to the DB without an EDR restart or
	// a topology-file edit. DCS keys identity on mgmt_ip, so the row is renamed
	// in place rather than duplicated.
	var sysName string
	for _, p := range pkt.Variables {
		if strings.TrimPrefix(p.Name, ".") == OIDSysName {
			sysName = PDUString(p)
		}
	}
	// Simulator: the 161 .snmprec sysName is patched lazily (rename-time patch is
	// conditional; the periodic shard-sync lags minutes), so a rename can take
	// minutes to appear here. The SET management agent on mgmt_port serves the
	// LIVE in-memory device name, so prefer it when configured — renames then
	// reflect on the very next FAST poll. Falls back to the 161 sysName on any
	// failure. mgmt_port = 0 (production) skips this entirely.
	if c.mgmtPort > 0 {
		if live, ok := MgmtSysName(c.target.IP, uint16(c.mgmtPort), c.target.Community, c.mgmtTimeoutMs); ok {
			sysName = live
		}
	}
	if sysName != "" {
		c.target.SetHostname(sysName)
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pkt.Variables {
		// gosnmp PDU names have a leading "." — strip before comparing.
		name := strings.TrimPrefix(p.Name, ".")
		switch OIDColumn(name) {
		case strings.TrimSuffix(OIDSysUpTime, ".0"), OIDSysUpTime:
			pkts = append(pkts, c.newMetric("system.uptime_centiseconds", "", ToFloat64(p), c.hostMeta()))
		}
	}
	return pkts, nil
}

// ─── interfaces ──────────────────────────────────────────────────────────────

// collectInterfacesStatus walks only the cheap state columns (admin/oper/speed).
// Used by the MEDIUM tier — runs every 60s. ~3 walks per device.
func (c *Collector) collectInterfacesStatus() ([]*v1.TelemetryPacket, error) {
	return c.walkIfMap(IfMetricStatus)
}

// collectInterfacesCounters walks the expensive counter columns (octets, errors, etc).
// Used by the SLOW tier — runs every 5min. ~10 walks per device.
func (c *Collector) collectInterfacesCounters() ([]*v1.TelemetryPacket, error) {
	return c.walkIfMap(IfMetricCounters)
}

// collectInterfaceAddresses walks the IP address table (ipAdEntAddr + ipAdEntIfIndex)
// and emits one interface_address packet per (ifIndex, IP). DCS routes these to
// interface_addresses. Best-effort: returns nil on any walk error so it never
// fails the MEDIUM tier. Also derives prod/oob/loopback IPs onto the target so the
// device row's address columns populate.
func (c *Collector) collectInterfaceAddresses() []*v1.TelemetryPacket {
	addrPDUs, err := c.client.Walk(OIDIpAdEntAddr)
	if err != nil || len(addrPDUs) == 0 {
		return nil
	}
	idxPDUs, _ := c.client.Walk(OIDIpAdEntIfIndex)
	ipToIdx := make(map[string]int, len(idxPDUs))
	for _, p := range idxPDUs {
		if ip := ipSuffix(p.Name, OIDIpAdEntIfIndex); ip != "" {
			ipToIdx[ip] = int(ToFloat64(p))
		}
	}

	var prodIP, oobIP string
	var pkts []*v1.TelemetryPacket
	for _, p := range addrPDUs {
		ip := ipSuffix(p.Name, OIDIpAdEntAddr)
		if ip == "" {
			continue
		}
		if strings.HasPrefix(ip, "10.") && prodIP == "" {
			prodIP = ip
		}
		if strings.HasPrefix(ip, "172.") && oobIP == "" {
			oobIP = ip
		}
		meta := c.hostMeta()
		meta["interface_index"] = strconv.Itoa(ipToIdx[ip])
		meta["address"] = ip
		meta["address_family"] = "ipv4"
		pkts = append(pkts, c.newInterfaceAddress(ip, meta))
	}

	// Backfill target IPs so the device row's prod/oob/loopback columns fill in.
	if prodIP != "" {
		c.target.ProdIP = prodIP
	}
	if oobIP != "" {
		c.target.OOBIP = oobIP
	}
	switch c.target.DeviceType {
	case "router", "switch", "firewall", "load_balancer":
		if prodIP != "" {
			c.target.LoopbackIP = prodIP
		}
	}
	return pkts
}

// ipSuffix returns the dotted-IP suffix of an ipAddrTable OID (the part after the
// column prefix), e.g. "1.3.6.1.2.1.4.20.1.1.10.50.0.8" → "10.50.0.8".
func ipSuffix(oidName, columnPrefix string) string {
	name := strings.TrimPrefix(oidName, ".")
	pfx := strings.TrimPrefix(columnPrefix, ".") + "."
	if !strings.HasPrefix(name, pfx) {
		return ""
	}
	return strings.TrimPrefix(name, pfx)
}

// newInterfaceAddress builds a Kind="interface_address" packet (DCS upserts it
// into interface_addresses by device + ifIndex).
func (c *Collector) newInterfaceAddress(addr string, meta map[string]string) *v1.TelemetryPacket {
	now := time.Now().UnixNano()
	id := packet.NewID()
	nonce := c.signer.NextNonce()
	canonical := packet.CanonicalBytes(id, c.target.SourceID(), now, "interface.address", addr, 0, nonce)
	sig := c.signer.Sign(canonical)
	return &v1.TelemetryPacket{
		Id:           id,
		OrgId:        c.base.orgID,
		DatacenterId: c.base.dcID,
		FloorId:      c.base.floorID,
		NetworkId:    c.base.netID,
		GroupId:      c.base.grpID,
		SourceType:   "device",
		SourceId:     c.target.SourceID(),
		ReaderId:     c.base.readerID,
		TimestampNs:  now,
		Name:         "interface.address",
		Tag:          addr,
		Value:        0,
		Meta:         meta,
		Kind:         "interface_address",
		Signature:    sig,
		Nonce:        nonce,
	}
}

// walkIfMap is the shared driver for both interface walks. Resolves the
// canonical interface name once per call by walking ifXTable.ifName and
// falling back to ifTable.ifDescr (which carries vendor-style port names
// like "GigabitEthernet0/1" or "eth1"). LLDP emits the same human-readable
// names in its port-id varbinds — so using ifDescr here makes name-based
// topology lookups (src_interface_id / dst_interface_id) succeed.
func (c *Collector) walkIfMap(oidMap map[string]string) ([]*v1.TelemetryPacket, error) {
	nameMap := make(map[string]string) // ifIndex → preferred name

	// Primary source: ifXTable.ifName.
	namePDUs, _ := c.client.Walk(OIDIfName)
	for _, p := range namePDUs {
		idx := LastOIDComponent(p.Name)
		if s, ok := p.Value.(string); ok && s != "" {
			nameMap[idx] = s
		}
	}
	// Fallback source: ifTable.ifDescr. Same human-readable string LLDP uses
	// in lldpRemPortId / lldpLocPortDesc, so neighbor lookups match.
	descrPDUs, _ := c.client.Walk(OIDIfDescr)
	for _, p := range descrPDUs {
		idx := LastOIDComponent(p.Name)
		if _, ok := nameMap[idx]; ok {
			continue // ifName already set
		}
		switch v := p.Value.(type) {
		case string:
			if v != "" {
				nameMap[idx] = v
			}
		case []byte:
			if len(v) > 0 {
				nameMap[idx] = string(v)
			}
		}
	}

	var pkts []*v1.TelemetryPacket
	for oidPrefix, metricName := range oidMap {
		pdus, err := c.client.Walk(oidPrefix)
		if err != nil {
			continue
		}
		for _, p := range pdus {
			idx := LastOIDComponent(p.Name)
			ifName := nameMap[idx]
			if ifName == "" {
				// Last-resort synthetic name. Won't match LLDP, but the
				// ifIndex fallback in DCS topology resolution still works.
				ifName = "if" + idx
			}
			meta := c.hostMeta()
			meta["interface_index"] = idx
			meta["interface_name"] = ifName
			pkts = append(pkts, c.newMetric(metricName, ifName, ToFloat64(p), meta))
		}
	}
	return pkts, nil
}

// ─── server HR-MIB ───────────────────────────────────────────────────────────

func (c *Collector) collectServerHR() ([]*v1.TelemetryPacket, error) {
	var pkts []*v1.TelemetryPacket

	// CPU per core
	cpuPDUs, err := c.client.Walk(OIDHrProcessorLoad)
	if err == nil {
		for _, p := range cpuPDUs {
			idx := LastOIDComponent(p.Name)
			pkts = append(pkts, c.newMetric("server.cpu_per_core_percent", idx, ToFloat64(p),
				c.hostMeta()))
		}
	}

	// Storage: collect type, alloc units, size, used; compute KB values
	typeMap := walkIndexed(c.client, OIDHrStorageType)
	descrMap := walkIndexedStr(c.client, OIDHrStorageDescr)
	allocMap := walkIndexed(c.client, OIDHrStorageAllocUnits)
	sizePDUs, _ := c.client.Walk(OIDHrStorageSize)
	usedPDUs, _ := c.client.Walk(OIDHrStorageUsed)

	for _, p := range sizePDUs {
		idx := LastOIDComponent(p.Name)
		allocUnits := allocMap[idx]
		sizeUnits := ToFloat64(p)
		usedUnits := walkIndexed(c.client, OIDHrStorageUsed)[idx]
		sizeKB := sizeUnits * allocUnits / 1024
		usedKB := usedUnits * allocUnits / 1024
		avail := sizeKB - usedKB

		descr := descrMap[idx]
		if descr == "" {
			descr = idx
		}
		storType := int(typeMap[idx])
		typeName := hrStorageTypeName(storType)
		meta := c.hostMeta()
		meta["storage_type"] = typeName
		meta["storage_descr"] = descr
		pkts = append(pkts,
			c.newMetric("server.storage_size_kb", descr, sizeKB, meta),
			c.newMetric("server.storage_used_kb", descr, usedKB, meta),
			c.newMetric("server.storage_available_kb", descr, avail, meta),
		)
		_ = usedPDUs // consumed via walkIndexed above
	}

	return pkts, nil
}

// ─── UCD-SNMP ────────────────────────────────────────────────────────────────

func (c *Collector) collectUCD() ([]*v1.TelemetryPacket, error) {
	oids := []string{
		OIDUcdSsCpuUser, OIDUcdSsCpuSystem, OIDUcdSsCpuIdle,
		OIDUcdMemTotalReal, OIDUcdMemAvailReal, OIDUcdMemCached, OIDUcdMemBuffer,
	}
	pkt, err := c.client.Get(oids)
	if err != nil {
		return nil, err
	}
	nameMap := map[string]string{
		OIDUcdSsCpuUser:    "server.cpu_user_percent",
		OIDUcdSsCpuSystem:  "server.cpu_system_percent",
		OIDUcdSsCpuIdle:    "server.cpu_idle_percent",
		OIDUcdMemTotalReal: "server.memory_total_kb",
		OIDUcdMemAvailReal: "server.memory_available_kb",
		OIDUcdMemCached:    "server.memory_cached_kb",
		OIDUcdMemBuffer:    "server.memory_buffer_kb",
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pkt.Variables {
		col := strings.TrimPrefix(p.Name, ".")
		if name, ok := nameMap[col]; ok {
			pkts = append(pkts, c.newMetric(name, "", ToFloat64(p), c.hostMeta()))
		}
	}
	return pkts, nil
}

// ─── UPS-MIB ─────────────────────────────────────────────────────────────────

func (c *Collector) collectUPS() ([]*v1.TelemetryPacket, error) {
	scalars := []string{
		OIDUpsBatteryStatus,
		OIDUpsSecondsOnBattery,
		OIDUpsEstimatedMinutesRemain,
		OIDUpsEstimatedChargeRemaining,
		OIDUpsBatteryVoltage,
	}
	pkt, err := c.client.Get(scalars)
	if err != nil {
		return nil, err
	}
	nameMap := map[string]string{
		OIDUpsBatteryStatus:            "environment.ups_battery_status",
		OIDUpsSecondsOnBattery:         "environment.ups_seconds_on_battery",
		OIDUpsEstimatedMinutesRemain:   "environment.ups_minutes_remaining",
		OIDUpsEstimatedChargeRemaining: "environment.ups_charge_percent",
		OIDUpsBatteryVoltage:           "environment.ups_battery_voltage_v",
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pkt.Variables {
		col := strings.TrimPrefix(p.Name, ".")
		if name, ok := nameMap[col]; ok {
			pkts = append(pkts, c.newMetric(name, "", ToFloat64(p), c.hostMeta()))
		}
	}

	// Input/output tables
	inVoltPDUs, _ := c.client.Walk(OIDUpsInputVoltage)
	for _, p := range inVoltPDUs {
		pkts = append(pkts, c.newMetric("environment.ups_input_voltage_v",
			LastOIDComponent(p.Name), ToFloat64(p), c.hostMeta()))
	}
	outVoltPDUs, _ := c.client.Walk(OIDUpsOutputVoltage)
	for _, p := range outVoltPDUs {
		pkts = append(pkts, c.newMetric("environment.ups_output_voltage_v",
			LastOIDComponent(p.Name), ToFloat64(p), c.hostMeta()))
	}
	outCurrPDUs, _ := c.client.Walk(OIDUpsOutputCurrent)
	for _, p := range outCurrPDUs {
		pkts = append(pkts, c.newMetric("environment.ups_output_current_a",
			LastOIDComponent(p.Name), ToFloat64(p), c.hostMeta()))
	}
	outLoadPDUs, _ := c.client.Walk(OIDUpsOutputLoad)
	for _, p := range outLoadPDUs {
		pkts = append(pkts, c.newMetric("environment.ups_output_load_percent",
			LastOIDComponent(p.Name), ToFloat64(p), c.hostMeta()))
	}
	return pkts, nil
}

// ─── enterprise power-device scalars (PDU / generator / UPS-enterprise) ──────

// scalarMetric describes one enterprise scalar column (base.col.0): the metric
// name to emit, an optional tag, and the divisor that undoes the simulator's
// fixed-point ×N encoding (1 = no scaling).
type scalarMetric struct {
	col   string
	name  string
	tag   string
	scale float64
}

// pduScalars maps the simulator's _pdu_entries (99999.5.N.0) to metrics. Temp
// and humidity are folded into the shared environment.* names (tag PDU) so they
// land on the heatmap alongside sensor/server/ups readings.
var pduScalars = []scalarMetric{
	{"1", "pdu.load_percent", "", 1},
	{"2", "pdu.voltage_v", "", 1},
	{"3", "pdu.power_factor", "", 100},
	{"4", "pdu.phase_imbalance_percent", "", 1},
	{"5", "pdu.outlet_status", "", 1},
	{"6", "pdu.breaker_status", "", 1},
	{"7", "pdu.outlet_failure", "", 1},
	{"8", "pdu.smoke_detected", "", 1},
	{"9", "pdu.current_a", "", 10},
	{"10", "pdu.ground_fault", "", 1},
	{"11", "pdu.real_power_w", "", 1},
	{"12", "pdu.apparent_power_va", "", 1},
	{"13", "pdu.energy_kwh", "", 10},
	{"14", "pdu.frequency_hz", "", 10},
	{"15", "environment.temperature_c", "PDU", 10},
	{"16", "environment.humidity_percent", "", 10},
	{"17", "pdu.outlet_power_w", "", 1},
}

// genScalars maps the simulator's _generator_entries (99999.7.N.0) to metrics.
var genScalars = []scalarMetric{
	{"1", "generator.fuel_percent", "", 1},
	{"2", "generator.run_hours", "", 1},
	{"3", "generator.status", "", 1},
	{"4", "generator.load_percent", "", 1},
	{"5", "generator.output_kw", "", 1},
	{"6", "generator.output_voltage_v", "PhA", 10},
	{"7", "generator.output_voltage_v", "PhB", 10},
	{"8", "generator.output_voltage_v", "PhC", 10},
	{"9", "generator.frequency_hz", "", 10},
	{"10", "generator.coolant_status", "", 1},
	{"11", "generator.oil_pressure_status", "", 1},
	{"12", "generator.battery_status", "", 1},
	{"13", "generator.start_attempts", "", 1},
}

// upsEntScalars maps the UPS enterprise status block (99999.4.N.0) to metrics.
var upsEntScalars = []scalarMetric{
	{"1", "environment.ups_fan_status", "", 1},
	{"2", "environment.ups_charger_status", "", 1},
	{"3", "environment.ups_rectifier_status", "", 1},
	{"4", "environment.ups_phase_status", "", 1},
	{"5", "environment.ups_battery_status_ex", "", 1},
}

func (c *Collector) collectPDU() ([]*v1.TelemetryPacket, error) {
	return c.collectScalars(OIDPDUBase, pduScalars)
}

func (c *Collector) collectGenerator() ([]*v1.TelemetryPacket, error) {
	return c.collectScalars(OIDGeneratorBase, genScalars)
}

func (c *Collector) collectUPSEnterprise() ([]*v1.TelemetryPacket, error) {
	return c.collectScalars(OIDUPSEntBase, upsEntScalars)
}

// collectScalars reads a set of enterprise scalar OIDs (base.col.0) in a single
// SNMP GET and emits one metric per readable varbind. Unanswered columns
// (NoSuchInstance/Object) are skipped so a partial agent never fails the tier.
func (c *Collector) collectScalars(base string, table []scalarMetric) ([]*v1.TelemetryPacket, error) {
	oids := make([]string, len(table))
	idx := make(map[string]scalarMetric, len(table))
	for i, s := range table {
		oids[i] = base + "." + s.col + ".0"
		idx[s.col] = s
	}
	pkt, err := c.client.Get(oids)
	if err != nil {
		return nil, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pkt.Variables {
		switch p.Type {
		case gosnmp.NoSuchObject, gosnmp.NoSuchInstance, gosnmp.EndOfMibView, gosnmp.Null:
			continue
		}
		s, ok := idx[scalarCol(p.Name, base)]
		if !ok {
			continue
		}
		scale := s.scale
		if scale == 0 {
			scale = 1
		}
		pkts = append(pkts, c.newMetric(s.name, s.tag, ToFloat64(p)/scale, c.hostMeta()))
	}
	return pkts, nil
}

// scalarCol extracts the column number from an enterprise scalar OID, i.e. the
// "N" in base.N.0 (e.g. "1.3.6.1.4.1.99999.5.15.0" with base 99999.5 → "15").
func scalarCol(oidName, base string) string {
	n := strings.TrimPrefix(oidName, ".")
	n = strings.TrimPrefix(n, strings.TrimPrefix(base, ".")+".")
	return strings.TrimSuffix(n, ".0")
}

// collectUPSTemp reads upsBatteryTemperature (whole °C). Emitted as
// environment.temperature_c (tag BATTERY) so UPS thermals join the heatmap
// alongside network/server/sensor temperature. Lives in the ENVIRONMENT tier
// because the rest of the UPS walk is in the often-disabled SLOW tier.
func (c *Collector) collectUPSTemp() ([]*v1.TelemetryPacket, error) {
	pkt, err := c.client.Get([]string{OIDUpsBatteryTemperature})
	if err != nil {
		return nil, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pkt.Variables {
		switch p.Type {
		case gosnmp.NoSuchObject, gosnmp.NoSuchInstance, gosnmp.EndOfMibView, gosnmp.Null:
			continue
		}
		meta := c.hostMeta()
		meta["sensor_index"] = "battery"
		pkts = append(pkts, c.newMetric("environment.temperature_c", "BATTERY", ToFloat64(p), meta))
	}
	return pkts, nil
}

// ─── server temperature (ENTITY-SENSOR-MIB) ──────────────────────────────────

// collectServerTemp walks ENTITY-SENSOR-MIB entPhySensorValue. Servers expose
// chassis/inlet temp at index 1 and CPU die temp at index 2 (×10 Celsius).
// Emitted as environment.temperature_c tagged CHASSIS / CPU to match the
// network-device (CISCO-ENVMON / gNMI) convention so all temperature lives under
// one metric name for the heatmap.
func (c *Collector) collectServerTemp() ([]*v1.TelemetryPacket, error) {
	pdus, err := c.client.Walk(OIDEntPhySensorValue)
	if err != nil {
		return nil, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pdus {
		idx := LastOIDComponent(p.Name)
		meta := c.hostMeta()
		meta["sensor_index"] = idx
		pkts = append(pkts, c.newMetric("environment.temperature_c", entSensorTag(idx), ToFloat64(p)/10.0, meta))
	}
	return pkts, nil
}

// entSensorTag maps an entPhySensorValue row index to the same tag vocabulary
// the network temp path uses (index 1 = chassis/inlet, 2 = CPU).
func entSensorTag(idx string) string {
	switch idx {
	case "1":
		return "CHASSIS"
	case "2":
		return "CPU"
	default:
		return "SENSOR" + idx
	}
}

// ─── sensors ─────────────────────────────────────────────────────────────────

func (c *Collector) collectSensors() ([]*v1.TelemetryPacket, error) {
	switch c.target.Vendor {
	case "raritan":
		return c.collectRaritanSensors()
	case "vertiv":
		return c.collectVertivSensors()
	case "apc":
		return c.collectAPCSensors()
	default:
		// Try Raritan first, then Vertiv as fallback
		pkts, err := c.collectRaritanSensors()
		if err == nil && len(pkts) > 0 {
			return pkts, nil
		}
		return c.collectVertivSensors()
	}
}

func (c *Collector) collectRaritanSensors() ([]*v1.TelemetryPacket, error) {
	typeMap := walkIndexedInt(c.client, OIDRaritanSensorType)
	valuePDUs, err := c.client.Walk(OIDRaritanSensorValue)
	if err != nil {
		return nil, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range valuePDUs {
		idx := LastOIDComponent(p.Name)
		sensorType := typeMap[idx]
		val := ToFloat64(p) / 10.0 // Raritan encodes ×10
		meta := c.hostMeta()
		meta["sensor_index"] = idx
		switch sensorType {
		case RaritanTypeTemp:
			pkts = append(pkts, c.newMetric("environment.temperature_c", idx, val, meta))
		case RaritanTypeHumidity:
			pkts = append(pkts, c.newMetric("environment.humidity_percent", idx, val, meta))
		}
	}
	c.warnEmptySensorWalk("raritan", len(valuePDUs), len(pkts))
	return pkts, nil
}

func (c *Collector) collectVertivSensors() ([]*v1.TelemetryPacket, error) {
	var pkts []*v1.TelemetryPacket
	seen := 0
	if p, n, err := walkEnvMetric(c.client, OIDVertivTempValue, "environment.temperature_c", c); err == nil {
		pkts, seen = append(pkts, p...), seen+n
	}
	if p, n, err := walkEnvMetric(c.client, OIDVertivHumValue, "environment.humidity_percent", c); err == nil {
		pkts, seen = append(pkts, p...), seen+n
	}
	if p, n, err := walkEnvMetric(c.client, OIDVertivDewValue, "environment.dew_point_c", c); err == nil {
		pkts, seen = append(pkts, p...), seen+n
	}
	c.warnEmptySensorWalk("vertiv", seen, len(pkts))
	return pkts, nil
}

// collectAPCSensors reads the APC NetBotz sensor table. Classification is by the
// per-row LABEL column (.2.{idx}: "Temperature"/"Humidity"/"Airflow"), falling
// back to the row index (1=temperature, 2=humidity, 3=airflow) when the label is
// absent. The simulator's type column (.11.{idx}) carries the sensor STATE
// (4=normal), not a usable type code, so the old switch on it matched nothing and
// dropped every reading. Per-type scaling matches the simulator encoding:
// temperature and airflow are ×10 (÷10 here); humidity is written as a whole
// percent (no division).
func (c *Collector) collectAPCSensors() ([]*v1.TelemetryPacket, error) {
	labelMap := walkIndexedStr(c.client, OIDAPCNetBotzLabel)
	valuePDUs, err := c.client.Walk(OIDAPCNetBotzValue)
	if err != nil {
		return nil, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range valuePDUs {
		idx := LastOIDComponent(p.Name)
		meta := c.hostMeta()
		meta["sensor_index"] = idx
		raw := ToFloat64(p)

		switch apcSensorKind(labelMap[idx], idx) {
		case "temperature":
			pkts = append(pkts, c.newMetric("environment.temperature_c", idx, raw/10.0, meta))
		case "humidity":
			pkts = append(pkts, c.newMetric("environment.humidity_percent", idx, raw, meta))
		case "airflow":
			pkts = append(pkts, c.newMetric("environment.airflow_cfm", idx, raw/10.0, meta))
		}
	}
	c.warnEmptySensorWalk("apc", len(valuePDUs), len(pkts))
	return pkts, nil
}

// apcSensorKind classifies an APC NetBotz sensor row by its label, falling back
// to the row index (1=temperature, 2=humidity, 3=airflow) when the label is
// empty or unrecognised.
func apcSensorKind(label, idx string) string {
	switch l := strings.ToLower(label); {
	case strings.Contains(l, "temp"):
		return "temperature"
	case strings.Contains(l, "humid"):
		return "humidity"
	case strings.Contains(l, "airflow"), strings.Contains(l, "air flow"):
		return "airflow"
	}
	switch idx {
	case "1":
		return "temperature"
	case "2":
		return "humidity"
	case "3":
		return "airflow"
	}
	return ""
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (c *Collector) newMetric(name, tag string, value float64, meta map[string]string) *v1.TelemetryPacket {
	now := time.Now().UnixNano()
	id := packet.NewID()
	nonce := c.signer.NextNonce()
	canonical := packet.CanonicalBytes(id, c.target.SourceID(), now, name, tag, value, nonce)
	sig := c.signer.Sign(canonical)

	return &v1.TelemetryPacket{
		Id:           id,
		OrgId:        c.base.orgID,
		DatacenterId: c.base.dcID,
		FloorId:      c.base.floorID,
		NetworkId:    c.base.netID,
		GroupId:      c.base.grpID,
		SourceType:   "device",
		SourceId:     c.target.SourceID(),
		ReaderId:     c.base.readerID,
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

func (c *Collector) hostMeta() map[string]string {
	a := c.target.Asset()
	m := map[string]string{
		// Network identity
		"hostname":    c.target.SourceID(),
		"mgmt_ip":     c.target.MgmtIP, // operator-facing mgmt IP (192.168.x in sim)
		"prod_ip":     c.target.ProdIP, // production / data-plane IP (10.x in sim)
		"loopback_ip": c.target.LoopbackIP,
		"oob_ip":      c.target.OOBIP,
		// Device identity
		"device_type": c.target.DeviceType,
		"vendor":      c.target.Vendor,
		"model_name":  a.ModelName,
		// Physical location (seeded from topology JSON; updated in place after a
		// UI edit is applied so the change reflects on the next push)
		"country":         a.Country,
		"datacenter":      a.DatacenterName,
		"datacenter_city": a.DatacenterCity,
		"room":            a.Room,
		// Collector provenance
		"collector_agent":    "EDR",
		"collector_protocol": "SNMP",
		"snmp_enabled":       "true",
		"gnmi_enabled":       strconv.FormatBool(c.target.Has(target.CapGNMI)),
	}
	if a.RackRow > 0 {
		m["rack_row"] = strconv.Itoa(a.RackRow)
	}
	if a.RackNum > 0 {
		m["rack_num"] = strconv.Itoa(a.RackNum)
	}
	if a.RackUnit > 0 {
		m["rack_unit"] = strconv.Itoa(a.RackUnit)
	}
	for k, v := range c.target.Labels {
		m[k] = v
	}
	return m
}

func walkIndexed(cl *Client, oid string) map[string]float64 {
	m := make(map[string]float64)
	pdus, err := cl.Walk(oid)
	if err != nil {
		return m
	}
	for _, p := range pdus {
		m[LastOIDComponent(p.Name)] = ToFloat64(p)
	}
	return m
}

func walkIndexedInt(cl *Client, oid string) map[string]int {
	m := make(map[string]int)
	for k, v := range walkIndexed(cl, oid) {
		m[k] = int(v)
	}
	return m
}

func walkIndexedStr(cl *Client, oid string) map[string]string {
	m := make(map[string]string)
	pdus, err := cl.Walk(oid)
	if err != nil {
		return m
	}
	for _, p := range pdus {
		switch v := p.Value.(type) {
		case string:
			m[LastOIDComponent(p.Name)] = v
		case []byte:
			m[LastOIDComponent(p.Name)] = string(v)
		}
	}
	return m
}

// walkEnvMetric walks one environment OID and emits one metric per PDU (÷10 to
// undo the simulator's ×10 encoding). Returns the packets and the number of PDUs
// the walk returned (so callers can detect a walk that yielded rows but no usable
// packets — the signature of an OID mismatch).
func walkEnvMetric(cl *Client, oid, name string, c *Collector) ([]*v1.TelemetryPacket, int, error) {
	pdus, err := cl.Walk(oid)
	if err != nil {
		return nil, 0, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pdus {
		idx := LastOIDComponent(p.Name)
		meta := c.hostMeta()
		meta["sensor_index"] = idx
		pkts = append(pkts, c.newMetric(name, idx, ToFloat64(p)/10.0, meta))
	}
	return pkts, len(pdus), nil
}

// warnEmptySensorWalk logs a debug line when a sensor walk returned PDUs but
// produced zero metric packets — i.e. every reading was filtered out by
// type/label classification, the classic symptom of a wrong type/value OID.
func (c *Collector) warnEmptySensorWalk(vendor string, pduCount, pktCount int) {
	if c.log == nil || pduCount == 0 || pktCount > 0 {
		return
	}
	c.log.Debug("sensor walk returned PDUs but produced no metrics — check sensor OID mapping",
		zap.String("vendor", vendor),
		zap.String("hostname", c.target.SourceID()),
		zap.String("device_type", c.target.DeviceType),
		zap.Int("pdus", pduCount))
}

func hrStorageTypeName(t int) string {
	switch t {
	case 1:
		return "other"
	case 2:
		return "unknown"
	case 3:
		return "fixed-disk"
	case 4:
		return "removal-disk"
	case 5:
		return "floppy-disk"
	case 6:
		return "compact-disc"
	case 7:
		return "ram-disk"
	case 8:
		return "flash-memory"
	case 9:
		return "network-disk"
	default:
		return strconv.Itoa(t)
	}
}

// PDUString returns the string value of a PDU, or empty if not a string type.
func PDUString(pdu gosnmp.SnmpPDU) string {
	switch v := pdu.Value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	return fmt.Sprintf("%v", pdu.Value)
}

// ─── LLDP topology ───────────────────────────────────────────────────────────

// collectLLDP walks the LLDP-MIB remote table and returns one topology packet
// per discovered neighbor. Only called for router/switch/firewall/load_balancer.
func (c *Collector) collectLLDP() ([]*v1.TelemetryPacket, error) {
	// Walk lldpRemSysName — index: timeMark.portNum.remIdx
	remSysNames := make(map[string]string)
	pdus, err := c.client.Walk(OIDLLDPRemSysName)
	if err != nil || len(pdus) == 0 {
		return nil, nil
	}
	for _, p := range pdus {
		suf := oidSuffix(p.Name, OIDLLDPRemSysName)
		if suf != "" {
			remSysNames[suf] = PDUString(p)
		}
	}
	if len(remSysNames) == 0 {
		return nil, nil
	}

	remPortIds := make(map[string]string)
	if pdus2, err2 := c.client.Walk(OIDLLDPRemPortId); err2 == nil {
		for _, p := range pdus2 {
			if suf := oidSuffix(p.Name, OIDLLDPRemPortId); suf != "" {
				remPortIds[suf] = PDUString(p)
			}
		}
	}

	// lldpRemChassisId carries the neighbor's MANAGEMENT IP (the simulator encodes
	// chassis-id = neighbor.mgmt_ip with subtype network-address). This is a STABLE
	// identifier: unlike lldpRemSysName it never changes when a device is renamed.
	// DCS resolves the link's far end by this mgmt_ip first (hostname only as
	// fallback), so renames and name/shard-sync lag no longer drop links.
	remChassis := make(map[string]string)
	if pdus5, err5 := c.client.Walk(OIDLLDPRemChassisId); err5 == nil {
		for _, p := range pdus5 {
			if suf := oidSuffix(p.Name, OIDLLDPRemChassisId); suf != "" {
				remChassis[suf] = PDUString(p)
			}
		}
	}

	// Local port name resolution — three levels of fallback:
	//   1. lldpLocPortId   (portNum → port identifier string)
	//   2. lldpLocPortDesc (portNum → port description)
	//   3. ifName by portNum (portNum == ifIndex in most LLDP implementations)
	locPortNames := make(map[string]string)
	if pdus3, err3 := c.client.Walk(OIDLLDPLocPortId); err3 == nil {
		for _, p := range pdus3 {
			if v := PDUString(p); v != "" {
				locPortNames[LastOIDComponent(p.Name)] = v
			}
		}
	}
	if pdus4, err4 := c.client.Walk(OIDLLDPLocPortDesc); err4 == nil {
		for _, p := range pdus4 {
			idx := LastOIDComponent(p.Name)
			if locPortNames[idx] == "" {
				if v := PDUString(p); v != "" {
					locPortNames[idx] = v
				}
			}
		}
	}
	// ifDescr fallback: LLDP portNum is 0-indexed; ifDescr uses 1-indexed ifIndex,
	// so look up ifDescr[portNum+1] to get the correct interface name.
	ifDescrs := walkIndexedStr(c.client, OIDIfDescr)

	var pkts []*v1.TelemetryPacket
	for suf, remoteSysName := range remSysNames {
		if remoteSysName == "" {
			continue
		}
		parts := strings.Split(suf, ".")
		if len(parts) < 3 {
			continue
		}
		portNum := parts[1]

		localPort := locPortNames[portNum]
		if localPort == "" {
			portNumInt, _ := strconv.Atoi(portNum)
			localPort = ifDescrs[strconv.Itoa(portNumInt+1)]
		}
		if localPort == "" {
			localPort = "if" + portNum
		}

		// LLDP portNum is 0-indexed; SNMP IF-MIB ifIndex is 1-indexed. Emit
		// the +1-corrected value so DCS's ifIndex fallback resolves correctly.
		portNumInt, _ := strconv.Atoi(portNum)
		meta := c.hostMeta()
		meta["local_port_id"] = localPort
		meta["local_port_index"] = strconv.Itoa(portNumInt + 1)
		meta["remote_sys_name"] = remoteSysName
		meta["remote_port_id"] = remPortIds[suf]
		// Stable far-end identifier (mgmt IP from lldpRemChassisId). Only set when
		// it parses as an IP — real devices may use a MAC chassis-id subtype, which
		// we ignore so DCS falls back to hostname resolution.
		if ch := remChassis[suf]; ch != "" && net.ParseIP(ch) != nil {
			meta["remote_mgmt_ip"] = ch
		}
		// Classify the link layer so the UI can render the production fabric only.
		// A link is "management" when either endpoint is an out-of-band management
		// switch (OOB-SW-*) or this device is itself an OOB switch — i.e. it rides
		// the management network, not the production data-plane. Everything else is
		// "network" (production). Without this, mgmt links were all tagged "network"
		// and inflated the topology past the real production link count.
		layer := "network"
		if c.target.DeviceType == "oob_switch" ||
			strings.Contains(strings.ToUpper(remoteSysName), "OOB") {
			layer = "management"
		}
		meta["layer"] = layer

		pkts = append(pkts, c.newTopology("lldp.neighbor", localPort, meta))
	}
	return pkts, nil
}

func (c *Collector) newTopology(name, tag string, meta map[string]string) *v1.TelemetryPacket {
	now := time.Now().UnixNano()
	id := packet.NewID()
	nonce := c.signer.NextNonce()
	canonical := packet.CanonicalBytes(id, c.target.SourceID(), now, name, tag, 0, nonce)
	sig := c.signer.Sign(canonical)
	return &v1.TelemetryPacket{
		Id:           id,
		OrgId:        c.base.orgID,
		DatacenterId: c.base.dcID,
		FloorId:      c.base.floorID,
		NetworkId:    c.base.netID,
		GroupId:      c.base.grpID,
		SourceType:   "device",
		SourceId:     c.target.SourceID(),
		ReaderId:     c.base.readerID,
		TimestampNs:  now,
		Name:         name,
		Tag:          tag,
		Value:        0,
		Meta:         meta,
		Kind:         "topology",
		Signature:    sig,
		Nonce:        nonce,
	}
}

// oidSuffix returns the index suffix after prefix in a dotted OID string.
// e.g. oidSuffix(".1.0.8802.1.1.2.1.4.1.1.9.0.1.2", "1.0.8802.1.1.2.1.4.1.1.9") → "0.1.2"
func oidSuffix(pduName, prefix string) string {
	n := strings.TrimPrefix(pduName, ".")
	p := strings.TrimPrefix(prefix, ".")
	if strings.HasPrefix(n, p+".") {
		return n[len(p)+1:]
	}
	return ""
}
