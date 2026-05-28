package snmp

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"

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
}

type basePacket struct {
	orgID, dcID, floorID, netID, grpID, readerID string
}

// NewCollector creates a Collector. The caller must Close() when done.
func NewCollector(
	t *target.Target,
	client *Client,
	orgID, dcID, floorID, netID, grpID, readerID string,
	signer *packet.Signer,
) *Collector {
	return &Collector{
		target: t,
		client: client,
		base:   basePacket{orgID, dcID, floorID, netID, grpID, readerID},
		signer: signer,
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
	TierFast     Tier = iota // sys.uptime only — 1 Get per device
	TierMedium               // interface state (admin/oper/speed) — 3 walks per device
	TierSlow                 // interface counters, server HR/UCD, UPS, sensors
	TierTopology             // LLDP neighbor walks — separate tier, fired once after discovery + periodic refresh
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
	case TierTopology:
		switch c.target.DeviceType {
		case "router", "switch", "firewall", "load_balancer":
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
	return pkts, nil
}

func (c *Collector) collectVertivSensors() ([]*v1.TelemetryPacket, error) {
	var pkts []*v1.TelemetryPacket
	if p, err := walkEnvMetric(c.client, OIDVertivTempValue, "environment.temperature_c", c); err == nil {
		pkts = append(pkts, p...)
	}
	if p, err := walkEnvMetric(c.client, OIDVertivHumValue, "environment.humidity_percent", c); err == nil {
		pkts = append(pkts, p...)
	}
	if p, err := walkEnvMetric(c.client, OIDVertivDewValue, "environment.dew_point_c", c); err == nil {
		pkts = append(pkts, p...)
	}
	return pkts, nil
}

func (c *Collector) collectAPCSensors() ([]*v1.TelemetryPacket, error) {
	typeMap := walkIndexedInt(c.client, OIDAPCNetBotzSensorType)
	valuePDUs, err := c.client.Walk(OIDAPCNetBotzValue)
	if err != nil {
		return nil, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range valuePDUs {
		idx := LastOIDComponent(p.Name)
		meta := c.hostMeta()
		meta["sensor_index"] = idx
		val := ToFloat64(p) / 10.0
		switch typeMap[idx] {
		case 1: // temperature
			pkts = append(pkts, c.newMetric("environment.temperature_c", idx, val, meta))
		case 2: // humidity
			pkts = append(pkts, c.newMetric("environment.humidity_percent", idx, val, meta))
		case 3: // airflow
			pkts = append(pkts, c.newMetric("environment.airflow_cfm", idx, val, meta))
		}
	}
	return pkts, nil
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
		"model_name":  c.target.ModelName,
		// Physical location (populated from topology JSON; empty in discovery mode)
		"country":         c.target.Country,
		"datacenter":      c.target.DatacenterName,
		"datacenter_city": c.target.DatacenterCity,
		"room":            c.target.Room,
		// Collector provenance
		"collector_agent":    "EDR",
		"collector_protocol": "SNMP",
		"snmp_enabled":       "true",
		"gnmi_enabled":       strconv.FormatBool(c.target.Has(target.CapGNMI)),
	}
	if c.target.RackRow > 0 {
		m["rack_row"] = strconv.Itoa(c.target.RackRow)
	}
	if c.target.RackNum > 0 {
		m["rack_num"] = strconv.Itoa(c.target.RackNum)
	}
	if c.target.RackUnit > 0 {
		m["rack_unit"] = strconv.Itoa(c.target.RackUnit)
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

func walkEnvMetric(cl *Client, oid, name string, c *Collector) ([]*v1.TelemetryPacket, error) {
	pdus, err := cl.Walk(oid)
	if err != nil {
		return nil, err
	}
	var pkts []*v1.TelemetryPacket
	for _, p := range pdus {
		idx := LastOIDComponent(p.Name)
		meta := c.hostMeta()
		meta["sensor_index"] = idx
		pkts = append(pkts, c.newMetric(name, idx, ToFloat64(p)/10.0, meta))
	}
	return pkts, nil
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
		meta["layer"] = "network"

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
