// Package snmp provides SNMP polling and trap reception for EDR.
package snmp

// OID constants for all MIBs used by EDR.
const (
	// SNMPv2-MIB / system group
	OIDSysDescr    = "1.3.6.1.2.1.1.1.0"
	OIDSysUpTime   = "1.3.6.1.2.1.1.3.0"
	OIDSysName     = "1.3.6.1.2.1.1.5.0"
	OIDSysLocation = "1.3.6.1.2.1.1.6.0"

	// IF-MIB — ifTable (columnar, walk from prefix)
	OIDIfTable        = "1.3.6.1.2.1.2.2"
	OIDIfIndex        = "1.3.6.1.2.1.2.2.1.1"
	OIDIfDescr        = "1.3.6.1.2.1.2.2.1.2"
	OIDIfAdminStatus  = "1.3.6.1.2.1.2.2.1.7"
	OIDIfOperStatus   = "1.3.6.1.2.1.2.2.1.8"
	OIDIfInOctets     = "1.3.6.1.2.1.2.2.1.10"
	OIDIfInUcastPkts  = "1.3.6.1.2.1.2.2.1.11"
	OIDIfInDiscards   = "1.3.6.1.2.1.2.2.1.13"
	OIDIfInErrors     = "1.3.6.1.2.1.2.2.1.14"
	OIDIfOutOctets    = "1.3.6.1.2.1.2.2.1.16"
	OIDIfOutUcastPkts = "1.3.6.1.2.1.2.2.1.17"
	OIDIfOutDiscards  = "1.3.6.1.2.1.2.2.1.19"
	OIDIfOutErrors    = "1.3.6.1.2.1.2.2.1.20"

	// IF-MIB — ifXTable (64-bit counters + name)
	OIDIfXTable      = "1.3.6.1.2.1.31.1.1"
	OIDIfName        = "1.3.6.1.2.1.31.1.1.1.1"
	OIDIfHCInOctets  = "1.3.6.1.2.1.31.1.1.1.6"
	OIDIfHCOutOctets = "1.3.6.1.2.1.31.1.1.1.10"
	OIDIfHighSpeed   = "1.3.6.1.2.1.31.1.1.1.15"

	// HOST-RESOURCES-MIB — hrProcessorTable
	OIDHrProcessorTable = "1.3.6.1.2.1.25.3.3"
	OIDHrProcessorLoad  = "1.3.6.1.2.1.25.3.3.1.2"

	// HOST-RESOURCES-MIB — hrStorageTable
	OIDHrStorageTable      = "1.3.6.1.2.1.25.2.3"
	OIDHrStorageType       = "1.3.6.1.2.1.25.2.3.1.2"
	OIDHrStorageDescr      = "1.3.6.1.2.1.25.2.3.1.3"
	OIDHrStorageAllocUnits = "1.3.6.1.2.1.25.2.3.1.4"
	OIDHrStorageSize       = "1.3.6.1.2.1.25.2.3.1.5"
	OIDHrStorageUsed       = "1.3.6.1.2.1.25.2.3.1.6"

	// UCD-SNMP-MIB — CPU
	OIDUcdSsCpuUser   = "1.3.6.1.4.1.2021.11.9.0"
	OIDUcdSsCpuSystem = "1.3.6.1.4.1.2021.11.10.0"
	OIDUcdSsCpuIdle   = "1.3.6.1.4.1.2021.11.11.0"

	// UCD-SNMP-MIB — Memory
	OIDUcdMemTotalReal = "1.3.6.1.4.1.2021.4.5.0"
	OIDUcdMemAvailReal = "1.3.6.1.4.1.2021.4.6.0"
	OIDUcdMemCached    = "1.3.6.1.4.1.2021.4.14.0"
	OIDUcdMemBuffer    = "1.3.6.1.4.1.2021.4.15.0"

	// UPS-MIB (RFC 1628)
	OIDUpsBatteryStatus            = "1.3.6.1.2.1.33.1.2.1.0"
	OIDUpsSecondsOnBattery         = "1.3.6.1.2.1.33.1.2.2.0"
	OIDUpsEstimatedMinutesRemain   = "1.3.6.1.2.1.33.1.2.3.0"
	OIDUpsEstimatedChargeRemaining = "1.3.6.1.2.1.33.1.2.4.0"
	OIDUpsBatteryVoltage           = "1.3.6.1.2.1.33.1.2.5.0"
	// upsBatteryTemperature (whole °C). The simulator serves it at .2.8.0
	// because .2.7 carries upsBatteryCurrent. Collected by the ENVIRONMENT tier
	// and emitted as environment.temperature_c (tag BATTERY) for the heatmap.
	OIDUpsBatteryTemperature = "1.3.6.1.2.1.33.1.2.8.0"
	OIDUpsInputTable         = "1.3.6.1.2.1.33.1.3.3"
	OIDUpsInputFrequency     = "1.3.6.1.2.1.33.1.3.3.1.2"
	OIDUpsInputVoltage       = "1.3.6.1.2.1.33.1.3.3.1.3"
	OIDUpsOutputTable        = "1.3.6.1.2.1.33.1.4.4"
	OIDUpsOutputVoltage      = "1.3.6.1.2.1.33.1.4.4.1.2"
	OIDUpsOutputCurrent      = "1.3.6.1.2.1.33.1.4.4.1.3"
	OIDUpsOutputLoad         = "1.3.6.1.2.1.33.1.4.4.1.5"

	// Raritan PX2 / DPX2 sensor table. Column layout (RARITAN-PX2-MIB,
	// externalSensorTable .3.1.N): .2=index, .3=type, .4=value, .5=state.
	// The simulator (snmprec_generator._sensor_entries) writes type at .3.1.x —
	// reading type from .2 returns the row index, not the sensor type, so every
	// reading was misclassified and dropped.
	OIDRaritanSensorTable = "1.3.6.1.4.1.13742.6.5.5.3"
	OIDRaritanSensorType  = "1.3.6.1.4.1.13742.6.5.5.3.1.3"
	OIDRaritanSensorValue = "1.3.6.1.4.1.13742.6.5.5.3.1.4"
	OIDRaritanSensorState = "1.3.6.1.4.1.13742.6.5.5.3.1.5"

	// Raritan sensor type values
	RaritanTypeTemp     = 10
	RaritanTypeHumidity = 11

	// Vertiv Geist temperature probes
	OIDVertivTempTable = "1.3.6.1.4.1.21239.5.1.4.1"
	OIDVertivTempValue = "1.3.6.1.4.1.21239.5.1.4.1.4"
	OIDVertivHumTable  = "1.3.6.1.4.1.21239.5.1.5.1"
	OIDVertivHumValue  = "1.3.6.1.4.1.21239.5.1.5.1.4"
	OIDVertivDewTable  = "1.3.6.1.4.1.21239.5.1.6.1"
	OIDVertivDewValue  = "1.3.6.1.4.1.21239.5.1.6.1.4"

	// APC NetBotz sensor table
	OIDAPCNetBotzTable      = "1.3.6.1.4.1.318.1.1.10.4.2.2.1"
	OIDAPCNetBotzLabel      = "1.3.6.1.4.1.318.1.1.10.4.2.2.1.2"
	OIDAPCNetBotzValue      = "1.3.6.1.4.1.318.1.1.10.4.2.2.1.10"
	OIDAPCNetBotzSensorType = "1.3.6.1.4.1.318.1.1.10.4.2.2.1.11"

	// ENTITY-SENSOR-MIB — entPhySensorValue (columnar, walk from prefix).
	// Servers expose chassis/inlet temp at index 1 and CPU die temp at index 2.
	// The simulator encodes both ×10 Celsius. Collected by the ENVIRONMENT tier
	// and emitted as environment.temperature_c (tags CHASSIS / CPU).
	OIDEntPhySensorValue = "1.3.6.1.2.1.99.1.1.1.4"

	// Enterprise power-device scalar trees (simulator snmprec_generator). Each is a
	// flat set of scalar columns base.N.0, read with one SNMP GET. EDR has no other
	// source for these (PDUs/generators are SNMP-only — not gNMI-capable).
	OIDPDUBase       = "1.3.6.1.4.1.99999.5" // _pdu_entries: load/volt/pf/current/power/energy/freq/temp/humidity
	OIDGeneratorBase = "1.3.6.1.4.1.99999.7" // _generator_entries: fuel/runhours/status/load/kW/phase volts/freq/coolant/oil/battery
	OIDUPSEntBase    = "1.3.6.1.4.1.99999.4" // UPS enterprise status: fan/charger/rectifier/phase/batteryEx

	// IP-MIB — ipAddrTable
	OIDIpAdEntAddr    = "1.3.6.1.2.1.4.20.1.1" // ipAdEntAddr: OID suffix is the IP address
	OIDIpAdEntIfIndex = "1.3.6.1.2.1.4.20.1.2" // ipAdEntIfIndex: value is the ifIndex; suffix is the IP

	// LLDP-MIB (IEEE 802.1AB-2009) — lldpRemTable indexed by timeMark.portNum.remIdx
	OIDLLDPRemChassisId = "1.0.8802.1.1.2.1.4.1.1.5"
	OIDLLDPRemPortId    = "1.0.8802.1.1.2.1.4.1.1.7"
	OIDLLDPRemSysName   = "1.0.8802.1.1.2.1.4.1.1.9"
	OIDLLDPLocPortId    = "1.0.8802.1.1.2.1.3.7.1.3" // lldpLocPortId  (port identifier)
	OIDLLDPLocPortDesc  = "1.0.8802.1.1.2.1.3.7.1.4" // lldpLocPortDesc (port description)
)

// IfMetricStatus — cheap state walks that should run frequently. ~3 walks/device.
// These rarely change; UI uses them for up/down state.
var IfMetricStatus = map[string]string{
	OIDIfAdminStatus: "interface.admin_status",
	OIDIfOperStatus:  "interface.operational_status",
	OIDIfHighSpeed:   "interface.speed_mbps",
}

// IfMetricCounters — heavy counter walks. ~10 walks/device. Suitable for the
// SLOW tier (every 5 min). Producing these on every cycle is what overloads
// single-threaded SNMP agents like SNMPSim.
var IfMetricCounters = map[string]string{
	OIDIfInOctets:     "interface.bytes_received",
	OIDIfInUcastPkts:  "interface.packets_received_unicast",
	OIDIfInDiscards:   "interface.discards_received",
	OIDIfInErrors:     "interface.errors_received",
	OIDIfOutOctets:    "interface.bytes_sent",
	OIDIfOutUcastPkts: "interface.packets_sent_unicast",
	OIDIfOutDiscards:  "interface.discards_sent",
	OIDIfOutErrors:    "interface.errors_sent",
	OIDIfHCInOctets:   "interface.bytes_received_hc",
	OIDIfHCOutOctets:  "interface.bytes_sent_hc",
}
