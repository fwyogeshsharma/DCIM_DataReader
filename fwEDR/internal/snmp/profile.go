package snmp

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Profile holds the device-family SNMP mappings that legitimately differ between
// the simulator and real hardware — the enterprise OID trees and per-column
// scaling for power devices (PDU / generator / UPS-enterprise). These are the
// SIM-SPECIFIC parts of mibs.go: real PDUs/generators/UPSs answer under vendor
// MIBs, not the simulator's placeholder 1.3.6.1.4.1.99999 tree.
//
// The point of externalizing them is decoupling: the collector reads mappings
// from a Profile instead of hardcoded constants, so pointing EDR at real hardware
// is a config change (a different profile file), not a code change.
//
// DefaultProfile() reproduces the simulator's EXACT bases, columns, and scaling,
// so with no profile file configured behavior is byte-identical to the previous
// hardcoded implementation. A profile file overrides only the sections it sets;
// anything omitted falls back to the default (partial profiles are safe).
type Profile struct {
	Name string

	// Enterprise power-device scalar trees. Each is a flat set of scalar columns
	// base.N.0, read with one SNMP GET.
	PDUBase          string
	GeneratorBase    string
	UPSEntBase       string
	PDUScalars       []scalarMetric
	GeneratorScalars []scalarMetric
	UPSEntScalars    []scalarMetric

	// Server OS OIDs whose PLACEMENT varies between the simulator and real agents:
	//   - hrStorage: the sim serves the storage table under .25.2.2.1.x, NOT the
	//     standard hrStorageTable .25.2.3.1.x.
	//   - UCD ssCpu / memory columns differ by net-snmp version and agent.
	// Externalizing them lets a real-hardware profile use the standard indices
	// without a code change. Defaults reproduce the current mibs.go constants.
	HrProcessorLoad     string
	HrStorageType       string
	HrStorageDescr      string
	HrStorageAllocUnits string
	HrStorageSize       string
	HrStorageUsed       string
	UcdCpuUser          string
	UcdCpuSystem        string
	UcdCpuIdle          string
	UcdMemTotal         string
	UcdMemAvail         string
	UcdMemCached        string
	UcdMemBuffer        string

	// Environmental sensor vendor tables (Raritan / Vertiv / APC). These are real
	// vendor OIDs, but externalizing them lets a deployment add/retarget sensor
	// vendors — including the per-vendor type/value column indices and Raritan's
	// numeric type codes — without a code change. Defaults = current mibs.go values.
	RaritanSensorType   string
	RaritanSensorValue  string
	RaritanTypeTemp     int
	RaritanTypeHumidity int
	VertivTempValue     string
	VertivHumValue      string
	VertivDewValue      string
	APCSensorLabel      string
	APCSensorValue      string
}

// DefaultProfile returns the built-in "simulator" profile — the exact OIDs and
// scaling that were previously hardcoded in metrics.go / mibs.go. Used whenever
// no profile file is configured, guaranteeing unchanged behavior against the sim.
func DefaultProfile() *Profile {
	return &Profile{
		Name:          "simulator",
		PDUBase:       OIDPDUBase,
		GeneratorBase: OIDGeneratorBase,
		UPSEntBase:    OIDUPSEntBase,
		// pdu _pdu_entries (99999.5.N.0). Temp/humidity fold into environment.*.
		PDUScalars: []scalarMetric{
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
		},
		// generator _generator_entries (99999.7.N.0).
		GeneratorScalars: []scalarMetric{
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
		},
		// ups enterprise status block (99999.4.N.0).
		UPSEntScalars: []scalarMetric{
			{"1", "environment.ups_fan_status", "", 1},
			{"2", "environment.ups_charger_status", "", 1},
			{"3", "environment.ups_rectifier_status", "", 1},
			{"4", "environment.ups_phase_status", "", 1},
			{"5", "environment.ups_battery_status_ex", "", 1},
		},
		// Server OS OIDs — default to the current mibs.go constants (sim placement
		// for hrStorage; standard-index UCD columns as shipped).
		HrProcessorLoad:     OIDHrProcessorLoad,
		HrStorageType:       OIDHrStorageType,
		HrStorageDescr:      OIDHrStorageDescr,
		HrStorageAllocUnits: OIDHrStorageAllocUnits,
		HrStorageSize:       OIDHrStorageSize,
		HrStorageUsed:       OIDHrStorageUsed,
		UcdCpuUser:          OIDUcdSsCpuUser,
		UcdCpuSystem:        OIDUcdSsCpuSystem,
		UcdCpuIdle:          OIDUcdSsCpuIdle,
		UcdMemTotal:         OIDUcdMemTotalReal,
		UcdMemAvail:         OIDUcdMemAvailReal,
		UcdMemCached:        OIDUcdMemCached,
		UcdMemBuffer:        OIDUcdMemBuffer,
		// Sensor vendor tables (Raritan / Vertiv / APC).
		RaritanSensorType:   OIDRaritanSensorType,
		RaritanSensorValue:  OIDRaritanSensorValue,
		RaritanTypeTemp:     RaritanTypeTemp,
		RaritanTypeHumidity: RaritanTypeHumidity,
		VertivTempValue:     OIDVertivTempValue,
		VertivHumValue:      OIDVertivHumValue,
		VertivDewValue:      OIDVertivDewValue,
		APCSensorLabel:      OIDAPCNetBotzLabel,
		APCSensorValue:      OIDAPCNetBotzValue,
	}
}

// ─── profile file (YAML) ─────────────────────────────────────────────────────

// scalarYAML is the exported DTO for one scalar column in a profile file
// (scalarMetric's fields are unexported so cannot be unmarshalled directly).
type scalarYAML struct {
	Col   string  `yaml:"col"`
	Name  string  `yaml:"name"`
	Tag   string  `yaml:"tag"`
	Scale float64 `yaml:"scale"`
}

type profileFile struct {
	Name       string `yaml:"name"`
	Enterprise struct {
		PDUBase          string       `yaml:"pdu_base"`
		GeneratorBase    string       `yaml:"generator_base"`
		UPSEntBase       string       `yaml:"ups_ent_base"`
		PDUScalars       []scalarYAML `yaml:"pdu_scalars"`
		GeneratorScalars []scalarYAML `yaml:"generator_scalars"`
		UPSEntScalars    []scalarYAML `yaml:"ups_ent_scalars"`
	} `yaml:"enterprise"`
	Server struct {
		HrProcessorLoad     string `yaml:"hr_processor_load"`
		HrStorageType       string `yaml:"hr_storage_type"`
		HrStorageDescr      string `yaml:"hr_storage_descr"`
		HrStorageAllocUnits string `yaml:"hr_storage_alloc_units"`
		HrStorageSize       string `yaml:"hr_storage_size"`
		HrStorageUsed       string `yaml:"hr_storage_used"`
		UcdCpuUser          string `yaml:"ucd_cpu_user"`
		UcdCpuSystem        string `yaml:"ucd_cpu_system"`
		UcdCpuIdle          string `yaml:"ucd_cpu_idle"`
		UcdMemTotal         string `yaml:"ucd_mem_total"`
		UcdMemAvail         string `yaml:"ucd_mem_avail"`
		UcdMemCached        string `yaml:"ucd_mem_cached"`
		UcdMemBuffer        string `yaml:"ucd_mem_buffer"`
	} `yaml:"server"`
	Sensors struct {
		RaritanSensorType   string `yaml:"raritan_sensor_type"`
		RaritanSensorValue  string `yaml:"raritan_sensor_value"`
		RaritanTypeTemp     int    `yaml:"raritan_type_temp"`
		RaritanTypeHumidity int    `yaml:"raritan_type_humidity"`
		VertivTempValue     string `yaml:"vertiv_temp_value"`
		VertivHumValue      string `yaml:"vertiv_hum_value"`
		VertivDewValue      string `yaml:"vertiv_dew_value"`
		APCSensorLabel      string `yaml:"apc_sensor_label"`
		APCSensorValue      string `yaml:"apc_sensor_value"`
	} `yaml:"sensors"`
}

// LoadProfile returns the SNMP profile for the given file path. An empty path
// returns DefaultProfile() (the simulator profile) — the zero-config, unchanged
// behavior. When a path is set, the file overrides only the sections it defines;
// every unset section keeps its default value, so a partial profile is valid.
func LoadProfile(path string) (*Profile, error) {
	p := DefaultProfile()
	if path == "" {
		return p, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snmp profile %q: %w", path, err)
	}
	var f profileFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse snmp profile %q: %w", path, err)
	}
	if f.Name != "" {
		p.Name = f.Name
	}
	if f.Enterprise.PDUBase != "" {
		p.PDUBase = f.Enterprise.PDUBase
	}
	if f.Enterprise.GeneratorBase != "" {
		p.GeneratorBase = f.Enterprise.GeneratorBase
	}
	if f.Enterprise.UPSEntBase != "" {
		p.UPSEntBase = f.Enterprise.UPSEntBase
	}
	if s := toScalars(f.Enterprise.PDUScalars); s != nil {
		p.PDUScalars = s
	}
	if s := toScalars(f.Enterprise.GeneratorScalars); s != nil {
		p.GeneratorScalars = s
	}
	if s := toScalars(f.Enterprise.UPSEntScalars); s != nil {
		p.UPSEntScalars = s
	}
	// Server OS OIDs — override each only when the file sets it (non-empty).
	setIf(&p.HrProcessorLoad, f.Server.HrProcessorLoad)
	setIf(&p.HrStorageType, f.Server.HrStorageType)
	setIf(&p.HrStorageDescr, f.Server.HrStorageDescr)
	setIf(&p.HrStorageAllocUnits, f.Server.HrStorageAllocUnits)
	setIf(&p.HrStorageSize, f.Server.HrStorageSize)
	setIf(&p.HrStorageUsed, f.Server.HrStorageUsed)
	setIf(&p.UcdCpuUser, f.Server.UcdCpuUser)
	setIf(&p.UcdCpuSystem, f.Server.UcdCpuSystem)
	setIf(&p.UcdCpuIdle, f.Server.UcdCpuIdle)
	setIf(&p.UcdMemTotal, f.Server.UcdMemTotal)
	setIf(&p.UcdMemAvail, f.Server.UcdMemAvail)
	setIf(&p.UcdMemCached, f.Server.UcdMemCached)
	setIf(&p.UcdMemBuffer, f.Server.UcdMemBuffer)
	// Sensor vendor tables.
	setIf(&p.RaritanSensorType, f.Sensors.RaritanSensorType)
	setIf(&p.RaritanSensorValue, f.Sensors.RaritanSensorValue)
	setIfInt(&p.RaritanTypeTemp, f.Sensors.RaritanTypeTemp)
	setIfInt(&p.RaritanTypeHumidity, f.Sensors.RaritanTypeHumidity)
	setIf(&p.VertivTempValue, f.Sensors.VertivTempValue)
	setIf(&p.VertivHumValue, f.Sensors.VertivHumValue)
	setIf(&p.VertivDewValue, f.Sensors.VertivDewValue)
	setIf(&p.APCSensorLabel, f.Sensors.APCSensorLabel)
	setIf(&p.APCSensorValue, f.Sensors.APCSensorValue)
	return p, nil
}

// setIf overwrites *dst with v only when v is non-empty, so an omitted profile
// field keeps its default.
func setIf(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}

// setIfInt overwrites *dst with v only when v is non-zero, so an omitted numeric
// field keeps its default.
func setIfInt(dst *int, v int) {
	if v != 0 {
		*dst = v
	}
}

// toScalars converts the YAML DTO slice to internal scalarMetric. Returns nil for
// an empty slice so the caller keeps the default for that section.
func toScalars(in []scalarYAML) []scalarMetric {
	if len(in) == 0 {
		return nil
	}
	out := make([]scalarMetric, len(in))
	for i, s := range in {
		scale := s.Scale
		if scale == 0 {
			scale = 1 // an omitted/zero scale means "no scaling", never divide-by-zero
		}
		out[i] = scalarMetric{col: s.Col, name: s.Name, tag: s.Tag, scale: scale}
	}
	return out
}
