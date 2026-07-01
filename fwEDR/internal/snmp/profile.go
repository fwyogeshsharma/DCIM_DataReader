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
	return p, nil
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
