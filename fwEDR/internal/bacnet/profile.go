package bacnet

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Profile holds the BACnet cooling-plant object maps — per device type, the
// (object type, instance) → EDR metric mapping. On the simulator these instance
// numbers are POSITIONAL (spec-list order), which real plant gear does not share.
// Externalizing the maps decouples EDR: a real deployment supplies a profile file
// with the target's actual instances, no code change.
//
// DefaultProfile() reproduces the previously hardcoded plant maps, with ONE
// correction: the cdu map now includes cooling.tcs_loop_pressure_kpa at AI:7 (the
// object the simulator actually serves there), fixing the prior off-by-one that
// mis-mapped heat_load/pump_power/pump_speed/approach/filter_dp/run_hours.
//
// NOTE: instance-keyed maps are still positional; true runtime Object_Name
// discovery (self-adjusting, immune to reordering) is a later, opt-in step.
type Profile struct {
	Name  string
	Plant map[string][]objMeta
}

// DefaultProfile returns the built-in simulator plant object maps.
func DefaultProfile() *Profile {
	ai, bi := ObjAnalogInput, ObjBinaryInput
	return &Profile{
		Name: "simulator",
		Plant: map[string][]objMeta{
			"chiller": {
				{ai, 1, "cooling.chw_supply_temp_c", "", 1},
				{ai, 2, "cooling.chw_return_temp_c", "", 1},
				{ai, 3, "cooling.chw_setpoint_c", "", 1},
				{ai, 4, "cooling.chw_flow_lps", "", 1},
				{ai, 5, "cooling.cond_supply_temp_c", "", 1},
				{ai, 6, "cooling.cond_return_temp_c", "", 1},
				{ai, 7, "cooling.compressor_load_pct", "", 1},
				{ai, 8, "cooling.active_power_kw", "", 1},
				{ai, 9, "cooling.cooling_capacity_kw", "", 1},
				{ai, 10, "cooling.cop", "", 1},
				{ai, 11, "cooling.evap_pressure_kpa", "", 1},
				{ai, 12, "cooling.cond_pressure_kpa", "", 1},
				{ai, 13, "cooling.run_hours_h", "", 1},
				{bi, 1, "cooling.chiller_running", "", 1},
				{bi, 2, "cooling.alarm_high_pressure", "", 1},
				{bi, 3, "cooling.alarm_low_evap_temp", "", 1},
				{bi, 4, "cooling.alarm_flow_loss", "", 1},
			},
			"pump": {
				{ai, 1, "cooling.pump_speed_pct", "", 1},
				{ai, 2, "cooling.pump_flow_lps", "", 1},
				{ai, 3, "cooling.discharge_pressure_kpa", "", 1},
				{ai, 4, "cooling.suction_pressure_kpa", "", 1},
				{ai, 5, "cooling.diff_pressure_kpa", "", 1},
				{ai, 6, "cooling.motor_power_kw", "", 1},
				{ai, 7, "cooling.motor_temp_c", "", 1},
				{ai, 8, "cooling.vfd_frequency_hz", "", 1},
				{ai, 9, "cooling.run_hours_h", "", 1},
				{bi, 1, "cooling.pump_running", "", 1},
				{bi, 2, "cooling.alarm_fault", "", 1},
				{bi, 3, "cooling.alarm_low_flow", "", 1},
			},
			"cooling_tower": {
				{ai, 1, "cooling.fan_speed_pct", "", 1},
				{ai, 2, "cooling.basin_temp_c", "", 1},
				{ai, 3, "cooling.cond_water_in_c", "", 1},
				{ai, 4, "cooling.cond_water_out_c", "", 1},
				{ai, 5, "cooling.fan_power_kw", "", 1},
				{ai, 6, "cooling.basin_level_pct", "", 1},
				{ai, 7, "cooling.makeup_flow_lpm", "", 1},
				{ai, 8, "cooling.vibration_mms", "", 1},
				{ai, 9, "cooling.run_hours_h", "", 1},
				{bi, 1, "cooling.fan_running", "", 1},
				{bi, 2, "cooling.alarm_high_vibration", "", 1},
				{bi, 3, "cooling.alarm_low_basin", "", 1},
			},
			"valve": {
				{ai, 1, "cooling.valve_position_pct", "", 1},
				{ai, 2, "cooling.valve_commanded_position_pct", "", 1},
				{ai, 3, "cooling.actuator_temp_c", "", 1},
				{bi, 1, "cooling.valve_modulating", "", 1},
				{bi, 2, "cooling.alarm_actuator_fault", "", 1},
			},
			// cdu — CORRECTED: TCS_Loop_Pressure is AI:7 in the sim PLANT_SPEC, so
			// every point after it is +1 vs the old (buggy) map.
			"cdu": {
				{ai, 1, "cooling.tcs_supply_temp_c", "", 1},
				{ai, 2, "cooling.tcs_return_temp_c", "", 1},
				{ai, 3, "cooling.tcs_setpoint_c", "", 1},
				{ai, 4, "cooling.tcs_flow_lps", "", 1},
				{ai, 5, "cooling.facility_chw_valve_pct", "", 1},
				{ai, 6, "cooling.facility_chw_flow_lps", "", 1},
				{ai, 7, "cooling.tcs_loop_pressure_kpa", "", 1},
				{ai, 8, "cooling.heat_load_kw", "", 1},
				{ai, 9, "cooling.pump_power_kw", "", 1},
				{ai, 10, "cooling.pump_speed_pct", "", 1},
				{ai, 11, "cooling.approach_temp_c", "", 1},
				{ai, 12, "cooling.filter_dp_kpa", "", 1},
				{ai, 13, "cooling.run_hours_h", "", 1},
				{bi, 1, "cooling.cdu_running", "", 1},
				{bi, 2, "cooling.alarm_leak", "", 1},
				{bi, 3, "cooling.alarm_high_supply_temp", "", 1},
				{bi, 4, "cooling.alarm_pump_fault", "", 1},
				{bi, 5, "cooling.alarm_low_flow", "", 1},
			},
			"crah": {
				{ai, 1, "cooling.supply_air_temp_c", "", 1},
				{ai, 2, "cooling.return_air_temp_c", "", 1},
				{ai, 3, "cooling.crah_setpoint_c", "", 1},
				{ai, 4, "cooling.fan_speed_pct", "", 1},
				{ai, 5, "cooling.chw_valve_pct", "", 1},
				{ai, 6, "cooling.cooling_capacity_pct", "", 1},
				{ai, 7, "cooling.supply_humidity_pct", "", 1},
				{ai, 8, "cooling.airflow_pct", "", 1},
				{ai, 9, "cooling.fan_power_kw", "", 1},
				{ai, 10, "cooling.run_hours_h", "", 1},
				{bi, 1, "cooling.crah_running", "", 1},
				{bi, 2, "cooling.alarm_high_temp", "", 1},
				{bi, 3, "cooling.alarm_airflow_loss", "", 1},
				{bi, 4, "cooling.alarm_filter_dirty", "", 1},
			},
		},
	}
}

// ─── profile file (YAML) ─────────────────────────────────────────────────────

type objYAML struct {
	Type  string  `yaml:"type"` // "analogInput"/"ai" | "binaryInput"/"bi"
	Inst  int     `yaml:"inst"`
	Name  string  `yaml:"name"`
	Tag   string  `yaml:"tag"`
	Scale float64 `yaml:"scale"`
}

type profileFile struct {
	Name  string               `yaml:"name"`
	Plant map[string][]objYAML `yaml:"plant"`
}

// LoadProfile returns the BACnet plant profile for path. Empty path = the
// built-in simulator default (unchanged behavior). A file overrides only the
// device types it defines; each provided type REPLACES that type's object list
// (positional maps aren't safely merged). Omitted types keep the default.
func LoadProfile(path string) (*Profile, error) {
	p := DefaultProfile()
	if path == "" {
		return p, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bacnet profile %q: %w", path, err)
	}
	var f profileFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse bacnet profile %q: %w", path, err)
	}
	if f.Name != "" {
		p.Name = f.Name
	}
	for dtype, objs := range f.Plant {
		converted, err := toObjMetas(objs)
		if err != nil {
			return nil, fmt.Errorf("bacnet profile %q, device_type %q: %w", path, dtype, err)
		}
		if len(converted) > 0 {
			p.Plant[dtype] = converted
		}
	}
	return p, nil
}

func toObjMetas(in []objYAML) ([]objMeta, error) {
	out := make([]objMeta, 0, len(in))
	for _, o := range in {
		ot, ok := objTypeFromName(o.Type)
		if !ok {
			return nil, fmt.Errorf("unknown object type %q (want analogInput|binaryInput)", o.Type)
		}
		scale := o.Scale
		if scale == 0 {
			scale = 1
		}
		out = append(out, objMeta{objType: ot, inst: o.Inst, name: o.Name, tag: o.Tag, scale: scale})
	}
	return out, nil
}

func objTypeFromName(s string) (int, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "analoginput", "ai":
		return ObjAnalogInput, true
	case "binaryinput", "bi":
		return ObjBinaryInput, true
	}
	return 0, false
}
