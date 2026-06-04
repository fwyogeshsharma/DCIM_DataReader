package bacnet

import "fmt"

// objMeta maps one EV2 BACnet object to the EDR metric it produces.
type objMeta struct {
	objType int
	inst    int
	name    string  // EDR metric name
	tag     string  // metric tag (phase / harmonic / circuit label); "" if none
	scale   float64 // value divisor (1 = none)
}

// panelObjects are the fixed panel-level objects every Verdigris EV2 exposes:
// 16 Analog Inputs (1001–1016) + 5 alarm Binary Inputs (1020–1024). Mirrors
// core/bacnet_ev2_generator.py _PANEL_AI / _PANEL_BI.
var panelObjects = []objMeta{
	{ObjAnalogInput, 1001, "energy.active_power_kw", "", 1},
	{ObjAnalogInput, 1002, "energy.energy_kwh", "", 1},
	{ObjAnalogInput, 1003, "energy.voltage_v", "PhA", 1},
	{ObjAnalogInput, 1004, "energy.voltage_v", "PhB", 1},
	{ObjAnalogInput, 1005, "energy.voltage_v", "PhC", 1},
	{ObjAnalogInput, 1006, "energy.current_a", "PhA", 1},
	{ObjAnalogInput, 1007, "energy.current_a", "PhB", 1},
	{ObjAnalogInput, 1008, "energy.current_a", "PhC", 1},
	{ObjAnalogInput, 1009, "energy.frequency_hz", "", 1},
	{ObjAnalogInput, 1010, "energy.power_factor", "", 1},
	{ObjAnalogInput, 1011, "energy.voltage_thd_percent", "", 1},
	{ObjAnalogInput, 1012, "energy.current_thd_percent", "", 1},
	{ObjAnalogInput, 1013, "energy.harmonic_current_percent", "H3", 1},
	{ObjAnalogInput, 1014, "energy.harmonic_current_percent", "H5", 1},
	{ObjAnalogInput, 1015, "energy.harmonic_current_percent", "H7", 1},
	{ObjAnalogInput, 1016, "energy.harmonic_current_percent", "H9", 1},
	{ObjBinaryInput, 1020, "energy.alarm_overcurrent", "", 1},
	{ObjBinaryInput, 1021, "energy.alarm_voltage_imbalance", "", 1},
	{ObjBinaryInput, 1022, "energy.alarm_high_thd", "", 1},
	{ObjBinaryInput, 1023, "energy.alarm_phase_loss", "", 1},
	{ObjBinaryInput, 1024, "energy.alarm_sensor_fault", "", 1},
}

// circuitOffsets are the 5 per-circuit Analog Inputs (base+1 .. base+5), where
// base = (circuit+1)*1000. Mirrors _CKT_AI_OFFSETS.
var circuitOffsets = []struct {
	offset int
	name   string
}{
	{1, "energy.circuit_current_a"},
	{2, "energy.circuit_power_kw"},
	{3, "energy.circuit_energy_kwh"},
	{4, "energy.circuit_power_factor"},
	{5, "energy.circuit_current_thd_percent"},
}

// circuitObjects builds the per-circuit object metadata for an n-circuit meter.
// Circuit N base instance = (N+1)*1000; the tag carries the circuit label CktNN.
func circuitObjects(circuits int) []objMeta {
	out := make([]objMeta, 0, circuits*len(circuitOffsets))
	for ckt := 1; ckt <= circuits; ckt++ {
		base := (ckt + 1) * 1000
		label := fmt.Sprintf("Ckt%02d", ckt)
		for _, off := range circuitOffsets {
			out = append(out, objMeta{ObjAnalogInput, base + off.offset, off.name, label, 1})
		}
	}
	return out
}

// objectsFor returns the full object set EDR reads for one EV2 device: the fixed
// panel/alarm block, plus per-circuit objects when readCircuits is set.
func objectsFor(circuits int, readCircuits bool) []objMeta {
	objs := make([]objMeta, len(panelObjects))
	copy(objs, panelObjects)
	if readCircuits && circuits > 0 {
		objs = append(objs, circuitObjects(circuits)...)
	}
	return objs
}

// metaIndex keys object metadata by (objType, instance) for fast lookup when
// decoding read results and COV notifications.
func metaIndex(objs []objMeta) map[[2]int]objMeta {
	m := make(map[[2]int]objMeta, len(objs))
	for _, o := range objs {
		m[[2]int{o.objType, o.inst}] = o
	}
	return m
}
