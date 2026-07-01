package bacnet

import (
	"os"
	"path/filepath"
	"testing"
)

// find returns the objMeta with the given (objType, instance) from a list.
func find(objs []objMeta, ot, inst int) (objMeta, bool) {
	for _, o := range objs {
		if o.objType == ot && o.inst == inst {
			return o, true
		}
	}
	return objMeta{}, false
}

// TestDefaultProfilePlantCounts pins each plant type's object-list size so a
// future edit to the default maps fails loudly.
func TestDefaultProfilePlantCounts(t *testing.T) {
	p := DefaultProfile()
	want := map[string]int{
		"chiller": 17, "pump": 12, "cooling_tower": 12,
		"valve": 5, "cdu": 18, "crah": 14,
	}
	for dt, n := range want {
		if got := len(p.Plant[dt]); got != n {
			t.Errorf("plant[%q] size = %d, want %d", dt, got, n)
		}
	}
}

// TestDefaultProfileCDUFixed verifies the corrected cdu instance map: AI:7 is
// tcs_loop_pressure and the shifted points land on their real instances.
func TestDefaultProfileCDUFixed(t *testing.T) {
	cdu := DefaultProfile().Plant["cdu"]
	cases := map[int]string{
		7:  "cooling.tcs_loop_pressure_kpa",
		8:  "cooling.heat_load_kw",
		9:  "cooling.pump_power_kw",
		12: "cooling.filter_dp_kpa",
		13: "cooling.run_hours_h",
	}
	for inst, name := range cases {
		o, ok := find(cdu, ObjAnalogInput, inst)
		if !ok || o.name != name {
			t.Errorf("cdu AI:%d = %q (ok=%v), want %q", inst, o.name, ok, name)
		}
	}
}

// TestLoadProfileEmptyIsDefault proves the zero-config path keeps the default maps.
func TestLoadProfileEmptyIsDefault(t *testing.T) {
	p, err := LoadProfile("")
	if err != nil {
		t.Fatalf("LoadProfile(\"\"): %v", err)
	}
	if p.Name != "simulator" || len(p.Plant["cdu"]) != 18 {
		t.Fatalf("empty profile is not the default: name=%s cdu=%d", p.Name, len(p.Plant["cdu"]))
	}
}

// TestLoadProfilePlantOverride proves a profile replaces one device type's map
// (by object type + instance) while inheriting the rest.
func TestLoadProfilePlantOverride(t *testing.T) {
	yaml := `
name: real-plant
plant:
  chiller:
    - {type: analogInput, inst: 5, name: cooling.chw_supply_temp_c, scale: 10}
    - {type: binaryInput,  inst: 9, name: cooling.chiller_running}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "plant.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := LoadProfile(path)
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}
	if p.Name != "real-plant" {
		t.Errorf("name = %q", p.Name)
	}
	ch := p.Plant["chiller"]
	if len(ch) != 2 {
		t.Fatalf("chiller should be replaced (2), got %d", len(ch))
	}
	o, ok := find(ch, ObjAnalogInput, 5)
	if !ok || o.name != "cooling.chw_supply_temp_c" || o.scale != 10 {
		t.Errorf("chiller AI:5 override wrong: %+v ok=%v", o, ok)
	}
	if _, ok := find(ch, ObjBinaryInput, 9); !ok {
		t.Error("chiller BI:9 override missing")
	}
	// Untouched types inherit the default (cdu still has the fixed 18-object map).
	if len(p.Plant["cdu"]) != 18 {
		t.Errorf("cdu should inherit default, got %d", len(p.Plant["cdu"]))
	}
}

// TestLoadProfileBadType rejects an unknown object type rather than silently
// dropping it.
func TestLoadProfileBadType(t *testing.T) {
	yaml := "plant:\n  chiller:\n    - {type: multiStateInput, inst: 1, name: x}\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProfile(path); err == nil {
		t.Fatal("expected error for unknown object type")
	}
}
