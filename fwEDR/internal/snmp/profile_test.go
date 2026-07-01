package snmp

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestDefaultProfileMatchesLegacy pins the built-in simulator profile to the exact
// enterprise bases, column counts, and scaling that were previously hardcoded in
// metrics.go. If a future edit drifts these, the sim collection silently changes —
// this test fails first.
func TestDefaultProfileMatchesLegacy(t *testing.T) {
	p := DefaultProfile()

	if p.PDUBase != "1.3.6.1.4.1.99999.5" ||
		p.GeneratorBase != "1.3.6.1.4.1.99999.7" ||
		p.UPSEntBase != "1.3.6.1.4.1.99999.4" {
		t.Fatalf("enterprise bases drifted: pdu=%s gen=%s ups=%s",
			p.PDUBase, p.GeneratorBase, p.UPSEntBase)
	}
	if len(p.PDUScalars) != 17 || len(p.GeneratorScalars) != 13 || len(p.UPSEntScalars) != 5 {
		t.Fatalf("scalar counts drifted: pdu=%d gen=%d ups=%d",
			len(p.PDUScalars), len(p.GeneratorScalars), len(p.UPSEntScalars))
	}
	// Spot-check the scaled/tagged columns that are easy to get wrong.
	want := map[int]scalarMetric{
		2:  {"3", "pdu.power_factor", "", 100},           // ÷100
		8:  {"9", "pdu.current_a", "", 10},               // ÷10
		14: {"15", "environment.temperature_c", "PDU", 10},
	}
	for i, w := range want {
		if p.PDUScalars[i] != w {
			t.Errorf("PDUScalars[%d] = %+v, want %+v", i, p.PDUScalars[i], w)
		}
	}
	if g := p.GeneratorScalars[5]; g != (scalarMetric{"6", "generator.output_voltage_v", "PhA", 10}) {
		t.Errorf("GeneratorScalars[5] = %+v", g)
	}
	if u := p.UPSEntScalars[4]; u != (scalarMetric{"5", "environment.ups_battery_status_ex", "", 1}) {
		t.Errorf("UPSEntScalars[4] = %+v", u)
	}
}

// TestLoadProfileEmptyIsDefault proves the zero-config path is byte-identical to
// the built-in default (the "no break" guarantee).
func TestLoadProfileEmptyIsDefault(t *testing.T) {
	got, err := LoadProfile("")
	if err != nil {
		t.Fatalf("LoadProfile(\"\"): %v", err)
	}
	if !reflect.DeepEqual(got, DefaultProfile()) {
		t.Fatalf("LoadProfile(\"\") != DefaultProfile()")
	}
}

// TestLoadProfilePartialOverride proves a profile file overrides only what it sets
// and inherits the default for everything else.
func TestLoadProfilePartialOverride(t *testing.T) {
	yaml := `
name: acme-pdu
enterprise:
  pdu_base: "1.3.6.1.4.1.318.1.1.12"
  pdu_scalars:
    - {col: "1", name: "pdu.load_percent", tag: "", scale: 1}
    - {col: "2", name: "pdu.voltage_v", tag: "", scale: 10}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "acme.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := LoadProfile(path)
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}
	if p.Name != "acme-pdu" {
		t.Errorf("name = %q, want acme-pdu", p.Name)
	}
	if p.PDUBase != "1.3.6.1.4.1.318.1.1.12" {
		t.Errorf("pdu_base override lost: %s", p.PDUBase)
	}
	if len(p.PDUScalars) != 2 || p.PDUScalars[1] != (scalarMetric{"2", "pdu.voltage_v", "", 10}) {
		t.Errorf("pdu_scalars override wrong: %+v", p.PDUScalars)
	}
	// Untouched sections inherit the default.
	if p.GeneratorBase != DefaultProfile().GeneratorBase ||
		len(p.GeneratorScalars) != 13 {
		t.Errorf("generator section should inherit default, got base=%s n=%d",
			p.GeneratorBase, len(p.GeneratorScalars))
	}
}
