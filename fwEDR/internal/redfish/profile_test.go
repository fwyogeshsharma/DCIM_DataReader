package redfish

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// sampleSystem mimics the simulator's ComputerSystem document.
const sampleSystem = `{
  "PowerState": "On",
  "Oem": {"Simulator": {
    "CpuUtilizationPercent": 37.5,
    "MemoryUtilizationPercent": 61.0,
    "MemoryUsedBytes": 12884901888,
    "DiskUtilizationPercent": 44.0,
    "DiskUsedBytes": 500000000000,
    "DiskTotalBytes": 1000000000000,
    "NetworkRxMbps": 120.0,
    "NetworkTxMbps": 80.0,
    "AlarmCount": 0
  }}
}`

func decodeDoc(t *testing.T, s string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(s), &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return doc
}

// TestDefaultProfilePathsMatchSim proves the default profile extracts every
// OS-usage metric from the sim document (byte-identical to the old struct-tag
// reads) and that PowerState resolves.
func TestDefaultProfilePathsMatchSim(t *testing.T) {
	doc := decodeDoc(t, sampleSystem)
	p := DefaultProfile()

	if got := stringAtPath(doc, p.PowerStatePath); got != "On" {
		t.Errorf("PowerState = %q, want On", got)
	}
	want := map[string]float64{
		"server.cpu_percent":         37.5,
		"server.memory_used_percent": 61.0,
		"server.memory_used_bytes":   12884901888,
		"server.disk_total_bytes":    1000000000000,
		"server.alarm_count":         0,
	}
	got := map[string]float64{}
	for _, f := range p.OSUsage {
		if v := floatAtPath(doc, f.Path); v != nil {
			got[f.Metric] = *v
		}
	}
	if len(p.OSUsage) != 9 {
		t.Fatalf("OSUsage size = %d, want 9", len(p.OSUsage))
	}
	for metric, w := range want {
		if got[metric] != w {
			t.Errorf("%s = %v, want %v", metric, got[metric], w)
		}
	}
}

// TestFloatAtPathAbsent proves missing keys and JSON null both yield nil (the
// "absent → skip" semantics the old *float64 fields had).
func TestFloatAtPathAbsent(t *testing.T) {
	doc := decodeDoc(t, `{"PowerState":"Off","Oem":{"Simulator":{"CpuUtilizationPercent":null}}}`)
	if v := floatAtPath(doc, "Oem.Simulator.CpuUtilizationPercent"); v != nil {
		t.Errorf("null → %v, want nil", *v)
	}
	if v := floatAtPath(doc, "Oem.Simulator.MemoryUsedBytes"); v != nil {
		t.Errorf("missing → %v, want nil", *v)
	}
	if v := floatAtPath(doc, "Oem.Nope.Deep.Path"); v != nil {
		t.Errorf("missing branch → %v, want nil", *v)
	}
	if s := stringAtPath(doc, "PowerState"); s != "Off" {
		t.Errorf("PowerState = %q, want Off", s)
	}
}

// TestLoadProfileRedfishOverride proves a real-BMC profile retargets the paths
// and replaces the whole OS-usage list.
func TestLoadProfileRedfishOverride(t *testing.T) {
	yaml := `
name: real-bmc
power_state_path: PowerState
os_usage:
  - {metric: server.cpu_percent, path: "ProcessorSummary.Oem.Cpu"}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "rf.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := LoadProfile(path)
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}
	if p.Name != "real-bmc" || len(p.OSUsage) != 1 {
		t.Fatalf("override wrong: name=%s n=%d", p.Name, len(p.OSUsage))
	}
	if p.OSUsage[0] != (OSUsageField{"server.cpu_percent", "ProcessorSummary.Oem.Cpu"}) {
		t.Errorf("os_usage override wrong: %+v", p.OSUsage[0])
	}
	// No Oem.Simulator path leaks through.
	for _, f := range p.OSUsage {
		if f.Path == "Oem.Simulator.CpuUtilizationPercent" {
			t.Error("default sim path leaked into replaced list")
		}
	}
}

// TestLoadProfileEmptyIsDefault proves the zero-config path keeps the sim default.
func TestLoadProfileEmptyIsDefault(t *testing.T) {
	p, err := LoadProfile("")
	if err != nil {
		t.Fatalf("LoadProfile(\"\"): %v", err)
	}
	if p.Name != "simulator" || len(p.OSUsage) != 9 || p.PowerStatePath != "PowerState" {
		t.Fatalf("empty profile is not the default: %+v", p)
	}
}
