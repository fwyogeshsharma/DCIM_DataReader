package redfish

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Profile decouples the Redfish OS-usage field locations from the simulator. On
// the sim, CPU/mem/disk/network/alarm live under the Oem.Simulator extension of
// the ComputerSystem resource — a namespace real BMCs do not expose (they carry
// these in standard MetricReports/ProcessorMetrics, or the OS provides them). By
// reading each metric from a configured JSON path, EDR can be retargeted at a
// real BMC's field locations via config instead of code.
//
// DefaultProfile() uses the exact Oem.Simulator paths the collector used before,
// so with no profile file behavior against the simulator is byte-identical
// (including emit order — the fields are an ordered slice, not a map).
//
// NOTE: a flat dotted path can only reach fields WITHIN the ComputerSystem doc.
// Real BMCs that expose performance data in a SEPARATE MetricReport resource need
// a richer collector — that is out of scope here and called out in the plan.
type Profile struct {
	Name           string
	PowerStatePath string         // path to the chassis power state string
	OSUsage        []OSUsageField // ordered metric→path list for OS usage
}

// OSUsageField maps one server.* metric to a dotted JSON path in the
// ComputerSystem document (e.g. "Oem.Simulator.CpuUtilizationPercent").
type OSUsageField struct {
	Metric string
	Path   string
}

// DefaultProfile returns the built-in simulator profile (the previous hardcoded
// Oem.Simulator field locations, in the original emit order).
func DefaultProfile() *Profile {
	return &Profile{
		Name:           "simulator",
		PowerStatePath: "PowerState",
		OSUsage: []OSUsageField{
			{"server.cpu_percent", "Oem.Simulator.CpuUtilizationPercent"},
			{"server.memory_used_percent", "Oem.Simulator.MemoryUtilizationPercent"},
			{"server.memory_used_bytes", "Oem.Simulator.MemoryUsedBytes"},
			{"server.disk_used_percent", "Oem.Simulator.DiskUtilizationPercent"},
			{"server.disk_used_bytes", "Oem.Simulator.DiskUsedBytes"},
			{"server.disk_total_bytes", "Oem.Simulator.DiskTotalBytes"},
			{"server.network_rx_mbps", "Oem.Simulator.NetworkRxMbps"},
			{"server.network_tx_mbps", "Oem.Simulator.NetworkTxMbps"},
			{"server.alarm_count", "Oem.Simulator.AlarmCount"},
		},
	}
}

// ─── profile file (YAML) ─────────────────────────────────────────────────────

type profileFile struct {
	Name           string `yaml:"name"`
	PowerStatePath string `yaml:"power_state_path"`
	OSUsage        []struct {
		Metric string `yaml:"metric"`
		Path   string `yaml:"path"`
	} `yaml:"os_usage"`
}

// LoadProfile returns the Redfish profile for path. Empty path = the built-in
// simulator default (unchanged behavior). A file overrides only the fields it
// sets; a provided os_usage list REPLACES the default list (so no Oem.Simulator
// paths leak into a real deployment).
func LoadProfile(path string) (*Profile, error) {
	p := DefaultProfile()
	if path == "" {
		return p, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read redfish profile %q: %w", path, err)
	}
	var f profileFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse redfish profile %q: %w", path, err)
	}
	if f.Name != "" {
		p.Name = f.Name
	}
	if f.PowerStatePath != "" {
		p.PowerStatePath = f.PowerStatePath
	}
	if len(f.OSUsage) > 0 {
		out := make([]OSUsageField, 0, len(f.OSUsage))
		for _, u := range f.OSUsage {
			if u.Metric == "" || u.Path == "" {
				return nil, fmt.Errorf("redfish profile %q: os_usage entry needs metric+path", path)
			}
			out = append(out, OSUsageField{Metric: u.Metric, Path: u.Path})
		}
		p.OSUsage = out
	}
	return p, nil
}

// ─── dotted-path extraction over a decoded JSON document ─────────────────────

// floatAtPath returns the number at a dotted path (e.g. "Oem.Simulator.Cpu") in a
// decoded JSON object, or nil when any segment is missing or the leaf is not a
// number — matching the previous *float64 "absent → skip" semantics.
func floatAtPath(doc map[string]any, path string) *float64 {
	v, ok := valueAtPath(doc, path)
	if !ok {
		return nil
	}
	if f, ok := v.(float64); ok { // encoding/json decodes numbers to float64
		return &f
	}
	return nil
}

// stringAtPath returns the string at a dotted path, or "" when missing/non-string.
func stringAtPath(doc map[string]any, path string) string {
	v, ok := valueAtPath(doc, path)
	if !ok {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func valueAtPath(doc map[string]any, path string) (any, bool) {
	if path == "" || doc == nil {
		return nil, false
	}
	segs := strings.Split(path, ".")
	var cur any = doc
	for _, s := range segs {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[s]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
