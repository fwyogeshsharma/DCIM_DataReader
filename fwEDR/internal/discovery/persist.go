package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/faberwork/fwedr/internal/target"
)

// persistedTarget is the on-disk JSON form. Mirrors target.Target with
// explicit JSON tags so the file is human-readable and forward-compatible.
type persistedTarget struct {
	IP          string            `json:"ip"`
	MgmtIP      string            `json:"mgmt_ip"`
	ProdIP      string            `json:"prod_ip"`
	LoopbackIP  string            `json:"loopback_ip,omitempty"`
	OOBIP       string            `json:"oob_ip,omitempty"`
	GNMIIP      string            `json:"gnmi_ip"`
	Hostname    string            `json:"hostname"`
	DeviceType  string            `json:"device_type"`
	Vendor      string            `json:"vendor"`
	Labels      map[string]string `json:"labels,omitempty"`
	SNMPVersion int               `json:"snmp_version"`
	Community   string            `json:"community"`
	Caps        uint8             `json:"caps"`
}

// targetsFile is the persisted-state file format.
type targetsFile struct {
	Version       int               `json:"version"`
	LastSweepAtNs int64             `json:"last_sweep_at_ns"`
	Targets       []persistedTarget `json:"targets"`
}

// Save writes targets to path atomically. Used after a successful sweep so
// subsequent EDR startups can skip the simulator-hammering rediscovery burst.
func Save(path string, targets []*target.Target) error {
	if path == "" {
		return errors.New("discovery: empty persist path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("discovery: mkdir %s: %w", filepath.Dir(path), err)
	}
	out := targetsFile{
		Version:       1,
		LastSweepAtNs: time.Now().UnixNano(),
		Targets:       make([]persistedTarget, 0, len(targets)),
	}
	for _, t := range targets {
		if t == nil {
			continue
		}
		out.Targets = append(out.Targets, persistedTarget{
			IP:          t.IP,
			MgmtIP:      t.MgmtIP,
			ProdIP:      t.ProdIP,
			LoopbackIP:  t.LoopbackIP,
			OOBIP:       t.OOBIP,
			GNMIIP:      t.GNMIIP,
			Hostname:    t.Hostname,
			DeviceType:  t.DeviceType,
			Vendor:      t.Vendor,
			Labels:      t.Labels,
			SNMPVersion: t.SNMPVersion,
			Community:   t.Community,
			Caps:        uint8(t.Caps),
		})
	}
	buf, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("discovery: marshal: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return fmt.Errorf("discovery: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("discovery: rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}

// Load reads persisted targets if the file exists and is no older than maxAge.
// Returns (targets, age, true) when usable; (nil, 0, false) when missing,
// expired, or invalid. Callers fall back to running a fresh sweep on false.
func Load(path string, maxAge time.Duration) ([]*target.Target, time.Duration, bool) {
	if path == "" {
		return nil, 0, false
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false
	}
	var f targetsFile
	if err := json.Unmarshal(buf, &f); err != nil {
		return nil, 0, false
	}
	if f.Version != 1 {
		return nil, 0, false
	}
	age := time.Since(time.Unix(0, f.LastSweepAtNs))
	if maxAge > 0 && age > maxAge {
		return nil, age, false
	}
	out := make([]*target.Target, 0, len(f.Targets))
	for _, p := range f.Targets {
		out = append(out, &target.Target{
			IP:          p.IP,
			MgmtIP:      p.MgmtIP,
			ProdIP:      p.ProdIP,
			LoopbackIP:  p.LoopbackIP,
			OOBIP:       p.OOBIP,
			GNMIIP:      p.GNMIIP,
			Hostname:    p.Hostname,
			DeviceType:  p.DeviceType,
			Vendor:      p.Vendor,
			Labels:      p.Labels,
			SNMPVersion: p.SNMPVersion,
			Community:   p.Community,
			Caps:        target.Capability(p.Caps),
		})
	}
	return out, age, true
}

// DefaultPath returns the default targets.json location alongside the EDR queue.
func DefaultPath(queuePath string) string {
	if queuePath == "" {
		return "targets.json"
	}
	return filepath.Join(filepath.Dir(queuePath), "targets.json")
}
