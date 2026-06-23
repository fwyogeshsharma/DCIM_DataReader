// Package target defines the Target type representing one polled device.
package target

import (
	"strconv"
	"strings"
	"sync"

	"github.com/faberwork/fwedr/pkg/config"
)

// Capability flags for a target device.
type Capability uint8

const (
	CapSNMP Capability = 1 << iota
	CapGNMI
	CapBACnet
)

// Target is a resolved device ready for polling.
//
// IP roles follow industry-standard DCIM naming:
//
//	IP         — the address SNMP/UDP connects to (loopback in simulator mode,
//	             real device IP in production).
//	MgmtIP     — operator-facing management IP (always set; usually 192.168.x.x).
//	ProdIP     — main operational IP exposed to dashboards. For routers/switches
//	             this is the loopback; for servers it's the primary data-plane NIC.
//	LoopbackIP — explicit router/switch loopback if known (optional).
//	OOBIP      — out-of-band management network IP (optional).
//	GNMIIP     — gNMI connection IP (usually same as MgmtIP).
type Target struct {
	IP         string
	MgmtIP     string
	ProdIP     string
	LoopbackIP string
	OOBIP      string
	GNMIIP     string

	Hostname   string
	hostMu     sync.RWMutex // guards live Hostname updates (SNMP sysName adoption)
	DeviceType string       // router|switch|server|firewall|load_balancer|ups|pdu|floor_pdu|oob_switch|sensor
	Vendor     string       // cisco|juniper|arista|apc|raritan|vertiv|eaton|generic
	Labels     map[string]string

	SNMPVersion int    // 2 or 3
	Community   string // v2c community
	Caps        Capability

	// ActiveCircuits is the count of EV2 circuits with a load wired to them
	// (from the topology power graph). The BACnet manager reads only this many
	// circuits instead of the meter's full capacity. 0 = unknown → read all.
	ActiveCircuits int

	// Per-device routing key overrides. When non-empty these override the
	// global identity.datacenter_id / identity.floor_id in packet headers so
	// multi-datacenter topology files are handled correctly.
	// Empty = use the global identity config value.
	DatacenterID string // e.g. "DC1" from topology JSON
	FloorID      string // e.g. "floor-2" (empty until simulator exports it)

	// Physical location — written to devices table columns. All optional.
	ModelName      string
	Country        string
	DatacenterName string // informational physical name
	DatacenterCity string // city where datacenter is located, e.g. "Dallas"
	Room           string
	Floor          string
	RackRow        int
	RackNum        int
	RackUnit       int
}

// FromConfig converts a TargetConfig + global SNMPConfig into a Target.
func FromConfig(tc config.TargetConfig, global config.SNMPConfig) *Target {
	ver := tc.SNMPVersion
	if ver == 0 {
		ver = global.Version
	}
	comm := tc.Community
	if comm == "" {
		comm = global.Community
	}
	vendor := tc.Vendor
	if vendor == "" {
		vendor = "generic"
	}
	mgmtIP := tc.MgmtIP
	if mgmtIP == "" {
		mgmtIP = tc.IP
	}
	gnmiIP := tc.GNMIIP
	if gnmiIP == "" {
		gnmiIP = mgmtIP
	}
	caps := CapSNMP
	if tc.GNMIEnabled {
		caps |= CapGNMI
	}
	if tc.BACnetEnabled {
		caps |= CapBACnet
	}
	return &Target{
		IP:             tc.IP,
		MgmtIP:         mgmtIP,
		ProdIP:         tc.ProdIP,
		LoopbackIP:     tc.LoopbackIP,
		OOBIP:          tc.OOBIP,
		GNMIIP:         gnmiIP,
		Hostname:       tc.Hostname,
		DeviceType:     tc.DeviceType,
		Vendor:         vendor,
		Labels:         tc.Labels,
		SNMPVersion:    ver,
		Community:      comm,
		Caps:           caps,
		ActiveCircuits: tc.ActiveCircuits,
		DatacenterID:   tc.DatacenterID,
		FloorID:        tc.FloorID,
		ModelName:      tc.ModelName,
		Country:        tc.Country,
		DatacenterName: tc.DatacenterName,
		DatacenterCity: tc.DatacenterCity,
		Room:           tc.Room,
		Floor:          tc.Floor,
		RackRow:        tc.RackRow,
		RackNum:        tc.RackNum,
		RackUnit:       tc.RackUnit,
	}
}

// Has returns true if the target supports the given capability.
func (t *Target) Has(c Capability) bool { return t.Caps&c != 0 }

// ClearCap removes a capability flag (e.g. disable gNMI globally when no gNMI
// servers exist in the environment).
func (t *Target) ClearCap(c Capability) { t.Caps &^= c }

// SourceID returns the canonical source identifier (hostname or IP).
func (t *Target) SourceID() string {
	t.hostMu.RLock()
	h := t.Hostname
	t.hostMu.RUnlock()
	if h != "" {
		return h
	}
	return t.IP
}

// SetHostname adopts a new hostname — e.g. the live SNMP sysName — so a device
// rename at the source propagates downstream (DCS keys device identity on
// mgmt_ip and treats hostname as a mutable attribute, so the row updates in
// place). No-op when empty or unchanged.
func (t *Target) SetHostname(name string) {
	if name == "" {
		return
	}
	t.hostMu.Lock()
	t.Hostname = name
	t.hostMu.Unlock()
}

// AssetSnapshot is a consistent (locked) read of the mutable location/asset
// fields. Builders use it so a concurrent PatchAsset never tears a read.
type AssetSnapshot struct {
	ModelName      string
	Country        string
	DatacenterName string
	DatacenterCity string
	Room           string
	RackRow        int
	RackNum        int
	RackUnit       int
}

// Asset returns a locked snapshot of the asset/location fields.
func (t *Target) Asset() AssetSnapshot {
	t.hostMu.RLock()
	defer t.hostMu.RUnlock()
	return AssetSnapshot{
		ModelName:      t.ModelName,
		Country:        t.Country,
		DatacenterName: t.DatacenterName,
		DatacenterCity: t.DatacenterCity,
		Room:           t.Room,
		RackRow:        t.RackRow,
		RackNum:        t.RackNum,
		RackUnit:       t.RackUnit,
	}
}

// PatchAsset updates one mutable asset/location field after a UI edit was
// applied to the device, so the next telemetry push reflects the new value.
// (These fields are seeded from the topology JSON; EDR doesn't re-read them from
// the device, so we update our own copy to the value we just wrote.) Returns
// true if the field is a known asset field. Thread-safe.
func (t *Target) PatchAsset(field, value string) bool {
	t.hostMu.Lock()
	defer t.hostMu.Unlock()
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "country":
		t.Country = value
	case "datacenter":
		t.DatacenterName = value
	case "datacenter_city", "city":
		t.DatacenterCity = value
	case "room":
		t.Room = value
	case "floor":
		t.Floor = value
	case "model", "model_name":
		t.ModelName = value
	case "rack_row":
		if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			t.RackRow = n
		}
	case "rack_num":
		if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			t.RackNum = n
		}
	case "rack_unit":
		if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			t.RackUnit = n
		}
	default:
		return false
	}
	return true
}
