// Package target defines the Target type representing one polled device.
package target

import "github.com/faberwork/fwedr/pkg/config"

// Capability flags for a target device.
type Capability uint8

const (
	CapSNMP Capability = 1 << iota
	CapGNMI
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
	DeviceType string // router|switch|server|firewall|load_balancer|ups|pdu|floor_pdu|oob_switch|sensor
	Vendor     string // cisco|juniper|arista|apc|raritan|vertiv|eaton|generic
	Labels     map[string]string

	SNMPVersion int    // 2 or 3
	Community   string // v2c community
	Caps        Capability

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
	if t.Hostname != "" {
		return t.Hostname
	}
	return t.IP
}
