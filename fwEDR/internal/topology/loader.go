// Package topology loads device targets from a simulator topology JSON file.
package topology

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/faberwork/fwedr/pkg/config"
)

// gnmiCapable device types — these get gnmi:true when a topology is loaded.
var gnmiCapable = map[string]bool{
	"router":        true,
	"switch":        true,
	"firewall":      true,
	"load_balancer": true,
}

type topoFile struct {
	Nodes []topoNode `json:"nodes"`
}

type topoNode struct {
	Device topoDevice `json:"device"`
}

// topoDevice maps the simulator topology JSON "device" object.
// Supports both the legacy field layout (sim ≤ v2.1) and the explicit-IP
// layout introduced in v2.2. Unknown fields are silently ignored.
type topoDevice struct {
	Name       string `json:"name"`
	DeviceType string `json:"device_type"`
	Vendor     string `json:"vendor"`
	ModelName  string `json:"model_name"`

	// Legacy IP fields (sim ≤ v2.1).
	// ip_address      → production / data-plane IP (e.g. 10.x)
	// snmp_community  → carries the management IP in simulator mode
	IPAddress     string `json:"ip_address"`
	SNMPCommunity string `json:"snmp_community"`
	GNMIPort      int    `json:"gnmi_port"`

	// Explicit IP fields (sim v2.2+). When present these take precedence over
	// the legacy snmp_community / ip_address mapping.
	MgmtIPExplicit string `json:"mgmt_ip"`
	ProdIPExplicit string `json:"prod_ip"`

	// Physical location fields (sim v2.2+). All optional; zero-value when
	// not yet exported by the simulator.
	Country        string `json:"country"`
	Datacenter     string `json:"datacenter"`      // physical datacenter name, e.g. "DC1"
	DatacenterCity string `json:"datacenter_city"` // e.g. "Dallas", "Chicago"
	Room           string `json:"room"`
	Floor          string `json:"floor"` // populated in next simulator update
	RackRow        int    `json:"rack_row"`
	RackNum        int    `json:"rack_num"`
	RackUnit       int    `json:"rack_unit"`
}

// LoadTargets parses a simulator topology JSON and returns a target list.
//
// IP resolution (priority order):
//
//	mgmt_ip  = explicit "mgmt_ip" field  OR  snmp_community (sim ≤ v2.1 convention)
//	prod_ip  = explicit "prod_ip" field  OR  ip_address
//
// Physical location fields (country, datacenter, room, …) are read from the
// device object and stored in TargetConfig so they flow into the DCS
// `devices` table via registration packets.
func LoadTargets(path string) ([]config.TargetConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("topology: open %s: %w", path, err)
	}
	defer f.Close()

	var topo topoFile
	if err := json.NewDecoder(f).Decode(&topo); err != nil {
		return nil, fmt.Errorf("topology: decode %s: %w", path, err)
	}

	out := make([]config.TargetConfig, 0, len(topo.Nodes))
	for _, n := range topo.Nodes {
		d := n.Device
		if d.Name == "" || d.SNMPCommunity == "" {
			continue
		}

		mgmtIP := d.MgmtIPExplicit
		if mgmtIP == "" {
			// sim ≤ v2.1: community string carries the per-device mgmt IP.
			mgmtIP = d.SNMPCommunity
		}

		prodIP := d.ProdIPExplicit
		if prodIP == "" {
			prodIP = d.IPAddress
		}

		tc := config.TargetConfig{
			IP:         "127.0.0.1",
			MgmtIP:     mgmtIP,
			ProdIP:     prodIP,
			Hostname:   d.Name,
			DeviceType: d.DeviceType,
			Community:  d.SNMPCommunity,
			Vendor:     normalizeVendor(d.Vendor),
			ModelName:  d.ModelName,
			// Per-device routing keys (flow into pkt.DatacenterId / pkt.FloorId).
			// Fall back to global identity values when empty.
			DatacenterID: d.Datacenter,
			FloorID:      d.Floor, // floor from simulator → pkt.FloorId routing key
			// Physical location — stored in devices table columns.
			Country:        d.Country,
			DatacenterName: d.Datacenter,
			DatacenterCity: d.DatacenterCity,
			Room:           d.Room,
			Floor:          d.Floor,
			RackRow:        d.RackRow,
			RackNum:        d.RackNum,
			RackUnit:       d.RackUnit,
		}
		if gnmiCapable[d.DeviceType] && prodIP != "" {
			tc.GNMIEnabled = true
			tc.GNMIIP = mgmtIP // connect to per-device gNMI server on mgmt IP
		}
		out = append(out, tc)
	}
	return out, nil
}

func normalizeVendor(v string) string {
	v = strings.ToLower(v)
	switch {
	case strings.Contains(v, "cisco"):
		return "cisco"
	case strings.Contains(v, "juniper"):
		return "juniper"
	case strings.Contains(v, "arista"):
		return "arista"
	case strings.Contains(v, "apc"), strings.Contains(v, "schneider"):
		return "apc"
	case strings.Contains(v, "eaton"):
		return "eaton"
	case strings.Contains(v, "raritan"):
		return "raritan"
	case strings.Contains(v, "vertiv"), strings.Contains(v, "liebert"):
		return "vertiv"
	case strings.Contains(v, "palo alto"):
		return "paloalto"
	case strings.Contains(v, "f5"):
		return "f5"
	default:
		return "generic"
	}
}
