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
	Edges []topoEdge `json:"edges"`
}

type topoNode struct {
	ID     string     `json:"id"` // node id referenced by edge src/dst
	Device topoDevice `json:"device"`
}

// topoEdge maps one entry of the topology JSON "edges" array. The simulator tags
// every edge with a layer: "production" (data-plane fabric, also discoverable via
// LLDP), "management" (device ↔ OOB switch), or "power" (floor PDU → UPS → rack
// PDU → device). src/dst reference node ids; src_iface/dst_iface are 0-based
// interface-list positions on each endpoint.
type topoEdge struct {
	Src      string `json:"src"`
	Dst      string `json:"dst"`
	SrcIface int    `json:"src_iface"`
	DstIface int    `json:"dst_iface"`
	Layer    string `json:"layer"`
}

// LinkEdge is a topology edge resolved from node ids to device hostnames, ready
// to be emitted as a DCS topology packet. Carries the source device's
// datacenter/floor so the packet routes into the correct tenant scope.
type LinkEdge struct {
	SrcHost         string
	DstHost         string
	SrcMgmtIP       string // stable identifiers — DCS resolves endpoints by mgmt_ip first
	DstMgmtIP       string
	SrcDatacenterID string
	SrcFloorID      string
	Layer           string // "network" | "management" | "power" | "cooling"
	SrcPortIndex    int    // 0-based; +1 = SNMP ifIndex
	DstPortIndex    int
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

	// Power-graph helpers — used to count a Verdigris EV2's *active* circuits
	// (breakers with a real load wired to them) so the BACnet manager reads only
	// connected circuits instead of the meter's full physical capacity. Mirrors
	// the simulator's own derivation in api/routers/bacnet.py.
	idType := make(map[string]string, len(topo.Nodes))
	for _, n := range topo.Nodes {
		idType[n.ID] = n.Device.DeviceType
	}
	powerAdj := make(map[string][]string)
	for _, e := range topo.Edges {
		if e.Layer != "power" {
			continue
		}
		powerAdj[e.Src] = append(powerAdj[e.Src], e.Dst)
		powerAdj[e.Dst] = append(powerAdj[e.Dst], e.Src)
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
		// Servers run TWO SNMP agents: the OS agent on the PROD IP (ifTable, LLDP,
		// HOST-RESOURCES; sysName = the host name) and the BMC agent on the MGMT IP
		// (enterprise health; sysName = "<name>-bmc"). The simulator routes by
		// community = the agent's IP. Default community here is the mgmt IP, which
		// hits the BMC agent — so SNMP liveness adopted "<name>-bmc" and the hostname
		// flapped against the plain name from registration/Redfish, and the interface
		// walk found no ifTable. Route a server's SNMP to its OS agent (community =
		// prod IP). Hardware health still comes from Redfish on the mgmt IP.
		if d.DeviceType == "server" && prodIP != "" {
			tc.Community = prodIP
		}
		if gnmiCapable[d.DeviceType] && prodIP != "" {
			tc.GNMIEnabled = true
			tc.GNMIIP = mgmtIP // connect to per-device gNMI server on mgmt IP
		}
		// energy_monitor = Verdigris EV2 BACnet/IP power meter. Polled by the
		// BACnet manager at MgmtIP:47808 (EV2 is mgmt-only — no SNMP power data).
		if d.DeviceType == "energy_monitor" {
			tc.BACnetEnabled = true
			tc.ActiveCircuits = activeCircuitsFor(n.ID, powerAdj, idType)
		}
		// Chiller-plant devices expose process telemetry (water temps, flow,
		// pressures, fan/motor power, alarms) over BACnet too — poll them as well.
		if isPlantDeviceType(d.DeviceType) {
			tc.BACnetEnabled = true
		}
		out = append(out, tc)
	}
	return out, nil
}

// LoadTopologyLinks parses a simulator topology JSON and returns ALL edges
// (production/management/power) resolved from node ids to device hostnames +
// mgmt IPs. The simulator topology JSON is the complete, authoritative source of
// the port → connected-device relation, available instantly at startup — no
// dependence on the (slow, sometimes incomplete) LLDP walk. DCS builds
// topology_links from these, so a link-down trap can immediately look up the peer
// and write a single merged event.
//
// The sim's "production" layer is mapped to DCS's "network" layer value.
func LoadTopologyLinks(path string) ([]LinkEdge, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("topology: open %s: %w", path, err)
	}
	defer f.Close()

	var topo topoFile
	if err := json.NewDecoder(f).Decode(&topo); err != nil {
		return nil, fmt.Errorf("topology: decode %s: %w", path, err)
	}

	type nodeInfo struct{ host, mgmtIP, dc, floor string }
	idx := make(map[string]nodeInfo, len(topo.Nodes))
	for _, n := range topo.Nodes {
		if n.ID == "" || n.Device.Name == "" {
			continue
		}
		// mgmt_ip = explicit field, else snmp_community (sim ≤ v2.1 convention).
		mgmtIP := n.Device.MgmtIPExplicit
		if mgmtIP == "" {
			mgmtIP = n.Device.SNMPCommunity
		}
		idx[n.ID] = nodeInfo{host: n.Device.Name, mgmtIP: mgmtIP, dc: n.Device.Datacenter, floor: n.Device.Floor}
	}

	out := make([]LinkEdge, 0, len(topo.Edges))
	for _, e := range topo.Edges {
		layer := e.Layer
		if layer == "production" {
			layer = "network" // DCS layer convention
		}
		// cooling = chilled-water loop edges (chiller↔pump↔valve↔cooling_tower, plus
		// CRAH/sensor ties). Forwarded so the plant topology is queryable in DCS;
		// link-trap correlation only touches the network layer, so this is inert there.
		if layer != "network" && layer != "management" && layer != "power" && layer != "cooling" {
			continue
		}
		s, ok1 := idx[e.Src]
		d, ok2 := idx[e.Dst]
		if !ok1 || !ok2 || s.host == "" || d.host == "" {
			continue
		}
		out = append(out, LinkEdge{
			SrcHost:         s.host,
			DstHost:         d.host,
			SrcMgmtIP:       s.mgmtIP,
			DstMgmtIP:       d.mgmtIP,
			SrcDatacenterID: s.dc,
			SrcFloorID:      s.floor,
			Layer:           layer,
			SrcPortIndex:    e.SrcIface,
			DstPortIndex:    e.DstIface,
		})
	}
	return out, nil
}

// activeCircuitsFor counts the breakers actually wired to a load on the panel an
// EV2 monitors — i.e. how many per-circuit objects carry real data. It walks the
// power graph exactly like the simulator (api/routers/bacnet.py):
//
//  1. the EV2's power neighbour is the panel/PDU it clamps onto;
//  2. that panel's *other* power neighbours, excluding upstream feeds
//     (ups/generator), are the downstream loads → one circuit each.
//
// Returns 0 when the EV2 has no power edges (unknown topology) so the caller can
// fall back to the meter's full physical capacity.
func activeCircuitsFor(ev2ID string, powerAdj map[string][]string, idType map[string]string) int {
	neighbors := powerAdj[ev2ID]
	if len(neighbors) == 0 {
		return 0
	}
	pduID := neighbors[0] // a meter clamps onto exactly one panel
	upstream := map[string]bool{"ups": true, "generator": true}
	active := 0
	for _, nb := range powerAdj[pduID] {
		if nb == ev2ID || upstream[idType[nb]] {
			continue
		}
		active++
	}
	return active
}

// isPlantDeviceType reports whether a device_type is a chiller-plant BACnet
// device (chiller/pump/cooling_tower/valve/crah/cdu). Kept in sync with the bacnet
// package's plantObjects map.
func isPlantDeviceType(dt string) bool {
	switch dt {
	case "chiller", "pump", "cooling_tower", "valve", "crah", "cdu":
		return true
	default:
		return false
	}
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
