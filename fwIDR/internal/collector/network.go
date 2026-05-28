package collector

import (
	"context"
	"fmt"

	v1 "github.com/faberwork/fwidr/proto/v1"
	"github.com/shirou/gopsutil/v3/net"
)

// NetworkCollector collects per-interface counters (maps to IF-MIB).
type NetworkCollector struct{ Base }

func (c *NetworkCollector) Name() string { return "network" }

func (c *NetworkCollector) Collect(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	counters, err := net.IOCountersWithContext(ctx, true) // per-interface
	if err != nil {
		return nil, fmt.Errorf("network: io counters: %w", err)
	}

	var pkts []*v1.TelemetryPacket
	for _, iface := range counters {
		tag := iface.Name
		meta := map[string]string{
			"collector_agent":    "IDR",
			"collector_protocol": "AGENT_LOCAL",
			"interface_name":     iface.Name,
		}
		pkts = append(pkts,
			c.NewPacket("interface.bytes_received", tag, float64(iface.BytesRecv), meta),
			c.NewPacket("interface.bytes_sent", tag, float64(iface.BytesSent), meta),
			c.NewPacket("interface.packets_received_unicast", tag, float64(iface.PacketsRecv), meta),
			c.NewPacket("interface.packets_sent_unicast", tag, float64(iface.PacketsSent), meta),
			c.NewPacket("interface.errors_received", tag, float64(iface.Errin), meta),
			c.NewPacket("interface.errors_sent", tag, float64(iface.Errout), meta),
			c.NewPacket("interface.discards_received", tag, float64(iface.Dropin), meta),
			c.NewPacket("interface.discards_sent", tag, float64(iface.Dropout), meta),
		)
	}

	// Interface oper status
	ifaces, err := net.InterfacesWithContext(ctx)
	if err == nil {
		for _, iface := range ifaces {
			operStatus := 2.0 // down
			for _, flag := range iface.Flags {
				if flag == "up" {
					operStatus = 1.0
					break
				}
			}
			meta := map[string]string{
				"collector_agent":    "IDR",
				"collector_protocol": "AGENT_LOCAL",
				"interface_name":     iface.Name,
			}
			pkts = append(pkts, c.NewPacket("interface.operational_status", iface.Name, operStatus, meta))
		}
	}

	return pkts, nil
}
