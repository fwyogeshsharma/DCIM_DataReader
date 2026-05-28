package collector

import (
	"context"
	"fmt"

	v1 "github.com/faberwork/fwidr/proto/v1"
	"github.com/shirou/gopsutil/v3/disk"
)

// DiskCollector collects disk usage for all mounted partitions.
type DiskCollector struct{ Base }

func (c *DiskCollector) Name() string { return "disk" }

func (c *DiskCollector) Collect(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	parts, err := disk.PartitionsWithContext(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("disk: partitions: %w", err)
	}

	var pkts []*v1.TelemetryPacket
	for _, p := range parts {
		usage, err := disk.UsageWithContext(ctx, p.Mountpoint)
		if err != nil {
			continue
		}
		meta := map[string]string{
			"collector_agent":    "IDR",
			"collector_protocol": "AGENT_LOCAL",
			"mountpoint":         p.Mountpoint,
			"filesystem_type":    p.Fstype,
		}
		tag := p.Mountpoint
		pkts = append(pkts,
			c.NewPacket("server.storage_size_kb", tag, float64(usage.Total/1024), meta),
			c.NewPacket("server.storage_used_kb", tag, float64(usage.Used/1024), meta),
			c.NewPacket("server.storage_available_kb", tag, float64(usage.Free/1024), meta),
			c.NewPacket("server.storage_used_percent", tag, usage.UsedPercent, meta),
		)
	}
	return pkts, nil
}
