package collector

import (
	"context"
	"fmt"

	v1 "github.com/faberwork/fwidr/proto/v1"
	"github.com/shirou/gopsutil/v3/cpu"
)

// CPUCollector collects CPU utilization — overall and per-core.
type CPUCollector struct{ Base }

func (c *CPUCollector) Name() string { return "cpu" }

func (c *CPUCollector) Collect(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	// Overall (blocking 1-second sample)
	overall, err := cpu.PercentWithContext(ctx, 0, false)
	if err != nil {
		return nil, fmt.Errorf("cpu: overall percent: %w", err)
	}

	// Per-core
	perCore, err := cpu.PercentWithContext(ctx, 0, true)
	if err != nil {
		return nil, fmt.Errorf("cpu: per-core percent: %w", err)
	}

	pkts := make([]*v1.TelemetryPacket, 0, 1+len(perCore))

	if len(overall) > 0 {
		pkts = append(pkts, c.NewPacket("system.cpu_utilization_percent", "", overall[0],
			map[string]string{"collector_agent": "IDR", "collector_protocol": "AGENT_LOCAL"}))
	}

	for i, pct := range perCore {
		pkts = append(pkts, c.NewPacket("server.cpu_per_core_percent", fmt.Sprintf("%d", i), pct,
			map[string]string{"collector_agent": "IDR", "collector_protocol": "AGENT_LOCAL"}))
	}

	return pkts, nil
}
