package collector

import (
	"context"
	"fmt"

	v1 "github.com/faberwork/fwidr/proto/v1"
	"github.com/shirou/gopsutil/v3/host"
)

// OSInfoCollector collects host identity and uptime — updates devices table.
type OSInfoCollector struct{ Base }

func (c *OSInfoCollector) Name() string { return "os" }

func (c *OSInfoCollector) Collect(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	info, err := host.InfoWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("os_info: host info: %w", err)
	}

	meta := map[string]string{
		"collector_agent":    "IDR",
		"collector_protocol": "AGENT_LOCAL",
		"os_name":            info.OS,
		"platform":           info.Platform,
		"platform_version":   info.PlatformVersion,
		"kernel_version":     info.KernelVersion,
		"architecture":       info.KernelArch,
		"device_type":        "server",
	}

	return []*v1.TelemetryPacket{
		c.NewPacket("system.uptime_centiseconds", "", float64(info.Uptime*100), meta),
	}, nil
}
