package collector

import (
	"context"
	"fmt"

	v1 "github.com/faberwork/fwidr/proto/v1"
	"github.com/shirou/gopsutil/v3/mem"
)

// MemoryCollector collects RAM utilization.
type MemoryCollector struct{ Base }

func (c *MemoryCollector) Name() string { return "memory" }

func (c *MemoryCollector) Collect(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("memory: virtual: %w", err)
	}

	meta := map[string]string{"collector_agent": "IDR", "collector_protocol": "AGENT_LOCAL"}
	return []*v1.TelemetryPacket{
		c.NewPacket("system.memory_total_bytes", "", float64(vm.Total), meta),
		c.NewPacket("system.memory_used_bytes", "", float64(vm.Used), meta),
		c.NewPacket("system.memory_free_bytes", "", float64(vm.Free), meta),
		c.NewPacket("server.memory_total_kb", "", float64(vm.Total/1024), meta),
		c.NewPacket("server.memory_used_kb", "", float64(vm.Used/1024), meta),
		c.NewPacket("server.memory_free_kb", "", float64(vm.Free/1024), meta),
		c.NewPacket("server.memory_cached_kb", "", float64(vm.Cached/1024), meta),
		c.NewPacket("server.memory_buffer_kb", "", float64(vm.Buffers/1024), meta),
	}, nil
}
