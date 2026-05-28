package collector

import (
	"context"
	"strings"

	v1 "github.com/faberwork/fwidr/proto/v1"
	"github.com/shirou/gopsutil/v3/host"
)

// TemperatureCollector collects hardware temperature sensors.
// Supported: Linux (coretemp/acpitz), macOS (SMC), Windows (limited).
type TemperatureCollector struct{ Base }

func (c *TemperatureCollector) Name() string { return "temperature" }

func (c *TemperatureCollector) Collect(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	sensors, err := host.SensorsTemperaturesWithContext(ctx)
	if err != nil || len(sensors) == 0 {
		// Not fatal — many cloud VMs expose no sensors.
		return nil, nil
	}

	var pkts []*v1.TelemetryPacket
	for _, s := range sensors {
		meta := map[string]string{
			"collector_agent":    "IDR",
			"collector_protocol": "AGENT_LOCAL",
			"sensor_key":         s.SensorKey,
		}

		// Map sensor keys to canonical metric names.
		name := "system.inlet_temperature_c"
		key := strings.ToLower(s.SensorKey)
		switch {
		case strings.ContainsAny(key, "") && (strings.Contains(key, "cpu") ||
			strings.Contains(key, "core") || strings.Contains(key, "package")):
			name = "system.cpu_temperature_c"
		case strings.Contains(key, "chassis") || strings.Contains(key, "inlet") ||
			strings.Contains(key, "ambient") || strings.Contains(key, "acpitz"):
			name = "system.chassis_temperature_c"
		}

		pkts = append(pkts, c.NewPacket(name, s.SensorKey, s.Temperature, meta))
	}
	return pkts, nil
}
