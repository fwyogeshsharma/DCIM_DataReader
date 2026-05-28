// Package collector gathers host metrics using gopsutil (cross-platform).
package collector

import (
	"context"
	"time"

	v1 "github.com/faberwork/fwidr/proto/v1"
)

// Collector is implemented by every metric source (CPU, memory, disk, etc.).
type Collector interface {
	Name() string
	Collect(ctx context.Context) ([]*v1.TelemetryPacket, error)
}

// Base carries the shared context injected into every packet.
type Base struct {
	OrgID        string
	DatacenterID string
	FloorID      string
	NetworkID    string
	GroupID      string
	SourceID     string // hostname
	ReaderID     string
}

// NewPacket builds a metric packet with identity pre-filled.
func (b *Base) NewPacket(name, tag string, value float64, meta map[string]string) *v1.TelemetryPacket {
	return &v1.TelemetryPacket{
		OrgId:        b.OrgID,
		DatacenterId: b.DatacenterID,
		FloorId:      b.FloorID,
		NetworkId:    b.NetworkID,
		GroupId:      b.GroupID,
		SourceType:   "host",
		SourceId:     b.SourceID,
		ReaderId:     b.ReaderID,
		TimestampNs:  time.Now().UnixNano(),
		Kind:         "metric",
		Name:         name,
		Tag:          tag,
		Value:        value,
		Meta:         meta,
	}
}
