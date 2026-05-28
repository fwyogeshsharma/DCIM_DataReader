package ingest

import (
	"strings"
	"time"

	v1 "github.com/faberwork/fwdcs/proto/v1"
)

// Normalize enforces canonical units on a packet in-place:
//   - timestamp_ns already UTC nanoseconds — no conversion needed
//   - temperature values must be °C (EDR normalizes vendor ×10 encoding)
//   - tag trimmed of whitespace
func Normalize(pkt *v1.TelemetryPacket) {
	pkt.Tag = strings.TrimSpace(pkt.Tag)

	// Clamp timestamp: reject future values beyond 5 minutes
	now := time.Now().UnixNano()
	if pkt.TimestampNs > now+int64(5*time.Minute) {
		pkt.TimestampNs = now
	}
	// Floor: reject timestamps older than 24 hours
	if pkt.TimestampNs < now-int64(24*time.Hour) {
		pkt.TimestampNs = now
	}
}
