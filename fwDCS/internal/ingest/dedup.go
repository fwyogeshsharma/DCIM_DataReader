// Package ingest handles validation, dedup, normalization, and storage of
// incoming TelemetryPackets.
package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deduper uses Redis to reject duplicate packets within a 1-hour window.
type Deduper struct {
	rdb *redis.Client
}

// NewDeduper creates a Deduper backed by the given Redis client.
func NewDeduper(rdb *redis.Client) *Deduper {
	return &Deduper{rdb: rdb}
}

// IsDuplicate returns true if this packet was already seen.
// Also validates the monotonic nonce to prevent replays.
func (d *Deduper) IsDuplicate(ctx context.Context, packetID, sourceID string, nonce uint64) (bool, error) {
	// Layer 1 — packet ID dedup (1-hour window covers any retry storm)
	pktKey := "pkt:" + packetID
	set, err := d.rdb.SetNX(ctx, pktKey, 1, time.Hour).Result()
	if err != nil {
		return false, fmt.Errorf("dedup: redis setNX: %w", err)
	}
	if !set {
		return true, nil // duplicate packet ID
	}

	// Layer 2 — nonce replay protection per source
	nonceKey := "nonce:" + sourceID
	prev, err := d.rdb.Get(ctx, nonceKey).Uint64()
	if err != nil && err != redis.Nil {
		return false, fmt.Errorf("dedup: redis get nonce: %w", err)
	}
	if nonce <= prev {
		return true, nil // replay or out-of-order
	}
	d.rdb.Set(ctx, nonceKey, nonce, 24*time.Hour) //nolint:errcheck

	return false, nil
}
