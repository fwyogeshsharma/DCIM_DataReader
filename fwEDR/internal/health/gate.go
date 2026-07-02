// Package health carries a small, lock-free signal between the publisher (which
// knows whether DCS is reachable) and the poller (which must stop collecting
// while it can't ship, so the local queue can't grow without bound).
package health

import "sync/atomic"

// Gate is the EDR collection gate. Two independent reasons pause collection:
//   - dcsDown:      DCS is unreachable (publisher's push-failure streak). Written
//     by the publisher.
//   - backpressure: DCS is reachable but SLOW — the local queue is filling faster
//     than it drains (e.g. a slow/overwhelmed aggregator starving DCS). Written by
//     the queue-depth monitor.
//
// The poller calls ShouldPause() and stops on EITHER. This closes the gap where a
// slow-but-successful DCS never trips the down-streak, so the poller kept producing
// until EDR ran out of resources. The zero value reports "up, no backpressure".
type Gate struct {
	dcsDown      atomic.Bool
	backpressure atomic.Bool
}

// SetDCSDown records DCS reachability. The publisher calls it with true once its
// push-failure streak crosses the configured threshold, and false on the first
// push/probe that succeeds again.
func (g *Gate) SetDCSDown(down bool) { g.dcsDown.Store(down) }

// DCSDown reports the DCS-unreachable signal only (used by the publisher's own
// probe/resume logic — NOT the pause decision).
func (g *Gate) DCSDown() bool { return g.dcsDown.Load() }

// SetBackpressure records whether the local queue is too full to keep accepting
// new telemetry. Written by the queue-depth monitor at its high/low watermarks.
func (g *Gate) SetBackpressure(on bool) { g.backpressure.Store(on) }

// Backpressure reports the queue-full signal only.
func (g *Gate) Backpressure() bool { return g.backpressure.Load() }

// ShouldPause reports whether collection must pause — DCS down OR local queue
// backpressure. This is the poller's pause decision.
func (g *Gate) ShouldPause() bool { return g.dcsDown.Load() || g.backpressure.Load() }
