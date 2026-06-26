// Package health carries a small, lock-free signal between the publisher (which
// knows whether DCS is reachable) and the poller (which must stop collecting
// while DCS is down, so the local queue can't grow without bound).
package health

import "sync/atomic"

// Gate is the EDR collection gate. The publisher is the sole writer; the poller
// reads it. The zero value is ready to use and reports DCS up.
type Gate struct {
	dcsDown atomic.Bool
}

// SetDCSDown records DCS reachability. The publisher calls it with true once its
// push-failure streak crosses the configured threshold, and false on the first
// push/probe that succeeds again.
func (g *Gate) SetDCSDown(down bool) { g.dcsDown.Store(down) }

// DCSDown reports whether collection should pause because DCS is unreachable.
func (g *Gate) DCSDown() bool { return g.dcsDown.Load() }
