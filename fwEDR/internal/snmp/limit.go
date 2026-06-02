package snmp

import (
	"context"
	"sync"
)

var socketGate = struct {
	mu  sync.Mutex
	sem chan struct{}
}{
	sem: make(chan struct{}, 1),
}

// ConfigureSocketLimit sets the process-wide cap for SNMP UDP socket users.
// Polling, discovery, and enrichment all share this gate so auxiliary walks
// cannot bypass the poller's max_concurrent protection and trigger WSAENOBUFS.
func ConfigureSocketLimit(max int) {
	if max < 1 {
		max = 1
	}
	socketGate.mu.Lock()
	socketGate.sem = make(chan struct{}, max)
	socketGate.mu.Unlock()
}

// AcquireSocket reserves one process-wide SNMP socket slot.
func AcquireSocket(ctx context.Context) (func(), bool) {
	socketGate.mu.Lock()
	sem := socketGate.sem
	socketGate.mu.Unlock()

	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	case <-ctx.Done():
		return nil, false
	}
}
