// Package poller runs periodic SNMP and gNMI polls for all configured targets.
//
// Resilience design:
//   - global token-bucket rate limiter (snmp.rate_limit_per_sec) caps SNMP req/sec
//   - global semaphore (snmp.max_concurrent) caps simultaneous UDP sockets
//   - per-target circuit breaker: after N consecutive timeouts, pause that target
//     for breaker_cooldown_ms; recover on first successful Get
//   - jittered initial poll spreads 1000s of goroutines across the poll interval
//     so we never burst the SNMP listener (real device or SNMPSim)
//   - graceful shutdown: context.AfterFunc closes the UDP socket on cancel,
//     aborting in-flight BulkWalks so the process exits within a few seconds
package poller

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/gnmi"
	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

// Poller manages polling goroutines for all targets.
type Poller struct {
	targets  []*target.Target
	snmpCfg  config.SNMPConfig
	gnmiCfg  config.GNMIConfig
	identity config.IdentityConfig
	signer   *packet.Signer
	out      chan<- *v1.TelemetryPacket
	log      *zap.Logger

	snmpSem  chan struct{} // semaphore: caps simultaneous SNMP UDP sockets
	limiter  *rateLimiter  // global SNMP token bucket
	breakers sync.Map      // map[targetKey]*circuitBreaker
	conns    sync.Map      // map[targetKey]*targetConn — reused SNMP sessions

	health *healthMonitor // global pause/resume based on breaker ratio

	mu      sync.Mutex
	running map[string]context.CancelFunc // tracks running target goroutines by SourceID
	rootCtx context.Context               // captured in Run for AddTarget to spawn against
}

// New creates a Poller. out receives all collected packets.
func New(
	targets []*target.Target,
	snmpCfg config.SNMPConfig,
	gnmiCfg config.GNMIConfig,
	identity config.IdentityConfig,
	signer *packet.Signer,
	out chan<- *v1.TelemetryPacket,
	log *zap.Logger,
) *Poller {
	maxConc := snmpCfg.MaxConcurrent
	if maxConc <= 0 {
		maxConc = 50
	}
	p := &Poller{
		targets:  targets,
		snmpCfg:  snmpCfg,
		gnmiCfg:  gnmiCfg,
		identity: identity,
		signer:   signer,
		out:      out,
		log:      log,
		snmpSem:  make(chan struct{}, maxConc),
		health:   newHealthMonitor(len(targets), log),
		running:  make(map[string]context.CancelFunc),
	}
	if snmpCfg.RateLimitPerSec > 0 {
		p.limiter = newRateLimiter(snmpCfg.RateLimitPerSec)
	}
	return p
}

// Run starts one goroutine per target and blocks until ctx is cancelled.
// On cancel, it waits up to 5 seconds for all polls to drain.
func (p *Poller) Run(ctx context.Context) {
	p.rootCtx = ctx

	// Global health monitor — pauses SNMP polling when too many breakers are open.
	go p.health.run(ctx, &p.breakers)

	var wg sync.WaitGroup
	for _, t := range p.targets {
		t := t
		tCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.running[t.SourceID()] = cancel
		p.mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.runTarget(tCtx, t)
		}()
	}
	// Block here until shutdown is signalled.
	<-ctx.Done()

	// Graceful drain: wait up to 5s for in-flight polls to finish after cancel.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		p.log.Warn("poller: shutdown timeout — some SNMP sessions did not drain cleanly")
	}

	// Release reused SNMP sockets.
	p.closeConns()
}

func (p *Poller) runTarget(ctx context.Context, t *target.Target) {
	fastInterval := time.Duration(p.snmpCfg.FastIntervalMs) * time.Millisecond
	mediumInterval := time.Duration(p.snmpCfg.MediumIntervalMs) * time.Millisecond
	slowInterval := time.Duration(p.snmpCfg.SlowIntervalMs) * time.Millisecond
	topoInterval := time.Duration(p.snmpCfg.TopologyIntervalMs) * time.Millisecond
	if topoInterval <= 0 {
		topoInterval = 10 * time.Minute
	}

	fastTicker := time.NewTicker(fastInterval)
	defer fastTicker.Stop()
	mediumTicker := time.NewTicker(mediumInterval)
	defer mediumTicker.Stop()
	slowTicker := time.NewTicker(slowInterval)
	defer slowTicker.Stop()
	topoTicker := time.NewTicker(topoInterval)
	defer topoTicker.Stop()

	// gNMI: long-lived STREAM subscription, not periodic polling. Counters
	// arrive on SAMPLE intervals, link state changes arrive ON_CHANGE. This
	// bypasses SNMPSim entirely for network devices that support gNMI.
	// Stream opens are spread across a 30 s window so we don't burst-accept
	// against the simulator's per-device gNMI servers all at once.
	if t.Has(target.CapGNMI) {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(rand.Int63n(int64(30 * time.Second)))):
			}
			p.runGNMIStream(ctx, t)
		}()
	}

	// Spread initial polls across the full FAST interval. With 1000+ targets,
	// starting all at once produces a thundering herd.
	jitter := time.Duration(rand.Int63n(int64(fastInterval)))
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitter):
	}

	// First poll: FAST tier registers the device. Topology fires ~10s later
	// so LLDP neighbor data appears soon after registration (was 150s avg jitter
	// on the old SLOW tier — empty topology_links until cycle 2).
	p.pollSNMP(ctx, t, snmp.TierFast)

	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(10*time.Second + time.Duration(rand.Int63n(int64(5*time.Second)))):
			p.pollSNMP(ctx, t, snmp.TierTopology)
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(rand.Int63n(int64(mediumInterval / 2)))):
			p.pollSNMP(ctx, t, snmp.TierMedium)
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(rand.Int63n(int64(slowInterval / 2)))):
			p.pollSNMP(ctx, t, snmp.TierSlow)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-fastTicker.C:
			p.pollSNMP(ctx, t, snmp.TierFast)
		case <-mediumTicker.C:
			// Always poll SNMP TierMedium regardless of gNMI capability.
			// gNMI streams may not be available (e.g. simulator without gNMI
			// servers), so SNMP is the reliable path. gNMI supplements when live.
			p.pollSNMP(ctx, t, snmp.TierMedium)
		case <-slowTicker.C:
			// Always poll SNMP TierSlow regardless of gNMI capability.
			// Skipping this caused interface counters, HR-MIB, UPS and sensor
			// metrics to disappear for all gNMI-capable device types (router,
			// switch, firewall, load_balancer) when using topology mode.
			p.pollSNMP(ctx, t, snmp.TierSlow)
		case <-topoTicker.C:
			p.pollSNMP(ctx, t, snmp.TierTopology)
		}
	}
}

// AddTarget starts polling a target that arrived after Run started (e.g. via
// the background rediscovery loop in main). Idempotent — duplicates are
// ignored. The new target inherits the same root context as Run; cancelling
// the root context cancels all per-target goroutines.
func (p *Poller) AddTarget(t *target.Target) bool {
	if t == nil {
		return false
	}
	key := t.SourceID()
	p.mu.Lock()
	if _, exists := p.running[key]; exists {
		p.mu.Unlock()
		return false
	}
	if p.rootCtx == nil {
		// Run not yet called.
		p.mu.Unlock()
		return false
	}
	tCtx, cancel := context.WithCancel(p.rootCtx)
	p.running[key] = cancel
	p.health.totalAdd(1)
	p.mu.Unlock()
	go p.runTarget(tCtx, t)
	p.log.Info("poller: target added at runtime",
		zap.String("source_id", key),
		zap.String("type", t.DeviceType),
		zap.Bool("gnmi", t.Has(target.CapGNMI)))
	return true
}

// runGNMIStream maintains a persistent gNMI STREAM subscription with backoff
// reconnect. One goroutine per gnmi-enabled target for the lifetime of the EDR.
func (p *Poller) runGNMIStream(ctx context.Context, t *target.Target) {
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		// Per-device datacenter/floor override: topology JSON may carry the
		// device's own datacenter_id (e.g. "DC1"). Use it when present so
		// multi-datacenter topology files are routed correctly in DCS.
		dcID := p.identity.DatacenterID
		if t.DatacenterID != "" {
			dcID = t.DatacenterID
		}
		floorID := p.identity.FloorID
		if t.FloorID != "" {
			floorID = t.FloorID
		}
		sub := gnmi.NewSubscriber(
			t, p.gnmiCfg,
			p.identity.OrgID, dcID,
			floorID, p.identity.NetworkID,
			p.identity.GroupID, p.identity.ReaderID,
			p.signer, p.log,
		)
		p.log.Debug("gnmi stream connecting", zap.String("target", t.SourceID()), zap.String("ip", t.GNMIIP))
		err := sub.RunStream(ctx, p.out)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			p.log.Warn("gnmi stream error — reconnecting",
				zap.String("target", t.SourceID()),
				zap.Duration("retry_in", backoff),
				zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 60*time.Second {
			backoff *= 2
		}
	}
}

// targetConn holds one reusable SNMP session per target. mu serializes a
// target's concurrent tier polls over the shared session (gosnmp is not safe
// for concurrent use on one connection).
type targetConn struct {
	mu sync.Mutex
	cl *snmp.Client // nil until first connect / after an error reset
}

// getConn returns the reusable connection holder for a target, creating it on
// first use. The holder persists for the life of the poller so the underlying
// UDP socket is reused across polls instead of churned each cycle.
func (p *Poller) getConn(t *target.Target) *targetConn {
	key := t.SourceID()
	if v, ok := p.conns.Load(key); ok {
		return v.(*targetConn)
	}
	actual, _ := p.conns.LoadOrStore(key, &targetConn{})
	return actual.(*targetConn)
}

// closeConns closes every reused SNMP session. Called on shutdown so sockets
// are released cleanly.
func (p *Poller) closeConns() {
	p.conns.Range(func(_, v any) bool {
		tc := v.(*targetConn)
		tc.mu.Lock()
		if tc.cl != nil {
			tc.cl.Close()
			tc.cl = nil
		}
		tc.mu.Unlock()
		return true
	})
}

// pollSNMP acquires the global rate limit and concurrency semaphore, runs one
// tiered collection, and updates the per-target circuit breaker.
func (p *Poller) pollSNMP(ctx context.Context, t *target.Target, tier snmp.Tier) {
	if p.health.paused() {
		p.log.Debug("snmp poll skipped — global pause (breaker ratio over threshold)",
			zap.String("target", t.SourceID()),
			zap.Int("tier", int(tier)))
		return
	}

	br := p.getBreaker(t)
	if br.tripped() {
		p.log.Debug("snmp poll skipped — circuit open",
			zap.String("target", t.SourceID()),
			zap.Int("tier", int(tier)))
		return
	}

	if p.limiter != nil {
		if !p.limiter.wait(ctx) {
			return
		}
	}

	// Reuse one SNMP session per target instead of opening a fresh UDP socket
	// every poll. Per-poll socket churn was exhausting Windows socket buffers
	// (WSAENOBUFS) and capping throughput. tc.mu serializes this target's tiers
	// over the shared session; acquired before the semaphore so a blocked
	// same-target tier doesn't hold a concurrency slot.
	tc := p.getConn(t)
	tc.mu.Lock()
	defer tc.mu.Unlock()

	select {
	case p.snmpSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-p.snmpSem }()

	if tc.cl == nil {
		cl, err := snmp.NewClient(t, p.snmpCfg)
		if err != nil {
			br.recordFailure()
			p.log.Warn("snmp connect failed",
				zap.String("target", t.IP),
				zap.Error(err))
			return
		}
		tc.cl = cl
	}
	cl := tc.cl

	// Close the socket on ctx cancel so an in-flight BulkWalk returns instantly.
	stop := context.AfterFunc(ctx, func() { cl.Close() })
	defer stop()

	// Per-device datacenter/floor override (same logic as runGNMIStream).
	snmpDCID := p.identity.DatacenterID
	if t.DatacenterID != "" {
		snmpDCID = t.DatacenterID
	}
	snmpFloorID := p.identity.FloorID
	if t.FloorID != "" {
		snmpFloorID = t.FloorID
	}
	collector := snmp.NewCollector(
		t, cl,
		p.identity.OrgID, snmpDCID,
		snmpFloorID, p.identity.NetworkID,
		p.identity.GroupID, p.identity.ReaderID,
		p.signer,
	)
	pkts, err := collector.Collect(tier)
	if err != nil {
		br.recordFailure()
		// Session may be broken (timeout, or socket closed by the cancel hook
		// above) — drop it so the next poll reconnects with a fresh socket.
		cl.Close()
		tc.cl = nil
		p.log.Warn("snmp collect failed",
			zap.String("target", t.IP),
			zap.Int("tier", int(tier)),
			zap.Error(err))
		return
	}
	br.recordSuccess()
	p.log.Debug("snmp collected",
		zap.String("target", t.IP),
		zap.Int("tier", int(tier)),
		zap.Int("packets", len(pkts)))
	for _, pkt := range pkts {
		select {
		case p.out <- pkt:
		case <-ctx.Done():
			return
		}
	}
}

// ─── health monitor ──────────────────────────────────────────────────────────

// healthMonitor tracks the ratio of open circuit breakers across all targets.
// When the ratio exceeds threshold, polling is paused site-wide for cooldown.
// Prevents EDR from hammering a wedged SNMP simulator and cascading breaker trips.
type healthMonitor struct {
	total      atomic.Int64 // total targets currently being polled
	threshold  float64      // pause when ratio >= threshold
	cooldown   time.Duration
	pauseUntil atomic.Int64 // unix nanos
	log        *zap.Logger
}

func newHealthMonitor(totalTargets int, log *zap.Logger) *healthMonitor {
	h := &healthMonitor{
		threshold: 0.30,
		cooldown:  60 * time.Second,
		log:       log,
	}
	h.totalAdd(int64(totalTargets))
	return h
}

// totalAdd updates the target count atomically so dynamic target adds
// (rediscovery) are reflected in the breaker ratio computation.
func (h *healthMonitor) totalAdd(delta int64) {
	h.total.Add(delta)
}

func (h *healthMonitor) paused() bool {
	u := h.pauseUntil.Load()
	if u == 0 {
		return false
	}
	if time.Now().UnixNano() >= u {
		h.pauseUntil.CompareAndSwap(u, 0)
		return false
	}
	return true
}

func (h *healthMonitor) run(ctx context.Context, breakers *sync.Map) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		total := h.total.Load()
		if total <= 0 {
			continue
		}
		open := 0
		breakers.Range(func(_, v any) bool {
			if v.(*circuitBreaker).tripped() {
				open++
			}
			return true
		})
		ratio := float64(open) / float64(total)
		if ratio >= h.threshold && !h.paused() {
			h.pauseUntil.Store(time.Now().Add(h.cooldown).UnixNano())
			h.log.Warn("global SNMP pause engaged",
				zap.Int("open_breakers", open),
				zap.Int64("total_targets", total),
				zap.Float64("ratio", ratio),
				zap.Duration("cooldown", h.cooldown))
		}
	}
}

// ─── circuit breaker ─────────────────────────────────────────────────────────

type circuitBreaker struct {
	threshold int
	cooldown  time.Duration
	fails     atomic.Int32
	openUntil atomic.Int64 // unix nanos
}

func (b *circuitBreaker) tripped() bool {
	until := b.openUntil.Load()
	if until == 0 {
		return false
	}
	if time.Now().UnixNano() < until {
		return true
	}
	// Cooldown elapsed — give it a probe attempt.
	b.openUntil.Store(0)
	b.fails.Store(0)
	return false
}

func (b *circuitBreaker) recordFailure() {
	n := b.fails.Add(1)
	if int(n) >= b.threshold {
		b.openUntil.Store(time.Now().Add(b.cooldown).UnixNano())
	}
}

func (b *circuitBreaker) recordSuccess() {
	b.fails.Store(0)
	b.openUntil.Store(0)
}

func (p *Poller) getBreaker(t *target.Target) *circuitBreaker {
	key := t.SourceID() + "|" + t.IP
	if v, ok := p.breakers.Load(key); ok {
		return v.(*circuitBreaker)
	}
	cooldown := time.Duration(p.snmpCfg.BreakerCooldownMs) * time.Millisecond
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	threshold := p.snmpCfg.BreakerThreshold
	if threshold <= 0 {
		threshold = 3
	}
	br := &circuitBreaker{threshold: threshold, cooldown: cooldown}
	actual, _ := p.breakers.LoadOrStore(key, br)
	return actual.(*circuitBreaker)
}

// ─── rate limiter ────────────────────────────────────────────────────────────

// rateLimiter is a simple token bucket producing N tokens/sec, bursts up to N.
type rateLimiter struct {
	tokens chan struct{}
}

func newRateLimiter(perSec int) *rateLimiter {
	r := &rateLimiter{tokens: make(chan struct{}, perSec)}
	// Fill the bucket so initial burst is allowed.
	for i := 0; i < perSec; i++ {
		r.tokens <- struct{}{}
	}
	interval := time.Second / time.Duration(perSec)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for range t.C {
			select {
			case r.tokens <- struct{}{}:
			default:
			}
		}
	}()
	return r
}

// wait blocks for a token or until ctx is cancelled.
func (r *rateLimiter) wait(ctx context.Context) bool {
	select {
	case <-r.tokens:
		return true
	case <-ctx.Done():
		return false
	}
}
