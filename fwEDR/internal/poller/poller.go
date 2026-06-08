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
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
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

	// socketSem is the SINGLE global cap on concurrent SNMP UDP sockets. EVERY tier
	// (fast + heavy) must hold a token before snmp.NewClient or any SNMP UDP op, so
	// total in-flight sockets never exceeds its size — this is what prevents the
	// Windows WSAENOBUFS ("lacked sufficient buffer space") socket exhaustion.
	socketCap int
	// heavySem caps the heavy tiers (Medium/Slow/Topology) BELOW the global cap so
	// the cheap FAST liveness Get is always guaranteed a free socket slot and can't
	// be starved behind a burst of BulkWalks. Heavy tiers take heavySem AND
	// socketSem; fast takes only socketSem.
	heavySem chan struct{}
	limiter  *rateLimiter // global SNMP token bucket

	// Separate circuit breakers per target. The FAST liveness probe and the heavy
	// walks must NOT share a breaker: heavy BulkWalks time out first under an
	// overloaded responder and, on a shared breaker, would open it and silence the
	// heartbeat — flipping a reachable device "offline". liveBreakers gates only the
	// FAST tier (tolerant threshold); heavyBreakers gates Medium/Slow/Topology.
	liveBreakers  sync.Map // map[targetKey]*circuitBreaker — FAST heartbeat
	heavyBreakers sync.Map // map[targetKey]*circuitBreaker — Medium/Slow/Topology
	conns         sync.Map // map[connKey]*connPool — pooled SNMP sessions per responder endpoint

	// pooled is true when many targets share one responder endpoint (the simulator
	// case: all devices polled at 127.0.0.1:161, routed by community). There we keep
	// a SMALL pool of persistent sockets to that one endpoint and spread devices
	// across them (community swapped per poll) — concurrent walks against the single
	// responder WITHOUT a socket-per-device (which churns hundreds of sockets and
	// exhausts the Windows pool → WSAENOBUFS). Real devices have distinct IPs → each
	// endpoint gets its own single-socket pool.
	pooled bool
	// poolSize is the number of concurrent sockets per shared responder endpoint
	// (= the global socket cap in pooled mode, 1 otherwise). Spreading devices over
	// poolSize sockets lets that many walks run concurrently against the one
	// responder instead of serializing every device through a single socket.
	poolSize int

	metrics  *pollMetrics // poll/timeout counters, summarized periodically
	throttle *logThrottle // rate-limits repeated connect/gnmi warnings per target

	health *healthMonitor // global pause/resume based on heavy-breaker ratio

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
	// Effective global socket cap. In simulator sharding mode the SNMP load is
	// spread across only `shards` responder processes (16100..), so a handful of
	// sockets saturates them; clamp the global cap to shards*2 so EDR never opens
	// more UDP sockets than the simulator (and the Windows socket pool) can absorb.
	effCap := maxConc
	if snmpCfg.Shards > 1 {
		if c := snmpCfg.Shards * 2; c < effCap {
			effCap = c
		}
	}
	if effCap < 1 {
		effCap = 1
	}
	// Reserve at least one socket slot for the FAST heartbeat by capping heavy
	// tiers one below the global cap.
	heavyCap := effCap - 1
	if heavyCap < 1 {
		heavyCap = 1
	}
	// Pool sockets when multiple targets resolve to the SAME responder endpoint —
	// the simulator case, where every device is polled at 127.0.0.1:161 and routed
	// by community. Then one persistent socket per endpoint is shared across all
	// devices (community swapped per poll) instead of a socket per device, which
	// churns hundreds of sockets and exhausts the Windows pool (WSAENOBUFS). Real
	// devices have distinct IPs → no collisions → one socket per device as before.
	pooled := false
	endpoints := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		k := fmt.Sprintf("%s:%d", t.IP, snmp.ShardPort(t.Community, snmpCfg.Shards, snmpCfg.ShardBasePort))
		if _, dup := endpoints[k]; dup {
			pooled = true
			break
		}
		endpoints[k] = struct{}{}
	}
	// In pooled (shared-responder) mode, run effCap concurrent sockets to the one
	// endpoint so up to effCap devices walk in parallel against it — clearing the
	// head-of-line serialization that made the single-socket walk time out. One
	// socket otherwise (real devices: one endpoint each).
	poolSize := 1
	if pooled {
		poolSize = effCap
	}
	log.Info("snmp socket cap",
		zap.Int("max_concurrent", maxConc),
		zap.Int("shards", snmpCfg.Shards),
		zap.Int("effective_global_sockets", effCap),
		zap.Int("heavy_sockets", heavyCap),
		zap.Bool("pooled_sockets", pooled),
		zap.Int("pool_size", poolSize))
	snmp.ConfigureSocketLimit(effCap)
	p := &Poller{
		targets:   targets,
		snmpCfg:   snmpCfg,
		gnmiCfg:   gnmiCfg,
		identity:  identity,
		signer:    signer,
		out:       out,
		log:       log,
		socketCap: effCap,
		heavySem:  make(chan struct{}, heavyCap),
		health:    newHealthMonitor(len(targets), log),
		running:   make(map[string]context.CancelFunc),
		metrics:   &pollMetrics{},
		throttle:  newLogThrottle(time.Duration(snmpCfg.LogThrottleMs) * time.Millisecond),
		pooled:    pooled,
		poolSize:  poolSize,
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

	// Global health monitor — pauses heavy SNMP walks when too many heavy
	// breakers are open. The FAST heartbeat is exempt (see pollSNMP).
	go p.health.run(ctx, &p.heavyBreakers)

	// Periodic poll/timeout metrics summary (item: simulator-instability visibility).
	go p.metricsLoop(ctx)

	// SNMP one-shot discovery walks build the inventory first. gNMI telemetry is
	// started ONLY after this wave finishes (see startGNMIAfterDiscovery) — the
	// architecture is: SNMP walk once for discovery, THEN gNMI subscriptions for
	// ongoing telemetry. Starting gNMI concurrently floods the pipeline with the
	// initial full-tree snapshot during discovery.
	var walkWG sync.WaitGroup
	walkWG.Add(len(p.targets))

	var wg sync.WaitGroup
	for _, t := range p.targets {
		t := t
		tCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.running[t.SourceID()] = cancel
		p.mu.Unlock()
		wg.Add(1)
		var once sync.Once
		go func() {
			defer wg.Done()
			// Guarantee the discovery signal fires exactly once even if the target
			// goroutine returns before its first walk (e.g. ctx cancel during jitter).
			defer once.Do(walkWG.Done)
			p.runTarget(tCtx, t, func() { once.Do(walkWG.Done) })
		}()
	}

	// Start gNMI once SNMP discovery has completed (or a safety timeout elapses).
	go p.startGNMIAfterDiscovery(ctx, &walkWG)
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

// startGNMIAfterDiscovery blocks until the SNMP discovery walk wave completes
// (every target has attempted its first walk) or a safety timeout elapses, then
// starts the single aggregated gNMI subscription. This enforces the ordering
// "SNMP walk once for discovery → gNMI for ongoing telemetry" so gNMI's initial
// snapshot does not contend with discovery for the pipeline. The timeout ensures
// a few unreachable devices can't hold telemetry off forever.
func (p *Poller) startGNMIAfterDiscovery(ctx context.Context, walkWG *sync.WaitGroup) {
	done := make(chan struct{})
	go func() {
		walkWG.Wait()
		close(done)
	}()
	// Safety net only — don't hold telemetry off forever if a few devices are
	// slow or unreachable. Scale with fleet size (~8s/device) so a larger
	// inventory gets proportionally longer to finish its one-shot walk instead of
	// tripping a fixed 5-minute deadline mid-wave. Bounded [60s, 15m]. With the
	// faster discovery jitter the walk normally completes well inside this and we
	// take the `done` path.
	maxWait := time.Duration(len(p.targets)) * 8 * time.Second
	if maxWait < time.Minute {
		maxWait = time.Minute
	}
	if maxWait > 15*time.Minute {
		maxWait = 15 * time.Minute
	}
	select {
	case <-done:
		p.log.Info("snmp discovery walk complete — starting gNMI telemetry subscription")
	case <-time.After(maxWait):
		p.log.Warn("snmp discovery still in progress after timeout — starting gNMI telemetry anyway",
			zap.Duration("waited", maxWait))
	case <-ctx.Done():
		return
	}
	gnmi.NewManager(p.targets, p.gnmiCfg, p.identity, p.signer, p.log).Run(ctx, p.out)
}

// runTarget drives one device under the event-driven model: a single full SNMP
// walk to build inventory, then it stops and relies on SNMP traps for ongoing
// state/topology events. Continuous tiered polling was removed — it was the
// steady-state load that destabilized the simulator. A periodic full re-walk is
// available (rewalk_interval_ms > 0) for lightweight inventory/topology refresh.
func (p *Poller) runTarget(ctx context.Context, t *target.Target, firstWalkDone func()) {
	// gNMI is handled centrally by the aggregated gNMI Manager (one proxy
	// subscription for all devices). It is started by Run ONLY after this SNMP
	// discovery walk wave finishes — firstWalkDone() signals that this target has
	// attempted its initial walk. Running gNMI concurrently would flood the
	// pipeline with telemetry during discovery.

	// Spread the initial walk across a window so 1000+ targets don't burst the
	// responder at once (thundering herd). Scale the window with the fleet size
	// (~50ms/device) so a SMALL fleet — the simulator's ~40 devices — starts
	// within a couple of seconds instead of idling up to a full poll interval
	// (which pushed the discovery tail past the gNMI-start safety timeout).
	// Bounded [500ms, FastIntervalMs] so large fleets keep the old wide spread.
	spread := time.Duration(len(p.targets)) * 50 * time.Millisecond
	if maxSpread := time.Duration(p.snmpCfg.FastIntervalMs) * time.Millisecond; maxSpread > 0 && spread > maxSpread {
		spread = maxSpread
	}
	if spread < 500*time.Millisecond {
		spread = 500 * time.Millisecond
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Duration(rand.Int63n(int64(spread)))):
	}

	rewalk := time.Duration(p.snmpCfg.RewalkIntervalMs) * time.Millisecond
	if rewalk > 0 {
		// Periodic full re-walk mode: walk now, then on the configured interval.
		p.walkAll(ctx, t)
		firstWalkDone()
		ticker := time.NewTicker(rewalk)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.walkAll(ctx, t)
			}
		}
	}

	// One-shot mode (default): walk until the device answers once, then stop and
	// let traps drive events. Retrying the initial walk means a device that was
	// unreachable at startup still gets inventoried when it comes up — without any
	// steady-state polling afterward.
	retry := time.Duration(p.snmpCfg.WalkRetryIntervalMs) * time.Millisecond
	if retry <= 0 {
		retry = 30 * time.Second
	}
	walkedOnce := false
	for {
		ok := p.walkAll(ctx, t)
		if !walkedOnce {
			firstWalkDone() // signal discovery progress after the first attempt (success or not)
			walkedOnce = true
		}
		if ok {
			// Lightweight liveness heartbeat: after the one-shot walk, keep doing a
			// single FAST (sysName+uptime) GET on an interval so a source-side rename
			// propagates to the DB without a full re-walk. Much cheaper than rewalk —
			// one OID-set, no interface/counter/sensor walks. Keeps the session open
			// (the heartbeat reuses it). Only when rewalk isn't already running.
			if hb := time.Duration(p.snmpCfg.LivenessIntervalMs) * time.Millisecond; hb > 0 {
				p.log.Debug("initial SNMP walk complete — entering FAST liveness heartbeat",
					zap.String("target", t.SourceID()), zap.Duration("interval", hb))
				ticker := time.NewTicker(hb)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						p.closeConn(t)
						return
					case <-ticker.C:
						p.pollSNMP(ctx, t, snmp.TierFast) // 1 GET: sysName+uptime → rename + last_seen
					}
				}
			}
			// One-shot done: release this device's SNMP session. Traps don't use
			// it, and ~hundreds of idle UDP sockets lingering is what exhausts
			// Windows socket buffers (WSAENOBUFS) — keeping only in-flight walks
			// holding sockets avoids that. A re-walk (rewalk mode) keeps its
			// session; this path is one-shot only.
			p.closeConn(t)
			// Debug, not Info: this fires once per device (hundreds of lines on a
			// serial walk). Watch the single "topology links emitted from JSON" line
			// for readiness instead.
			p.log.Debug("initial SNMP walk complete — switching to trap-driven monitoring",
				zap.String("target", t.SourceID()))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

// closeConn frees a one-shot target's SNMP session. In pooled (simulator) mode the
// session is SHARED by every device on a shard endpoint, so it must stay open for
// the run (closed only on shutdown via closeConns) and this is a no-op — closing it
// per device would churn the 4 shard sockets back into the exhaustion this pooling
// fixes. In non-pooled (real-device) mode the session belongs to one device, so its
// socket is released here right after the one-shot walk.
func (p *Poller) closeConn(t *target.Target) {
	if p.pooled {
		return
	}
	if v, ok := p.conns.LoadAndDelete(p.connKey(t)); ok {
		for _, tc := range v.(*connPool).conns {
			tc.mu.Lock()
			if tc.cl != nil {
				tc.cl.Close()
				tc.cl = nil
			}
			tc.mu.Unlock()
		}
	}
}

// walkAll performs one full inventory walk of a target: FAST (system/liveness),
// MEDIUM (interface state), SLOW (counters/HR/UPS/sensors) and TOPOLOGY (LLDP).
// Returns true when the FAST liveness tier succeeded — i.e. the device answered
// and was inventoried — so the caller can stop retrying.
func (p *Poller) walkAll(ctx context.Context, t *target.Target) bool {
	ok := p.pollSNMP(ctx, t, snmp.TierFast) // always — liveness + registration
	if p.snmpCfg.WalkMedium {
		p.pollSNMP(ctx, t, snmp.TierMedium)
	}
	// Topology before the slow counter walk: LLDP only needs the interface names
	// gathered by MEDIUM, and getting topology_links populated early means
	// link-down/up traps can correlate (and dedup) sooner.
	if p.snmpCfg.WalkTopology {
		p.pollSNMP(ctx, t, snmp.TierTopology)
	}
	if p.snmpCfg.WalkSlow {
		p.pollSNMP(ctx, t, snmp.TierSlow)
	}
	// Environment sensors (temperature/humidity) — light, sensor/PDU-only tier for
	// the heatmap. Independent of WalkSlow so we get temperature without the heavy
	// counter/HR/UCD load. No-op for non-sensor device types (see Collect).
	if p.snmpCfg.WalkSensors {
		p.pollSNMP(ctx, t, snmp.TierEnvironment)
	}
	// Light server CPU/RAM tier (UCD scalars only). Independent of WalkSlow so we
	// get server CPU/RAM without the heavy HR/counter walks. No-op for non-servers.
	if p.snmpCfg.WalkServerHealth {
		p.pollSNMP(ctx, t, snmp.TierServerHealth)
	}
	return ok
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
	// Runtime-added targets (rediscovery) don't gate gNMI startup — it's already
	// running by then — so pass a no-op discovery signal.
	go p.runTarget(tCtx, t, func() {})
	p.log.Info("poller: target added at runtime",
		zap.String("source_id", key),
		zap.String("type", t.DeviceType),
		zap.Bool("gnmi", t.Has(target.CapGNMI)))
	return true
}

// targetConn holds one reusable SNMP session for a responder endpoint. mu
// serializes all polls over the shared session (gosnmp is not safe for concurrent
// use on one connection). In pooled mode many devices share one targetConn, so mu
// also serializes those devices onto the single shard socket — which matches the
// single-process snmpsim responder's serial capacity.
type targetConn struct {
	mu sync.Mutex
	cl *snmp.Client // nil until first connect / after an error reset
}

// connKey identifies the pooled SNMP session: the responder socket the target
// dials (IP + shard port). In simulator mode every device shares one of a few
// loopback shard endpoints, so this collapses hundreds of per-device sockets to a
// handful. With real devices each IP is distinct, giving one session per device.
func (p *Poller) connKey(t *target.Target) string {
	return fmt.Sprintf("%s:%d", t.IP, snmp.ShardPort(t.Community, p.snmpCfg.Shards, p.snmpCfg.ShardBasePort))
}

// connPool holds poolSize reusable sessions for one responder endpoint. Devices
// are spread across the sessions by community hash so different devices walk
// concurrently (each session is serialized by its own mu), while a given device
// always lands on the same session.
type connPool struct {
	conns []*targetConn
}

func newConnPool(n int) *connPool {
	if n < 1 {
		n = 1
	}
	cp := &connPool{conns: make([]*targetConn, n)}
	for i := range cp.conns {
		cp.conns[i] = &targetConn{}
	}
	return cp
}

func (cp *connPool) pick(community string) *targetConn {
	if len(cp.conns) == 1 {
		return cp.conns[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(community))
	return cp.conns[h.Sum32()%uint32(len(cp.conns))]
}

// getConn returns one of the reusable sessions for a target's responder endpoint,
// creating the pool on first use. Sessions persist for the life of the poller so
// the underlying UDP sockets are reused instead of churned each cycle.
func (p *Poller) getConn(t *target.Target) *targetConn {
	key := p.connKey(t)
	v, ok := p.conns.Load(key)
	if !ok {
		v, _ = p.conns.LoadOrStore(key, newConnPool(p.poolSize))
	}
	return v.(*connPool).pick(t.Community)
}

// closeConns closes every reused SNMP session. Called on shutdown so sockets
// are released cleanly.
func (p *Poller) closeConns() {
	p.conns.Range(func(_, v any) bool {
		for _, tc := range v.(*connPool).conns {
			tc.mu.Lock()
			if tc.cl != nil {
				tc.cl.Close()
				tc.cl = nil
			}
			tc.mu.Unlock()
		}
		return true
	})
}

// pollSNMP acquires the global rate limit and concurrency semaphore, runs one
// tiered collection, and updates the per-target circuit breaker.
// pollSNMP runs one tiered collection and returns true only when the collection
// succeeded and its packets were emitted. The boolean lets walkAll know whether
// the FAST liveness tier answered so the one-shot walker can stop retrying.
func (p *Poller) pollSNMP(ctx context.Context, t *target.Target, tier snmp.Tier) bool {
	// Global pause applies to heavy walks only. The FAST heartbeat must keep
	// flowing during a site-wide pause, otherwise the very mechanism meant to
	// protect a wedged responder blanks every device "offline" on the UI.
	if tier != snmp.TierFast && p.health.paused() {
		p.log.Debug("snmp poll skipped — global pause (heavy-breaker ratio over threshold)",
			zap.String("target", t.SourceID()),
			zap.Int("tier", int(tier)))
		return false
	}

	br := p.getBreaker(t, tier)
	if br.tripped() {
		p.log.Debug("snmp poll skipped — circuit open",
			zap.String("target", t.SourceID()),
			zap.Int("tier", int(tier)))
		return false
	}

	if p.limiter != nil {
		if !p.limiter.wait(ctx) {
			return false
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

	// Heavy tiers (Medium/Slow/Topology) first take heavySem, which is sized one
	// below the global socket cap. This guarantees the cheap FAST liveness Get
	// always has at least one free socket slot and can't be starved behind a burst
	// of expensive BulkWalks (which would let last_seen_at go stale and flip a
	// reachable device "offline").
	if tier != snmp.TierFast {
		select {
		case p.heavySem <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		defer func() { <-p.heavySem }()
	}

	// Global socket cap: EVERY tier must hold a socketSem token before opening
	// (snmp.NewClient) or using a UDP socket. This single gate bounds the TOTAL
	// number of concurrent SNMP sockets — the fix for Windows WSAENOBUFS socket
	// exhaustion. Held for the whole poll (through Collect) so concurrent UDP
	// operations are bounded too, then released on return.
	releaseSocket, ok := snmp.AcquireSocket(ctx)
	if !ok {
		return false
	}
	defer releaseSocket()

	if tc.cl == nil {
		cl, err := snmp.NewClient(t, p.snmpCfg)
		if err != nil {
			p.metrics.fails[int(tier)].Add(1)
			if br.recordFailure() {
				p.metrics.breakerOpens.Add(1)
			}
			if ok, suppressed := p.throttle.allow("connect|" + t.SourceID()); ok {
				p.log.Warn("snmp connect failed",
					zap.String("target", t.IP),
					zap.Int("suppressed_since_last", suppressed),
					zap.Error(err))
			}
			return false
		}
		tc.cl = cl
	}
	cl := tc.cl
	// Pooled session is shared across devices on this shard endpoint; set THIS
	// device's community (= its mgmt IP, the snmpsim routing key) before polling.
	// Safe under tc.mu, which serializes every device on the shared socket.
	cl.SetCommunity(t.Community)

	// Bound the whole poll. Without this, a wedged responder that stops answering
	// mid-BulkWalk holds this target's mutex AND a concurrency-semaphore slot
	// indefinitely, starving every other target ("goroutines stuck N minutes").
	// A per-poll deadline (also tripped by parent-ctx cancel) closes the socket so
	// the BulkWalk returns promptly and the locks release.
	pollTimeout := time.Duration(p.snmpCfg.Timeout) * time.Millisecond * 3
	if pollTimeout < 15*time.Second {
		pollTimeout = 15 * time.Second
	}
	pollCtx, cancelPoll := context.WithTimeout(ctx, pollTimeout)
	defer cancelPoll()
	stop := context.AfterFunc(pollCtx, func() { cl.Close() })
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
		p.snmpCfg.MgmtPort, p.snmpCfg.Timeout,
		p.log,
	)
	pkts, err := collector.Collect(tier)
	ti := int(tier)
	if err != nil {
		isTimeout := strings.Contains(strings.ToLower(err.Error()), "timeout")
		if isTimeout {
			p.metrics.timeouts[ti].Add(1)
		} else {
			p.metrics.fails[ti].Add(1)
		}
		// Socket lifecycle: KEEP the session only on a plain SNMP *timeout* — the
		// responder was just slow, the UDP socket is still valid, and reusing it
		// avoids the per-poll open/close churn that exhausted Windows socket
		// buffers (WSAENOBUFS). For ANY other error the session is dead — a closed
		// socket ("use of closed network connection", e.g. after the responder
		// restarted or the per-poll deadline fired), or shutdown — so drop it and
		// reconnect next poll. Reusing a dead socket made every subsequent poll
		// fail forever; that's the bug this guards against.
		if pollCtx.Err() != nil || !isTimeout {
			cl.Close()
			tc.cl = nil
		}

		// Transient-vs-real (simulator-only, gated by mgmt_port>0): a FAST timeout
		// may just mean the shared snmpsim responder on 161 is overloaded, not that
		// the device is down. The simulator's SET management agent runs on its own
		// socket (mgmt_port, default 1161) and stays responsive when 161 is wedged.
		// If it answers, the device is alive — DON'T count this miss toward the
		// liveness breaker, so the heartbeat keeps probing and recovers the instant
		// 161 frees up (no 30s breaker blackout, no false offline).
		if tier == snmp.TierFast && isTimeout && p.snmpCfg.MgmtPort > 0 &&
			snmp.ProbeMgmt(t.IP, uint16(p.snmpCfg.MgmtPort), t.Community, p.snmpCfg.Timeout) {
			p.metrics.mgmtConfirmed.Add(1)
			p.log.Debug("snmp fast timeout but mgmt agent alive — transient overload, not counting against liveness",
				zap.String("target", t.IP),
				zap.Int("mgmt_port", p.snmpCfg.MgmtPort))
			return false
		}

		if br.recordFailure() {
			p.metrics.breakerOpens.Add(1)
		}
		// Per-poll failures are expected/transient (slow or dead responder) and the
		// circuit breaker + health monitor already report sustained failure at a
		// higher level. Log each one at Debug so a flapping responder doesn't flood
		// the console — switch log.level to debug if you need per-poll detail.
		p.log.Debug("snmp collect failed",
			zap.String("target", t.IP),
			zap.Int("tier", ti),
			zap.Error(err))
		return false
	}
	p.metrics.ok[ti].Add(1)
	br.recordSuccess()
	p.log.Debug("snmp collected",
		zap.String("target", t.IP),
		zap.Int("tier", int(tier)),
		zap.Int("packets", len(pkts)))
	for _, pkt := range pkts {
		select {
		case p.out <- pkt:
		case <-ctx.Done():
			return false
		}
	}
	return true
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

// recordFailure increments the consecutive-failure counter and opens the breaker
// once the threshold is reached. Returns true only on the transition from closed
// to open (so the caller can count distinct open events for metrics).
func (b *circuitBreaker) recordFailure() bool {
	n := b.fails.Add(1)
	if int(n) >= b.threshold {
		wasOpen := b.openUntil.Load() != 0
		b.openUntil.Store(time.Now().Add(b.cooldown).UnixNano())
		return !wasOpen
	}
	return false
}

func (b *circuitBreaker) recordSuccess() {
	b.fails.Store(0)
	b.openUntil.Store(0)
}

// getBreaker returns the per-target circuit breaker for the tier's class. FAST
// uses the tolerant liveBreakers map (fast_breaker_threshold); every heavy tier
// shares the heavyBreakers map (breaker_threshold). Splitting them keeps a
// flapping heavy walk from ever silencing the liveness heartbeat.
func (p *Poller) getBreaker(t *target.Target, tier snmp.Tier) *circuitBreaker {
	key := t.SourceID() + "|" + t.IP
	m := &p.heavyBreakers
	threshold := p.snmpCfg.BreakerThreshold
	if tier == snmp.TierFast {
		m = &p.liveBreakers
		threshold = p.snmpCfg.FastBreakerThreshold
	}
	if v, ok := m.Load(key); ok {
		return v.(*circuitBreaker)
	}
	cooldown := time.Duration(p.snmpCfg.BreakerCooldownMs) * time.Millisecond
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	if threshold <= 0 {
		threshold = 3
	}
	br := &circuitBreaker{threshold: threshold, cooldown: cooldown}
	actual, _ := m.LoadOrStore(key, br)
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

// ─── poll metrics ──────────────────────────────────────────────────────────────

// pollMetrics holds lifetime counters per tier (index = snmp.Tier) plus a couple
// of global ones. Read/written with atomics so the metrics goroutine can sample
// them without locking the poll path. Cumulative, not windowed — the periodic
// summary reports running totals so a climbing timeout count signals instability.
type pollMetrics struct {
	ok       [snmp.NumTiers]atomic.Int64 // successful collects
	timeouts [snmp.NumTiers]atomic.Int64 // SNMP timeouts (the simulator-overload signal)
	fails    [snmp.NumTiers]atomic.Int64 // non-timeout failures (connect, closed socket, …)

	breakerOpens  atomic.Int64 // distinct breaker open events (live + heavy)
	mgmtConfirmed atomic.Int64 // FAST timeouts proven transient via the mgmt agent
}

// metricsLoop logs a one-line poll/timeout summary every MetricsLogIntervalMs so
// simulator instability is visible without a metrics endpoint. Disabled when the
// interval is 0.
func (p *Poller) metricsLoop(ctx context.Context) {
	iv := time.Duration(p.snmpCfg.MetricsLogIntervalMs) * time.Millisecond
	if iv <= 0 {
		return
	}
	ticker := time.NewTicker(iv)
	defer ticker.Stop()
	tierName := [snmp.NumTiers]string{"fast", "medium", "slow", "topology", "environment", "server_health"}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		var okT, toT, failT int64
		fields := make([]zap.Field, 0, snmp.NumTiers+5)
		for i := 0; i < snmp.NumTiers; i++ {
			ok := p.metrics.ok[i].Load()
			to := p.metrics.timeouts[i].Load()
			fa := p.metrics.fails[i].Load()
			okT += ok
			toT += to
			failT += fa
			fields = append(fields, zap.String(tierName[i],
				fmt.Sprintf("ok=%d timeout=%d fail=%d", ok, to, fa)))
		}
		liveOpen, heavyOpen := 0, 0
		p.liveBreakers.Range(func(_, v any) bool {
			if v.(*circuitBreaker).tripped() {
				liveOpen++
			}
			return true
		})
		p.heavyBreakers.Range(func(_, v any) bool {
			if v.(*circuitBreaker).tripped() {
				heavyOpen++
			}
			return true
		})
		var timeoutRate float64
		if total := okT + toT + failT; total > 0 {
			timeoutRate = float64(toT) / float64(total)
		}
		fields = append(fields,
			zap.Float64("timeout_rate", timeoutRate),
			zap.Int("live_breakers_open", liveOpen),
			zap.Int("heavy_breakers_open", heavyOpen),
			zap.Int64("breaker_opens_total", p.metrics.breakerOpens.Load()),
			zap.Int64("mgmt_confirmed_alive", p.metrics.mgmtConfirmed.Load()),
		)
		p.log.Info("snmp poll metrics", fields...)
	}
}

// ─── log throttle ──────────────────────────────────────────────────────────────

// logThrottle rate-limits repeated log lines keyed by an arbitrary string (here:
// "connect|<target>" / "gnmi|<target>"). A bursty/flapping responder otherwise
// floods the console with one identical Warn per failure across thousands of
// targets. window<=0 disables throttling (every call is allowed — old behavior).
type logThrottle struct {
	window time.Duration
	mu     sync.Mutex
	m      map[string]*throttleEntry
}

type throttleEntry struct {
	last       time.Time
	suppressed int
}

func newLogThrottle(window time.Duration) *logThrottle {
	return &logThrottle{window: window, m: make(map[string]*throttleEntry)}
}

// allow reports whether a log line for key may be emitted now, and how many were
// suppressed for that key since the last emit (always 0 when throttling is off or
// on the first sighting).
func (lt *logThrottle) allow(key string) (bool, int) {
	if lt == nil || lt.window <= 0 {
		return true, 0
	}
	now := time.Now()
	lt.mu.Lock()
	defer lt.mu.Unlock()
	e := lt.m[key]
	if e == nil {
		lt.m[key] = &throttleEntry{last: now}
		return true, 0
	}
	if now.Sub(e.last) >= lt.window {
		n := e.suppressed
		e.last = now
		e.suppressed = 0
		return true, n
	}
	e.suppressed++
	return false, 0
}
