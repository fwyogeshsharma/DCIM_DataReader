package poller

import (
	"container/heap"
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/internal/target"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

// planpoll.go implements the walk-once → GET SNMP poller (docs/SNMP_POLL_REDESIGN.md).
//
//   Phase A: fixed-OID types (pdu/generator/ups) — plan built from the profile.
//   Phase B: server + sensor — plan built by ONE discovery walk, then GET-polled.
//
// A SINGLE worker discovers (staggered, one device at a time), then GET-polls jobs
// on their per-class due time (min-heap). Everything rides the normal pollSNMP
// guards. Flag-gated: no-op unless plan_poll_enabled.

// planJob is one (device, poll group) to fetch on an interval.
type planJob struct {
	t        *target.Target
	group    snmp.PollGroup
	interval time.Duration
	nextDue  time.Time
	index    int // heap index
}

// runPlanScheduler discovers each device's plan once (caching it), then GET-polls.
func (p *Poller) runPlanScheduler(ctx context.Context) {
	if !p.snmpCfg.PlanPollEnabled || p.planStore == nil {
		return
	}

	// Small stagger so the discovery walks don't burst the responder.
	discoverySpacing := 200 * time.Millisecond
	minSpacing := time.Duration(p.snmpCfg.SweepMinSpacingMs) * time.Millisecond
	if minSpacing <= 0 {
		minSpacing = 250 * time.Millisecond
	}

	var jobs planJobHeap
	now := time.Now()
	stagger := 0
	for _, t := range p.targets {
		if ctx.Err() != nil {
			return
		}
		plan := p.discoverOrBuild(ctx, t)
		if plan == nil || plan.Empty() {
			continue
		}
		_ = p.planStore.Put(ctx, t.SourceID(), plan)
		for _, g := range plan.Groups {
			// Spread first-due times across the class interval so polling doesn't
			// all fire at once right after discovery.
			iv := p.classInterval(g.Class)
			jobs = append(jobs, &planJob{
				t: t, group: g, interval: iv,
				nextDue: now.Add(time.Duration(stagger) * discoverySpacing % iv),
			})
			stagger++
		}
		// Stagger the discovery walks (only devices that needed a walk pace here).
		if needsWalk(t.DeviceType) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(discoverySpacing):
			}
		}
	}
	if len(jobs) == 0 {
		return
	}
	heap.Init(&jobs)
	p.log.Info("plan poller started (walk-once GET)", zap.Int("jobs", len(jobs)))

	for {
		if ctx.Err() != nil {
			return
		}
		j := jobs[0] // earliest due
		wait := time.Until(j.nextDue)
		if wait < minSpacing {
			wait = minSpacing // floor: never poll faster than min spacing
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		p.pollPlanGroup(ctx, j.t, j.group)
		j.nextDue = time.Now().Add(j.interval)
		heap.Fix(&jobs, j.index)
	}
}

// discoverOrBuild returns a device's poll plan: profile-built for fixed-OID types,
// or a guarded discovery walk for server/sensor. nil for types not handled.
func (p *Poller) discoverOrBuild(ctx context.Context, t *target.Target) *snmp.PollPlan {
	switch t.DeviceType {
	case "pdu", "floor_pdu", "generator", "ups":
		return snmp.BuildStaticPlan(t.DeviceType, p.profile, time.Now().UnixNano())
	case "server", "sensor":
		var plan *snmp.PollPlan
		p.runGuarded(ctx, t, func(c *snmp.Collector) ([]*v1.TelemetryPacket, error) {
			plan = c.DiscoverPlan(p.snmpCfg.WalkServerHealth)
			return nil, nil // discovery emits nothing
		})
		return plan
	default:
		return nil // router/switch = gNMI; firewall/lb/oob = later phase
	}
}

func needsWalk(dt string) bool { return dt == "server" || dt == "sensor" }

func (p *Poller) classInterval(class snmp.PollClass) time.Duration {
	ms := 0
	switch class {
	case snmp.ClassPower:
		ms = p.snmpCfg.PlanPowerIntervalMs
	case snmp.ClassServer:
		ms = p.snmpCfg.PlanServerIntervalMs
	case snmp.ClassEnvironment:
		ms = p.snmpCfg.PlanEnvIntervalMs
	}
	if ms <= 0 {
		ms = 300000 // 5 min default for any class
	}
	return time.Duration(ms) * time.Millisecond
}

// pollPlanGroup GET-polls one device's poll group (guarded) and emits the packets.
func (p *Poller) pollPlanGroup(ctx context.Context, t *target.Target, group snmp.PollGroup) bool {
	return p.runGuarded(ctx, t, func(c *snmp.Collector) ([]*v1.TelemetryPacket, error) {
		return c.CollectGroup(group)
	})
}

// runGuarded runs fn against a device's SNMP session under the full guard stack
// (DCS-down gate, global pause, circuit breaker, rate limiter, per-target session
// mutex, socket cap, per-poll timeout), emits any returned packets, and manages
// the breaker + dead-session cleanup. Shared by discovery and polling.
func (p *Poller) runGuarded(ctx context.Context, t *target.Target,
	fn func(*snmp.Collector) ([]*v1.TelemetryPacket, error)) bool {

	if p.gate != nil && p.gate.ShouldPause() {
		return false
	}
	if p.health.paused() {
		return false
	}
	br := p.getBreaker(t, snmp.TierSlow)
	if br.tripped() {
		return false
	}
	if p.limiter != nil && !p.limiter.wait(ctx) {
		return false
	}

	tc := p.getConn(t)
	tc.mu.Lock()
	defer tc.mu.Unlock()

	select {
	case p.heavySem <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	defer func() { <-p.heavySem }()
	releaseSocket, ok := snmp.AcquireSocket(ctx)
	if !ok {
		return false
	}
	defer releaseSocket()

	if tc.cl == nil {
		cl, err := snmp.NewClient(t, p.snmpCfg)
		if err != nil {
			if br.recordFailure() {
				p.metrics.breakerOpens.Add(1)
			}
			return false
		}
		tc.cl = cl
	}
	cl := tc.cl
	cl.SetCommunity(t.Community)

	pollTimeout := time.Duration(p.snmpCfg.Timeout) * time.Millisecond * 3
	if pollTimeout < 15*time.Second {
		pollTimeout = 15 * time.Second
	}
	pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()
	stop := context.AfterFunc(pollCtx, func() { cl.Close() })
	defer stop()

	dcID := p.identity.DatacenterID
	if t.DatacenterID != "" {
		dcID = t.DatacenterID
	}
	flID := p.identity.FloorID
	if t.FloorID != "" {
		flID = t.FloorID
	}
	collector := snmp.NewCollector(
		t, cl,
		p.identity.OrgID, dcID, flID, t.NetworkID(p.identity.NetworkID),
		p.identity.GroupID, p.identity.ReaderID,
		p.signer, p.snmpCfg.MgmtPort, p.snmpCfg.Timeout,
		p.profile, p.log,
	)
	pkts, err := fn(collector)
	if err != nil {
		if pollCtx.Err() != nil || !strings.Contains(strings.ToLower(err.Error()), "timeout") {
			cl.Close()
			tc.cl = nil
		}
		if br.recordFailure() {
			p.metrics.breakerOpens.Add(1)
		}
		return false
	}
	br.recordSuccess()
	for _, pkt := range pkts {
		select {
		case p.out <- pkt:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// ─── job heap (ordered by nextDue) ────────────────────────────────────────────

type planJobHeap []*planJob

func (h planJobHeap) Len() int           { return len(h) }
func (h planJobHeap) Less(i, j int) bool { return h[i].nextDue.Before(h[j].nextDue) }
func (h planJobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *planJobHeap) Push(x any)        { j := x.(*planJob); j.index = len(*h); *h = append(*h, j) }
func (h *planJobHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return j
}
