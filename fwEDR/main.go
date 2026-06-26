// EDR — External Data Reader
// Polls network devices via SNMP and gNMI; forwards telemetry to DCS.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/bacnet"
	"github.com/faberwork/fwedr/internal/command"
	"github.com/faberwork/fwedr/internal/discovery"
	"github.com/faberwork/fwedr/internal/health"
	"github.com/faberwork/fwedr/internal/poller"
	"github.com/faberwork/fwedr/internal/publisher"
	"github.com/faberwork/fwedr/internal/queue"
	"github.com/faberwork/fwedr/internal/redfish"
	"github.com/faberwork/fwedr/internal/shardsim"
	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/internal/threshold"
	"github.com/faberwork/fwedr/internal/topology"
	"github.com/faberwork/fwedr/pkg/config"
	"github.com/faberwork/fwedr/pkg/identity"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

func main() {
	cfgPath := flag.String("config", "edr.yaml", "path to EDR config file")
	rediscover := flag.Bool("rediscover", false, "force fresh SNMP sweep even if a recent targets.json exists")
	flag.Parse()

	if err := run(*cfgPath, *rediscover); err != nil {
		fmt.Fprintf(os.Stderr, "edr: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath string, forceRediscover bool) error {
	cfg, err := config.LoadEDR(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := buildLogger(cfg.Log.Level, cfg.Log.Format)
	defer log.Sync() //nolint:errcheck

	// Validate identity
	rp := identity.ResourcePath{
		OrgID:        cfg.Identity.OrgID,
		DatacenterID: cfg.Identity.DatacenterID,
		FloorID:      cfg.Identity.FloorID,
		NetworkID:    cfg.Identity.NetworkID,
		GroupID:      cfg.Identity.GroupID,
		SourceID:     "edr",
	}
	if err := rp.Validate(); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	configureSNMPSocketLimit(cfg.SNMP)

	if cfg.Identity.ReaderID == "" {
		host, _ := os.Hostname()
		cfg.Identity.ReaderID = "edr-1.0.0-" + host
	}

	// Packet signer
	signer, err := packet.NewSigner(cfg.Identity.ReaderID)
	if err != nil {
		return fmt.Errorf("signer: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Optional sim/test convenience: launch the snmpsim responders (sharded) so
	// one `edr.exe` brings up the whole stack. OFF in production (snmp.shard_spawn
	// defaults false); there EDR polls real devices and spawns nothing.
	if cfg.SNMP.ShardSpawn && cfg.SNMP.Shards > 1 {
		ports := shardsim.Ports(cfg.SNMP.Shards, cfg.SNMP.ShardBasePort)
		sp, err := shardsim.Spawn(cfg.SNMP.ShardResponderPath, cfg.SNMP.ShardDataDir, ports, log)
		if err != nil {
			log.Warn("shardsim: spawn failed — continuing; polls will fail until responders exist", zap.Error(err))
		} else {
			defer sp.Stop()
			log.Info("shardsim: responders launched; waiting for bind", zap.Ints("ports", ports))
			select {
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
				return nil
			}
		}
	}

	// Persistent queue
	qPath := cfg.Queue.Path
	if qPath == "" {
		qPath = defaultQueuePath()
	}
	maxBytes := cfg.Queue.MaxBytes
	if maxBytes == 0 {
		maxBytes = 512 * 1024 * 1024
	}
	q, err := queue.Open(qPath, maxBytes)
	if err != nil {
		return fmt.Errorf("queue: %w", err)
	}
	defer q.Close()
	log.Info("queue opened", zap.String("path", qPath))

	// Packet fan-in channel sized for high-burst polling cycles.
	pktCh := make(chan *v1.TelemetryPacket, 16384)

	// Shared DCS-down gate: the publisher flips it when DCS is unreachable, the
	// poller reads it to pause/resume collection (so the queue can't grow without
	// bound during a DCS outage).
	gate := &health.Gate{}

	// Publisher (drains queue → DCS gRPC batches)
	pub := publisher.New(q, cfg.DCS, cfg.Publisher, gate, log)
	go pub.Run(ctx)

	// Batched enqueue goroutine: accumulates packets in memory and flushes to
	// the bbolt queue either when the buffer is full or when flush_interval_ms
	// elapses. Single bbolt fsync per flush amortizes write cost across many
	// packets (10-100x faster than per-packet Push).
	go func() {
		batchSize := cfg.Publisher.BatchSize
		if batchSize <= 0 {
			batchSize = 256
		}
		flushInterval := time.Duration(cfg.Publisher.FlushIntervalMs) * time.Millisecond
		if flushInterval <= 0 {
			flushInterval = 200 * time.Millisecond
		}
		buf := make([]*v1.TelemetryPacket, 0, batchSize)
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		flush := func() {
			if len(buf) == 0 {
				return
			}
			// Backpressure before dropping: the queue only returns ErrFull when
			// the publisher is transiently behind (DCS reconnecting, a counter
			// burst). Retry briefly so a short stall is absorbed instead of
			// silently losing telemetry — the cause of the demo "queue: full"
			// drops. Capped (~1s) so a genuinely-down DCS can't block the fan-in
			// goroutine forever; only then do we drop and move on.
			var err error
			for attempt := 0; attempt < 50; attempt++ {
				if err = pub.EnqueueBatch(buf); err == nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if err != nil {
				log.Warn("enqueue batch failed after backpressure — dropping",
					zap.Error(err), zap.Int("dropped", len(buf)))
			}
			buf = buf[:0]
		}
		for {
			select {
			case pkt := <-pktCh:
				buf = append(buf, pkt)
				if len(buf) >= batchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-ctx.Done():
				// Drain remaining channel buffer before exit.
				for {
					select {
					case pkt := <-pktCh:
						buf = append(buf, pkt)
						if len(buf) >= batchSize {
							flush()
						}
					default:
						flush()
						return
					}
				}
			}
		}
	}()

	// Build targets: topology file → manual config → dynamic SNMP discovery.
	targets := make([]*target.Target, 0)

	if cfg.TopologyFile != "" {
		topoTargets, err := topology.LoadTargets(cfg.TopologyFile)
		if err != nil {
			log.Warn("topology load failed", zap.Error(err))
		} else {
			for _, tc := range topoTargets {
				targets = append(targets, target.FromConfig(tc, cfg.SNMP))
			}
			log.Info("topology targets loaded", zap.Int("count", len(topoTargets)))
		}
	}

	for _, tc := range cfg.Targets {
		targets = append(targets, target.FromConfig(tc, cfg.SNMP))
	}

	if len(targets) == 0 && len(cfg.Discovery.Subnets) > 0 {
		persistPath := discovery.DefaultPath(qPath)
		// Phase 0i: persist targets between EDR runs. If targets.json is fresh
		// (default <24h, configurable), skip the SNMP sweep entirely on
		// startup — the biggest single source of SNMPSim load. --rediscover
		// forces a fresh sweep regardless of cached state.
		maxAge := time.Duration(cfg.Discovery.TargetCacheHours) * time.Hour
		if maxAge <= 0 {
			maxAge = 24 * time.Hour
		}
		if !forceRediscover {
			if cached, age, ok := discovery.Load(persistPath, maxAge); ok {
				targets = append(targets, cached...)
				log.Info("loaded persisted targets — sweep skipped",
					zap.String("path", persistPath),
					zap.Int("count", len(cached)),
					zap.Duration("age", age),
					zap.Duration("max_age", maxAge))
			}
		}
		if len(targets) == 0 {
			sweeper := discovery.New(cfg.Discovery.Subnets, cfg.Discovery.SNMPAgent, cfg.Discovery.SeedIP, cfg.SNMP, cfg.GNMI, log)
			found, err := sweeper.Sweep(ctx)
			if err != nil {
				log.Warn("discovery sweep failed", zap.Error(err))
			} else {
				targets = append(targets, found...)
			}
			if len(found) > 0 {
				if err := discovery.Save(persistPath, found); err != nil {
					log.Warn("persist targets failed — sweep will repeat next restart",
						zap.String("path", persistPath), zap.Error(err))
				} else {
					log.Info("persisted targets to disk",
						zap.String("path", persistPath),
						zap.Int("count", len(found)))
				}
			}
			// Interface IP addresses + ProdIP/OOBIP/LoopbackIP are now derived by the
			// poller's pooled MEDIUM walk (snmp.Collector.collectInterfaceAddresses),
			// so no separate enrichment pass is started here.
		}
	}
	// Strip gNMI capability when gNMI is globally disabled (e.g. SNMP-only
	// simulator with no gNMI servers). Prevents 41 reconnect-loop goroutines
	// and their log spam / socket churn against a port that always refuses.
	if !cfg.GNMI.Enabled {
		gnmiStripped := 0
		for _, t := range targets {
			if t.Has(target.CapGNMI) {
				t.ClearCap(target.CapGNMI)
				gnmiStripped++
			}
		}
		if gnmiStripped > 0 {
			log.Info("gnmi disabled by config — stripped capability", zap.Int("targets", gnmiStripped))
		}
	}

	log.Info("edr targets loaded", zap.Int("count", len(targets)))

	// Discovery-time registration: emit one synthetic system.uptime_centiseconds=0
	// packet per target immediately. DCS upserts the device row on this packet,
	// so the `devices` table is populated within seconds of sweep completion —
	// even before the first real SNMP poll, and even if the simulator wedges
	// before TierFast can run for every target.
	emitRegistrationPackets(targets, cfg.Identity, signer, pktCh, log)

	// Per-country network_id resolver for the sources that lack a *target.Target
	// in scope (topology links by host/IP, traps by community IP). Must match the
	// per-target ids the collectors stamp, or DCS can't resolve those packets.
	netIDFor := buildNetIDResolver(targets, cfg.Identity.NetworkID)

	// Interface IP addresses are collected by the poller's pooled MEDIUM walk
	// (see snmp.Collector.collectInterfaceAddresses) — folded in there so it reuses
	// the persistent pooled socket. The old standalone enrichment pass opened a
	// fresh socket per device and got starved by the single responder, so it is no
	// longer started here.

	// Power topology links. Production links come from LLDP on network devices
	// Topology links from the simulator JSON — the complete, authoritative
	// port → connected-device relation, available instantly (no waiting on the
	// LLDP walk). DCS builds topology_links from these, so a link-down trap can
	// immediately find the peer and write ONE merged event. Covers all layers
	// (network/management/power). When EDR sources topology from the JSON, the
	// LLDP topology tier is disabled (snmp.walk_topology:false) to avoid duplicate
	// rows. Re-emitted on the topology interval so rows stay fresh and links to
	// not-yet-registered devices retry.
	if cfg.TopologyFile != "" {
		if links, err := topology.LoadTopologyLinks(cfg.TopologyFile); err != nil {
			log.Warn("topology links load failed", zap.Error(err))
		} else if len(links) > 0 {
			go func() {
				// Let discovery-time registrations land first so DCS can resolve
				// both endpoints (links to unknown devices are dropped).
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				topoIv := time.Duration(cfg.SNMP.TopologyIntervalMs) * time.Millisecond
				if topoIv <= 0 {
					topoIv = 10 * time.Minute
				}
				// The JSON link set is static for a run, so re-emitting all of it every
				// interval is pure waste (DCS now ignores unchanged links). But we DO
				// need a few early re-emits so links whose endpoint device registered
				// late still resolve, plus a sparse re-assert to heal a DCS restart.
				// warmupCycles: re-emit each interval at first (catch late registrations).
				// reassertEvery: afterwards, re-emit only every Nth interval.
				const warmupCycles = 3
				const reassertEvery = 12
				for cycle := 0; ; cycle++ {
					emit := cycle < warmupCycles || cycle%reassertEvery == 0
					if emit {
						reason := "warmup"
						switch {
						case cycle == 0:
							reason = "initial"
						case cycle >= warmupCycles:
							reason = "reassert"
						}
						n := emitTopologyLinks(links, cfg.Identity, netIDFor, signer, pktCh, log)
						log.Info("topology links emitted from JSON",
							zap.Int("emitted", n), zap.Int("total", len(links)), zap.String("reason", reason))
					} else {
						log.Debug("topology links unchanged — skip re-emit", zap.Int("total", len(links)))
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(topoIv):
					}
				}
			}()
		}
	}

	// SNMP trap receiver
	if cfg.SNMP.TrapAddr != "" {
		trapSigner, _ := packet.NewSigner(cfg.Identity.ReaderID + "-trap")
		trapReceiver := snmp.NewTrapReceiver(
			cfg.SNMP.TrapAddr,
			cfg.Identity.OrgID, cfg.Identity.DatacenterID,
			cfg.Identity.FloorID,
			netIDFor,
			cfg.Identity.GroupID, cfg.Identity.ReaderID,
			trapSigner, pktCh, log,
		)
		go func() {
			if err := trapReceiver.Listen(); err != nil {
				log.Warn("trap receiver stopped", zap.Error(err))
			}
		}()
		log.Info("snmp trap receiver started", zap.String("addr", cfg.SNMP.TrapAddr))
	}

	// Poller
	pollerDone := make(chan struct{})
	p := poller.New(targets, cfg.SNMP, cfg.GNMI, cfg.Identity, signer, pktCh, gate, log)
	go func() {
		p.Run(ctx)
		close(pollerDone)
	}()

	// BACnet/IP manager — polls Verdigris EV2 energy monitors
	// (device_type=energy_monitor). Independent of the SNMP one-shot walk: its
	// own periodic ReadPropertyMultiple loop (+ optional COV push). No-op when
	// disabled or when no energy_monitor devices are present.
	if cfg.BACnet.Enabled {
		bm := bacnet.NewManager(targets, cfg.BACnet, cfg.Identity, signer, log)
		if bm.Count() > 0 {
			go bm.Run(ctx, pktCh)
		}
	}

	// Redfish manager — polls each server's BMC over HTTP (device_type=server) for
	// OS usage (CPU/mem/disk/net via the Oem extension) and hardware health (temps,
	// fans, PSU watts, chassis power state). The server analog of gNMI; replaces
	// SNMP polling of server health. No-op when disabled or when no servers exist.
	if cfg.Redfish.Enabled {
		rf := redfish.NewManager(targets, cfg.Redfish, cfg.Identity, signer, log)
		if rf.Count() > 0 {
			go rf.Run(ctx, pktCh)
		}
	}

	// Downstream control plane — pull UI edits from DCS and apply them to devices
	// via SNMP SET / Redfish. Disabled unless command_apply.enabled is set.
	if cfg.CommandApply.Enabled {
		if cfg.CommandApply.DCSBaseURL == "" {
			log.Warn("command_apply enabled but dcs_base_url is empty — skipping")
		} else {
			cr := command.New(cfg.CommandApply, cfg.Redfish, targets, log)
			go cr.Run(ctx)
		}
	}

	// Threshold sync — pull per-device alert thresholds from the SNMP mgmt plane
	// (1161) into DCS device_thresholds. Disabled unless threshold_sync.enabled.
	if cfg.ThresholdSync.Enabled {
		tp := threshold.New(targets, cfg.ThresholdSync, cfg.Identity, signer, log)
		if tp.Count() > 0 {
			go tp.Run(ctx, pktCh)
		}
	}

	// Background rediscovery loop — picks up devices that were not yet up
	// when EDR started, and new devices that appear between full sweep cycles.
	// Runs every IntervalHours. Disabled when interval_hours <= 0 (rediscovery is
	// then manual only, via restart with --rediscover) so EDR stays idle after the
	// one-shot walk.
	if len(cfg.Discovery.Subnets) > 0 && cfg.Discovery.IntervalHours > 0 {
		known := make(map[string]struct{}, len(targets))
		for _, t := range targets {
			known[t.SourceID()] = struct{}{}
		}
		go rediscoveryLoop(ctx, cfg, signer, p, pktCh, known, log)
	}

	log.Info("edr started",
		zap.String("reader_id", cfg.Identity.ReaderID),
		zap.Int("targets", len(targets)))

	<-ctx.Done()
	log.Info("edr shutting down — waiting up to 6s for SNMP sessions to drain")
	// Wait for poller to drain in-flight SNMP. Poller has its own 5s internal
	// timeout; we add a small buffer here so logs flush cleanly.
	select {
	case <-pollerDone:
		log.Info("edr poller drained")
	case <-time.After(6 * time.Second):
		log.Warn("edr drain timeout — forcing exit")
	}
	return nil
}

// rediscoveryLoop re-runs SNMP discovery periodically. New targets are emitted
// as registration packets and added to the running poller. Uses a shorter
// 60-second interval when no targets are known yet (so a dead-simulator-at-
// startup scenario recovers quickly once the simulator comes back).
func rediscoveryLoop(
	ctx context.Context,
	cfg *config.EDRConfig,
	signer *packet.Signer,
	p *poller.Poller,
	pktCh chan<- *v1.TelemetryPacket,
	known map[string]struct{},
	log *zap.Logger,
) {
	longInterval := time.Duration(cfg.Discovery.IntervalHours) * time.Hour
	if longInterval <= 0 {
		longInterval = 1 * time.Hour
	}
	shortInterval := 60 * time.Second

	for {
		wait := longInterval
		if len(known) == 0 {
			wait = shortInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		sweeper := discovery.New(
			cfg.Discovery.Subnets, cfg.Discovery.SNMPAgent, cfg.Discovery.SeedIP,
			cfg.SNMP, cfg.GNMI, log,
		)
		found, err := sweeper.Sweep(ctx)
		if err != nil {
			log.Warn("rediscovery sweep failed", zap.Error(err))
			continue
		}

		var newTargets []*target.Target
		for _, t := range found {
			if _, ok := known[t.SourceID()]; ok {
				continue
			}
			known[t.SourceID()] = struct{}{}
			newTargets = append(newTargets, t)
		}
		if len(newTargets) == 0 {
			log.Debug("rediscovery: no new targets", zap.Int("known", len(known)))
			continue
		}

		emitRegistrationPackets(newTargets, cfg.Identity, signer, pktCh, log)
		for _, t := range newTargets {
			p.AddTarget(t)
		}
		log.Info("rediscovery added new targets",
			zap.Int("new", len(newTargets)),
			zap.Int("total_known", len(known)))
	}
}

// enrichTargetsBackground runs the ipAdEntAddr walk against every freshly
// discovered target at a paced rate (default 200 ms between walks) so the
// SNMPSim event loop never sees a burst. After each walk the target's
// ProdIP / OOBIP / LoopbackIP are updated and a fresh registration packet
// is emitted so DCS reflects the new addresses without an EDR restart.
// Persisted targets.json is re-saved at the end so subsequent runs already
// have enriched data and the loop becomes a no-op.
func enrichTargetsBackground(
	ctx context.Context,
	targets []*target.Target,
	cfg *config.EDRConfig,
	signer *packet.Signer,
	out chan<- *v1.TelemetryPacket,
	persistPath string,
	log *zap.Logger,
) {
	if len(targets) == 0 {
		return
	}
	// Allow EDR's polling tickers to settle before we layer enrichment walks
	// on top of them. 15 s after sweep completion is well past the first
	// TierFast wave for all targets.
	select {
	case <-ctx.Done():
		return
	case <-time.After(15 * time.Second):
	}
	pace := 200 * time.Millisecond
	if cfg.Discovery.EnrichmentPaceMs > 0 {
		pace = time.Duration(cfg.Discovery.EnrichmentPaceMs) * time.Millisecond
	}
	sweeper := discovery.New(cfg.Discovery.Subnets, cfg.Discovery.SNMPAgent, cfg.Discovery.SeedIP, cfg.SNMP, cfg.GNMI, log)
	log.Info("background enrichment started",
		zap.Int("targets", len(targets)),
		zap.Duration("pace", pace))
	enriched := 0
	for _, t := range targets {
		select {
		case <-ctx.Done():
			log.Info("background enrichment cancelled",
				zap.Int("enriched", enriched),
				zap.Int("remaining", len(targets)-enriched))
			return
		case <-time.After(pace):
		}
		oldPrimary := t.ProdIP
		ifaceAddrs := sweeper.EnrichTarget(t)
		if t.ProdIP != oldPrimary || t.OOBIP != "" || t.LoopbackIP != "" {
			emitRegistrationPackets([]*target.Target{t}, cfg.Identity, signer, out, log)
		}
		emitInterfaceAddressPackets(ctx, t, ifaceAddrs, cfg.Identity, signer, out, log)
		enriched++
	}
	if persistPath != "" {
		if err := discovery.Save(persistPath, targets); err != nil {
			log.Warn("post-enrichment persist failed", zap.Error(err))
		}
	}
	log.Info("background enrichment complete", zap.Int("targets", enriched))
}

// emitInterfaceAddressPackets sends one TelemetryPacket of Kind "interface_address"
// per (ifindex, ip) tuple. DCS routes the kind to UpsertInterfaceAddress
// without needing a bespoke RPC. Skipped silently when ifAddrs is empty.
func emitInterfaceAddressPackets(
	ctx context.Context,
	t *target.Target,
	ifAddrs []discovery.InterfaceAddress,
	id config.IdentityConfig,
	signer *packet.Signer,
	out chan<- *v1.TelemetryPacket,
	log *zap.Logger,
) {
	if len(ifAddrs) == 0 {
		return
	}
	// Per-device routing override (same logic as emitRegistrationPackets).
	ifaDCID := id.DatacenterID
	if t.DatacenterID != "" {
		ifaDCID = t.DatacenterID
	}
	ifaFloorID := id.FloorID
	if t.FloorID != "" {
		ifaFloorID = t.FloorID
	}

	now := time.Now().UnixNano()
	for _, a := range ifAddrs {
		meta := map[string]string{
			"hostname":           t.SourceID(),
			"mgmt_ip":            t.MgmtIP,
			"prod_ip":            t.ProdIP,
			"loopback_ip":        t.LoopbackIP,
			"oob_ip":             t.OOBIP,
			"device_type":        t.DeviceType,
			"vendor":             t.Vendor,
			"collector_agent":    "EDR",
			"collector_protocol": "SNMP",
			"interface_index":    strconv.Itoa(a.IfIndex),
			"address":            a.Address,
			"address_family":     a.Family,
		}
		pid := packet.NewID()
		nonce := signer.NextNonce()
		canonical := packet.CanonicalBytes(pid, t.SourceID(), now, "interface.address", a.Address, 0, nonce)
		sig := signer.Sign(canonical)
		pkt := &v1.TelemetryPacket{
			Id:           pid,
			OrgId:        id.OrgID,
			DatacenterId: ifaDCID,
			FloorId:      ifaFloorID,
			NetworkId:    t.NetworkID(id.NetworkID),
			GroupId:      id.GroupID,
			SourceType:   "device",
			SourceId:     t.SourceID(),
			ReaderId:     id.ReaderID,
			TimestampNs:  now,
			Name:         "interface.address",
			Tag:          a.Address,
			Value:        0,
			Meta:         meta,
			Kind:         "interface_address",
			Signature:    sig,
			Nonce:        nonce,
		}
		// Blocking send: interface_address packets are low-volume inventory data and
		// must NOT be dropped under load. A non-blocking send raced the gNMI/metric
		// flood and silently discarded every address (interface_addresses stayed
		// empty). Wait for queue space; bail only on shutdown.
		select {
		case out <- pkt:
		case <-ctx.Done():
			return
		}
	}
}

// buildNetIDResolver returns a lookup from a device's hostname or any of its IPs
// to its per-country network id, used by the two packet sources that don't carry
// a *target.Target in scope (topology-link emit, SNMP trap receiver). The device
// identity in DCS is keyed partly by network_id, so these MUST agree with the
// per-target network id stamped by the collectors — otherwise a trap/link can't
// resolve the device (DeviceByIP filters strictly by network_id). Unknown keys
// fall back to the global id. Built from the startup target set (rediscovery is
// off in the steady-state deployment).
func buildNetIDResolver(targets []*target.Target, fallback string) func(keys ...string) string {
	m := make(map[string]string, len(targets)*4)
	for _, t := range targets {
		nid := t.NetworkID(fallback)
		for _, k := range []string{t.SourceID(), t.IP, t.MgmtIP, t.ProdIP, t.LoopbackIP, t.OOBIP} {
			if k != "" {
				m[k] = nid
			}
		}
	}
	return func(keys ...string) string {
		for _, k := range keys {
			if nid, ok := m[k]; ok {
				return nid
			}
		}
		return fallback
	}
}

// emitRegistrationPackets pushes a synthetic system.uptime_centiseconds=0 packet
// per discovered target so DCS can upsert all devices into the metadata table
// before any real polling starts. The real uptime overwrites this on the first
// successful TierFast poll.
func emitRegistrationPackets(
	targets []*target.Target,
	id config.IdentityConfig,
	signer *packet.Signer,
	out chan<- *v1.TelemetryPacket,
	log *zap.Logger,
) {
	now := time.Now().UnixNano()
	for _, t := range targets {
		// Per-device datacenter/floor override: if the topology JSON provides
		// the device's own datacenter (e.g. "DC1"), use it; otherwise fall back
		// to the global identity config value. This ensures multi-datacenter
		// topologies route each device into the correct DC scope in DCS.
		dcID := id.DatacenterID
		if t.DatacenterID != "" {
			dcID = t.DatacenterID
		}
		floorID := id.FloorID
		if t.FloorID != "" {
			floorID = t.FloorID
		}

		meta := map[string]string{
			"hostname":           t.SourceID(),
			"mgmt_ip":            t.MgmtIP,
			"prod_ip":            t.ProdIP,
			"loopback_ip":        t.LoopbackIP,
			"oob_ip":             t.OOBIP,
			"device_type":        t.DeviceType,
			"vendor":             t.Vendor,
			"model_name":         t.ModelName,
			"country":            t.Country,
			"datacenter":         t.DatacenterName,
			"datacenter_city":    t.DatacenterCity,
			"room":               t.Room,
			"collector_agent":    "EDR",
			"collector_protocol": "SNMP",
			"registration":       "discovery",
		}
		if t.RackRow > 0 {
			meta["rack_row"] = strconv.Itoa(t.RackRow)
		}
		if t.RackNum > 0 {
			meta["rack_num"] = strconv.Itoa(t.RackNum)
		}
		if t.RackUnit > 0 {
			meta["rack_unit"] = strconv.Itoa(t.RackUnit)
		}
		pid := packet.NewID()
		nonce := signer.NextNonce()
		canonical := packet.CanonicalBytes(pid, t.SourceID(), now, "system.uptime_centiseconds", "", 0, nonce)
		sig := signer.Sign(canonical)
		pkt := &v1.TelemetryPacket{
			Id:           pid,
			OrgId:        id.OrgID,
			DatacenterId: dcID,
			FloorId:      floorID,
			NetworkId:    t.NetworkID(id.NetworkID),
			GroupId:      id.GroupID,
			SourceType:   "device",
			SourceId:     t.SourceID(),
			ReaderId:     id.ReaderID,
			TimestampNs:  now,
			Name:         "system.uptime_centiseconds",
			Tag:          "",
			Value:        0,
			Meta:         meta,
			Kind:         "metric",
			Signature:    sig,
			Nonce:        nonce,
		}
		select {
		case out <- pkt:
		default:
			log.Warn("registration packet dropped — channel full",
				zap.String("target", t.SourceID()))
		}
	}
	// Debug, not Info: enrichment re-emits one registration per target (count:1),
	// which floods the console. The startup count is already implied by
	// "edr targets loaded".
	log.Debug("registration packets emitted", zap.Int("count", len(targets)))
}

// emitTopologyLinks sends one Kind="topology" packet per topology-JSON edge
// (network/management/power). DCS (worker → WriteTopologyLink) resolves the source
// device by Meta["mgmt_ip"] and the destination by Meta["remote_mgmt_ip"] (stable,
// rename-proof), then writes a topology_links row carrying Meta["layer"]. This is
// the authoritative port → connected-device relation a link-down trap looks up to
// merge the two endpoint traps into one event.
//
// Returns the number of packets accepted into the publish channel. A packet whose
// endpoints aren't in the DCS devices table yet resolves to no row and is silently
// dropped DCS-side; the periodic re-emit (see caller) retries until both endpoints
// have registered.
func emitTopologyLinks(
	links []topology.LinkEdge,
	id config.IdentityConfig,
	netIDFor func(keys ...string) string,
	signer *packet.Signer,
	out chan<- *v1.TelemetryPacket,
	log *zap.Logger,
) int {
	now := time.Now().UnixNano()
	emitted := 0
	for _, e := range links {
		dcID := id.DatacenterID
		if e.SrcDatacenterID != "" {
			dcID = e.SrcDatacenterID
		}
		floorID := id.FloorID
		if e.SrcFloorID != "" {
			floorID = e.SrcFloorID
		}

		// Port name = the raw 0-based JSON iface index, because the simulator's
		// linkDown trap carries iface_index = edge.src_iface (0-based, topology.py).
		// Storing the SAME value as src_port_name lets the trap correlate to this
		// edge by index alone — no dependence on the interfaces table or vendor
		// port-name strings. (local/remote_port_index below stay +1 = the real
		// 1-based ifIndex, used only to resolve interface_id for display.)
		srcPort := strconv.Itoa(e.SrcPortIndex)
		dstPort := strconv.Itoa(e.DstPortIndex)

		meta := map[string]string{
			"layer":              e.Layer,
			"mgmt_ip":            e.SrcMgmtIP, // stable src identity — DCS resolves by mgmt_ip first
			"remote_sys_name":    e.DstHost,
			"remote_mgmt_ip":     e.DstMgmtIP, // stable dst identity — rename-proof peer resolution
			"remote_port_id":     dstPort,
			"local_port_index":   strconv.Itoa(e.SrcPortIndex + 1),
			"remote_port_index":  strconv.Itoa(e.DstPortIndex + 1),
			"hostname":           e.SrcHost,
			"collector_agent":    "EDR",
			"collector_protocol": "TOPO",
		}

		pid := packet.NewID()
		nonce := signer.NextNonce()
		const name = "topology.link"
		canonical := packet.CanonicalBytes(pid, e.SrcHost, now, name, srcPort, 0, nonce)
		sig := signer.Sign(canonical)
		pkt := &v1.TelemetryPacket{
			Id:           pid,
			OrgId:        id.OrgID,
			DatacenterId: dcID,
			FloorId:      floorID,
			NetworkId:    netIDFor(e.SrcHost, e.SrcMgmtIP),
			GroupId:      id.GroupID,
			SourceType:   "device",
			SourceId:     e.SrcHost,
			ReaderId:     id.ReaderID,
			TimestampNs:  now,
			Name:         name,
			Tag:          srcPort,
			Value:        0,
			Meta:         meta,
			Kind:         "topology",
			Signature:    sig,
			Nonce:        nonce,
		}
		select {
		case out <- pkt:
			emitted++
		default:
			log.Warn("topology link packet dropped — channel full",
				zap.String("src", e.SrcHost), zap.String("dst", e.DstHost), zap.String("layer", e.Layer))
		}
	}
	return emitted
}

func defaultQueuePath() string {
	if runtime.GOOS == "windows" {
		return `C:\ProgramData\fwdcim\edr\queue.db`
	}
	return "/var/lib/fwdcim/edr/queue.db"
}

func configureSNMPSocketLimit(cfg config.SNMPConfig) {
	maxConc := cfg.MaxConcurrent
	if maxConc <= 0 {
		maxConc = 1
	}
	effCap := maxConc
	if cfg.Shards > 1 {
		if shardCap := cfg.Shards * 2; shardCap < effCap {
			effCap = shardCap
		}
	}
	snmp.ConfigureSocketLimit(effCap)
}

func buildLogger(level, format string) *zap.Logger {
	var lvl zap.AtomicLevel
	switch level {
	case "debug":
		lvl = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		lvl = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		lvl = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		lvl = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	enc := "json"
	if format == "console" {
		enc = "console"
	}
	cfg := zap.Config{
		Level:            lvl,
		Development:      false,
		Encoding:         enc,
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	log, _ := cfg.Build()
	return log
}
