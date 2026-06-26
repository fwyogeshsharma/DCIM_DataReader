// Package publisher drains the local queue and ships batches to DCS via gRPC.
//
// Throughput design:
//   - drain via Queue.PopBatch (single bbolt txn → one fsync per batch)
//   - ship via BatchPush unary RPC (single gRPC frame per batch)
//   - on send failure, requeue the batch and reconnect with exponential backoff
//   - bounded parallelism: up to MaxInFlight concurrent BatchPush RPCs
package publisher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/faberwork/fwedr/internal/health"
	"github.com/faberwork/fwedr/internal/queue"
	"github.com/faberwork/fwedr/pkg/config"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

const (
	defaultBatchSize       = 512
	defaultFlushIntervalMs = 200
	defaultMaxInFlight     = 8
	defaultDCSDownAfter    = 3
	defaultDCSProbeMs      = 5000
)

// Publisher drains the queue and pushes packets to DCS in batches.
type Publisher struct {
	q        *queue.Queue
	cfg      config.DCSClientConfig
	pubCfg   config.PublisherConfig
	log      *zap.Logger
	client   v1.IngestServiceClient
	conn     *grpc.ClientConn
	inFlight chan struct{}

	// DCS-down detection. gate is shared with the poller: while DCSDown() is
	// true the poller stops collecting. healthMu guards failStreak and the
	// gate transitions (sendBatch runs from up to MaxInFlight goroutines).
	gate       *health.Gate
	downAfter  int
	probeEvery time.Duration
	healthMu   sync.Mutex
	failStreak int
}

// New creates a Publisher. Call Run to start draining. gate may be nil (then the
// DCS-down pause is disabled and the publisher just buffers as before).
func New(q *queue.Queue, cfg config.DCSClientConfig, pubCfg config.PublisherConfig, gate *health.Gate, log *zap.Logger) *Publisher {
	if pubCfg.BatchSize <= 0 {
		pubCfg.BatchSize = defaultBatchSize
	}
	if pubCfg.FlushIntervalMs <= 0 {
		pubCfg.FlushIntervalMs = defaultFlushIntervalMs
	}
	if pubCfg.MaxInFlight <= 0 {
		pubCfg.MaxInFlight = defaultMaxInFlight
	}
	if pubCfg.DCSDownAfter <= 0 {
		pubCfg.DCSDownAfter = defaultDCSDownAfter
	}
	if pubCfg.DCSProbeIntervalMs <= 0 {
		pubCfg.DCSProbeIntervalMs = defaultDCSProbeMs
	}
	return &Publisher{
		q:          q,
		cfg:        cfg,
		pubCfg:     pubCfg,
		log:        log,
		inFlight:   make(chan struct{}, pubCfg.MaxInFlight),
		gate:       gate,
		downAfter:  pubCfg.DCSDownAfter,
		probeEvery: time.Duration(pubCfg.DCSProbeIntervalMs) * time.Millisecond,
	}
}

// recordResult updates the DCS-down gate from a push/probe outcome and logs the
// pause/resume transitions exactly once. Safe to call concurrently.
func (p *Publisher) recordResult(ok bool) {
	if p.gate == nil {
		return
	}
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	if ok {
		p.failStreak = 0
		if p.gate.DCSDown() {
			p.gate.SetDCSDown(false)
			p.log.Info("publisher: DCS reachable again — resuming collection")
		}
		return
	}
	p.failStreak++
	if p.failStreak >= p.downAfter && !p.gate.DCSDown() {
		p.gate.SetDCSDown(true)
		p.log.Warn("publisher: DCS unreachable — pausing collection",
			zap.Int("consecutive_failures", p.failStreak),
			zap.String("endpoint", p.cfg.Endpoint))
	}
}

// probeDCS sends an empty BatchPush to test whether DCS is reachable while
// paused, and feeds the result back into the gate.
func (p *Publisher) probeDCS(ctx context.Context) {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := p.client.BatchPush(rpcCtx, &v1.BatchRequest{Packets: nil})
	p.recordResult(err == nil)
	if err != nil {
		p.log.Debug("publisher: DCS probe failed", zap.Error(err))
	}
}

// Run drains the queue in a loop. Blocks until ctx is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if err := p.connect(ctx); err != nil {
			p.log.Warn("publisher: connect failed", zap.Error(err),
				zap.Duration("retry_in", backoff))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < 60*time.Second {
					backoff *= 2
				}
			}
			continue
		}
		backoff = time.Second
		p.drain(ctx)
		if ctx.Err() != nil {
			return
		}
	}
}

// Enqueue adds a packet to the local queue (called by the collector loop).
func (p *Publisher) Enqueue(pkt *v1.TelemetryPacket) error {
	return p.q.Push(pkt)
}

// EnqueueBatch is the high-throughput entry point — adds N packets in one txn.
func (p *Publisher) EnqueueBatch(pkts []*v1.TelemetryPacket) error {
	return p.q.PushBatch(pkts)
}

// ─── private ────────────────────────────────────────────────────────────────

func (p *Publisher) connect(ctx context.Context) error {
	if p.conn != nil {
		p.conn.Close()
	}
	var dialOpts []grpc.DialOption
	if p.cfg.TLS.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsCfg, err := buildTLSConfig(p.cfg.TLS)
		if err != nil {
			return fmt.Errorf("publisher: tls: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	}
	// Allow large batches (proto-encoded BatchRequest can exceed 4 MB default).
	dialOpts = append(dialOpts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(64*1024*1024),
			grpc.MaxCallRecvMsgSize(64*1024*1024),
		),
	)
	conn, err := grpc.DialContext(ctx, p.cfg.Endpoint, dialOpts...)
	if err != nil {
		return fmt.Errorf("publisher: dial %s: %w", p.cfg.Endpoint, err)
	}
	p.conn = conn
	p.client = v1.NewIngestServiceClient(conn)
	p.log.Info("publisher: connected to DCS",
		zap.String("endpoint", p.cfg.Endpoint),
		zap.Int("batch_size", p.pubCfg.BatchSize),
		zap.Int("max_in_flight", p.pubCfg.MaxInFlight))
	return nil
}

// drain pops batches and ships them in parallel up to MaxInFlight. Returns
// when the stream/conn breaks (caller reconnects) or ctx is cancelled.
func (p *Publisher) drain(ctx context.Context) {
	flushInterval := time.Duration(p.pubCfg.FlushIntervalMs) * time.Millisecond
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		if ctx.Err() != nil {
			return
		}

		// DCS down: don't hammer the dead endpoint with the backlog in a tight
		// loop. Park, probe every probeEvery, and resume draining (which flushes
		// the buffered backlog) the moment the probe succeeds.
		if p.gate != nil && p.gate.DCSDown() {
			select {
			case <-time.After(p.probeEvery):
			case <-ctx.Done():
				return
			}
			p.probeDCS(ctx)
			continue
		}

		batch, err := p.q.PopBatch(p.pubCfg.BatchSize)
		if errors.Is(err, queue.ErrEmpty) {
			// Idle: wait on ticker or ctx.
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
			continue
		}
		if err != nil {
			p.log.Warn("publisher: pop batch failed", zap.Error(err))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(batch) == 0 {
			continue
		}

		// Bound parallelism. inFlight blocks when MaxInFlight is reached.
		select {
		case p.inFlight <- struct{}{}:
		case <-ctx.Done():
			// Requeue the popped batch so we don't lose it on shutdown.
			_ = p.q.PushBatch(batch)
			return
		}

		wg.Add(1)
		go func(b []*v1.TelemetryPacket) {
			defer wg.Done()
			defer func() { <-p.inFlight }()
			p.sendBatch(ctx, b)
		}(batch)
	}
}

// sendBatch ships one batch. On failure, requeues every packet and lets the
// outer loop reconnect. Idempotency: DCS dedups via pkt.id / pkt.nonce.
func (p *Publisher) sendBatch(ctx context.Context, pkts []*v1.TelemetryPacket) {
	req := &v1.BatchRequest{Packets: pkts}
	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := p.client.BatchPush(rpcCtx, req)
	if err != nil {
		// Requeue everything — connection or server error.
		p.recordResult(false)
		_ = p.q.PushBatch(pkts)
		p.log.Warn("publisher: BatchPush failed — requeued",
			zap.Int("packets", len(pkts)),
			zap.Error(err))
		return
	}
	p.recordResult(true)
	p.log.Debug("publisher: batch shipped",
		zap.Int("packets", len(pkts)),
		zap.Uint32("accepted", resp.Accepted),
		zap.Uint32("rejected", resp.Rejected),
		zap.Uint32("queue_depth", resp.QueueDepth))
}
