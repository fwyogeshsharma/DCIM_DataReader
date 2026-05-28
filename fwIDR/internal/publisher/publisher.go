// Package publisher drains the local queue and streams packets to DCS via gRPC.
package publisher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/faberwork/fwidr/internal/queue"
	"github.com/faberwork/fwidr/pkg/config"
	v1 "github.com/faberwork/fwidr/proto/v1"
)

// Publisher drains the queue and pushes packets to DCS.
type Publisher struct {
	q      *queue.Queue
	cfg    config.DCSClientConfig
	log    *zap.Logger
	client v1.IngestServiceClient
	conn   *grpc.ClientConn
}

// New creates a Publisher. Call Run to start draining.
func New(q *queue.Queue, cfg config.DCSClientConfig, log *zap.Logger) *Publisher {
	return &Publisher{q: q, cfg: cfg, log: log}
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

	conn, err := grpc.DialContext(ctx, p.cfg.Endpoint, dialOpts...)
	if err != nil {
		return fmt.Errorf("publisher: dial %s: %w", p.cfg.Endpoint, err)
	}
	p.conn = conn
	p.client = v1.NewIngestServiceClient(conn)
	p.log.Info("publisher: connected to DCS", zap.String("endpoint", p.cfg.Endpoint))
	return nil
}

func (p *Publisher) drain(ctx context.Context) {
	stream, err := p.client.PushStream(ctx)
	if err != nil {
		p.log.Warn("publisher: open stream failed", zap.Error(err))
		return
	}
	defer stream.CloseAndRecv() //nolint:errcheck

	idle := 0
	for {
		if ctx.Err() != nil {
			return
		}

		pkt, err := p.q.Pop()
		if errors.Is(err, queue.ErrEmpty) {
			idle++
			if idle > 5 {
				time.Sleep(200 * time.Millisecond)
				idle = 0
			}
			continue
		}
		if err != nil {
			p.log.Warn("publisher: pop failed", zap.Error(err))
			continue
		}
		idle = 0

		if err := stream.Send(pkt); err != nil {
			// Re-queue the packet — stream is broken, reconnect outer loop.
			_ = p.q.Push(pkt)
			p.log.Warn("publisher: send failed — reconnecting", zap.Error(err))
			return
		}
	}
}
