// Package grpc implements the gRPC IngestService for DCS.
package grpc

import (
	"context"
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/faberwork/fwdcs/internal/ingest"
	"github.com/faberwork/fwdcs/internal/store"
	v1 "github.com/faberwork/fwdcs/proto/v1"
)

// IngestServer implements v1.IngestServiceServer.
//
// Hot path (high volume): BatchPush → pipeline.Submit → worker COPY.
// Slow path (legacy/single): Push → pipeline.Submit with 1-element batch.
// PushStream is kept for backwards compatibility but routes through the
// same pipeline.
type IngestServer struct {
	v1.UnimplementedIngestServiceServer
	pipe  *ingest.Pipeline
	dedup *ingest.Deduper // retained for legacy Push path
	db    *store.DB
	log   *zap.Logger
}

// NewIngestServer creates a new gRPC ingest server.
func NewIngestServer(pipe *ingest.Pipeline, dedup *ingest.Deduper, db *store.DB, log *zap.Logger) *IngestServer {
	return &IngestServer{pipe: pipe, dedup: dedup, db: db, log: log}
}

// Push handles single-packet ingest (legacy path).
func (s *IngestServer) Push(ctx context.Context, pkt *v1.TelemetryPacket) (*v1.PushResponse, error) {
	if pkt == nil {
		return &v1.PushResponse{Accepted: false, Reason: "nil packet"}, nil
	}
	acc, rej := s.pipe.Submit([]*v1.TelemetryPacket{pkt})
	depth := s.pipe.QueueDepth()
	if rej > 0 {
		return &v1.PushResponse{Accepted: false, Reason: "ingest queue full", QueueDepth: depth}, nil
	}
	_ = acc
	return &v1.PushResponse{Accepted: true, QueueDepth: depth}, nil
}

// PushStream handles streaming ingest (legacy path). Batches under the hood.
func (s *IngestServer) PushStream(stream v1.IngestService_PushStreamServer) error {
	const flushAt = 256
	buf := make([]*v1.TelemetryPacket, 0, flushAt)
	var accepted, rejected uint32

	flush := func() {
		if len(buf) == 0 {
			return
		}
		a, r := s.pipe.Submit(buf)
		accepted += a
		rejected += r
		buf = buf[:0]
	}

	for {
		pkt, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			flush()
			return status.Errorf(codes.Internal, "stream recv: %v", err)
		}
		buf = append(buf, pkt)
		if len(buf) >= flushAt {
			// Re-allocate so the submitted slice isn't reused under the worker's feet.
			submit := make([]*v1.TelemetryPacket, len(buf))
			copy(submit, buf)
			a, r := s.pipe.Submit(submit)
			accepted += a
			rejected += r
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		submit := make([]*v1.TelemetryPacket, len(buf))
		copy(submit, buf)
		a, r := s.pipe.Submit(submit)
		accepted += a
		rejected += r
	}

	s.log.Info("stream closed",
		zap.Uint32("accepted", accepted),
		zap.Uint32("rejected", rejected))
	return stream.SendAndClose(&v1.PushResponse{Accepted: true, QueueDepth: s.pipe.QueueDepth()})
}

// BatchPush is the high-throughput ingest entrypoint. One RPC = N packets.
// The handler returns as soon as the batch is queued for a worker — the
// actual DB COPY happens async. EDR uses this for all polling traffic.
func (s *IngestServer) BatchPush(ctx context.Context, req *v1.BatchRequest) (*v1.BatchResponse, error) {
	if req == nil || len(req.Packets) == 0 {
		return &v1.BatchResponse{Accepted: 0, Rejected: 0, QueueDepth: s.pipe.QueueDepth()}, nil
	}
	accepted, rejected := s.pipe.Submit(req.Packets)
	resp := &v1.BatchResponse{
		Accepted:   accepted,
		Rejected:   rejected,
		QueueDepth: s.pipe.QueueDepth(),
	}
	if rejected > 0 {
		resp.Reason = "ingest queue full — backpressure"
	}
	return resp, nil
}
