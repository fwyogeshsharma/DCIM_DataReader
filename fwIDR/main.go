// IDR — Internal Data Reader
// Collects host metrics and streams them to DataCenterStore.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/faberwork/fwidr/internal/collector"
	"github.com/faberwork/fwidr/internal/publisher"
	"github.com/faberwork/fwidr/internal/queue"
	"github.com/faberwork/fwidr/pkg/config"
	"github.com/faberwork/fwidr/pkg/identity"
	"github.com/faberwork/fwidr/pkg/packet"
)

const version = "1.0.0"

func main() {
	cfgPath := flag.String("config", "idr.yaml", "path to IDR config file")
	flag.Parse()

	if err := run(*cfgPath); err != nil {
		fmt.Fprintf(os.Stderr, "idr: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath string) error {
	cfg, err := config.LoadIDR(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := buildLogger(cfg.Log)
	defer log.Sync() //nolint:errcheck

	log.Info("idr starting",
		zap.String("version", version),
		zap.String("os", runtime.GOOS),
		zap.String("arch", runtime.GOARCH),
	)

	// Validate identity
	hostname, _ := os.Hostname()
	if cfg.Identity.ReaderID == "" {
		cfg.Identity.ReaderID = fmt.Sprintf("idr-%s-%s", version, hostname)
	}
	rp := identity.ResourcePath{
		OrgID:        cfg.Identity.OrgID,
		DatacenterID: cfg.Identity.DatacenterID,
		FloorID:      cfg.Identity.FloorID,
		NetworkID:    cfg.Identity.NetworkID,
		GroupID:      cfg.Identity.GroupID,
		SourceID:     hostname,
	}
	if err := rp.Validate(); err != nil {
		return err
	}
	log.Info("identity", zap.String("path", rp.String()))

	// Packet signer
	signer, err := packet.NewSigner(hostname)
	if err != nil {
		return fmt.Errorf("signer: %w", err)
	}

	// Persistent queue
	qPath := cfg.Queue.Path
	if qPath == "" {
		qPath = defaultQueuePath()
	}
	q, err := queue.Open(qPath, cfg.Queue.MaxBytes)
	if err != nil {
		return fmt.Errorf("queue: %w", err)
	}
	defer q.Close()
	log.Info("queue opened", zap.String("path", qPath), zap.Int("depth", q.Len()))

	// Build collector base (shared identity context)
	base := collector.Base{
		OrgID:        rp.OrgID,
		DatacenterID: rp.DatacenterID,
		FloorID:      rp.FloorID,
		NetworkID:    rp.NetworkID,
		GroupID:      rp.GroupID,
		SourceID:     hostname,
		ReaderID:     cfg.Identity.ReaderID,
	}

	// Register enabled collectors
	enabled := enabledSet(cfg.Collectors.Enable)
	collectors := buildCollectors(base, enabled)
	log.Info("collectors ready", zap.Int("count", len(collectors)))

	// Publisher
	pub := publisher.New(q, cfg.DCS, log)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	// Publisher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		pub.Run(ctx)
	}()

	// Collector loop
	interval := time.Duration(cfg.Collectors.IntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info("collection started", zap.Duration("interval", interval))
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collect(ctx, collectors, pub, signer, log)
			}
		}
	}()

	<-ctx.Done()
	log.Info("idr shutting down")
	wg.Wait()
	log.Info("idr stopped")
	return nil
}

func collect(ctx context.Context, collectors []collector.Collector,
	pub *publisher.Publisher, sgn *packet.Signer, log *zap.Logger) {

	for _, c := range collectors {
		pkts, err := c.Collect(ctx)
		if err != nil {
			log.Warn("collector error", zap.String("collector", c.Name()), zap.Error(err))
			continue
		}
		for _, pkt := range pkts {
			pkt.Id = packet.NewID()
			pkt.Nonce = sgn.NextNonce()
			msg := packet.CanonicalBytes(pkt.Id, pkt.SourceId, pkt.TimestampNs,
				pkt.Name, pkt.Tag, pkt.Value, pkt.Nonce)
			pkt.Signature = sgn.Sign(msg)

			if err := pub.Enqueue(pkt); err != nil {
				log.Warn("enqueue dropped", zap.String("metric", pkt.Name), zap.Error(err))
			}
		}
	}
}

func buildCollectors(base collector.Base, enabled map[string]bool) []collector.Collector {
	all := map[string]collector.Collector{
		"cpu":         &collector.CPUCollector{Base: base},
		"memory":      &collector.MemoryCollector{Base: base},
		"disk":        &collector.DiskCollector{Base: base},
		"network":     &collector.NetworkCollector{Base: base},
		"temperature": &collector.TemperatureCollector{Base: base},
		"os":          &collector.OSInfoCollector{Base: base},
	}
	var out []collector.Collector
	for name, c := range all {
		if enabled[name] {
			out = append(out, c)
		}
	}
	return out
}

func enabledSet(list []string) map[string]bool {
	m := make(map[string]bool, len(list))
	for _, s := range list {
		m[s] = true
	}
	return m
}

func defaultQueuePath() string {
	switch runtime.GOOS {
	case "windows":
		return `C:\ProgramData\fwdcim\idr\queue.db`
	default:
		return "/var/lib/fwdcim/idr/queue.db"
	}
}

func buildLogger(cfg config.LogConfig) *zap.Logger {
	level := zapcore.InfoLevel
	_ = level.UnmarshalText([]byte(cfg.Level))

	var zapCfg zap.Config
	if cfg.Format == "console" {
		zapCfg = zap.NewDevelopmentConfig()
	} else {
		zapCfg = zap.NewProductionConfig()
	}
	zapCfg.Level = zap.NewAtomicLevelAt(level)
	log, _ := zapCfg.Build()
	return log
}
