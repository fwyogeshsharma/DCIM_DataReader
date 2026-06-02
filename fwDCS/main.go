// DCS — DataCenterStore
// Receives telemetry from IDR and EDR, writes to TimescaleDB.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	dcsgrpc "github.com/faberwork/fwdcs/internal/api/grpc"
	"github.com/faberwork/fwdcs/internal/forwarder"
	"github.com/faberwork/fwdcs/internal/ingest"
	"github.com/faberwork/fwdcs/internal/store"
	"github.com/faberwork/fwdcs/pkg/config"
	v1 "github.com/faberwork/fwdcs/proto/v1"
)

func main() {
	cfgPath := flag.String("config", "dcs.yaml", "path to DCS config file")
	flag.Parse()

	if err := run(*cfgPath); err != nil {
		fmt.Fprintf(os.Stderr, "dcs: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath string) error {
	cfg, err := config.LoadDCS(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := buildLogger(cfg.Log.Level, cfg.Log.Format)
	defer log.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// TimescaleDB — migrations applied automatically on connect
	db, err := store.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("db: %w", err)
	}
	defer db.Close()
	log.Info("postgres connected", zap.String("dsn", cfg.Postgres.DSN))

	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	log.Info("redis connected", zap.String("addr", cfg.Redis.Addr))

	// Deduper + async ingest pipeline + gRPC server
	deduper := ingest.NewDeduper(rdb)
	pipe := ingest.NewPipeline(db, deduper, cfg.Ingest, log)
	pipe.Start(ctx)
	srv := dcsgrpc.NewIngestServer(pipe, deduper, db, log)

	// gRPC server — allow up to 64 MB messages to fit big BatchRequests.
	lis, err := net.Listen("tcp", cfg.GRPC.Addr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", cfg.GRPC.Addr, err)
	}
	grpcSrv := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
		grpc.MaxRecvMsgSize(64*1024*1024),
		grpc.MaxSendMsgSize(64*1024*1024),
	)
	v1.RegisterIngestServiceServer(grpcSrv, srv)
	reflection.Register(grpcSrv) // enables grpcurl

	log.Info("dcs listening", zap.String("grpc", cfg.GRPC.Addr))
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error("grpc serve", zap.Error(err))
		}
	}()

	// Admin REST server — health probes + cache flush. Optional; skipped if
	// cfg.REST.Addr is empty.
	var httpSrv *http.Server
	if cfg.REST.Addr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			if err := db.Ping(r.Context()); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"db_unreachable"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		})
		mux.HandleFunc("/admin/caches/flush", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", "POST")
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			devs, ifaces := pipe.FlushCaches()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"flushed_devices":    devs,
				"flushed_interfaces": ifaces,
			})
		})
		httpSrv = &http.Server{
			Addr:              cfg.REST.Addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Info("dcs admin REST listening", zap.String("addr", cfg.REST.Addr))
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("admin REST serve", zap.Error(err))
			}
		}()
	}

	// Aggregator forwarder — starts only when explicitly enabled in config.
	// Runs as a background goroutine; ctx cancellation stops it cleanly.
	if cfg.Aggregator.Enabled {
		if cfg.Aggregator.Endpoint == "" || cfg.Aggregator.IngestKey == "" {
			log.Warn("aggregator forwarder enabled but endpoint or ingest_key is empty — skipping")
		} else {
			fwd := forwarder.New(db, cfg.Aggregator, log)
			go fwd.Run(ctx)
			log.Info("aggregator forwarder enabled",
				zap.String("endpoint", cfg.Aggregator.Endpoint),
				zap.Int("interval_ms", cfg.Aggregator.IntervalMs))
		}
	}

	// Optional external classifier rules (lookup tables / weights / thresholds).
	// Empty path → compiled-in defaults.
	if cfg.Topology.RoleRulesPath != "" {
		if err := store.LoadRoleRules(cfg.Topology.RoleRulesPath); err != nil {
			log.Warn("role rules load failed — using defaults", zap.Error(err))
		} else {
			log.Info("role classifier rules loaded", zap.String("path", cfg.Topology.RoleRulesPath))
		}
	}

	// Topology hierarchy runner — periodically rebuilds the parent-child
	// spanning tree (BFS over topology_links) so devices.parent_device_id stays
	// current. Tenant scope comes from the aggregator/identity config.
	if org := cfg.Aggregator.OrgID; org != "" {
		interval := time.Duration(cfg.Topology.RecomputeIntervalMs) * time.Millisecond
		go func() {
			// Let LLDP edges accumulate before the first build.
			select {
			case <-time.After(20 * time.Second):
			case <-ctx.Done():
				return
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			run := func() {
				root, n, err := db.RecomputeHierarchy(ctx,
					cfg.Aggregator.OrgID, cfg.Aggregator.NetworkID, cfg.Aggregator.GroupID,
					cfg.Topology.Root)
				if err != nil {
					log.Warn("topology hierarchy recompute failed", zap.Error(err))
					return
				}
				log.Info("topology hierarchy recomputed",
					zap.String("root", root), zap.Int("devices", n))

				// Fabric-role classification runs right after the graph is rebuilt,
				// so neighbor device_types (Signal 3) are current.
				if cfg.Topology.ClassifyRoles {
					nr, cerr := db.ClassifyRoles(ctx,
						cfg.Aggregator.OrgID, cfg.Aggregator.NetworkID, cfg.Aggregator.GroupID)
					if cerr != nil {
						log.Warn("device role classify failed", zap.Error(cerr))
					} else {
						log.Info("device roles classified", zap.Int("changed", nr))
					}
				}
			}
			run()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					run()
				}
			}
		}()
		log.Info("topology hierarchy runner enabled",
			zap.String("root_hint", cfg.Topology.Root),
			zap.Int("interval_ms", cfg.Topology.RecomputeIntervalMs))
	}

	<-ctx.Done()
	log.Info("dcs shutting down")
	if httpSrv != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
		_ = httpSrv.Shutdown(shutdownCtx)
		cancelShutdown()
	}
	grpcSrv.GracefulStop()
	pipe.Wait()
	log.Info("dcs stopped")
	return nil
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
	l, _ := cfg.Build()
	return l
}
