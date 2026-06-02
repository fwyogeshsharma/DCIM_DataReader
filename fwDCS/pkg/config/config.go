// Package config loads YAML configuration for DCS.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// DCSConfig is the configuration for DataCenterStore.
type DCSConfig struct {
	GRPC       GRPCServerConfig `yaml:"grpc"`
	REST       RESTServerConfig `yaml:"rest"`
	Postgres   PostgresConfig   `yaml:"postgres"`
	Redis      RedisConfig      `yaml:"redis"`
	Ingest     IngestConfig     `yaml:"ingest"`
	Aggregator AggregatorConfig `yaml:"aggregator"`
	Topology   TopologyConfig   `yaml:"topology"`
	TLS        TLSConfig        `yaml:"tls"`
	Log        LogConfig        `yaml:"log"`
}

// TopologyConfig controls the dynamic parent-child hierarchy DCS computes by
// BFS over the topology_links graph. Topology-agnostic — works for any graph
// shape. Root is the tree's top node; if Root is empty, DCS auto-selects the
// highest-degree node per connected component.
type TopologyConfig struct {
	Root                string `yaml:"root"`                  // root device hostname OR any of its IPs; empty = auto (highest-degree)
	RecomputeIntervalMs int    `yaml:"recompute_interval_ms"` // hierarchy recompute cadence (default 30000)
	ClassifyRoles       bool   `yaml:"classify_roles"`        // infer device_role (core/spine/leaf/...) after each recompute (default true)
	RoleRulesPath       string `yaml:"role_rules_path"`       // optional YAML overriding classifier lookup tables; empty = compiled defaults
}

// IngestConfig tunes the async ingest pipeline.
type IngestConfig struct {
	Workers         int  `yaml:"workers"`           // worker goroutines (default 4)
	BufferRows      int  `yaml:"buffer_rows"`       // per-worker buffer before COPY flush (default 5000)
	FlushIntervalMs int  `yaml:"flush_interval_ms"` // max delay before flushing partial buffer (default 500)
	ChannelSize     int  `yaml:"channel_size"`      // backpressure buffer between gRPC and workers (default 65536)
	DedupMetrics    bool `yaml:"dedup_metrics"`     // run Redis dedup for metric packets (default false; ON CONFLICT suffices)
	DedupEvents     bool `yaml:"dedup_events"`      // run Redis dedup for trap/event packets (default true)
	CacheTTLSeconds int  `yaml:"cache_ttl_seconds"` // LRU entry TTL — auto-recovers from external DB truncate (default 60)
}

// AggregatorConfig controls the DCS→Aggregator incremental push forwarder.
// Enabled defaults to false; set to true and fill Endpoint + IngestKey to
// activate. All cursor state is persisted in the forwarder_cursors table so
// DCS restarts resume exactly where they left off.
type AggregatorConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Endpoint   string `yaml:"endpoint"`    // e.g. https://fwdcim.faberwork.com/api/v1/ingest
	IngestKey  string `yaml:"ingest_key"`  // X-Ingest-Key header value
	IntervalMs int    `yaml:"interval_ms"` // push cadence (default 5000 ms)
	BatchLimit int    `yaml:"batch_limit"` // max rows per cursor per push (default 1000)
	OrgID      string `yaml:"org_id"`
	NetworkID  string `yaml:"network_id"`
	GroupID    string `yaml:"group_id"`
}

// ─── sub-structs ────────────────────────────────────────────────────────────

type GRPCServerConfig struct {
	Addr string `yaml:"addr"` // e.g. ":9090"
}

type RESTServerConfig struct {
	Addr string `yaml:"addr"` // e.g. ":8080"
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"` // postgresql://user:pass@host/db
}

type RedisConfig struct {
	Addr     string `yaml:"addr"` // host:port
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type TLSConfig struct {
	CertFile string `yaml:"cert"`
	KeyFile  string `yaml:"key"`
	CAFile   string `yaml:"ca"`
	Insecure bool   `yaml:"insecure"` // dev-only: skip verification
}

type LogConfig struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // json|console
}

// ─── loaders ────────────────────────────────────────────────────────────────

func LoadDCS(path string) (*DCSConfig, error) {
	cfg := &DCSConfig{}
	cfg.GRPC.Addr = ":9090"
	cfg.REST.Addr = ":8080"
	cfg.Ingest.Workers = 4
	cfg.Ingest.BufferRows = 5000
	cfg.Ingest.FlushIntervalMs = 500
	cfg.Ingest.ChannelSize = 65536
	cfg.Ingest.DedupMetrics = false
	cfg.Ingest.DedupEvents = true
	cfg.Ingest.CacheTTLSeconds = 60
	cfg.Aggregator.IntervalMs = 5000
	cfg.Aggregator.BatchLimit = 1000
	cfg.Topology.RecomputeIntervalMs = 30000
	cfg.Topology.ClassifyRoles = true // default on; YAML can set false explicitly
	if err := loadYAML(path, cfg); err != nil {
		return nil, err
	}
	if cfg.Ingest.Workers <= 0 {
		cfg.Ingest.Workers = 4
	}
	if cfg.Ingest.BufferRows <= 0 {
		cfg.Ingest.BufferRows = 5000
	}
	if cfg.Ingest.FlushIntervalMs <= 0 {
		cfg.Ingest.FlushIntervalMs = 500
	}
	if cfg.Ingest.ChannelSize <= 0 {
		cfg.Ingest.ChannelSize = 65536
	}
	if cfg.Ingest.CacheTTLSeconds <= 0 {
		cfg.Ingest.CacheTTLSeconds = 60
	}
	if cfg.Aggregator.IntervalMs <= 0 {
		cfg.Aggregator.IntervalMs = 5000
	}
	if cfg.Aggregator.BatchLimit <= 0 {
		cfg.Aggregator.BatchLimit = 1000
	}
	if cfg.Topology.RecomputeIntervalMs <= 0 {
		cfg.Topology.RecomputeIntervalMs = 30000
	}
	return cfg, nil
}

func loadYAML(path string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()
	if err := yaml.NewDecoder(f).Decode(out); err != nil {
		return fmt.Errorf("config: decode %s: %w", path, err)
	}
	return nil
}
