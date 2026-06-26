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
	Retention  RetentionConfig  `yaml:"retention"`
	TLS        TLSConfig        `yaml:"tls"`
	Log        LogConfig        `yaml:"log"`
}

// RetentionConfig controls TimescaleDB data lifecycle for the metrics and
// energy_metrics hypertables. Reconciled on EVERY boot from this config — NOT a
// one-shot migration. Change a value, restart DCS, and the policies are dropped
// and re-applied to match. This is the disk-pressure knob for small/demo
// deployments: shrink raw_retention + chunk_interval to keep the database tiny.
//
// Durations are Go duration strings ("20m", "24h", "90m") with an extra "d"
// day suffix ("7d"). An empty string disables that policy (data kept forever /
// no compression). Enabled=false skips reconciliation entirely.
type RetentionConfig struct {
	Enabled bool            `yaml:"enabled"`
	Metrics RetentionPolicy `yaml:"metrics"`
	Energy  RetentionPolicy `yaml:"energy"`
}

// RetentionPolicy is the per-hypertable lifecycle. The same shape applies to
// both `metrics` and `energy_metrics`.
type RetentionPolicy struct {
	// ChunkInterval sizes FUTURE chunks. For short raw_retention this MUST be
	// small (e.g. "5m"): TimescaleDB drops a chunk only once ALL its rows are
	// past retention, so a 1-day chunk never frees space under a 20m retention.
	ChunkInterval string `yaml:"chunk_interval"`
	// RawRetention drops raw chunks older than this. "" = keep forever.
	RawRetention string `yaml:"raw_retention"`
	// CompressAfter columnar-compresses chunks older than this. "" = no
	// compression (pointless when raw_retention is already tiny).
	CompressAfter string `yaml:"compress_after"`
	// RollupRetention drops the 5-minute continuous aggregate older than this.
	// "" = keep forever. Trend charts read from the rollup once raw is gone.
	RollupRetention string `yaml:"rollup_retention"`
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

	// MetricMultiplier scales BatchLimit for the high-volume metrics and energy
	// cursors only (metrics volume >> device/topology volume). The effective
	// per-push metric ceiling is BatchLimit*MetricMultiplier. Default 50 (so the
	// old hardcoded 50k at BatchLimit=1000 is preserved exactly). LOWER this in
	// production (e.g. 10 → 10k) to cap the transient heap a single push
	// materializes+serializes, which is the main driver of the forwarder's memory
	// burst against GOMEMLIMIT. Smaller batches just take more ticks to drain a
	// backlog — the ts-ordered cursor never loses or double-sends rows. <= 0 → 50.
	MetricMultiplier int `yaml:"metric_multiplier"`

	// IdleIntervalMs caps how rarely a *drained* table is re-polled. After a
	// table returns zero rows the forwarder backs off, skipping that table's
	// query for a growing number of ticks until it is only checked once per
	// IdleIntervalMs (default 60000 ms). The first non-empty poll snaps it back
	// to the full IntervalMs cadence. This stops fixed tables (devices,
	// topology) from being scanned every tick once fully forwarded, cutting
	// idle CPU. Set <= IntervalMs to disable backoff (poll every tick).
	IdleIntervalMs int `yaml:"idle_interval_ms"`

	// EventDebounceMs is how long the forwarder coalesces an event-triggered
	// wake before pushing (default 300 ms). EVERY event DCS writes — any trap,
	// alarm, link state change, hostname change, or future event type, with no
	// per-type allowlist — signals the forwarder to push immediately instead of
	// waiting for the next IntervalMs tick, so the change reflects on the UI in
	// ~EventDebounceMs rather than seconds. The debounce collapses a burst (e.g.
	// a link-flap storm) into ONE forced push that bypasses idle backoff, which
	// also bounds forced pushes to at most one per window. <= 0 → 300 ms default.
	EventDebounceMs int    `yaml:"event_debounce_ms"`
	OrgID           string `yaml:"org_id"`
	NetworkID       string `yaml:"network_id"`
	GroupID         string `yaml:"group_id"`

	// Fallback scope keys used when a device/event row carries an empty
	// datacenter_id or floor_id. The Aggregator rejects payloads with empty
	// datacenter_id/floor_id, so without a fallback those rows would wedge the
	// whole push (no cursor advances). Empty values default to "unknown" so
	// every row is forwarded regardless. Set explicitly to bucket unscoped
	// devices under a chosen name.
	DefaultDatacenterID string `yaml:"default_datacenter_id"`
	DefaultFloorID      string `yaml:"default_floor_id"`
}

// ─── sub-structs ────────────────────────────────────────────────────────────

type GRPCServerConfig struct {
	Addr string `yaml:"addr"` // e.g. ":9090"
}

type RESTServerConfig struct {
	Addr string `yaml:"addr"` // e.g. ":8080"
	// CommandKey guards the downstream-control endpoints (/admin/commands*) that
	// EDR polls. EDR must send it as X-Command-Key. Empty = no auth (dev only).
	CommandKey string `yaml:"command_key"`
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
	cfg.Aggregator.IdleIntervalMs = 60000
	cfg.Aggregator.EventDebounceMs = 300
	cfg.Topology.RecomputeIntervalMs = 30000
	cfg.Topology.ClassifyRoles = true // default on; YAML can set false explicitly
	// Retention defaults tuned for a small/demo deployment: keep raw telemetry
	// only ~20 min on tiny 5-min chunks so disk stays minimal; bump these in
	// dcs.yaml for production. yaml.v3 leaves unspecified keys at these values.
	cfg.Retention.Enabled = true
	cfg.Retention.Metrics = RetentionPolicy{ChunkInterval: "5m", RawRetention: "30m", RollupRetention: "24h"}
	cfg.Retention.Energy = RetentionPolicy{ChunkInterval: "5m", RawRetention: "30m", RollupRetention: "168h"}
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
	if cfg.Aggregator.MetricMultiplier <= 0 {
		cfg.Aggregator.MetricMultiplier = 50
	}
	if cfg.Aggregator.IdleIntervalMs <= 0 {
		cfg.Aggregator.IdleIntervalMs = 60000
	}
	if cfg.Aggregator.EventDebounceMs <= 0 {
		cfg.Aggregator.EventDebounceMs = 300
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
