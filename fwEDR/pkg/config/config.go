// Package config loads YAML configuration for EDR.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// EDRConfig is the full configuration for the External Data Reader.
type EDRConfig struct {
	Identity     IdentityConfig  `yaml:"identity"`
	DCS          DCSClientConfig `yaml:"dcs"`
	Queue        QueueConfig     `yaml:"queue"`
	Publisher    PublisherConfig `yaml:"publisher"`
	Discovery    DiscoveryConfig `yaml:"discovery"`
	TopologyFile string          `yaml:"topology_file"` // path to simulator topology JSON
	SNMP         SNMPConfig      `yaml:"snmp"`
	GNMI         GNMIConfig      `yaml:"gnmi"`
	Targets      []TargetConfig  `yaml:"targets"`
	Log          LogConfig       `yaml:"log"`
}

// ─── sub-structs ────────────────────────────────────────────────────────────

type IdentityConfig struct {
	OrgID        string `yaml:"org_id"`
	DatacenterID string `yaml:"datacenter_id"`
	FloorID      string `yaml:"floor_id"`
	NetworkID    string `yaml:"network_id"`
	GroupID      string `yaml:"group_id"`
	ReaderID     string `yaml:"reader_id"` // e.g. "edr-1.0.0"
}

type DCSClientConfig struct {
	Endpoint string    `yaml:"endpoint"` // host:port
	TLS      TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	CertFile string `yaml:"cert"`
	KeyFile  string `yaml:"key"`
	CAFile   string `yaml:"ca"`
	Insecure bool   `yaml:"insecure"` // dev-only: skip verification
}

type QueueConfig struct {
	Path     string `yaml:"path"`      // local bbolt DB path
	MaxBytes int64  `yaml:"max_bytes"` // max queue size (bytes); 0 = 512 MB
}

type DiscoveryConfig struct {
	Subnets          []string `yaml:"subnets"`            // CIDR ranges to sweep
	SNMPAgent        string   `yaml:"snmp_agent"`         // SNMP socket target; "127.0.0.1" for simulator, "" = probe device IP directly
	SeedIP           string   `yaml:"seed_ip"`            // first known device IP; used as readiness probe before sweep (community = seed_ip)
	IntervalHours    int      `yaml:"interval_hours"`     // background rediscovery interval (hours)
	TargetCacheHours int      `yaml:"target_cache_hours"` // skip sweep on startup if targets.json is younger than this (default 24)
	EnrichmentPaceMs int      `yaml:"enrichment_pace_ms"` // delay between per-target ipAdEntAddr walks during background enrichment (default 200)
}

// SNMPConfig holds global SNMP defaults; per-target config can override.
//
// Tiered polling: liveness vs counter walks are scheduled independently so we
// keep state fresh without overloading the SNMP agent.
//
//	FAST   tier — sys.uptime only (1 Get/device). default 30 s.
//	MEDIUM tier — interface admin/oper/speed (3 walks/device). default 60 s.
//	SLOW   tier — counter walks, server HR/UCD, LLDP, UPS, sensors (10+ walks). default 300 s.
//
// PollInterval is kept for backwards compatibility and seeds the FAST tier
// when FastIntervalMs is unset.
type SNMPConfig struct {
	Community          string `yaml:"community"`
	Version            int    `yaml:"version"`
	V3                 SNMPV3 `yaml:"v3"`
	PollInterval       int    `yaml:"poll_interval_ms"`     // legacy; seeds fast tier if fast_interval_ms unset
	FastIntervalMs     int    `yaml:"fast_interval_ms"`     // default 30000
	MediumIntervalMs   int    `yaml:"medium_interval_ms"`   // default 120000
	SlowIntervalMs     int    `yaml:"slow_interval_ms"`     // default 300000
	TopologyIntervalMs int    `yaml:"topology_interval_ms"` // default 600000 — LLDP refresh tier
	TrapAddr           string `yaml:"trap_addr"`
	Timeout            int    `yaml:"timeout_ms"`
	Retries            int    `yaml:"retries"`
	MaxConcurrent      int    `yaml:"max_concurrent"`
	RateLimitPerSec    int    `yaml:"rate_limit_per_sec"`
	BreakerThreshold   int    `yaml:"breaker_threshold"`
	BreakerCooldownMs  int    `yaml:"breaker_cooldown_ms"`

	// Sharding: spread devices across N snmpsim responder processes to avoid the
	// single-process wedge and raise throughput. Shard 0 stays on port 161 (so
	// enrichment/discovery, which target 161, keep working); shards 1..N-1 use
	// ShardBasePort+i. Routing key is the device community (= device IP).
	// Shards <= 1 disables sharding (everything on 161 — original behavior).
	Shards        int `yaml:"shards"`          // number of responder processes (default 1 = off)
	ShardBasePort int `yaml:"shard_base_port"` // base port for shards 1..N-1 (default 16100)

	// Sim/test convenience: when ShardSpawn is true, EDR launches the snmpsim
	// responders itself (one per shard port) so a single `edr.exe` brings up the
	// stack. OFF by default — production EDR polls real devices and spawns nothing.
	ShardSpawn         bool   `yaml:"shard_spawn"`          // launch responders from EDR (sim only)
	ShardResponderPath string `yaml:"shard_responder_path"` // path to snmpsim-command-responder(.exe)
	ShardDataDir       string `yaml:"shard_data_dir"`       // simulator datasets/snmp directory
}

// PublisherConfig controls EDR → DCS batching behavior.
type PublisherConfig struct {
	BatchSize       int `yaml:"batch_size"`        // max packets per BatchPush RPC (default 256)
	FlushIntervalMs int `yaml:"flush_interval_ms"` // max time to wait before forcing partial flush (default 200)
	MaxInFlight     int `yaml:"max_in_flight"`     // parallel BatchPush RPCs (default 2)
}

type SNMPV3 struct {
	Username     string `yaml:"username"`
	AuthProtocol string `yaml:"auth_protocol"` // MD5|SHA|SHA224|SHA256|SHA384|SHA512
	AuthPassword string `yaml:"auth_password"`
	PrivProtocol string `yaml:"priv_protocol"` // DES|AES|AES192|AES256
	PrivPassword string `yaml:"priv_password"`
}

// GNMIConfig holds gNMI subscriber settings.
type GNMIConfig struct {
	Enabled      bool      `yaml:"enabled"` // default true; set false when no gNMI servers exist (e.g. SNMP-only simulator)
	Port         int       `yaml:"port"`    // default 9339
	TLS          TLSConfig `yaml:"tls"`
	Username     string    `yaml:"username"`
	Password     string    `yaml:"password"`
	PollInterval int       `yaml:"poll_interval_ms"` // default 30000
}

// TargetConfig describes one device to poll. IP roles follow DCIM conventions —
// see internal/target/target.go for full semantics.
type TargetConfig struct {
	IP          string            `yaml:"ip"`          // SNMP socket target (loopback in sim mode, real device IP in prod)
	MgmtIP      string            `yaml:"mgmt_ip"`     // operator-facing mgmt IP (192.168.x in sim); defaults to IP if empty
	ProdIP      string            `yaml:"prod_ip"`     // production / data-plane IP (10.x in sim); shown on dashboards
	LoopbackIP  string            `yaml:"loopback_ip"` // router/switch loopback
	OOBIP       string            `yaml:"oob_ip"`      // out-of-band mgmt IP
	GNMIIP      string            `yaml:"gnmi_ip"`     // gNMI connection IP; defaults to mgmt IP if empty
	Hostname    string            `yaml:"hostname"`
	DeviceType  string            `yaml:"device_type"`  // router|switch|server|firewall|load_balancer|ups|pdu|floor_pdu|sensor
	SNMPVersion int               `yaml:"snmp_version"` // 2|3; 0 = use global default
	Community   string            `yaml:"community"`    // override global community
	GNMIEnabled bool              `yaml:"gnmi"`         // whether to also subscribe via gNMI
	Vendor      string            `yaml:"vendor"`       // cisco|juniper|arista|apc|raritan|vertiv|eaton|generic
	Labels      map[string]string `yaml:"labels"`       // arbitrary key=value passed into attributes

	// Per-device routing key overrides. When non-empty these override the
	// global identity.datacenter_id / identity.floor_id for this device's
	// packets — handles multi-datacenter topology files correctly.
	// Empty = fall back to global identity config value.
	DatacenterID string `yaml:"datacenter_id"` // e.g. "DC1" from topology JSON
	FloorID      string `yaml:"floor_id"`      // e.g. "floor-2" (set when simulator exports it)

	// Physical location — written to devices table columns. All optional.
	ModelName      string `yaml:"model_name"`
	Country        string `yaml:"country"`
	DatacenterName string `yaml:"datacenter_name"` // informational physical name (same as DatacenterID for now)
	DatacenterCity string `yaml:"datacenter_city"` // e.g. "Dallas" — from topology JSON
	Room           string `yaml:"room"`
	Floor          string `yaml:"floor"`
	RackRow        int    `yaml:"rack_row"`
	RackNum        int    `yaml:"rack_num"`
	RackUnit       int    `yaml:"rack_unit"`
}

type LogConfig struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // json|console
}

// ─── loaders ────────────────────────────────────────────────────────────────

func LoadEDR(path string) (*EDRConfig, error) {
	cfg := &EDRConfig{}
	cfg.Queue.MaxBytes = 512 * 1024 * 1024
	cfg.SNMP.Community = "public"
	cfg.SNMP.Version = 2
	cfg.SNMP.PollInterval = 30000
	cfg.SNMP.FastIntervalMs = 30000
	cfg.SNMP.MediumIntervalMs = 120000
	cfg.SNMP.SlowIntervalMs = 300000
	cfg.SNMP.TopologyIntervalMs = 600000
	cfg.SNMP.Timeout = 2000
	cfg.SNMP.Retries = 1
	cfg.SNMP.TrapAddr = ":162"
	cfg.SNMP.MaxConcurrent = 5
	cfg.SNMP.RateLimitPerSec = 50
	cfg.SNMP.BreakerThreshold = 3
	cfg.SNMP.BreakerCooldownMs = 30000
	cfg.Publisher.BatchSize = 256
	cfg.Publisher.FlushIntervalMs = 200
	cfg.Publisher.MaxInFlight = 2
	cfg.GNMI.Enabled = true // default on; YAML "enabled: false" overrides
	cfg.GNMI.Port = 9339
	cfg.GNMI.PollInterval = 30000
	if err := loadYAML(path, cfg); err != nil {
		return nil, err
	}
	if cfg.SNMP.MaxConcurrent <= 0 {
		cfg.SNMP.MaxConcurrent = 50
	}
	if cfg.SNMP.FastIntervalMs <= 0 {
		if cfg.SNMP.PollInterval > 0 {
			cfg.SNMP.FastIntervalMs = cfg.SNMP.PollInterval
		} else {
			cfg.SNMP.FastIntervalMs = 30000
		}
	}
	if cfg.SNMP.MediumIntervalMs <= 0 {
		cfg.SNMP.MediumIntervalMs = 120000
	}
	if cfg.SNMP.SlowIntervalMs <= 0 {
		cfg.SNMP.SlowIntervalMs = 300000
	}
	if cfg.SNMP.TopologyIntervalMs <= 0 {
		cfg.SNMP.TopologyIntervalMs = 600000
	}
	if cfg.SNMP.BreakerThreshold <= 0 {
		cfg.SNMP.BreakerThreshold = 3
	}
	if cfg.SNMP.BreakerCooldownMs <= 0 {
		cfg.SNMP.BreakerCooldownMs = 30000
	}
	if cfg.SNMP.ShardBasePort <= 0 {
		cfg.SNMP.ShardBasePort = 16100
	}
	if cfg.Publisher.BatchSize <= 0 {
		cfg.Publisher.BatchSize = 256
	}
	if cfg.Publisher.FlushIntervalMs <= 0 {
		cfg.Publisher.FlushIntervalMs = 200
	}
	if cfg.Publisher.MaxInFlight <= 0 {
		cfg.Publisher.MaxInFlight = 2
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
