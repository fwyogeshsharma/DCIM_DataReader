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
	BACnet       BACnetConfig    `yaml:"bacnet"`
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
	IntervalHours    int      `yaml:"interval_hours"`     // background rediscovery interval (hours). 0 = disabled (manual --rediscover only)
	TargetCacheHours int      `yaml:"target_cache_hours"` // skip sweep on startup if targets.json is younger than this (default 24)
	EnrichmentPaceMs int      `yaml:"enrichment_pace_ms"` // delay between per-target ipAdEntAddr walks during background enrichment (default 200)
	Enrich           bool     `yaml:"enrich"`             // run the post-walk ipAdEntAddr enrichment pass (interface IPs + OOB/loopback). default true; set false to stay idle/trap-driven after the one-shot walk
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
	// MaxConcurrent is the GLOBAL cap on concurrent SNMP UDP sockets across ALL
	// tiers (fast + heavy combined) — not per-tier. The poller enforces it with a
	// single global semaphore every tier must acquire before opening a socket, so
	// the total number of in-flight UDP sockets never exceeds this. In simulator
	// sharding mode the effective cap is further clamped to shards*2 (load is
	// spread over only `shards` responders). Keep this modest on Windows: too many
	// simultaneous UDP binds trigger WSAENOBUFS ("lacked sufficient buffer space").
	MaxConcurrent     int `yaml:"max_concurrent"`
	RateLimitPerSec   int `yaml:"rate_limit_per_sec"`
	BreakerThreshold  int `yaml:"breaker_threshold"` // consecutive failures to open the HEAVY-walk breaker (Medium/Slow/Topology). default 3
	BreakerCooldownMs int `yaml:"breaker_cooldown_ms"`

	// FastBreakerThreshold is the separate, more tolerant threshold for the FAST
	// liveness breaker. The cheap 1-Get heartbeat must NOT share the heavy-walk
	// breaker: under a slow/overloaded SNMP responder the heavy BulkWalks time out
	// first and, on a shared breaker, would open it and silence the heartbeat —
	// flipping a perfectly reachable device "offline" downstream. Splitting the
	// breakers + requiring more consecutive FAST misses absorbs transient blips.
	// default 5.
	FastBreakerThreshold int `yaml:"fast_breaker_threshold"`

	// MgmtPort, when > 0, enables a SIMULATOR-ONLY confirmation probe: on a FAST
	// timeout EDR does one cheap sysName Get against ip:MgmtPort (the simulator's
	// SNMP SET management agent, default 1161). That agent runs on its own socket,
	// independent of the shared snmpsim responder on 161, so it stays responsive
	// even when 161 is wedged. If it answers, the device is alive and 161 is merely
	// overloaded → EDR treats the miss as transient and does NOT count it toward the
	// liveness breaker. 0 = off (production: real devices have no such agent).
	MgmtPort int `yaml:"mgmt_port"`

	// MetricsLogIntervalMs controls how often the poller emits a one-line summary
	// of poll/timeout counters (per tier) so simulator instability is visible
	// without a metrics server. default 60000. 0 disables.
	MetricsLogIntervalMs int `yaml:"metrics_log_interval_ms"`

	// LogThrottleMs is the minimum interval between repeated connect-failure /
	// gNMI-reconnect warnings PER target. Bursty responders otherwise flood the
	// console; throttled lines carry a suppressed-since-last count. default 30000.
	// 0 disables throttling (log every occurrence — old behavior).
	LogThrottleMs int `yaml:"log_throttle_ms"`

	// Event-driven monitoring model. By default EDR no longer polls continuously:
	// it does ONE full SNMP walk per device (all tiers — system, interfaces,
	// counters, LLDP topology) to build inventory, then stops and relies on SNMP
	// traps for state/topology change events. This removes the steady-state poll
	// load that destabilized the simulator.
	//
	//   RewalkIntervalMs  > 0 → repeat the full walk on this interval (lightweight
	//                           periodic inventory/topology refresh; also re-reads
	//                           sysName so renames reflect). 0 = pure one-shot
	//                           (default) — walk once, then traps only.
	//   WalkRetryIntervalMs    retry cadence for the INITIAL walk while a device is
	//                           unreachable, so a device that was down at startup
	//                           still gets inventoried once it answers. Retries stop
	//                           after the first successful walk. default 30000.
	RewalkIntervalMs    int `yaml:"rewalk_interval_ms"`
	WalkRetryIntervalMs int `yaml:"walk_retry_interval_ms"`

	// LivenessIntervalMs > 0 enables a lightweight FAST-only heartbeat after the
	// one-shot walk: a single sysName+uptime GET per device on this interval — NOT
	// a full re-walk (no interface/counter/sensor walks). It refreshes last_seen
	// and, crucially, re-reads sysName so a source-side rename propagates to the DB
	// (and thus the UI and future events) without the cost of RewalkIntervalMs.
	// 0 = off (pure one-shot). Ignored when RewalkIntervalMs > 0 (re-walk already
	// re-reads sysName). default 0.
	LivenessIntervalMs int `yaml:"liveness_interval_ms"`

	// Per-tier toggles for the inventory walk. FAST (system/liveness) always runs.
	// Set walk_slow:false to skip the heavy counter/HR/UPS/sensor walks — the
	// biggest SNMP load and not needed for device inventory or link topology.
	// Defaults: all true (full walk). Combined with max_concurrent:1 this keeps
	// the single-threaded simulator from being hit by parallel requests.
	WalkMedium   bool `yaml:"walk_medium"`   // interface admin/oper/speed (also creates the interfaces table). default true
	WalkSlow     bool `yaml:"walk_slow"`     // counters, server HR/UCD, UPS, sensors. default true
	WalkTopology bool `yaml:"walk_topology"` // LLDP neighbor discovery → topology_links. default true
	WalkSensors  bool `yaml:"walk_sensors"`  // environment sensors ONLY (temperature/humidity from sensor+PDU devices) WITHOUT the heavy counter walk. default false — opt-in for the heatmap
	// WalkServerHealth collects server CPU/RAM via UCD-SNMP scalars ONLY (a few
	// Gets, no HOST-RESOURCES walks) — independent of the heavy walk_slow tier. Lets
	// you get server CPU/RAM cheaply. default false — opt-in.
	WalkServerHealth bool `yaml:"walk_server_health"`

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
	Enabled bool `yaml:"enabled"` // default true; set false when no gNMI servers exist (e.g. SNMP-only simulator)
	Port    int  `yaml:"port"`    // default 9339

	// ProxyAddr is the single aggregating gNMI proxy endpoint (host:port). One
	// subscription with an empty prefix.target streams telemetry for ALL devices
	// — the collector holds ONE connection to the proxy, not one per device. This
	// is the preferred path. Empty disables the proxy and uses direct mode only.
	ProxyAddr string `yaml:"proxy_addr"` // e.g. "127.0.0.1:50051"

	// FallbackPort is the per-device direct gNMI port used ONLY when the proxy is
	// unavailable. The collector opens direct connections to each device on this
	// port as a temporary measure and automatically tears them down and returns to
	// the single proxy subscription once the proxy recovers. default 57400.
	FallbackPort int `yaml:"fallback_port"`

	// ProxyProbeIntervalMs is how often to re-check proxy availability while in
	// direct-fallback mode, so recovery is detected and the proxy resumed. default 5000.
	ProxyProbeIntervalMs int `yaml:"proxy_probe_interval_ms"`

	TLS          TLSConfig `yaml:"tls"`
	Username     string    `yaml:"username"`
	Password     string    `yaml:"password"`
	PollInterval int       `yaml:"poll_interval_ms"` // default 30000; gNMI SAMPLE interval
}

// BACnetConfig holds the BACnet/IP collector settings. EDR polls Verdigris EV2
// energy monitors (device_type=energy_monitor) via BACnet/IP — ReadPropertyMultiple
// on a periodic cadence, plus optional SubscribeCOV push notifications.
type BACnetConfig struct {
	Enabled        bool `yaml:"enabled"`          // default true; set false to skip BACnet entirely
	Port           int  `yaml:"port"`             // device UDP port (default 47808)
	PollIntervalMs int  `yaml:"poll_interval_ms"` // ReadPropertyMultiple cadence (default 30000)
	TimeoutMs      int  `yaml:"timeout_ms"`       // per-request response timeout (default 2000)
	Retries        int  `yaml:"retries"`          // per-request retries (default 1)
	ReadCircuits   bool `yaml:"read_circuits"`    // also read per-circuit objects (default true)
	ObjectsPerRead int  `yaml:"objects_per_read"` // objects per ReadPropertyMultiple to fit the APDU (default 12)
	UseCOV         bool `yaml:"use_cov"`          // also SubscribeCOV for push notifications (default true)
	COVLifetimeSec int  `yaml:"cov_lifetime_sec"` // COV subscription lifetime; renewed before expiry (default 300)
}

// TargetConfig describes one device to poll. IP roles follow DCIM conventions —
// see internal/target/target.go for full semantics.
type TargetConfig struct {
	IP          string `yaml:"ip"`          // SNMP socket target (loopback in sim mode, real device IP in prod)
	MgmtIP      string `yaml:"mgmt_ip"`     // operator-facing mgmt IP (192.168.x in sim); defaults to IP if empty
	ProdIP      string `yaml:"prod_ip"`     // production / data-plane IP (10.x in sim); shown on dashboards
	LoopbackIP  string `yaml:"loopback_ip"` // router/switch loopback
	OOBIP       string `yaml:"oob_ip"`      // out-of-band mgmt IP
	GNMIIP      string `yaml:"gnmi_ip"`     // gNMI connection IP; defaults to mgmt IP if empty
	Hostname    string `yaml:"hostname"`
	DeviceType  string `yaml:"device_type"`  // router|switch|server|firewall|load_balancer|ups|pdu|floor_pdu|sensor
	SNMPVersion int    `yaml:"snmp_version"` // 2|3; 0 = use global default
	Community   string `yaml:"community"`    // override global community
	GNMIEnabled bool   `yaml:"gnmi"`         // whether to also subscribe via gNMI
	// BACnetEnabled marks a device as a BACnet/IP target (Verdigris EV2 energy
	// monitors). Set automatically for device_type=energy_monitor by the topology
	// loader. The BACnet manager polls these at MgmtIP:bacnet.port.
	BACnetEnabled bool `yaml:"bacnet"`
	// ActiveCircuits is the number of EV2 circuits with a load actually wired to
	// them (derived from the topology power graph). The BACnet manager reads only
	// these circuits instead of the meter's full physical capacity, so spare
	// breakers don't write junk zero rows. 0 = unknown → read full capacity.
	ActiveCircuits int               `yaml:"active_circuits"`
	Vendor         string            `yaml:"vendor"` // cisco|juniper|arista|apc|raritan|vertiv|eaton|generic
	Labels         map[string]string `yaml:"labels"` // arbitrary key=value passed into attributes

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
	cfg.SNMP.FastBreakerThreshold = 5
	cfg.SNMP.BreakerCooldownMs = 30000
	cfg.SNMP.MetricsLogIntervalMs = 60000
	cfg.SNMP.LogThrottleMs = 30000
	cfg.Publisher.BatchSize = 512
	cfg.Publisher.FlushIntervalMs = 200
	cfg.Publisher.MaxInFlight = 8
	cfg.Discovery.Enrich = true // default on; YAML "enrich: false" overrides
	cfg.SNMP.WalkMedium = true  // default on; full walk unless YAML overrides
	cfg.SNMP.WalkSlow = true
	cfg.SNMP.WalkTopology = true
	cfg.BACnet.Enabled = true // default on; YAML "enabled: false" overrides
	cfg.BACnet.Port = 47808
	cfg.BACnet.PollIntervalMs = 30000
	cfg.BACnet.TimeoutMs = 2000
	cfg.BACnet.Retries = 1
	cfg.BACnet.ReadCircuits = true
	cfg.BACnet.ObjectsPerRead = 12
	cfg.BACnet.UseCOV = true
	cfg.BACnet.COVLifetimeSec = 300
	cfg.GNMI.Enabled = true // default on; YAML "enabled: false" overrides
	cfg.GNMI.Port = 9339
	cfg.GNMI.PollInterval = 30000
	cfg.GNMI.FallbackPort = 57400
	cfg.GNMI.ProxyProbeIntervalMs = 5000
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
	if cfg.SNMP.FastBreakerThreshold <= 0 {
		cfg.SNMP.FastBreakerThreshold = 5
	}
	if cfg.SNMP.WalkRetryIntervalMs <= 0 {
		cfg.SNMP.WalkRetryIntervalMs = 30000
	}
	if cfg.SNMP.BreakerCooldownMs <= 0 {
		cfg.SNMP.BreakerCooldownMs = 30000
	}
	if cfg.SNMP.ShardBasePort <= 0 {
		cfg.SNMP.ShardBasePort = 16100
	}
	if cfg.Publisher.BatchSize <= 0 {
		cfg.Publisher.BatchSize = 512
	}
	if cfg.Publisher.FlushIntervalMs <= 0 {
		cfg.Publisher.FlushIntervalMs = 200
	}
	if cfg.Publisher.MaxInFlight <= 0 {
		cfg.Publisher.MaxInFlight = 8
	}
	if cfg.GNMI.FallbackPort <= 0 {
		cfg.GNMI.FallbackPort = 57400
	}
	if cfg.GNMI.ProxyProbeIntervalMs <= 0 {
		cfg.GNMI.ProxyProbeIntervalMs = 5000
	}
	if cfg.GNMI.PollInterval <= 0 {
		cfg.GNMI.PollInterval = 30000
	}
	if cfg.BACnet.Port <= 0 {
		cfg.BACnet.Port = 47808
	}
	if cfg.BACnet.PollIntervalMs <= 0 {
		cfg.BACnet.PollIntervalMs = 30000
	}
	if cfg.BACnet.TimeoutMs <= 0 {
		cfg.BACnet.TimeoutMs = 2000
	}
	if cfg.BACnet.ObjectsPerRead <= 0 {
		cfg.BACnet.ObjectsPerRead = 12
	}
	if cfg.BACnet.COVLifetimeSec <= 0 {
		cfg.BACnet.COVLifetimeSec = 300
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
