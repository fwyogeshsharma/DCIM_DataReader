# fwDCIM Architecture

Implementation reference for the **IDR / EDR / DCS** scope.
Aggregator + UI are owned by a separate team and are out of scope for this document. See `REDESIGN.md` for the full phased plan toward the spec target.

---

## 1. Component overview

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          Site (one datacenter)                              │
│                                                                             │
│  ┌──────────┐    ┌──────────┐                ┌────────────┐                │
│  │   IDR    │    │   IDR    │   host         │   EDR      │   network +     │
│  │  agent   │    │  agent   │   collectors   │  reader    │   sensors       │
│  │ (server) │    │ (server) │                │ (SNMP+gNMI │                │
│  └────┬─────┘    └────┬─────┘                │  + traps)  │                │
│       │ gRPC          │ gRPC                 └─────┬──────┘                │
│       └────────┬──────┘                            │ gRPC                  │
│                │                                   │                       │
│                ▼                                   ▼                       │
│           ┌─────────────────────────────────────────────┐                  │
│           │              DCS (gRPC + REST)              │                  │
│           │   ingest pipeline → store writers           │                  │
│           │   gRPC :9090   admin REST :8080             │                  │
│           └────────────────┬────────────────────────────┘                  │
│                            │                                               │
│         ┌──────────────────┼─────────────────┐                             │
│         ▼                  ▼                 ▼                             │
│   TimescaleDB :5432   PostgreSQL :5432   Redis :6379                       │
│   (metrics +          (devices,          (dedup +                          │
│    events as          interfaces,         hot cache)                       │
│    hypertables)       topology, …)                                         │
└────────────────────────────────────────────────────────────────────────────┘
                            │
                            │ proto/v1.QueryService (future Phase 1)
                            ▼
                  Aggregator / UI (other team)
```

| Component | Role |
|---|---|
| **IDR** (`fwIDR/`) | Host agent. Collects CPU / memory / disk / network / OS / temperature using gopsutil. Persistent bbolt queue. mTLS-ready gRPC push to DCS. |
| **EDR** (`fwEDR/`) | Agentless network reader. SNMP polling + gNMI Subscribe streaming + SNMP trap receiver. Persistent bbolt queue + persisted target catalogue. |
| **DCS** (`fwDCS/`) | Site authoritative store. gRPC ingest + admin REST. TimescaleDB hypertables for metrics + events; PostgreSQL for inventory + topology; Redis for dedup + hot cache. |

---

## 2. Data flow

### 2.1 Metric path

```
device → EDR adapter → packet signer → in-memory channel
       → bbolt queue (atomic batched write, 1 fsync per batch)
       → publisher (drains, gRPC BatchPush 256/batch, 2 in-flight)
       → DCS gRPC IngestServer
       → ingest.Pipeline (LRU device + iface UUID caches, TTL 60s)
       → workers (buffer ≤ 5000 rows or 500 ms → COPY into staging → ON CONFLICT into metrics hypertable)
```

End-to-end p95 latency (with one publisher hop): typically <200 ms on healthy network.

### 2.2 Topology path

LLDP packets emitted by EDR's `TierTopology` (every 10 min, first fire ~10 s after device registration) carry `Kind="topology"`. DCS routes them out of the COPY path → `WriteTopologyLink` resolves src + dst device UUIDs and src + dst interface UUIDs (name-first, ifIndex fallback) → upserts into `topology_links`.

### 2.3 Trap path

Real devices: trap arrives on `:162`. UDP source = device IP.
Simulator: all traps come from `127.0.0.1`. The simulator carries the originating device's IP in the **community string** (`fwEDR/internal/snmp/trap.go` extracts and uses it as `SourceId`).
DCS trap routing tries hostname lookup, falls back to `DeviceIDByIP` across all four IP columns (management / primary / loopback / oob).

### 2.4 Interface address path

EDR's background enrichment loop walks `ipAdEntAddr` + `ipAdEntIfIndex` per target at a paced 200 ms cadence. Emits one `Kind="interface_address"` packet per (ifIndex, IP) tuple. DCS routes the kind to `UpsertInterfaceAddress` after resolving the interface UUID via ifIndex.

---

## 3. EDR internals

### 3.1 Discovery

| Stage | Trigger | What it does |
|---|---|---|
| Sweep | EDR start, only if `targets.json` missing or >24 h old, or `--rediscover` flag | Parallel SNMP `sysName`+`sysDescr` Get on every host in configured subnets. Concurrency 10, retries 0 (dead IPs are cheap), progress log every 50 IPs |
| Persist | After successful sweep | Writes `C:\ProgramData\fwdcim\edr\targets.json` atomically |
| Reload | Next EDR start (within 24 h) | Skips sweep entirely. Sim never sees the discovery burst. |
| Enrichment | 15 s after EDR starts polling | Per-target `ipAdEntAddr` + `ipAdEntIfIndex` walks at 200 ms pace. Updates `primary_ip`/`loopback_ip`/`oob_ip` and emits `interface_address` packets |
| Rediscovery | Background loop | Re-sweeps every `interval_hours` (default 1 h); 60 s when 0 targets known (dead-sim startup case). New devices added to running poller via `Poller.AddTarget` |

### 3.2 Polling tiers

| Tier | Default interval | What it polls | Skipped for gNMI targets? |
|---|---|---|---|
| Fast | 30 s | `sysUpTime` only (1 Get/device) | No — used as liveness probe |
| Medium | 120 s | `ifAdminStatus` / `ifOperStatus` / `ifHighSpeed` | Yes — gNMI Subscribe provides this |
| Slow | 300 s | counters, server HR/UCD, UPS, sensors | Yes — gNMI Subscribe provides this |
| Topology | 600 s | LLDP neighbor walks | No — LLDP is SNMP-only |

Initial fires are staggered: Fast jittered across `[0, FastInterval)`, Topology fires ~10 s after first Fast, Medium + Slow jittered within half their intervals. Prevents thundering herd.

### 3.3 gNMI subscriptions

For every target advertising port 57400 (`CapGNMI`):
- One long-lived `STREAM` subscription per device
- Counters / system / memory / temperature: `SAMPLE` mode at gnmi.poll_interval_ms (default 30 s)
- `oper-status` / `admin-status`: `ON_CHANGE`
- Stream opens paced across a 30 s window per device so the simulator's per-device gNMI servers aren't burst-accepted
- Graceful close: `context.AfterFunc(stream.CloseSend)` so server coroutines resolve cleanly on EDR shutdown
- Backoff reconnect on stream error (2 s → 60 s max)

### 3.4 Resilience knobs

| Mechanism | Config | Purpose |
|---|---|---|
| Per-target circuit breaker | `snmp.breaker_threshold`, `snmp.breaker_cooldown_ms` | Pause polling for one target after N consecutive failures |
| Global health monitor | `>30 %` breakers open → 60 s site-wide pause | Stops EDR from hammering a wedged simulator |
| Token-bucket rate limiter | `snmp.rate_limit_per_sec` (default 50) | Caps sustained SNMP req/sec |
| Concurrency semaphore | `snmp.max_concurrent` (default 5) | Caps parallel UDP sockets |
| Persistent target catalogue | `targets.json` + 24 h TTL | Eliminates sweep burst on EDR restart |
| Graceful SNMP shutdown | `context.AfterFunc(cl.Close)` | Closes UDP sockets cleanly on cancel |

---

## 4. DCS internals

### 4.1 Ingest pipeline

```
gRPC BatchPush
   → Pipeline.Submit(batch)            // accept-or-reject queue
   → channel (size cfg.ChannelSize)
   → N worker goroutines (cfg.Workers)
      for each pkt:
         dedup (optional, Redis SETNX for traps)
         normalize
         lookupOrRegister(pkt) → device UUID
         if kind == topology    → WriteTopologyLink
         if kind == interface_address → writeInterfaceAddress
         if kind in {trap,alarm,event} → WriteEvent (with DeviceIDByIP fallback)
         else (metric) → buffer MetricRow
      flush:
         when buffer ≥ BufferRows or every FlushIntervalMs
         CREATE TEMP TABLE _metrics_stage
         pgx.CopyFrom into staging
         INSERT … SELECT FROM staging … ON CONFLICT DO NOTHING
```

### 4.2 Caches

| Cache | Size | TTL | Reset |
|---|---|---|---|
| `devCache` (devKey → UUID) | 10 000 | `ingest.cache_ttl_seconds` (default 60 s) | `POST /admin/caches/flush` or restart |
| `ifCache` ((device,iface) → UUID) | 100 000 | same | same |

TTL exists so external DB truncates self-heal within 1 min without DCS restart. Admin endpoint clears immediately.

### 4.3 Admin REST endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/healthz` | GET | Liveness — always returns 200 if process is up |
| `/readyz` | GET | Pings Postgres pool; 200 if reachable, 503 otherwise |
| `/admin/caches/flush` | POST | Flushes devCache + ifCache; returns counts |

---

## 5. Schema reference

### 5.1 devices

```
id              UUID PK
identity        org_id / datacenter_id / floor_id / network_id / group_id
hostname        UNIQUE within identity tuple
device_type     router | switch | server | firewall | load_balancer | ups | pdu | floor_pdu | sensor
vendor          cisco | juniper | arista | apc | raritan | vertiv | eaton | f5 | paloalto | generic
management_ip   INET — operator-facing mgmt IP (always set when known)
primary_ip      INET — main operational IP (10.x in sim)
loopback_ip     INET — router/switch L3 loopback (= primary_ip for net devices)
oob_ip          INET — out-of-band mgmt (172.x in sim)
snmp_enabled    BOOLEAN | gnmi_enabled BOOLEAN
collector_agent IDR | EDR
last_seen_at    TIMESTAMPTZ
```
UNIQUE: `(org_id, datacenter_id, floor_id, network_id, group_id, hostname)`.

### 5.2 interfaces

```
id                  UUID PK
device_id           → devices.id ON DELETE CASCADE
interface_name      ifName / ifDescr (matches LLDP port-id naming)
interface_index     IF-MIB ifIndex (1-based)
speed_mbps          ifHighSpeed
admin_status        1=up 2=down 3=testing
operational_status  1=up 2=down 3=testing 4=unknown …
```
UNIQUE: `(device_id, interface_name)` AND `(device_id, interface_index)` partial.

### 5.3 interface_addresses

```
id              UUID PK
interface_id    → interfaces.id ON DELETE CASCADE
address         INET
address_family  ipv4 | ipv6
is_primary      BOOLEAN
vrf             TEXT (optional)
```
UNIQUE: `(interface_id, address)`.

### 5.4 metrics (TimescaleDB hypertable)

```
device_id           NOT NULL UUID
ts                  TIMESTAMPTZ
metric_name         TEXT
tag                 TEXT
value               DOUBLE PRECISION
attributes          JSONB
collector_agent     IDR | EDR
collector_protocol  AGENT_LOCAL | SNMP | GNMI | TRAP
interface_id        UUID (nullable, ON DELETE SET NULL)
```
UNIQUE: `(device_id, metric_name, tag, ts)` ON CONFLICT DO NOTHING.
Continuous aggregate `metrics_5m`.

### 5.5 events (TimescaleDB hypertable)

```
id              UUID
device_id       UUID (nullable when source can't be resolved)
ts              TIMESTAMPTZ
kind            trap | alarm | event
event_name      e.g. linkDown, linkUp, coldStart
severity        informational | minor | major | critical
trap_oid        TEXT
source_ip       INET
event_payload   JSONB (varbinds)
collector_agent IDR | EDR
```
1-year retention via `add_retention_policy`.

### 5.6 topology_links

```
id                 UUID PK
layer              network | power | cooling
src_device_id      → devices.id ON DELETE CASCADE
src_port_name      TEXT
src_interface_id   → interfaces.id ON DELETE SET NULL
dst_device_id      → devices.id ON DELETE CASCADE
dst_port_name      TEXT
dst_interface_id   → interfaces.id ON DELETE SET NULL
link_speed_mbps    INTEGER (nullable)
link_type          TEXT
protocol           lldp | cdp | gnmi-lldp | manual | power-feed
is_active          BOOLEAN
```
UNIQUE: `(layer, src_device_id, src_port_name, dst_device_id)`.
Backfill semantics: `src/dst_interface_id` are filled via `COALESCE(EXCLUDED, existing)` — NULLs heal on subsequent topology cycles once interfaces table catches up.

### 5.7 device_state

Per-device per-category JSONB snapshots (`lldp`, `mac_table`, `vlans`, `bgp`, `ospf`, `routing`, `power_chain`). UPSERT only — no history. **Producer not yet wired** (Phase 1 work).

### 5.8 Views

| View | Purpose |
|---|---|
| `topology_view` | Joins topology_links + devices + interfaces; resolves UUIDs to human-readable hostnames + port names with COALESCE fallback |
| `device_inventory` | Devices + interface counts (total / up / down) for fleet dashboards |

---

## 6. Packet schema (`proto/v1.TelemetryPacket`)

| Field | Type | Purpose |
|---|---|---|
| `id` | UUID | Packet identity (dedup) |
| `org_id` / `datacenter_id` / `floor_id` / `network_id` / `group_id` | string | Tenant hierarchy |
| `source_type` | string | `device` |
| `source_id` | string | Hostname (or device IP for traps in sim mode) |
| `reader_id` | string | `edr-…` / `idr-…` |
| `timestamp_ns` | int64 | Wall-clock at collection |
| `name` | string | `interface.bytes_received_hc`, `system.uptime_centiseconds`, … |
| `tag` | string | Interface name, sensor index, etc. |
| `value` | double | Numeric value |
| `meta` | map<string,string> | Untyped extras — copied to `metrics.attributes` JSONB after stripping transport fields |
| `kind` | string | `metric` (default) \| `topology` \| `interface_address` \| `trap` \| `alarm` \| `event` |
| `severity` | string | `informational` \| `minor` \| `major` \| `critical` |
| `signature` | bytes | Ed25519 over canonical bytes |
| `nonce` | uint64 | Monotonic per reader — replay protection |

The signer + nonce live in `fwedr/pkg/packet` / `fwidr/pkg/packet`. DCS verifies signatures lazily (off the hot path) — replay protection is enforced by the (device_id, metric_name, tag, ts) unique index.

---

## 7. Metric name catalogue (current)

### 7.1 system / server

| Name | Tag | Unit |
|---|---|---|
| `system.uptime_centiseconds` | — | centiseconds |
| `system.cpu_utilization_percent` | cpu index | % |
| `system.memory_used_bytes` / `system.memory_total_bytes` | — | bytes |
| `server.cpu_user_percent` / `server.cpu_system_percent` / `server.cpu_idle_percent` | — | % |
| `server.cpu_per_core_percent` | core index | % |
| `server.memory_total_kb` / `server.memory_available_kb` / `server.memory_cached_kb` / `server.memory_buffer_kb` | — | KB |
| `server.storage_size_kb` / `server.storage_used_kb` / `server.storage_available_kb` | mount/descr | KB |

### 7.2 interface

| Name | Tag | Notes |
|---|---|---|
| `interface.admin_status` / `interface.operational_status` | interface_name | 1=up 2=down |
| `interface.speed_mbps` | interface_name | Mbps |
| `interface.bytes_received` / `interface.bytes_sent` | interface_name | 32-bit counter |
| `interface.bytes_received_hc` / `interface.bytes_sent_hc` | interface_name | 64-bit counter (preferred) |
| `interface.packets_received_unicast` / `interface.packets_sent_unicast` | interface_name | counter |
| `interface.errors_received` / `interface.errors_sent` | interface_name | counter |
| `interface.discards_received` / `interface.discards_sent` | interface_name | counter |
| `interface.address` (Kind = `interface_address`) | IP literal | written to `interface_addresses` table |

### 7.3 environment / UPS / sensor

| Name | Tag |
|---|---|
| `environment.temperature_c` / `environment.humidity_percent` / `environment.dew_point_c` / `environment.airflow_cfm` | sensor_index |
| `environment.ups_battery_status` / `environment.ups_battery_voltage_v` / `environment.ups_charge_percent` / `environment.ups_minutes_remaining` / `environment.ups_seconds_on_battery` | — |
| `environment.ups_input_voltage_v` / `environment.ups_output_voltage_v` / `environment.ups_output_current_a` / `environment.ups_output_load_percent` | port index |

---

## 8. Configuration reference

### 8.1 `edr.yaml`

```yaml
identity:
  org_id: ...
  datacenter_id: ...
  floor_id: ... | network_id: ... | group_id: ...
  reader_id: ""              # auto-derived from hostname when empty

dcs:
  endpoint: localhost:9090
  tls: { insecure: true }    # dev only

queue:
  max_bytes: 536870912       # 512 MB bbolt cap

discovery:
  subnets: ["192.168.1.0/24", "192.168.2.0/24"]
  snmp_agent: "127.0.0.1"    # SNMPSim socket; empty = direct device IPs
  seed_ip:    "192.168.1.6"  # readiness probe before sweep
  interval_hours:    1       # background rediscovery
  target_cache_hours: 24     # skip sweep on startup if targets.json younger than this
  enrichment_pace_ms: 200    # between per-target ipAdEntAddr walks

snmp:
  community: "public"
  version: 2
  fast_interval_ms:     30000
  medium_interval_ms:   120000
  slow_interval_ms:     300000
  topology_interval_ms: 600000
  timeout_ms: 2000
  retries:    1
  trap_addr:  ":162"
  max_concurrent:     5
  rate_limit_per_sec: 50
  breaker_threshold:  3
  breaker_cooldown_ms: 30000

gnmi:
  port: 57400
  poll_interval_ms: 30000    # SAMPLE interval for counter streams
  tls: { insecure: true }

log:
  level: info
  format: json
```

CLI flags:
- `-config <path>` — config file path (default `edr.yaml`)
- `-rediscover` — force fresh sweep even if `targets.json` is recent

### 8.2 `dcs.yaml`

```yaml
grpc: { addr: ":9090" }
rest: { addr: ":8080" }

postgres:
  dsn: postgresql://fwdcim:fwdcim@127.0.0.1:5438/fwdcim?sslmode=disable

redis: { addr: "localhost:6379", password: "", db: 0 }

tls: { insecure: true }

ingest:
  workers:           4
  buffer_rows:       5000
  flush_interval_ms: 500
  channel_size:      65536
  dedup_metrics:     false   # ON CONFLICT in SQL suffices
  dedup_events:      true
  cache_ttl_seconds: 60      # device + interface UUID cache TTL

log: { level: info, format: json }
```

---

## 9. Operational lifecycle

### 9.1 First-ever EDR start
1. Open bbolt queue at `C:\ProgramData\fwdcim\edr\queue.db`
2. Connect to DCS publisher channel
3. `waitForReady` — probe seed IP for up to 60 s
4. If `targets.json` missing → run sweep (concurrency 10, retries 0)
5. Persist results to `targets.json`
6. Emit registration packets for every target (one synthetic `system.uptime_centiseconds=0`)
7. Spawn poller goroutines per target with jittered Fast/Topology/Medium/Slow tickers
8. For gNMI-capable targets, spawn paced long-lived Subscribe streams
9. Start global health monitor + SNMP trap receiver on `:162`
10. Background: 15 s after start, run paced enrichment loop (200 ms between targets) walking ipAdEntAddr + ipAdEntIfIndex

### 9.2 Subsequent EDR restart (within 24 h)
1. bbolt queue reopens (data preserved if queue file intact)
2. `targets.json` loaded — **sweep skipped**
3. Registration packets re-emitted (cheap, ON CONFLICT updates existing rows)
4. Polling resumes immediately
5. No enrichment burst (targets already have `primary_ip`/`oob_ip`/`loopback_ip`)

### 9.3 DCS restart
- EDR publisher reconnects with exponential backoff (1 s → 60 s)
- bbolt queue holds packets through the outage (durability spec target: zero loss across single-component failure)
- On reconnect, the publisher drains queued packets in batches

### 9.4 Simulator wedge
- Per-target circuit breakers trip after `breaker_threshold` consecutive failures → cooldown
- If >30 % of targets have open breakers, global health monitor pauses **all** SNMP polling for 60 s
- gNMI streams keep running (separate code path, unaffected by SNMP timeouts)
- When sim recovers, breakers reset on first probe success

### 9.5 External DB truncate
- DCS's LRU caches auto-expire any entry older than `cache_ttl_seconds` (default 60 s)
- For immediate recovery: `curl -X POST http://localhost:8080/admin/caches/flush`
- EDR does not need restart — it continues publishing, caches refresh from new DB rows on next packet

---

## 10. Known limitations (post-Phase 0)

| Limitation | Why | Where it's fixed |
|---|---|---|
| Traps from sim arrive as community-encoded device IP, not real source IP | Sim uses one UDP socket for all 404 devices | Production: real devices have unique source IPs. Sim: handled via trap community parsing |
| Some `topology_links.dst_interface_id` still NULL | LLDP only carries neighbor port-id (name), not ifIndex. Name normalization across vendors not yet implemented | Phase 3 vendor profiles |
| `device_state` table empty | No producer wired | Phase 1 — BGP / OSPF / MAC-table snapshot writers |
| Vendor logic in `metrics.go` switch statements | Hardcoded UCD/Raritan/Vertiv/APC paths | Phase 3 pluggable adapters |
| mTLS not enforced | Dev convenience | Phase 5 |
| No backpressure metrics surfaced to EDR | NDC layer not yet built | Phase 1 (NDC) + Phase 2 (NATS broker) |

See `REDESIGN.md` for the full phased plan.
