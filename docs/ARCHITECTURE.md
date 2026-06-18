# Architecture Overview — DCS & EDR

A simple overview of how the collector plane fits together. See
[INSTALLATION.md](INSTALLATION.md) to set it up and [USAGE.md](USAGE.md) to run it.

---

## The picture

```
   Devices (routers · switches · servers · PDUs · sensors · energy monitors)
        │
        │  SNMP poll (161) · SNMP traps (162) · gNMI · BACnet (47808) · Redfish (443)
        ▼
   ┌─────────┐    gRPC :9090     ┌─────────┐    HTTPS POST      ┌──────────────────┐
   │   EDR   │ ────────────────► │   DCS   │ ─────────────────► │ Cloud Aggregator │
   └─────────┘   (BatchPush)     └────┬────┘   /api/v1/ingest   └──────────────────┘
                                      │
                          ┌───────────┴───────────┐
                          ▼                        ▼
                   ┌────────────┐           ┌────────────┐
                   │  Postgres  │           │   Redis    │
                   │(TimescaleDB)│          │ dedup+cache│
                   └────────────┘           └────────────┘
```

Two services, clear split:

- **EDR** — the collector. Talks to devices, talks to DCS. Nothing else.
- **DCS** — the hub. Owns the database, dedup, topology, retention, and the
  upstream forwarder.

EDR **never** connects to Postgres or Redis directly. That isolation means you can
run many EDRs against one DCS, and the database credentials never leave the DCS host.

---

## EDR — what it does

1. **Discovers devices** — from a topology JSON (device IPs + datacenter/floor/rack
   locations + links), or by SNMP-sweeping configured subnets.
2. **Collects** from each device by the right protocol:
   - **SNMP** — inventory, interface state, sensors; **traps** (link up/down, etc.) on UDP 162.
   - **gNMI** — ongoing streaming telemetry (counters, status), via one aggregating proxy.
   - **BACnet/IP** — energy monitors (power, energy, per-circuit).
   - **Redfish** — server BMC: CPU/RAM/disk + hardware health.
3. **Batches and ships** everything to DCS over gRPC. A local persistent queue
   (bbolt) buffers data if DCS is briefly unreachable, so nothing is lost.

---

## DCS — what it does

1. **Ingests** gRPC batches from collectors through an async worker pipeline
   (buffered channel → workers → bulk DB writes).
2. **Stores** in PostgreSQL/TimescaleDB. Metrics/energy are time-series with
   automatic retention + rollups; devices/interfaces/topology/events are relational.
3. **Deduplicates** repeat events via Redis.
4. **Computes topology** — a parent/child hierarchy (BFS over links) and fabric
   role classification (core/spine/leaf/...), refreshed on an interval.
5. **Forwards** incremental changes upstream to the cloud Aggregator.

---

## How data reaches the cloud

DCS forwarding is **incremental and cursor-based**: each table remembers the
timestamp of the last row it successfully sent (per network), so only new/changed
rows go up.

- **Steady state** — polls each table every `interval_ms` (default 5s). Tables with
  nothing new back off to `idle_interval_ms` (default 60s) to save CPU.
- **Events** — any event (trap/alarm/rename) triggers an *immediate* push within
  `event_debounce_ms` (default 300ms), so operational changes reach the UI fast
  instead of waiting for the next tick. Bursts are coalesced into one push.

See [USAGE.md](USAGE.md) for tuning these knobs and reading the forward-proof logs.

---

## Key technologies

| Area | Tech |
|---|---|
| Language | Go 1.25 |
| Transport (collector → hub) | gRPC |
| Database | PostgreSQL + TimescaleDB |
| Dedup / cache | Redis |
| Device protocols | SNMP v2c/v3, gNMI, BACnet/IP, Redfish (HTTP) |
| EDR local buffer | bbolt (embedded key-value) |
| Upstream | HTTPS JSON to the Aggregator |
