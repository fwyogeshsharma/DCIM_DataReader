# Architecture Overview — EDR (External Data Reader) & DCS (Data Center Store)

How the whole pipeline fits together, end to end — from the simulated devices all
the way up to the dashboard. See [INSTALLATION.md](INSTALLATION.md) to set it up
and [USAGE.md](USAGE.md) to run it.

---

## Contents

- [The picture](#the-picture)
- [EDR — what it does](#edr--what-it-does)
- [DCS — what it does](#dcs--what-it-does)
- [Resilience — outages don't lose data](#resilience--outages-dont-lose-data)
- [How data reaches the cloud](#how-data-reaches-the-cloud)
- [Key technologies](#key-technologies)

---

## The picture

The data flows up one path: the **Network Simulator** stands in for real datacenter
devices; **EDR (External Data Reader)** polls them; **DCS (Data Center Store)** stores
and forwards; the **Cloud Aggregator** merges many DCS feeds; the **DCIM UI** shows it.

![DCS &amp; EDR architecture — data path (solid blue, left to right) and command path (dashed amber, right to left).](diagrams/architecture.png)

Five parts, clear split:

- **Network Simulator** — simulates a full datacenter network (SNMP, gNMI, BACnet,
  Redfish, traps) plus a live topology view and its own web UI. EDR points at it
  exactly as it would at real devices, so the rest of the pipeline can't tell the
  difference.
- **EDR (External Data Reader)** — the collector. Talks to devices, talks to DCS.
  Nothing else.
- **DCS (Data Center Store)** — the hub. Owns the database, repeat-event skipping,
  topology, retention, and the upstream forwarder.
- **Cloud Aggregator** — merges the feeds from many DCS instances into one place,
  keeps its own Postgres + Redis, and serves a REST API.
- **DCIM UI** — the React dashboard. Reads aggregated data from the Cloud Aggregator;
  it never talks to EDR or DCS directly.

EDR **never** connects to Postgres or Redis directly. That isolation means you can
run many EDRs against one DCS, and the database credentials never leave the DCS host.
The same isolation holds upstream: the UI only ever reads from the Aggregator.

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
3. **Skips repeat events** via Redis.
4. **Computes topology** — a parent/child hierarchy (BFS over links) and fabric
   role classification (core/spine/leaf/...), refreshed on an interval.
5. **Forwards** incremental changes upstream to the cloud Aggregator.

---

## Resilience — outages don't lose data

The pipeline degrades gracefully when a dependency is unavailable, and recovers
on its own — no crash, no manual restart.

- **DCS unreachable** — EDR buffers to a local persistent queue (bbolt) and, after
  a short failure streak, **pauses collection** so the queue can't grow without
  bound. While paused it sends a lightweight probe to DCS; on the first success it
  **resumes** and drains the buffered backlog. Nothing is lost.
- **Devices/simulator unresponsive** — EDR's per-device circuit breakers open after
  repeated timeouts, and a site-wide health monitor pauses the heavy walks while a
  cheap liveness probe keeps watching. The moment a device answers again, polling
  resumes.
- **Process / VM restart** — both DCS and EDR run as systemd services
  (`Restart=always`, `enable`), so they come back automatically after a crash or
  reboot. See [INSTALLATION.md](INSTALLATION.md#7-run-as-a-service-systemd).

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
| Skips repeat events / fast lookups | Redis |
| Device protocols | SNMP v2c/v3, gNMI, BACnet/IP, Redfish (HTTP) |
| EDR local buffer | bbolt (embedded key-value) |
| Upstream | HTTPS JSON to the Aggregator |
