# Installation Guide — DCS & EDR

How to get **DCS** (ingest hub) and **EDR** (device collector) running.

- **DCS** stores telemetry in PostgreSQL/TimescaleDB + Redis, and forwards to the cloud Aggregator.
- **EDR** polls devices (SNMP/gNMI/BACnet/Redfish) and streams to DCS. EDR only talks to DCS.

See [USAGE.md](USAGE.md) for day-to-day operation and [ARCHITECTURE.md](ARCHITECTURE.md) for how it works.

---

## 1. Prerequisites

| Need | For |
|---|---|
| **Docker** (+ Compose) | Running Postgres + Redis |
| **Go 1.25.0+** | Only to *build* binaries — not needed on a host that just runs a prebuilt one |
| OS | **Linux, macOS, or Windows** — both services run natively on all three (Linux amd64, macOS amd64/arm64, Windows amd64) |

---

## 2. Start the data stores (Docker)

DCS needs PostgreSQL (TimescaleDB) and Redis. They run in Docker; the apps run as native binaries.

```bash
docker-compose -f docker-compose.data.yml up -d
```

This starts:
- **Postgres (TimescaleDB)** on `localhost:5438`, db/user/pass all `fwdcim`
- **Redis** on `localhost:6379`

Check they are healthy:

```bash
docker-compose -f docker-compose.data.yml ps
```

DCS creates its own schema on first connect — no manual SQL step.

---

## 3. Get the binaries

### Option A — use the prebuilt binary (recommended)

Prebuilt binaries are committed in the repo, one per platform:

| Platform | DCS | EDR |
|---|---|---|
| Linux amd64 | `fwDCS/build/linux-amd64/dcs` | `fwEDR/build/linux-amd64/edr` |
| macOS arm64 | `fwDCS/build/darwin-arm64/dcs` | `fwEDR/build/darwin-arm64/edr` |
| macOS amd64 | `fwDCS/build/darwin-amd64/dcs` | `fwEDR/build/darwin-amd64/edr` |
| Windows amd64 | `fwDCS/build/windows-amd64/dcs.exe` | `fwEDR/build/windows-amd64/edr.exe` |

Just pull the repo on the host and run the one for your platform. **Don't build on
a small VM** — a Go build can use ~1 GB RAM and may OOM-kill a running service.
Build on a workstation/CI and ship the binary.

### Option B — build from source

```bash
# DCS
cd fwDCS && ./build.sh -p linux      # windows | macos | all also valid

# EDR
cd fwEDR && ./build.sh -p linux
```

Windows: use `.\build.ps1 -Platform linux`. Output goes to `build/<os>-<arch>/`.

---

## 4. Two ways to run

There are two setups, differing only in which **config file** and **run script**
you use:

| | Local (dev workstation) | Production (server / VM) |
|---|---|---|
| Config file | `dcs.yaml` / `edr.yaml` | `dcs.prod.yaml` / `edr.prod.yaml` |
| Run script | `*_local.sh` / `*_local.ps1` | `*_prod.sh` / `*_prod.ps1` |
| Memory cap | none | `GOMEMLIMIT` set (DCS 384 MiB, EDR 192 MiB) |
| Upstream forward | optional | enabled, real `ingest_key` + endpoint |

In both cases the startup order is the same: **data stores → DCS (wait until
ready) → EDR**. Pick the matching section below.

---

## 5. Run locally (development)

For running everything on one workstation.

1. **Start data stores** (Section 2): `docker-compose -f docker-compose.data.yml up -d`
2. **Use the local config** — `fwDCS/dcs.yaml` and `fwEDR/edr.yaml` already point at
   `localhost`. EDR's `dcs.endpoint` should be `localhost:9090`. Set `topology_file`
   to your topology JSON, or leave empty to use subnet discovery.
3. **Run** (DCS first, then EDR):

   **Linux / macOS:**
   ```bash
   ./fwDCS/dcs_local.sh
   curl -s http://localhost:8080/readyz      # wait for {"status":"ready"}
   ./fwEDR/edr_local.sh
   ```

   **Windows (PowerShell):**
   ```powershell
   .\fwDCS\dcs_local.ps1
   curl http://localhost:8080/readyz          # wait for {"status":"ready"}
   .\fwEDR\edr_local.ps1
   ```

---

## 6. Run on production (server / VM)

1. **Pull the repo** on the server and use the **prebuilt binary** for the
   platform (don't build on a small VM — see Section 3).
2. **Start data stores** on the server: `docker-compose -f docker-compose.data.yml up -d`
3. **Edit the prod config** before first run:

   **DCS — `fwDCS/dcs.prod.yaml`:**
   ```yaml
   postgres:
     dsn: "postgresql://fwdcim:fwdcim@127.0.0.1:5438/fwdcim?sslmode=disable"
   redis:
     addr: "127.0.0.1:6379"
   aggregator:
     enabled:    true
     endpoint:   "https://fwdcim.faberwork.com/api/v1/ingest"
     ingest_key: "<your X-Ingest-Key>"
     org_id:     "faber"
     network_id: "net-prod"
     group_id:   "grp-core"
   ```

   **EDR — `fwEDR/edr.prod.yaml`:**
   ```yaml
   identity:
     org_id:     "faber"
     network_id: "net-prod"
     group_id:   "grp-core"
   dcs:
     endpoint: "localhost:9090"              # DCS gRPC target
   topology_file: "/path/to/topology.json"   # device list + locations
   snmp:
     community: "public"
     trap_addr: "0.0.0.0:162"
   ```
   > If `topology_file` is empty, EDR SNMP-sweeps `discovery.subnets` once at
   > startup. Other fields have sensible defaults — see the YAML comments.

4. **Run** (DCS first, then EDR):

   **Linux / macOS** (`.sh` scripts auto-detect OS + arch):
   ```bash
   ./fwDCS/dcs_prod.sh
   curl -s http://localhost:8080/readyz       # wait for {"status":"ready"}
   ./fwEDR/edr_prod.sh
   ```

   **Windows (PowerShell):**
   ```powershell
   .\fwDCS\dcs_prod.ps1
   curl http://localhost:8080/readyz           # wait for {"status":"ready"}
   .\fwEDR\edr_prod.ps1
   ```

To run a binary directly instead of via a script, point `-config` at the YAML,
e.g. `./fwDCS/build/linux-amd64/dcs -config dcs.prod.yaml`.

Stop (either setup) in reverse order: EDR → DCS → `docker-compose -f docker-compose.data.yml down`.

---

## 7. Verify

```bash
curl -s http://localhost:8080/healthz   # DCS alive  -> {"status":"ok"}
curl -s http://localhost:8080/readyz    # DCS + DB    -> {"status":"ready"}
```

DCS logs should show `postgres connected`, `redis connected`, `dcs listening`,
and `aggregator forwarder enabled`. EDR logs should show device discovery then
poll activity. Trip a device link and look for `event forwarded → aggregator` in
the DCS log (see [USAGE.md](USAGE.md)).

---

## 8. Ports

| Port | Proto | Who | Purpose |
|---|---|---|---|
| 9090 | TCP | DCS | gRPC ingest (EDR → DCS) |
| 8080 | TCP | DCS | Admin REST (health/ready) |
| 5438 | TCP | Postgres | Database |
| 6379 | TCP | Redis | Cache / dedup |
| 161 | UDP | Devices | SNMP polling |
| 162 | UDP | EDR | SNMP trap listener |
| 50051 / 57400 | TCP | gNMI | Telemetry (proxy / direct) |
| 47808 | UDP | Devices | BACnet (energy monitors) |
| 443 | TCP | Devices / Aggregator | Redfish BMC / upstream push |
