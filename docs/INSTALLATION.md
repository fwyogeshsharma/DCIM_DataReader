# Installation Guide — DCS (Data Center Store) & EDR (External Data Reader)

How to get **DCS (Data Center Store)** (ingest hub) and **EDR (External Data Reader)**
(device collector) running.

- **DCS** stores telemetry in PostgreSQL/TimescaleDB + Redis, and forwards to the cloud Aggregator.
- **EDR** polls devices (SNMP/gNMI/BACnet/Redfish) and streams to DCS. EDR only talks to DCS.

See [USAGE.md](USAGE.md) for day-to-day operation and [ARCHITECTURE.md](ARCHITECTURE.md) for how it works.

---

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Data stores (Docker)](#2-data-stores-docker)
3. [Get the binaries](#3-get-the-binaries)
4. [Two ways to run](#4-two-ways-to-run)
5. [Run locally (development)](#5-run-locally-development)
6. [Run on production (server / VM)](#6-run-on-production-server--vm)
7. [Run as a service (systemd)](#7-run-as-a-service-systemd)
8. [Verify](#8-verify)
9. [Ports](#9-ports)

---

## 1. Prerequisites

| Need | For |
|---|---|
| **Docker** (+ Compose) | Running Postgres + Redis |
| **Go 1.25.0+** | Only to *build* binaries — not needed on a host that just runs a prebuilt one |
| OS | **Linux, macOS, or Windows** — both services run natively on all three (Linux amd64, macOS amd64/arm64, Windows amd64) |

---

## 2. Data stores (Docker)

DCS needs PostgreSQL (TimescaleDB) and Redis. They run in Docker; the apps run as
native binaries. Both compose files start the same two services:

- **Postgres (TimescaleDB)** on `localhost:5438`, db/user/pass all `fwdcim`
- **Redis** on `localhost:6379`

There are two compose files — **use the one for your setup** (the exact command is
in each run section below):

| File | Use | Difference |
|---|---|---|
| `docker-compose.yml` | **Local** | plain — stops with your session |
| `docker-compose.data.yml` | **Production** | `restart: unless-stopped` + healthchecks so the stores survive reboots |

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

1. **Start data stores** with the plain compose file:
   ```bash
   docker-compose up -d          # uses docker-compose.yml
   docker-compose ps             # check they are running
   ```
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
2. **Start data stores** on the server with the production compose file
   (auto-restarts on reboot):
   ```bash
   docker-compose -f docker-compose.data.yml up -d
   docker-compose -f docker-compose.data.yml ps    # check they are healthy
   ```
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

**Stop** (reverse order: EDR → DCS → data stores). Use the compose file matching
your setup:
- Local: `docker-compose down`
- Production: `docker-compose -f docker-compose.data.yml down`

---

## 7. Run as a service (systemd)

The run scripts (Section 6) stop when your SSH session closes and do **not** come
back after a crash or reboot. On a Linux server, run DCS and EDR as **systemd
services** instead: they start on boot (`enable`) and respawn on crash
(`Restart=always`). Each unit simply calls the same `*_prod.sh` launcher, so the
binary, `GOMEMLIMIT`, and prod config are unchanged.

Unit files ship in the repo: `fwDCS/deploy/dcs.service`, `fwEDR/deploy/edr.service`.

**1. Install (run once).** Point the variables at the repo dirs on the server:

```bash
DCS_DIR=/path/to/fwDCS          # holds dcs_prod.sh, build/, dcs.prod.yaml
EDR_DIR=/path/to/fwEDR          # holds edr_prod.sh, build/, edr.prod.yaml

chmod +x "$DCS_DIR/dcs_prod.sh" "$EDR_DIR/edr_prod.sh"

# Fill the __INSTALL_DIR__ placeholder and install the units:
sed "s#__INSTALL_DIR__#$DCS_DIR#g" "$DCS_DIR/deploy/dcs.service" | sudo tee /etc/systemd/system/dcs.service
sed "s#__INSTALL_DIR__#$EDR_DIR#g" "$EDR_DIR/deploy/edr.service" | sudo tee /etc/systemd/system/edr.service
```

**2. Stop any manual copies** so you don't run two:

```bash
pkill -f dcs_prod.sh; pkill -f edr_prod.sh; pkill -x dcs; pkill -x edr
```

**3. Enable + start** (`enable` = auto-start on boot, `--now` = also start now):

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dcs.service     # start DCS first
sudo systemctl enable --now edr.service     # EDR follows (ordered After=dcs in the unit)
```

**4. Check and tail logs:**

```bash
systemctl status dcs edr                    # expect: active (running), enabled
journalctl -u dcs -f                        # live DCS log (Ctrl-C to stop)
journalctl -u edr -f
```

**5. Prove auto-restart:**

```bash
sudo systemctl kill -s SIGKILL dcs          # ~3s later: running again
sudo reboot                                 # after boot: both already running
```

Day-to-day after install:

| Action | Command |
|---|---|
| Start / stop | `sudo systemctl start dcs` / `stop dcs` |
| Restart after a new binary | `sudo systemctl restart dcs` |
| Status / logs | `systemctl status dcs` · `journalctl -u dcs -e` |

> **Ordering & recovery:** EDR's unit is `After=dcs.service`. If DCS is briefly
> down, EDR pauses collection and resumes automatically (see
> [ARCHITECTURE.md](ARCHITECTURE.md#resilience--outages-dont-lose-data)) — no need
> to restart it by hand.
>
> **Resource caps (optional):** `Nice` / `CPUQuota` / `MemoryMax` lines are present
> but commented in each unit. Uncomment, then `sudo systemctl daemon-reload && sudo
> systemctl restart dcs edr`.

---

## 8. Verify

```bash
curl -s http://localhost:8080/healthz   # DCS alive  -> {"status":"ok"}
curl -s http://localhost:8080/readyz    # DCS + DB    -> {"status":"ready"}
```

DCS logs should show `postgres connected`, `redis connected`, `dcs listening`,
and `aggregator forwarder enabled`. EDR logs should show device discovery then
poll activity. Trip a device link and look for `event forwarded → aggregator` in
the DCS log (see [USAGE.md](USAGE.md)).

---

## 9. Ports

| Port | Proto | Who | Purpose |
|---|---|---|---|
| 9090 | TCP | DCS | gRPC ingest (EDR → DCS) |
| 8080 | TCP | DCS | Admin REST (health/ready) |
| 5438 | TCP | Postgres | Database |
| 6379 | TCP | Redis | Fast lookups / skip repeat events |
| 161 | UDP | Devices | SNMP polling |
| 162 | UDP | EDR | SNMP trap listener |
| 50051 / 57400 | TCP | gNMI | Telemetry (proxy / direct) |
| 47808 | UDP | Devices | BACnet (energy monitors) |
| 443 | TCP | Devices / Aggregator | Redfish BMC / upstream push |
