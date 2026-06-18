# Usage Guide — DCS & EDR

Day-to-day operation. For first-time setup see [INSTALLATION.md](INSTALLATION.md).

---

## Start / stop

Order matters: data stores → DCS → EDR. Use the `*_prod` scripts +
`docker-compose.data.yml` in production, or the `*_local` scripts +
`docker-compose.yml` locally.

**Production — Linux / macOS** (`.sh` scripts auto-detect OS + arch):

```bash
docker-compose -f docker-compose.data.yml up -d   # Postgres + Redis
./fwDCS/dcs_prod.sh                                # DCS
./fwEDR/edr_prod.sh                                # EDR
```

**Production — Windows** (PowerShell):

```powershell
docker-compose -f docker-compose.data.yml up -d
.\fwDCS\dcs_prod.ps1
.\fwEDR\edr_prod.ps1
```

**Local** — same order, but use the plain compose file and `*_local` scripts:

```bash
docker-compose up -d            # docker-compose.yml
./fwDCS/dcs_local.sh            # or .\fwDCS\dcs_local.ps1 on Windows
./fwEDR/edr_local.sh            # or .\fwEDR\edr_local.ps1
```

Stop in reverse: Ctrl-C EDR, then DCS, then bring the data stores down with the
matching file (`docker-compose down` locally, `docker-compose -f docker-compose.data.yml down` in prod).

Health checks:

```bash
curl -s http://localhost:8080/healthz   # {"status":"ok"}
curl -s http://localhost:8080/readyz    # {"status":"ready"}
```

---

## Logs

Set `log.format` in the YAML:
- `console` — human-readable (default).
- `json` — structured, for log aggregators.

`log.level`: `debug | info | warn | error`. Changing log settings is config-only —
just restart the service.

---

## How forwarding to the Aggregator behaves

DCS pushes incremental changes (devices, metrics, energy, topology, events) to the
cloud Aggregator. Three knobs in `aggregator:` control timing:

| Knob | Default | Meaning |
|---|---|---|
| `interval_ms` | 5000 | Poll cadence for tables that have new rows |
| `idle_interval_ms` | 60000 | A drained table (no new rows) backs off to this cadence to save CPU |
| `event_debounce_ms` | 300 | Any event forces an immediate push after this short coalesce window |

**Idle backoff:** once a table is fully forwarded and keeps returning nothing, DCS
re-checks it only once per `idle_interval_ms` instead of every tick. Fixed tables
(devices, topology) settle to ~1 check/minute, cutting idle CPU. New rows snap it
back to the fast cadence automatically. Raise `idle_interval_ms` if the DCS host
shows high idle CPU; set it ≤ `interval_ms` to disable backoff.

**Immediate event push:** every event DCS writes — any SNMP trap (link up/down,
etc.), alarm, or device rename, with no hardcoded list — triggers an immediate
push. A burst (e.g. a link-flap storm) is coalesced into one push by the debounce
window. Lower `event_debounce_ms` for faster UI reflection; raise it to batch more.

---

## Proof that events were forwarded

After a successful push, DCS logs one line per event:

```
aggregator forwarder push ok   {"network_id":"net-prod","event_triggered":true,"events":2,...}
event forwarded → aggregator   {"event_id":"...","kind":"trap","event_name":"linkDown","severity":"major","hostname":"DC1-ER1","ts":"..."}
```

- `event_triggered:true` = push was caused by an event (not the periodic tick).
- To audit forwarding: `grep "event forwarded" <dcs log>`.

---

## Common operations

**Force EDR to re-discover devices** (only when using subnet discovery, no topology file):

```bash
./fwEDR/edr_prod.sh --rediscover
```

**Flush DCS lookup caches** (e.g. after an external DB change):

```bash
curl -X POST http://localhost:8080/admin/caches/flush
```

**Query the database from your workstation** — Postgres listens only on the DCS
host, so tunnel to it. For a GCP VM:

```bash
gcloud compute ssh <instance> --zone=<zone> --project=<project> -- -N -L 5438:localhost:5438
# then:
psql "postgresql://fwdcim:fwdcim@127.0.0.1:5438/fwdcim?sslmode=disable"
```

Generic host: `ssh -N -L 5438:127.0.0.1:5438 user@host`.

**Adjust memory caps** — launch scripts set `GOMEMLIMIT` (DCS 384 MiB, EDR 192 MiB).
Override: `GOMEMLIMIT=512MiB ./fwDCS/dcs_prod.sh`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `/readyz` = `db_unreachable` | Postgres down / wrong DSN | Check Docker container + `postgres.dsn` |
| DCS errors on `redis` at startup | Redis down | `docker-compose -f docker-compose.data.yml up -d` |
| EDR runs but no data | DCS not up first, or wrong endpoint | Start DCS first; check `dcs.endpoint` |
| No traps received | Listener not bound / firewall | Use `trap_addr: 0.0.0.0:162` (not `[::]` on Windows); open UDP/162 |
| Events slow on UI | `event_debounce_ms` high or forwarding off | Lower it; confirm `aggregator.enabled: true` + check `event forwarded` logs |
| Upstream push 4xx | Bad `ingest_key` | Verify `aggregator.ingest_key` |
| High idle CPU on DCS | Forwarder polling too often | Raise `idle_interval_ms` |
| OOM during build | Building on a tight VM | Build elsewhere, ship the prebuilt binary |
