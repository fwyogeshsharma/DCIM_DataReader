# fwDCIM — Setup & Testing Guide

Concise dev-setup for the IDR / EDR / DCS stack on Windows.
See `ARCHITECTURE.md` for the design reference.

---

## 1. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Go | 1.25+ | https://go.dev/dl/ |
| Docker Desktop | any | hosts TimescaleDB + Redis |
| Python | 3.11+ | sim only |
| PowerShell | 5.1+ | shell of choice on Windows |

---

## 2. Start the database layer

```powershell
cd C:\Users\Faber\Desktop\fwDCIM
docker compose up -d
```

The `docker-compose.yml` brings up:
- `fwdcim-postgres-1` — TimescaleDB on `127.0.0.1:5438` (user `fwdcim`, password `fwdcim`, db `fwdcim`)
- `fwdcim-redis-1` — Redis on `127.0.0.1:6379`

Schema migrations apply automatically when DCS first connects. Verify DB reachable:

```powershell
docker exec fwdcim-postgres-1 psql -U fwdcim -d fwdcim -c "\dt"
```

---

## 3. Build the binaries

```powershell
cd C:\Users\Faber\Desktop\fwDCIM\fwDCS ; go build -o build\windows-amd64\dcs.exe .
cd C:\Users\Faber\Desktop\fwDCIM\fwEDR ; go build -o build\windows-amd64\edr.exe .
cd C:\Users\Faber\Desktop\fwDCIM\fwIDR ; go build -o build\windows-amd64\idr.exe .
```

Output sizes (reference): dcs.exe ~33 MB, edr.exe ~20 MB, idr.exe ~15 MB.

---

## 4. Start the simulator (optional, dev only)

```powershell
cd C:\Users\Faber\Desktop\fwDCIM\Datacenter_Network_Simulator
.\.venv\Scripts\Activate.ps1
python main.py
```

In the simulator UI: start SNMP + start gNMI + start the trap engine. Devices bind on 192.168.1.0/24 and 192.168.2.0/24 virtual IPs; SNMPSim listens on `127.0.0.1:161` and routes by community-string-as-device-IP. gNMI per-device servers bind on the device IP at port 57400.

---

## 5. Start DCS

```powershell
cd C:\Users\Faber\Desktop\fwDCIM\fwDCS\build\windows-amd64
.\dcs.exe -config dcs.yaml
```

Expect log lines:
```
postgres connected     dsn=postgresql://fwdcim:fwdcim@127.0.0.1:5438/fwdcim
redis connected        addr=localhost:6379
ingest pipeline started workers=4
dcs listening          grpc=:9090
dcs admin REST listening addr=:8080
```

Verify:
```powershell
Invoke-RestMethod http://localhost:8080/healthz   # {"status":"ok"}
Invoke-RestMethod http://localhost:8080/readyz    # {"status":"ready"}
```

---

## 6. Start EDR

```powershell
cd C:\Users\Faber\Desktop\fwDCIM\fwEDR\build\windows-amd64
.\edr.exe -config edr.yaml
```

Expect log lines:
```
queue opened           path=C:\ProgramData\fwdcim\edr\queue.db
publisher connected to DCS  endpoint=localhost:9090
waiting for SNMPSim to become ready  agent=127.0.0.1 seed=192.168.1.6
SNMPSim ready
discovery sweep started   ips=508
sweep progress            probed=50 total=508 found=49
…
discovery sweep complete  found=404
persisted targets to disk path=C:\ProgramData\fwdcim\edr\targets.json count=404
discovery-time registration packets emitted count=404
snmp trap receiver listening addr=:162
edr started               reader_id=edr-1.0.0-<host> targets=404
background enrichment started targets=404 pace=200ms
```

CLI flags:
- `-config <path>` — config file path (default `edr.yaml`)
- `-rediscover` — force fresh sweep even if `targets.json` is recent

---

## 7. (Optional) Start IDR

```powershell
cd C:\Users\Faber\Desktop\fwDCIM\fwIDR\build\windows-amd64
.\idr.exe -config idr.yaml
```

Collects host metrics (CPU, mem, disk, net, OS, temp) and pushes to DCS over gRPC. Use only if you want to compare IDR-collected localhost data alongside EDR remote data.

---

## 8. Smoke test

After ~5 minutes of EDR runtime against a healthy sim:

```powershell
docker exec fwdcim-postgres-1 psql -U fwdcim -d fwdcim -c @"
SELECT 'devices' AS table, COUNT(*) FROM devices
UNION ALL SELECT 'interfaces',          COUNT(*) FROM interfaces
UNION ALL SELECT 'interface_addresses', COUNT(*) FROM interface_addresses
UNION ALL SELECT 'metrics',             COUNT(*) FROM metrics
UNION ALL SELECT 'topology_links',      COUNT(*) FROM topology_links
UNION ALL SELECT 'events',              COUNT(*) FROM events;
"@
```

Healthy targets after ~5 min on a 404-device sim:
- devices ~ 404
- interfaces ~ 800 (varies — only switches/routers + servers with TierMedium fired)
- interface_addresses ~ 100–600 (depends on enrichment progress)
- metrics ~ 5000+ (grows steadily)
- topology_links ~ 200–400 (LLDP neighbors)
- events 0+ (depends on whether you triggered traps)

To force a trap, use the simulator UI: select a device → toggle interface admin state → trap fires to EDR on `:162` → DCS routes to events table.

---

## 9. Common admin actions

### Wipe DB (development reset)

```powershell
docker exec fwdcim-postgres-1 psql -U fwdcim -d fwdcim -c "TRUNCATE devices, interfaces, interface_addresses, metrics, topology_links, events, device_state RESTART IDENTITY CASCADE;"
```

Then immediately:
```powershell
Invoke-RestMethod -Method Post http://localhost:8080/admin/caches/flush
```
DCS LRU caches are cleared instantly. No DCS restart required. (Without the flush, caches auto-expire within `ingest.cache_ttl_seconds` — 60 s default.)

### Force EDR fresh sweep

```powershell
Remove-Item C:\ProgramData\fwdcim\edr\targets.json -Force
# Or run with the explicit flag (sweep runs unconditionally):
.\edr.exe -config edr.yaml -rediscover
```

### Wipe EDR persistent queue

```powershell
# Only when EDR is stopped — bbolt holds an exclusive lock while running.
Remove-Item C:\ProgramData\fwdcim\edr\queue.db -Force
```

### Verify EDR / DCS state

```powershell
Get-CimInstance Win32_Process -Filter "Name='edr.exe' OR Name='dcs.exe' OR Name='idr.exe'" |
  Select-Object Name, ProcessId, CreationDate

netstat -ano | findstr "9090 8080 162"
```

---

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `queue: open queue.db: timeout` | Another EDR is still alive holding the bbolt lock | `Get-CimInstance Win32_Process -Filter "Name='edr.exe'"` → `Stop-Process -Id <PID> -Force` |
| `SNMPSim not ready — retrying in 15s` repeatedly | Sim wedged or not started | Restart the simulator |
| `metrics batch: upsert ... violates foreign key constraint "1_1_metrics_interface_id_fkey"` | DCS LRU cache stale after external truncate | `POST /admin/caches/flush` or wait 60 s for TTL expiry |
| EDR runs but DB stays empty | DCS unreachable | check `:9090` listening, check EDR log for `publisher connected to DCS` |
| Sweep takes minutes with no progress logs | Dead-IP grind (5–6 s per dead IP × concurrency 10) | Wait — progress log every 50 IPs. With 0 retries this is bounded |
| Trap arrives but `events.device_id = NULL` | Trap source IP couldn't be matched to any device | Check trap community string (= device IP in sim mode); verify the device was discovered (in `devices` table) |
| `topology_links.src/dst_interface_id` NULL | Interfaces row not yet upserted, or name/ifIndex mismatch | Wait for next topology cycle (10 min). dst-side NULLs are expected for now (Phase 3 fix) |

---

## 11. Useful database queries

```sql
-- Device inventory with interface stats
SELECT hostname, device_type, vendor, management_ip, interface_count, interfaces_up, interfaces_down
FROM device_inventory
ORDER BY hostname
LIMIT 30;

-- Latest metrics per device
SELECT m.metric_name, m.tag, m.value, m.ts
FROM metrics m JOIN devices d ON d.id = m.device_id
WHERE d.hostname = 'DC1-CORE1'
ORDER BY m.ts DESC LIMIT 20;

-- Topology view (joined, human-readable)
SELECT src_hostname, src_port_name, dst_hostname, dst_port_name, protocol, updated_at
FROM topology_view
ORDER BY updated_at DESC LIMIT 30;

-- Reverse IP → device
SELECT d.hostname, i.interface_name, ia.address
FROM interface_addresses ia
JOIN interfaces i ON i.id = ia.interface_id
JOIN devices d    ON d.id = i.device_id
WHERE ia.address = '10.5.1.4';

-- Recent traps
SELECT ts, event_name, severity, source_ip, d.hostname
FROM events e LEFT JOIN devices d ON d.id = e.device_id
WHERE kind = 'trap'
ORDER BY ts DESC LIMIT 20;
```

---

## 12. File / directory layout

```
fwDCIM/
├── fwIDR/                  # host agent
├── fwEDR/                  # network reader
├── fwDCS/                  # site store + ingest
│   ├── migrations/         # single-file schema (001_init.sql)
│   ├── internal/
│   ├── pkg/config/
│   └── proto/v1/
├── Datacenter_Network_Simulator/   # off-limits — read-only
├── image/                  # screenshots / diagrams
├── docker-compose.yml      # Postgres + Redis
├── ARCHITECTURE.md         # design reference (this dir)
├── SETUP_AND_TESTING.md    # this file
├── REDESIGN.md             # phased plan toward fwDCIM spec
├── fwDCIM 1.pdf            # master spec
├── Supported Device Catalogue 1.pdf
└── gNMI_Simulator_Reference.docx
```

State directories created at runtime:
- `C:\ProgramData\fwdcim\edr\queue.db`     — EDR durable packet queue
- `C:\ProgramData\fwdcim\edr\targets.json` — EDR discovered-target catalogue
