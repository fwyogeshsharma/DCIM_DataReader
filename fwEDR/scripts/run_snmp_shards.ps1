<#
run_snmp_shards.ps1 — launch N snmpsim responders that share the simulator's
existing dataset, one per shard port, so EDR can poll concurrently without
wedging a single responder.

This does NOT modify the simulator. It just runs extra copies of the
`snmpsim-command-responder` tool (already in the sim's venv) against the
existing datasets/snmp directory. Routing is by SNMP community (= device IP),
so every responder can answer for every device — no data partitioning.

Port scheme MUST match fwEDR edr.yaml `snmp.shards` / `snmp.shard_base_port`
and internal/snmp/client.go ShardPort():  base_port + i  for i in 0..N-1
    (e.g. 16100, 16101, 16102, 16103). Poller AND enrichment shard here;
    nothing uses 161, so the simulator GUI's responder won't conflict.

Usage (from anywhere):
    .\run_snmp_shards.ps1                  # 4 shards, base 16100 (matches edr.yaml)
    .\run_snmp_shards.ps1 -Shards 8
Stop: Ctrl-C in this window — all child responders are killed on exit.

PREREQS:
  - Stop the simulator GUI's SNMP responder first (it holds port 161).
  - The snmpsim index cache should already be built (the GUI builds it). First
    start is slower if not; subsequent starts are instant.
#>
param(
    [int]$Shards   = 4,
    [int]$BasePort = 16100,
    [string]$SimDir = "C:\Users\Faber\Desktop\fwDCIM\Datacenter_Network_Simulator"
)

$ErrorActionPreference = "Stop"

$responder = Join-Path $SimDir ".venv\Scripts\snmpsim-command-responder.exe"
$dataDir   = Join-Path $SimDir "datasets\snmp"

if (-not (Test-Path $responder)) { Write-Error "snmpsim responder not found: $responder"; exit 1 }
if (-not (Test-Path $dataDir))   { Write-Error "dataset dir not found: $dataDir"; exit 1 }
if ($Shards -lt 1) { Write-Error "Shards must be >= 1"; exit 1 }

# Build the shard port list exactly like EDR's ShardPort(): base + i.
$ports = @()
for ($i = 0; $i -lt $Shards; $i++) { $ports += ($BasePort + $i) }

Write-Host "Launching $Shards snmpsim responder(s)"
Write-Host "  data dir: $dataDir"
Write-Host "  ports:    $($ports -join ', ')"
Write-Host "  NOTE: stop the simulator GUI's SNMP (it holds port 161) before running this."
Write-Host ""

$procs = @()
$idx = 0
foreach ($p in $ports) {
    $shardArgs = @(
        "--data-dir=$dataDir",
        "--log-level=error",
        "--agent-udpv4-endpoint=0.0.0.0:$p"
    )
    $proc = Start-Process -FilePath $responder -ArgumentList $shardArgs -PassThru -NoNewWindow
    Write-Host ("  shard {0}: port {1,-6} PID {2}" -f $idx, $p, $proc.Id)
    $procs += $proc
    $idx++
}

Write-Host ""
Write-Host "All $($procs.Count) responder(s) started. Press Ctrl-C to stop them all."

try {
    while ($true) {
        Start-Sleep -Seconds 2
        foreach ($pr in $procs) {
            if ($pr.HasExited) {
                Write-Warning ("responder PID {0} exited (code {1}) — a port may already be in use" -f $pr.Id, $pr.ExitCode)
            }
        }
    }
} finally {
    Write-Host "`nStopping responders..."
    foreach ($pr in $procs) {
        if (-not $pr.HasExited) { Stop-Process -Id $pr.Id -Force -ErrorAction SilentlyContinue }
    }
}
