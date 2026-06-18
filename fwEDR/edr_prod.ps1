# Run EDR with prod config on Windows. Usage: .\edr_prod.ps1  (start AFTER dcs is up on :9090)
$ErrorActionPreference = 'Stop'
$DIR = $PSScriptRoot
$BIN = Join-Path $DIR 'build\windows-amd64\edr.exe'
# Soft heap cap: bounds RSS as the runtime approaches it. Raise if needed.
if (-not $env:GOMEMLIMIT) { $env:GOMEMLIMIT = '192MiB' }
& $BIN -config (Join-Path $DIR 'edr.prod.yaml') @args
