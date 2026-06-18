# Run EDR with local config on Windows. Usage: .\edr_local.ps1  (start AFTER dcs is up on :9090)
$ErrorActionPreference = 'Stop'
$DIR = $PSScriptRoot
$BIN = Join-Path $DIR 'build\windows-amd64\edr.exe'
& $BIN -config (Join-Path $DIR 'edr.yaml') @args
