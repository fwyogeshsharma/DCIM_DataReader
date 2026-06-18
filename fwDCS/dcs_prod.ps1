# Run DCS with prod config on Windows. Usage: .\dcs_prod.ps1
$ErrorActionPreference = 'Stop'
$DIR = $PSScriptRoot
$BIN = Join-Path $DIR 'build\windows-amd64\dcs.exe'
# Soft heap cap: the Go runtime GCs harder as it approaches this, bounding RSS.
if (-not $env:GOMEMLIMIT) { $env:GOMEMLIMIT = '384MiB' }
& $BIN -config (Join-Path $DIR 'dcs.prod.yaml') @args
