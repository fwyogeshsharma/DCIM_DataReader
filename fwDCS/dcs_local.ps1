# Run DCS with local config on Windows. Usage: .\dcs_local.ps1
$ErrorActionPreference = 'Stop'
$DIR = $PSScriptRoot
$BIN = Join-Path $DIR 'build\windows-amd64\dcs.exe'
& $BIN -config (Join-Path $DIR 'dcs.yaml') @args
