#Requires -Version 5.1
<#
.SYNOPSIS  Cross-compile DCS for all supported platforms.
.EXAMPLE
    .\build.ps1                    # all platforms
    .\build.ps1 -Platform windows  # Windows only
    .\build.ps1 -Platform linux
    .\build.ps1 -Platform macos
    .\build.ps1 -Clean
#>
param(
    [ValidateSet("all","windows","linux","macos")]
    [string] $Platform  = "all",
    [string] $OutputDir = "build",
    [switch] $Clean
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Clean) {
    if (Test-Path $OutputDir) { Remove-Item $OutputDir -Recurse -Force }
    Write-Host "$OutputDir/ removed." -ForegroundColor Green
    exit 0
}

Write-Host "================================" -ForegroundColor Cyan
Write-Host "DCS - Build Script"              -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Go not found. Install from https://go.dev/dl/" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] $(go version)" -ForegroundColor Green
Write-Host ""

$Version   = "1.0.0"
$BuildTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$LDFlags   = "-s -w -X main.Version=$Version -X 'main.BuildTime=$BuildTime'"

function Build-Platform {
    param([string]$OS, [string]$Arch, [string]$Ext = "")

    $platformDir = "$OutputDir\$OS-$Arch"
    $binary      = "dcs$Ext"
    $outputPath  = "$platformDir\$binary"

    Write-Host "Building for $OS/$Arch..." -ForegroundColor Yellow
    if (-not (Test-Path $platformDir)) { New-Item -ItemType Directory -Force -Path $platformDir | Out-Null }

    $env:GOOS        = $OS
    $env:GOARCH      = $Arch
    $env:CGO_ENABLED = "0"

    try {
        $out = & go build -trimpath -ldflags $LDFlags -o $outputPath . 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  [FAILED]" -ForegroundColor Red
            Write-Host $out -ForegroundColor Red
            return $false
        }
        $sizeMB = [math]::Round((Get-Item $outputPath).Length / 1MB, 1)
        Write-Host "  [OK] $binary ($sizeMB MB)" -ForegroundColor Green
        Copy-Item "dcs.yaml" "$platformDir\dcs.yaml" -Force
        Write-Host "  [OK] Copied dcs.yaml" -ForegroundColor Green
        return $true
    } finally {
        Remove-Item Env:\GOOS, Env:\GOARCH, Env:\CGO_ENABLED -ErrorAction SilentlyContinue
    }
    Write-Host ""
}

if (-not (Test-Path $OutputDir)) { New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null }

$ok = $true
switch ($Platform) {
    "windows" { $ok = Build-Platform "windows" "amd64" ".exe" }
    "linux"   { $ok = Build-Platform "linux"   "amd64" ""     }
    "macos"   {
        $ok = (Build-Platform "darwin" "amd64" "") -and $ok
        $ok = (Build-Platform "darwin" "arm64" "") -and $ok
    }
    "all" {
        $ok = (Build-Platform "windows" "amd64" ".exe") -and $ok
        $ok = (Build-Platform "linux"   "amd64" "")     -and $ok
        $ok = (Build-Platform "darwin"  "amd64" "")     -and $ok
        $ok = (Build-Platform "darwin"  "arm64" "")     -and $ok
    }
}

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
if ($ok) {
    Write-Host "Build complete! Output: $OutputDir\" -ForegroundColor Green
    Write-Host ""
    Write-Host "Run DCS (applies DB migrations automatically on start):" -ForegroundColor Yellow
    if ($Platform -eq "all" -or $Platform -eq "windows") {
        Write-Host "  Windows:  .\$OutputDir\windows-amd64\dcs.exe -config dcs.yaml" -ForegroundColor Cyan
    }
    if ($Platform -eq "all" -or $Platform -eq "linux") {
        Write-Host "  Linux:    ./$OutputDir/linux-amd64/dcs -config dcs.yaml" -ForegroundColor Cyan
    }
    if ($Platform -eq "all" -or $Platform -eq "macos") {
        Write-Host "  macOS:    ./$OutputDir/darwin-arm64/dcs -config dcs.yaml" -ForegroundColor Cyan
    }
    exit 0
} else {
    Write-Host "Some builds failed." -ForegroundColor Red
    exit 1
}
