<#
tune_windows_sockets.ps1 — one-time Windows network tuning to permanently remove
WSAENOBUFS (10055: "bind: ... lacked sufficient buffer space") when EDR polls
many SNMP devices from one host.

This is BELT-AND-SUSPENDERS. The primary fix is in EDR itself (sockets are now
reused per target and are NOT closed/reopened on a poll timeout, so there is no
open/close churn). This script just gives Windows extra socket headroom so even
a burst of connects at startup can never exhaust the ephemeral-port pool.

Microsoft guidance for WSAENOBUFS / port exhaustion:
  - widen the dynamic (ephemeral) port range
  - lower TcpTimedWaitDelay so closed ports are reclaimed in 30s not ~120s
  - raise MaxUserPort

Run ONCE, in an ADMIN PowerShell. The netsh changes apply immediately; the two
registry values take effect after a reboot. Re-running is safe (idempotent).

    Right-click PowerShell → "Run as administrator", then:
    powershell -ExecutionPolicy Bypass -File .\tune_windows_sockets.ps1
#>

$ErrorActionPreference = "Stop"

# admin check
$me = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $me.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {
    Write-Error "Run this in an ADMIN PowerShell (right-click → Run as administrator)."
    exit 1
}

Write-Host "== Widening dynamic port range (immediate, no reboot) =="
# 55000 ephemeral ports starting at 10000 → up to ~64999. Covers any startup burst.
netsh int ipv4 set dynamicport udp start=10000 num=55000 | Out-Null
netsh int ipv4 set dynamicport tcp start=10000 num=55000 | Out-Null
netsh int ipv4 show dynamicport udp

$params = "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"
Write-Host "`n== Registry (takes effect after reboot) =="
# Reclaim closed ports after 30s instead of the ~120s default → ports recycle fast.
New-ItemProperty -Path $params -Name "TcpTimedWaitDelay" -Value 30    -PropertyType DWord -Force | Out-Null
# Allow the full high range as user ports.
New-ItemProperty -Path $params -Name "MaxUserPort"       -Value 65534 -PropertyType DWord -Force | Out-Null
Write-Host "  TcpTimedWaitDelay = 30"
Write-Host "  MaxUserPort       = 65534"

Write-Host "`nDone. netsh changes are live now; reboot to apply the registry values."
Write-Host "After this + the EDR socket-reuse fix, WSAENOBUFS should not recur."
