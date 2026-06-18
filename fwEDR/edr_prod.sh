#!/usr/bin/env bash
# Run EDR with prod config. Usage: ./edr_prod.sh   (start AFTER dcs is up on :9090)
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Pick the binary matching this host (linux/macos, amd64/arm64).
case "$(uname -s)" in Darwin) OS=darwin ;; *) OS=linux ;; esac
case "$(uname -m)" in arm64|aarch64) ARCH=arm64 ;; *) ARCH=amd64 ;; esac
BIN="$DIR/build/$OS-$ARCH/edr"
[ -x "$BIN" ] || chmod +x "$BIN"
# Soft heap cap: bounds RSS as the runtime approaches it. EDR's steady heap is
# small (~50 MB); 192 MiB leaves headroom for poll bursts. Raise if needed.
export GOMEMLIMIT="${GOMEMLIMIT:-192MiB}"
exec "$BIN" -config "$DIR/edr.prod.yaml" "$@"
