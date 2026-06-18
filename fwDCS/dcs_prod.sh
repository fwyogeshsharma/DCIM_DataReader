#!/usr/bin/env bash
# Run DCS with prod config. Usage: ./dcs_prod.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Pick the binary matching this host (linux/macos, amd64/arm64).
case "$(uname -s)" in Darwin) OS=darwin ;; *) OS=linux ;; esac
case "$(uname -m)" in arm64|aarch64) ARCH=arm64 ;; *) ARCH=amd64 ;; esac
BIN="$DIR/build/$OS-$ARCH/dcs"
[ -x "$BIN" ] || chmod +x "$BIN"
# Soft heap cap: the Go runtime GCs harder as it approaches this, bounding RSS
# (forwarder bursts are the main allocator). Raise if the box has spare RAM.
export GOMEMLIMIT="${GOMEMLIMIT:-384MiB}"
exec "$BIN" -config "$DIR/dcs.prod.yaml" "$@"
