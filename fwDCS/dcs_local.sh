#!/usr/bin/env bash
# Run DCS with local config. Usage: ./dcs_local.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Pick the binary matching this host (linux/macos, amd64/arm64).
case "$(uname -s)" in Darwin) OS=darwin ;; *) OS=linux ;; esac
case "$(uname -m)" in arm64|aarch64) ARCH=arm64 ;; *) ARCH=amd64 ;; esac
BIN="$DIR/build/$OS-$ARCH/dcs"
[ -x "$BIN" ] || chmod +x "$BIN"
exec "$BIN" -config "$DIR/dcs.yaml" "$@"
