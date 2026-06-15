#!/usr/bin/env bash
# Run EDR with local config. Usage: ./edr_local.sh   (start AFTER dcs is up on :9090)
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/build/linux-amd64/edr"
[ -x "$BIN" ] || chmod +x "$BIN"
exec "$BIN" -config "$DIR/edr.yaml" "$@"
