#!/usr/bin/env bash
# Run DCS with local config. Usage: ./dcs_local.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/build/linux-amd64/dcs"
[ -x "$BIN" ] || chmod +x "$BIN"
exec "$BIN" -config "$DIR/dcs.yaml" "$@"
