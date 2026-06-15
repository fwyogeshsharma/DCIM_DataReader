#!/usr/bin/env bash
# Run DCS with prod config. Usage: ./dcs_prod.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/build/linux-amd64/dcs"
[ -x "$BIN" ] || chmod +x "$BIN"
exec "$BIN" -config "$DIR/dcs.prod.yaml" "$@"
