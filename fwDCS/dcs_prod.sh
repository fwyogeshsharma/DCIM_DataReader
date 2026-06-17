#!/usr/bin/env bash
# Run DCS with prod config. Usage: ./dcs_prod.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/build/linux-amd64/dcs"
[ -x "$BIN" ] || chmod +x "$BIN"
# Soft heap cap: the Go runtime GCs harder as it approaches this, bounding RSS
# (forwarder bursts are the main allocator). Raise if the box has spare RAM.
export GOMEMLIMIT="${GOMEMLIMIT:-384MiB}"
exec "$BIN" -config "$DIR/dcs.prod.yaml" "$@"
