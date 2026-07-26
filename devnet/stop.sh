#!/usr/bin/env bash
# Stop the local devnet started by devnet/start.sh.
set -uo pipefail

cd "$(dirname "$0")/.."

for i in 1 2 3 4; do
  pidfile="devnet/v$i/node.pid"
  [ -f "$pidfile" ] || continue
  pid="$(cat "$pidfile")"
  if kill "$pid" 2>/dev/null; then
    echo "v$i  stopped pid $pid"
  fi
  rm -f "$pidfile"
done

# Belt and suspenders: nothing left running from this repo's node binary.
pkill -f 'target/debug/node run' 2>/dev/null || true
