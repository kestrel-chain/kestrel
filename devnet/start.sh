#!/usr/bin/env bash
# Launch the 4-validator local devnet in the background.
# Assumes validator-init + genesis-create have already produced devnet/vN and
# devnet/genesis.json (see docs/testnet-operations.md).
set -euo pipefail

cd "$(dirname "$0")/.."

export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-/Library/Developer/CommandLineTools/usr/lib}"
export LIBCLANG_PATH="${LIBCLANG_PATH:-/Library/Developer/CommandLineTools/usr/lib}"

# Refuse to start on top of a running devnet: a second set would fail on the
# RocksDB LOCK and clobber the logs. Stop the existing one first.
if pgrep -f 'target/debug/node run' >/dev/null 2>&1; then
  echo "a devnet is already running. stop it first:  devnet/stop.sh" >&2
  exit 1
fi

# validator.json stores the id as a JSON byte array; the node wants hex.
id_hex() {
  jq -r '.validator.id
         | map("0123456789abcdef"[(./16|floor):(./16|floor)+1]
             + "0123456789abcdef"[(.%16):(.%16)+1])
         | join("")' "$1"
}

# Build once up front so the four launches don't race on the same target dir.
echo "building node binary..."
cargo build -p node --quiet
NODE=target/debug/node

for i in 1 2 3 4; do
  dir="devnet/v$i"
  rpc="127.0.0.1:$((8899 + i))"
  id="$(id_hex "$dir/validator.json")"
  RUST_LOG="${RUST_LOG:-info}" "$NODE" run \
    --genesis devnet/genesis.json \
    --rpc "$rpc" \
    --validator-id "$id" \
    --validator-key "$dir/validator.key" \
    --gossip-key "$dir/gossip.key" \
    --data-dir "$dir/data" \
    > "$dir/node.log" 2>&1 &
  echo "$!" > "$dir/node.pid"
  echo "v$i  pid $!  rpc $rpc  log $dir/node.log"
done

echo
echo "started. tail a log with:  tail -f devnet/v1/node.log"
echo "check status with:         curl -s 127.0.0.1:8900/ -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"kestrel_getStatus\"}'"
echo "stop everything with:      devnet/stop.sh"
