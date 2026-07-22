#!/usr/bin/env sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 GENESIS_JSON [RPC_ADDRESS]" >&2
  exit 64
fi

genesis_path=$1
rpc_address=${2:-127.0.0.1:8899}

exec cargo run -p node -- run --genesis "$genesis_path" --rpc "$rpc_address"
