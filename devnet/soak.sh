#!/usr/bin/env bash
# Liveness/safety soak against the real node binaries under continuous load and
# wall-clock fault churn. Hunts the INC-001/INC-002 class of bug: a validator
# that dies, stalls, or diverges under stress that looked healthy beforehand.
#
# Invariants checked continuously (the same two run_external enforces, plus
# crash detection):
#   SAFETY   — no two validators ever report different blocks at the same height.
#   LIVENESS — the max finalized height keeps advancing (a 3-of-4 quorum must
#              always make progress while at most one node is faulted).
#   NO-CRASH — a validator that is meant to be up never exits on its own.
#
# Usage:  devnet/soak.sh [DURATION_SECONDS]   (default 600)
#
# Requires the four validator identities + genesis produced by validator-init /
# genesis-create (see docs/testnet-operations.md); it wipes their data dirs and
# runs a fresh chain each time.
set -uo pipefail

cd "$(dirname "$0")/.."
export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-/Library/Developer/CommandLineTools/usr/lib}"
export LIBCLANG_PATH="${LIBCLANG_PATH:-/Library/Developer/CommandLineTools/usr/lib}"

DURATION="${1:-600}"
RUN="devnet/soak-run"
NODES=4                       # v1..v4, f=1: never fault more than one at a time.
STALL_LIMIT=45               # seconds the max height may stand still before it is a liveness failure.
FAULT_INTERVAL=18            # seconds between fault injections.
FAULT_HOLD=9                 # seconds a fault is held before healing.
SUBMIT="target/release/examples/submit_tx"

rpc_port() { echo $((8899 + $1)); }
id_hex() {
  jq -r '.validator.id
         | map("0123456789abcdef"[(./16|floor):(./16|floor)+1]
             + "0123456789abcdef"[(.%16):(.%16)+1])
         | join("")' "devnet/v$1/validator.json"
}

status() { curl -s -m 2 "127.0.0.1:$(rpc_port "$1")/" \
  -d '{"jsonrpc":"2.0","id":1,"method":"kestrel_getStatus"}' 2>/dev/null; }

start_node() { # start_node N [extra flags...]
  local n="$1"; shift
  local id; id="$(id_hex "$n")"
  # Do not inherit an ambient RUST_LOG (the Codex/dev shell commonly sets it
  # to `warn`): that silently removes the round/view diagnostics this harness
  # needs to explain a liveness failure. Use a soak-specific override instead.
  RUST_LOG="${KESTREL_SOAK_RUST_LOG:-info,node=debug}" target/release/node run \
    --genesis devnet/genesis.json --rpc "127.0.0.1:$(rpc_port "$n")" \
    --validator-id "$id" --validator-key "devnet/v$n/validator.key" \
    --gossip-key "devnet/v$n/gossip.key" --data-dir "devnet/v$n/data" \
    "$@" >> "$RUN/v$n.log" 2>&1 &
  echo "$!" > "$RUN/v$n.pid"
}
stop_node() { # stop_node N
  local n="$1" pid
  [ -f "$RUN/v$n.pid" ] || return 0
  pid="$(cat "$RUN/v$n.pid")"; kill "$pid" 2>/dev/null; rm -f "$RUN/v$n.pid"
}
node_alive() { [ -f "$RUN/v$1.pid" ] && kill -0 "$(cat "$RUN/v$1.pid")" 2>/dev/null; }
wait_ready() { # wait_ready N timeout
  local n="$1" t="$2" i=0
  while [ "$i" -lt "$t" ]; do
    [ "$(curl -s -m1 -o /dev/null -w '%{http_code}' "127.0.0.1:$(rpc_port "$n")/readyz" 2>/dev/null)" = 200 ] && return 0
    sleep 1; i=$((i+1))
  done; return 1
}

cleanup() {
  echo "" > "$RUN/stop" 2>/dev/null
  for n in $(seq 1 "$NODES"); do stop_node "$n"; done
  pkill -9 -f 'target/release/node run' 2>/dev/null
  jobs -p | xargs kill 2>/dev/null
}
trap cleanup EXIT INT TERM

# ---- build + fresh chain ---------------------------------------------------
echo "building release binaries..."
# Must name --bin node explicitly: `--example` alone rebuilds the library but
# NOT the node binary the soak actually runs, which would silently test a stale
# build.
cargo build --release -p node --bin node --example submit_tx --quiet \
  || { echo "build failed"; exit 1; }
pkill -9 -f 'target/release/node run' 2>/dev/null; sleep 1
rm -rf "$RUN"; mkdir -p "$RUN"
for n in $(seq 1 "$NODES"); do rm -rf "devnet/v$n/data"; : > "$RUN/v$n.log"; done
rm -f "$RUN/violation" "$RUN/faulted" "$RUN/stop"; echo 0 > "$RUN/faulted"

echo "starting $NODES validators (release)..."
for n in $(seq 1 "$NODES"); do start_node "$n"; done
wait_ready 1 30 || { echo "network never became ready"; exit 1; }
echo "ready. soaking for ${DURATION}s (faults every ${FAULT_INTERVAL}s, stall limit ${STALL_LIMIT}s)."

# ---- monitor: safety + liveness + crash ------------------------------------
# Safety map is a file ("height block"), not a bash associative array, because
# macOS ships bash 3.2 which has neither.
monitor() {
  : > "$RUN/canon"
  local max_h=0 last_progress; last_progress="$(date +%s)"
  while [ ! -f "$RUN/stop" ]; do
    local faulted; faulted="$(cat "$RUN/faulted" 2>/dev/null || echo 0)"
    local now cur_max=0
    now="$(date +%s)"
    for n in $(seq 1 "$NODES"); do
      # crash check: a node that should be up must still be running.
      if [ "$n" != "$faulted" ] && [ -f "$RUN/v$n.pid" ] && ! node_alive "$n"; then
        echo "CRASH v$n exited on its own (not the faulted node)" > "$RUN/violation"; return
      fi
      local s h b; s="$(status "$n")"; [ -z "$s" ] && continue
      h="$(echo "$s" | jq -r '.result.finalizedHeight // empty' 2>/dev/null)"
      b="$(echo "$s" | jq -r '.result.finalizedBlock // empty' 2>/dev/null)"
      [ -z "$h" ] && continue
      # safety: one block per height across all nodes, compared across time.
      local known; known="$(awk -v H="$h" '$1==H{print $2; exit}' "$RUN/canon")"
      if [ -n "$known" ]; then
        if [ "$known" != "$b" ]; then
          echo "SAFETY height $h: v$n=$b conflicts with $known" > "$RUN/violation"; return
        fi
      else
        echo "$h $b" >> "$RUN/canon"
      fi
      [ "$h" -gt "$cur_max" ] && cur_max="$h"
    done
    if [ "$cur_max" -gt "$max_h" ]; then max_h="$cur_max"; last_progress="$now"; fi
    if [ $((now - last_progress)) -gt "$STALL_LIMIT" ]; then
      echo "LIVENESS max height stuck at $max_h for >${STALL_LIMIT}s ($(node_heights))" > "$RUN/violation"; return
    fi
    echo "$now $max_h" > "$RUN/progress"
    sleep 2
  done
}

node_heights() { # one-line per-node height snapshot for diagnostics
  local out=""
  for n in $(seq 1 "$NODES"); do
    local h; h="$(status "$n" | jq -r '.result.finalizedHeight // "down"' 2>/dev/null)"
    out="${out}v$n=${h:-down} "
  done
  echo "$out"
}

verify_caught_up() { # verify_caught_up N — a healed node must rejoin the tip
  local n="$1" i=0 slack=30
  while [ "$i" -lt 40 ]; do
    local mx=0 hn=0
    for m in $(seq 1 "$NODES"); do
      local h; h="$(status "$m" | jq -r '.result.finalizedHeight // 0' 2>/dev/null)"; h="${h:-0}"
      [ "$m" = "$n" ] && hn="$h"
      [ "$h" -gt "$mx" ] && mx="$h"
    done
    [ $((mx - hn)) -le "$slack" ] && return 0
    sleep 1; i=$((i+1))
  done
  echo "CATCHUP v$n stuck at $hn while network reached $mx, 40s after heal" > "$RUN/violation"
  return 1
}

# ---- load: unlimited independent CreateObject txs --------------------------
load() {
  local seed=0
  while [ ! -f "$RUN/stop" ]; do
    seed=$((seed+1))
    local tx; tx="$($SUBMIT "$seed" 0 $((seed % 256)) 2>/dev/null)"
    local port; port=$(rpc_port $(( (seed % NODES) + 1 )))
    curl -s -m2 -o /dev/null "127.0.0.1:$port/" \
      -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"kestrel_submitTransaction\",\"params\":{\"transaction\":\"$tx\"}}" 2>/dev/null
    sleep 0.03
  done
}

monitor & MON=$!
load & LOAD=$!

# ---- fault loop ------------------------------------------------------------
faults=(kill isolate shred_drop gossip_delay consensus_drop)
inject() {
  local n="$1" kind="$2" others=""
  for o in $(seq 1 "$NODES"); do [ "$o" != "$n" ] && others="${others:+$others,}$(id_hex "$o")"; done
  echo "$n" > "$RUN/faulted"
  case "$kind" in
    kill)           stop_node "$n"; sleep "$FAULT_HOLD"; start_node "$n" ;;
    isolate)        stop_node "$n"; start_node "$n" --blocked-peers "$others"; sleep "$FAULT_HOLD"; stop_node "$n"; start_node "$n" ;;
    shred_drop)     stop_node "$n"; start_node "$n" --shred-drop-bps 4000; sleep "$FAULT_HOLD"; stop_node "$n"; start_node "$n" ;;
    gossip_delay)   stop_node "$n"; start_node "$n" --gossip-delay-ms 250; sleep "$FAULT_HOLD"; stop_node "$n"; start_node "$n" ;;
    consensus_drop) stop_node "$n"; start_node "$n" --drop-bps 2500;       sleep "$FAULT_HOLD"; stop_node "$n"; start_node "$n" ;;
  esac
  wait_ready "$n" 30 || true      # let it come back before clearing faulted
  sleep 2
  echo 0 > "$RUN/faulted"
  # The healed node must rejoin the tip. Catching this here attributes a
  # recovery failure to the exact fault, instead of waiting for the downstream
  # quorum-loss stall a second fault would cause.
  verify_caught_up "$n" || true
}

deadline=$(( $(date +%s) + DURATION ))
count=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  sleep "$FAULT_INTERVAL"
  [ -f "$RUN/violation" ] && break
  n=$(( (RANDOM % NODES) + 1 )); kind="${faults[$((RANDOM % ${#faults[@]}))]}"
  count=$((count+1))
  read -r _ mh < "$RUN/progress" 2>/dev/null || mh="?"
  echo "[$(( (deadline - $(date +%s)) ))s left] height=$mh  fault #$count: $kind on v$n"
  inject "$n" "$kind"
  [ -f "$RUN/violation" ] && break
done

echo "" > "$RUN/stop"; sleep 1
kill "$MON" "$LOAD" 2>/dev/null

# ---- verdict ---------------------------------------------------------------
read -r _ final_h < "$RUN/progress" 2>/dev/null || final_h="?"
echo "=================================================================="
if [ -f "$RUN/violation" ]; then
  echo "SOAK FAILED after $count faults — $(cat "$RUN/violation")"
  echo "--- fatal/interesting log lines (benign fee-settlement noise excluded) ---"
  grep -nEi 'error|panic|stopped|behind|backlog|caught up|catch-up' "$RUN"/v*.log \
    | grep -viE 'insufficient balance|fee settlement' | tail -30
  exit 1
else
  echo "SOAK PASSED: ${DURATION}s, $count faults injected, no safety/liveness/crash violation."
  echo "final max finalized height: $final_h"
  exit 0
fi
