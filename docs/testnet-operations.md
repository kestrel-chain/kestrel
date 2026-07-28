# Kestrel testnet operations

## Validator onboarding

Generate each validator identity on the operator's host:

```sh
cargo run -p cli -- validator-init NAME STAKE NETWORK_ADDRESS RPC_ADDRESS GOSSIP_ADDRESS OUTPUT_DIR
```

The command creates `validator.key` (the BLS consensus key) and `gossip.key` (the libp2p transaction-gossip/`KestrelCast` identity), both mode `0600` on Unix, and refuses to overwrite either. `validator.json` is public and includes the BLS public key, proof of possession, and the libp2p gossip peer ID/address. Transfer only the public profile to the genesis coordinator. Back up both secrets through the operator's normal encrypted key-custody process; do not commit either.

Combine the public profiles into one JSON array, then create and independently validate genesis:

```sh
cargo run -p cli -- genesis-create CHAIN_ID GENESIS_UNIX_MS validators.json genesis.json
cargo run -p cli -- genesis-validate genesis.json
```

Every operator must compare the printed genesis hash out of band before startup. Genesis validation accepts 4–500 validators, requires unique names/network/RPC endpoints, verifies every BLS proof of possession, validates the 20+20 stake-table prerequisites, builds the initial rent-enabled state, and hashes canonical BCS after sorting validators, objects, and active scheme IDs.

## Node and RPC

Start a genesis node on loopback:

```sh
RUST_LOG=info cargo run -p node -- run --genesis genesis.json --rpc 127.0.0.1:8899
```

The process emits JSON logs and shuts down gracefully on Ctrl-C. It refuses a non-loopback bind without `--allow-public-rpc`. That flag is an acknowledgement, not TLS: put the listener behind an authenticated, TLS-terminating reverse proxy with connection limits before public exposure.

To run a validator, provide all four identity/state flags:

```sh
cargo run -p node -- run --genesis genesis.json --rpc 127.0.0.1:8899 \
  --validator-id VALIDATOR_ID --validator-key validator.key --gossip-key gossip.key --data-dir validator-data
```

Each genesis validator needs its own process, RPC endpoint, both keys, and a unique data directory. This runs the full production pipeline, not a synthetic one: the raw-TCP consensus coordinator persists the replica's vote/lock safety snapshot, relays signed proposals, and exchanges BLS votes and certificates; a separate libp2p `NetworkNode` gossips signed transactions and relays `KestrelCast` erasure-coded shreds to the other genesis validators (addressed via each validator's `gossip_peer_id`/`gossip_address`); and the durable `BlockLifecycle` reconstructs each finalized payload, executes it, and atomically commits the block, certificate, and state root to RocksDB. A transaction submitted over any validator's `kestrel_submitTransaction` RPC method is admitted, gossiped, ordered, executed, and committed identically on every validator — proven across four separate OS processes in `crates/node/tests/stage2_node_rpc_integration.rs` and, for Byzantine-fault scenarios, across five in `crates/node/tests/stage_2_processes.rs`.

Optional fault-injection flags exercise the same scenarios Stage 2's campaigns need, without any code changes. On the raw-TCP consensus path: `--withhold-votes`, `--corrupt-votes`, `--equivocate` simulate a Byzantine validator; `--blocked-peers ID,ID` refuses messages from specific validators (simulating isolation/partition); `--delay-ms N` and `--drop-bps N` add outbound latency and random message loss to consensus messages; `--proposal-delay-ms N` slows a leader's proposals; `--stop-after-height N` halts the coordinator once a height finalizes, useful for scripted campaigns. On the separate libp2p transaction-gossip and `KestrelCast` shred path: `--gossip-delay-ms N` adds outbound latency to this node's gossip/shred sends, `--tx-drop-bps N` drops that fraction (in basis points, 0–10000) of its outbound transaction publishes, and `--shred-drop-bps N` drops that fraction of its outbound shred sends — set `--shred-drop-bps 10000` to model a fully dead relay. Drops are deterministic in the message payload, so a given transaction or shred is reproducibly either always or never dropped by a given node.

Health surfaces:

- `GET /healthz` proves the process can serve requests.
- `GET /readyz` returns 200 only after the node marks bootstrap complete.
- `GET /metrics` exports Prometheus text.
- `POST /` accepts JSON-RPC `kestrel_getStatus` and `kestrel_getObject`.

Defaults cap bodies at 512 KiB, batches at 64 calls, and each source IP at 1,000 JSON-RPC calls per second. Every element in a batch consumes one call from that allowance; a batch is not counted as a single request. No administrative or chaos method is exposed by public RPC.

### RPC load and abuse evidence

Run the bounded read-only harness against a loopback node:

```sh
python3 scripts/rpc_load_abuse.py \
  --url http://127.0.0.1:8899/ \
  --duration-seconds 30 \
  --requests-per-second 250 \
  --concurrency 16 \
  --max-p95-ms 250 \
  --output evidence/rpc-load-abuse.json
```

The paced workload uses `kestrel_getStatus` only and reports achieved call rate,
success ratio, and p50/p95/p99/max latency. Bounded abuse probes verify malformed
JSON errors, the configured batch and body limits, responsiveness while partial
request bodies are held open, exact fixed-window rate-limit exhaustion, and
Prometheus request/error/rejection counter deltas. The output path is immutable:
the harness refuses to overwrite existing evidence.

The defaults assume the node's default 512 KiB body, 64-call batch, one-second
window, and 1,000-call per-source limit. If the target uses different settings,
pass the matching `--maximum-body-bytes`, `--maximum-batch-length`,
`--rate-window-seconds`, and `--rate-limit-calls` values. Set
`--rate-limit-calls 0` only when the target's edge proxy makes deterministic
limit exhaustion impossible; the report records that probe as skipped.

Non-loopback targets are refused unless `--allow-non-loopback` is supplied.
That flag is authorization acknowledgement, not permission to test a public
endpoint: use it only for infrastructure the operator controls, agree the load
envelope in advance, and keep the offered call rate below upstream proxy and
provider limits. The harness never submits transactions or calls administrative
or chaos methods.

## Chaos campaigns

Reproduce the CI campaign:

```sh
cargo run -p testkit --example phase_6_report
```

For an external testnet, implement `testkit::ChaosTarget` in the operator-controlled deployment repository. The adapter maps `KillValidator`, `IsolateValidator`, message-drop, and `HealAll` requests to narrowly scoped infrastructure controls and returns finalized height/hash observations from independent nodes. `run_external` fails immediately on conflicting finalized hashes, fails after the configured number of stalled observations, and requests healing on success and error paths.

Never point a chaos adapter at mainnet or a network outside the operator's explicit authority. Start Stage 2 with team-controlled machines and a written rollback procedure.

## Stage 2 distributed campaign evidence

Use `testnets/configs/stage2-campaign.example.json` as the public,
provider-neutral campaign manifest. Each validator entry binds its genesis
validator ID to an HTTPS RPC endpoint and the structured JSON log copied from
that validator after the run. Stake is derived from the declared genesis
document (an optional manifest `stake` must match it), never trusted from a
free-standing label. Do not put SSH destinations, credentials, private
addresses, or secret-key paths in a committed manifest.

Before measuring propagation, synchronize every host clock with the operator's
normal NTP/PTP service and record the observed maximum skew in the private
campaign notes. Propagation is computed by correlating the same signed
transaction ID across validator log timestamps, so unsynchronized clocks make
that measurement invalid. Start validators with transaction-admission tracing
enabled:

```sh
RUST_LOG='info,node=debug,node::pipeline=trace' target/release/node run ...
```

Run the read-only monitor from an operator host for the initial six-hour
campaign:

```sh
python3 scripts/stage2_campaign_monitor.py \
  --manifest testnets/configs/stage2-campaign.json \
  --duration-seconds 21600 \
  --interval-seconds 2 \
  --stall-seconds 45 \
  --output evidence/observations.jsonl \
  --summary evidence/monitor-summary.json
```

It polls all RPC endpoints concurrently, verifies chain and genesis identity,
remembers every observed `(height, block)` decision, fails immediately on a
conflict, and fails if the maximum finalized height remains stationary beyond
the bound. It never starts, stops, partitions, or reconfigures a validator;
authorized fault mutation remains in the deployment repository's
`testkit::ChaosTarget` adapter.

After the run, copy each validator's complete JSON log to the path declared in
the manifest and compile the immutable evidence:

```sh
python3 scripts/stage2_campaign_report.py \
  --manifest testnets/configs/stage2-campaign.json \
  --output evidence/stage2-report.json \
  --markdown evidence/stage2-report.md
```

The report:

- hashes every input log with SHA-256;
- verifies every validator started from the declared genesis;
- fails on conflicting finalized blocks or a stopped coordinator/pipeline;
- computes time from first admission to the configured stake threshold
  (80% by default);
- summarizes node-observed finality latency, cross-validator finalization skew,
  view changes, and execution lag; and
- applies only the explicit gates in the manifest. Latency targets are not
  silently invented: record the real numbers first, then compare them honestly
  with the research targets.

Retain the raw logs, monitor observations, manifest, report, genesis, binary
digest, host/region inventory, clock-skew evidence, fault timeline, and rollback
record together. Publish only a redacted evidence bundle that does not expose
operator credentials or private infrastructure details.

## Promotion checklist

- Stage 1: run all workspace gates and the scripted 100-scenario campaign.
- Stage 2: the integrated 4–20-node transaction-processing network itself is built and proven in automation (real gossip, `KestrelCast`, consensus, execution, durable commit — see `crates/node/tests/stage_2_processes.rs` and `stage2_node_rpc_integration.rs`). What Stage 2 still requires: run it across real separate machines/geography rather than one host over loopback, run it under socket-level latency/loss/relay death on the libp2p transaction/shred paths specifically (the `--gossip-delay-ms`/`--tx-drop-bps`/`--shred-drop-bps` flags above now provide this on the gossip/shred transport, matching what the raw-TCP consensus path already had; what remains is running it across real hosts rather than loopback), and record propagation-to-80%-stake and end-to-end finality measurements under those real conditions. Retain leader death, equivocation, withholding, partition, latency, and loss coverage throughout, and verify execution and rent epochs keep advancing durably.
- Stage 3: onboard 50–100 external operators; define and measure genesis-sync time; run hot-object fee attacks and real wall-clock expiry for multiple weeks.
- Stage 4: reach 150–200 geographically distributed validators; publish sustained TPS/finality and participant concentration; run continuous 0.05% drop and leader-failure campaigns.
- Stage 5: require a separate go-live review, capped economic exposure, and an external security audit.

A later stage must not be promoted because an earlier simulation passed.
