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

Defaults cap bodies at 512 KiB, batches at 64 calls, and each source IP at 1,000 requests per second. No administrative or chaos method is exposed by public RPC.

## Chaos campaigns

Reproduce the CI campaign:

```sh
cargo run -p testkit --example phase_6_report
```

For an external testnet, implement `testkit::ChaosTarget` in the operator-controlled deployment repository. The adapter maps `KillValidator`, `IsolateValidator`, message-drop, and `HealAll` requests to narrowly scoped infrastructure controls and returns finalized height/hash observations from independent nodes. `run_external` fails immediately on conflicting finalized hashes, fails after the configured number of stalled observations, and requests healing on success and error paths.

Never point a chaos adapter at mainnet or a network outside the operator's explicit authority. Start Stage 2 with team-controlled machines and a written rollback procedure.

## Promotion checklist

- Stage 1: run all workspace gates and the scripted 100-scenario campaign.
- Stage 2: the integrated 4–20-node transaction-processing network itself is built and proven in automation (real gossip, `KestrelCast`, consensus, execution, durable commit — see `crates/node/tests/stage_2_processes.rs` and `stage2_node_rpc_integration.rs`). What Stage 2 still requires: run it across real separate machines/geography rather than one host over loopback, run it under socket-level latency/loss/relay death on the libp2p transaction/shred paths specifically (the `--gossip-delay-ms`/`--tx-drop-bps`/`--shred-drop-bps` flags above now provide this on the gossip/shred transport, matching what the raw-TCP consensus path already had; what remains is running it across real hosts rather than loopback), and record propagation-to-80%-stake and end-to-end finality measurements under those real conditions. Retain leader death, equivocation, withholding, partition, latency, and loss coverage throughout, and verify execution and rent epochs keep advancing durably.
- Stage 3: onboard 50–100 external operators; define and measure genesis-sync time; run hot-object fee attacks and real wall-clock expiry for multiple weeks.
- Stage 4: reach 150–200 geographically distributed validators; publish sustained TPS/finality and participant concentration; run continuous 0.05% drop and leader-failure campaigns.
- Stage 5: require a separate go-live review, capped economic exposure, and an external security audit.

A later stage must not be promoted because an earlier simulation passed.
