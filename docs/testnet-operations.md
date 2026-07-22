# Kestrel testnet operations

## Validator onboarding

Generate each validator identity on the operator's host:

```sh
cargo run -p cli -- validator-init NAME STAKE NETWORK_ADDRESS RPC_ADDRESS OUTPUT_DIR
```

The command creates `validator.key` with mode `0600` on Unix and refuses to overwrite it. `validator.json` is public and includes the BLS public key and proof of possession. Transfer only the public profile to the genesis coordinator. Back up the secret through the operator's normal encrypted key-custody process; do not commit it.

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

To run a validator in the raw-TCP consensus coordinator, provide all three identity/state flags:

```sh
cargo run -p node -- run --genesis genesis.json --rpc 127.0.0.1:8899 \
  --validator-id VALIDATOR_ID --validator-key validator.key --data-dir validator-data
```

Each genesis validator needs its own process, RPC endpoint, key, and data directory. The coordinator persists the replica's vote/lock safety snapshot, relays signed proposals, and exchanges BLS votes and certificates. It currently orders synthetic transaction hashes; it does not yet ingest libp2p gossip/KestrelCast payloads or execute and persist finalized application state.

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
- Stage 2: extend the existing five-process raw-TCP consensus fault harness into an integrated 4–20-node transaction-processing network; record propagation-to-80%-stake and end-to-end finality; retain leader death, equivocation, withholding, partitions, latency, and loss coverage; verify execution and rent epochs advance durably.
- Stage 3: onboard 50–100 external operators; define and measure genesis-sync time; run hot-object fee attacks and real wall-clock expiry for multiple weeks.
- Stage 4: reach 150–200 geographically distributed validators; publish sustained TPS/finality and participant concentration; run continuous 0.05% drop and leader-failure campaigns.
- Stage 5: require a separate go-live review, capped economic exposure, and an external security audit.

A later stage must not be promoted because an earlier simulation passed.
