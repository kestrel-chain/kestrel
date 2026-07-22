# Kestrel

**A single-shard, general-purpose Layer-1 blockchain, built in Rust from first principles.**

Kestrel pairs a dual-path BFT consensus protocol with Block-STM-style parallel execution, and runs a real Move VM side by side with a real EVM in the same state — so Move and Solidity contracts can read and write the same on-chain objects. Erasure-coded block propagation, per-object fee markets, and storage rent with cryptographic resurrection round out a design meant to scale without giving up safety.

[![CI](https://github.com/kestrel-chain/kestrel/actions/workflows/ci.yml/badge.svg)](https://github.com/kestrel-chain/kestrel/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.91.1-orange.svg)](rust-toolchain.toml)

---

## Why Kestrel

Most chains force a choice: pick one execution model, pick one virtual machine, pick one consensus family, and live with it. Kestrel is built around a different bet — that these decisions don't have to be bundled, and that a chain can be both fast under the common case and rigorously safe under an adversarial one.

- **Finality in one round when the network cooperates, two when it doesn't.** Kestrel-BFT commits at 80% stake in a single round on the happy path, falls back to a two-round 60%/60% path otherwise, and rotates the leader on a local timeout — all under a strict, formally-stated fault model (safety holds under &lt;20% Byzantine stake; liveness tolerates up to another 20% crashed/offline).
- **Parallel execution that doesn't trust itself.** Transactions speculate concurrently using a Block-STM-inspired scheduler, but every result is validated against read/write sets before it commits, and a deterministic sequential executor is run in parallel test suites specifically to catch any divergence. Single-owner-object transactions get a lock-free structural fast path.
- **Move and EVM, sharing one state — not two chains bolted together.** A real, pinned Move VM and a real `revm`-based EVM run in the same process against the same object store. A precompile bridge lets EVM bytecode read and write native Move-owned objects directly, in both directions, inside one atomic transaction.
- **Storage isn't free, and isn't lost forever either.** Objects carry a rent balance that's charged every epoch; when it runs out, the object moves to a compact expired-object tree instead of vanishing, and can be resurrected later with a cryptographic witness proof — no operator trust required.
- **Fee markets that don't let one hot object ruin everyone else's day.** Congestion pricing is scoped per-object and per-account, so a flood of transactions against one popular object doesn't degrade fees or latency for transactions touching anything else.
- **Gossip that survives a hostile peer.** Transaction propagation and block relay run over libp2p; a peer that repeatedly sends malformed or invalid data gets disconnected and blocked, automatically, without taking down the node that caught it.

## Architecture at a glance

Kestrel is a Cargo workspace of focused crates, each with its own README:

| Crate | What it does |
| --- | --- |
| [`consensus`](crates/consensus) | Kestrel-BFT: dual-path finality, BLS-aggregated quorum certificates, view changes, equivocation evidence |
| [`execution`](crates/execution) | Optimistic parallel execution, structural fast paths, deferred execution pipeline |
| [`vm-move`](crates/vm-move) | The Move VM host: module publication, entry-function calls, gas metering |
| [`vm-evm`](crates/vm-evm) | Co-resident `revm`-based EVM with a native-object precompile bridge to Move state |
| [`state`](crates/state) | Merkle-committed object store, storage rent, expiry, and witnessed resurrection |
| [`mempool`](crates/mempool) | Localized (per-object/per-account) fee-market admission and ordering |
| [`network`](crates/network) | libp2p transport: transaction gossip, `KestrelCast` erasure-coded shred relay, peer banning |
| [`node`](crates/node) | Genesis, the durable block lifecycle, and the production node binary |
| [`rpc`](crates/rpc) | Public JSON-RPC, health/readiness, and metrics |
| [`crypto`](crates/crypto) | Crypto-agile signature schemes (Ed25519, BLS12-381) behind a swappable registry |
| [`storage`](crates/storage) | RocksDB-backed durable key/value store |
| [`types`](crates/types) | Shared primitive types |
| [`cli`](crates/cli) | Validator onboarding and genesis authoring |
| [`testkit`](crates/testkit) | Deterministic consensus simulation, chaos/fault injection, and property tests |

## Getting started

Kestrel targets Rust 1.91.1 (pinned in [`rust-toolchain.toml`](rust-toolchain.toml)).

```sh
git clone https://github.com/kestrel-chain/kestrel.git
cd kestrel
cargo test --workspace --all-targets
```

The full local gate, run on every commit in CI:

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo bench --workspace
```

`rust-rocksdb` generates bindings with Clang. On macOS Command Line Tools installations where `libclang` isn't already discoverable, prefix your first build with:

```sh
DYLD_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib \
LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib \
cargo test --workspace --all-targets
```

To stand up a local devnet, see [`docs/testnet-operations.md`](docs/testnet-operations.md).

## Project status

Kestrel is under active development and **has not yet run as a public network.** The workspace's local test suite — including real multi-process Byzantine-fault scenarios, cross-executor equivalence checks, and Loom-based concurrency model checking — passes in full automation today. That's Stage 1 of a five-stage promotion plan toward a production public testnet; Stages 2–5 require real external validators, real network conditions, and sustained public operation, none of which a test suite can substitute for.

We track this honestly and in the open:

- [`docs/TECH_DEBT.md`](docs/TECH_DEBT.md) — the live register of what's proven, what's narrowed, and what's still open, updated as work lands.
- [`docs/phase-6-status.md`](docs/phase-6-status.md) — the current phase gate and stage-promotion criteria.
- [`docs/engineering-build-spec.md`](docs/engineering-build-spec.md) — the phase-by-phase build plan the codebase follows.

If a claim about Kestrel isn't backed by an automated test or an explicit, dated entry in those documents, treat it as aspirational rather than done.

## Documentation

Every major subsystem has a design spec in [`docs/`](docs): [`consensus-spec.md`](docs/consensus-spec.md), [`execution-spec.md`](docs/execution-spec.md), [`state-spec.md`](docs/state-spec.md), [`networking-spec.md`](docs/networking-spec.md), [`mempool-spec.md`](docs/mempool-spec.md), and [`evm-spec.md`](docs/evm-spec.md).

## Contributing

Issues and pull requests are welcome. Before opening a PR, run the full gate above locally — CI enforces `cargo fmt`, `cargo clippy -D warnings`, and the full test suite on every push.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
