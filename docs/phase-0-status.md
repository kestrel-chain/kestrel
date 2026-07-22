# Phase 0 status

## Implemented

- Cargo workspace and all planned Kestrel crate boundaries.
- Shared address, hash, slot, epoch, transaction, block, account, and object types.
- Domain-separated addresses derived from `BLAKE3("kestrel/address/v1" || scheme_id || public_key)`.
- Object-safe signature-scheme abstraction, Ed25519 implementation, and activation-gated registry.
- Storage abstraction with point operations, prefix iteration, atomic batches, and RocksDB persistence.
- Unit tests, Criterion benchmark targets, formatting, linting, and CI gates.

## Tested

- Ed25519 signing and verification round trips and rejection cases.
- Deterministic, scheme-separated address derivation that never aliases a legacy raw Ed25519 public key in the test corpus.
- Registry activation enforcement and duplicate registration rejection.
- RocksDB CRUD, ordered prefix iteration, atomic batches, and reopen persistence.

## Benchmarks

Baseline captured on 2026-07-21 on an arm64 macOS 26.3.1 development machine using Rust 1.91.1:

| Operation | Median estimate | Approximate throughput |
| --- | ---: | ---: |
| BLAKE3, 4 KiB | 2.210 µs | 1.73 GiB/s |
| Address derivation | 104.8 ns | 9.54 million/s |
| Ed25519 sign | 22.90 µs | 43,700/s |
| Ed25519 verify | 37.24 µs | 26,850/s |

Run `cargo bench -p types --bench hashing` and `cargo bench -p crypto --bench signatures` to reproduce the measurements. Criterion writes detailed machine-specific reports beneath `target/criterion`. These local development-machine results are a regression baseline, not protocol targets; release hardware must be measured before setting the consensus vote-processing budget.

## Explicitly deferred

- BLS aggregation and post-quantum schemes (their registry seam is present).
- VM, execution, networking, consensus, RPC, and node behavior beyond compiling crate boundaries.
- Governance-controlled activation; Phase 0 uses an immutable genesis allowlist.
