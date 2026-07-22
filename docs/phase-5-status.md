# Phase 5 status

## Implemented

- Replaced the `mempool` stub with object/account-local fee scopes, congestion pricing, deterministic round-robin selection, and per-scope block limits.
- Settled fees from actual compute consumed and transferred the entire charge to the validator with no burn.
- Added a pluggable, deterministic `OrderingPolicy` interface registered per application scope. The default remains priority-fee ordering with canonical tie-breakers.
- Integrated pinned `revm` 36 in `vm-evm` as a co-resident execution environment.
- Added a native-object precompile for reads and version-checked writes, plus an actually deployed EVM forwarding-contract fixture.
- Made EVM/native effects atomic by executing each EVM transaction against a candidate native state and publishing it only on success.
- Added compressed binary-Merkle resurrection witnesses, stateless witness verification, retained-history validation, fresh-rent enforcement, version increment, replay prevention, and atomic active/expired root updates.
- Extended execution deltas so expired-tree changes participate in deterministic parallel commits.

## Acceptance evidence

- The cross-VM test compiles and publishes an actual Move `Token` module, invokes Move `mint`, deploys an EVM contract, reads and writes the same Move resource from EVM bytecode, and then invokes Move `transfer` against the EVM-updated balance. A stale EVM write fails without changing the state root.
- The localized-market regression submits 1,000 transactions to one hot object and an unrelated transaction. The unrelated transaction retains the minimum local base price and appears within the first two selected positions; the hot lane cannot exceed its configured block share.
- Fee tests charge 10 units of actual compute against a limit of 100 and prove conservation between payer and validator.
- Application-policy tests prove custom ordering changes only the registered scope.
- Resurrection tests expire an object, verify its witness without a state database, resurrect it with fresh rent, mutate it, reject replay and tampering, and reproduce the exact final root on an independent state tree.
- Sequential and Phase 2 parallel execution produce the same root for a block containing resurrection followed by mutation.
- A 256-object fixture enforces fewer than 32 compressed proof steps and a serialized BCS witness below 2 KiB.

## Localized-market benchmark

Measurements were captured on 2026-07-21 on an arm64 macOS 26.3.1 development machine using Rust 1.91.1. `cargo bench -p mempool --bench localized_fee_market -- --quick` measured nonmutating selection of two transactions:

| Hot-object queue depth | Median preview time |
| ---: | ---: |
| 0 | 89.22 ns |
| 1,000 | 100.69 ns |
| 10,000 | 100.74 ns |

Increasing contention from 1,000 to 10,000 queued hot-object transactions changed preview time by about 0.05%, while the unrelated lane's fee and selection position remained unchanged. The zero-depth case has only one active scope, so its lower absolute time is expected. These figures isolate in-memory scheduling and are not end-to-end network latency measurements.

## Verification

The Phase 5 workspace passes:

- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`

On the benchmark machine, test and Clippy commands require Xcode's library directory in `LIBCLANG_PATH` and `DYLD_LIBRARY_PATH` for the existing RocksDB bindgen dependency.

## Explicitly deferred

- Full Verkle/vector-commitment migration, commitment setup, proof aggregation, and historical-root retention/pruning policy. Phase 5 uses compact proofs over the existing deterministic binary trie so root semantics do not silently change.
- TEE fair ordering, encrypted mempools, cross-scope policies, and adversarial scheduler infrastructure beyond the requested application hook.
- Durable mempool storage, networking ingress integration, signed EVM envelopes, persistent production EVM account storage, Solidity bindings, and cross-VM reentrancy rules.
- Production fee tuning and end-to-end latency measurements. Phase 6 code-readiness work now exists, but no public network has been launched.
