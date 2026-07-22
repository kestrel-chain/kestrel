# Phase 4 status

## Implemented

- Added Kestrel-BFT deterministic stake-table leader rotation and local-timeout view changes.
- Implemented the shared first-round order vote: an 80% aggregate finalizes on the fast path; a 60% aggregate forms a prepare certificate.
- Implemented the fallback's distinct second-round commit vote and 60% finality certificate.
- Added per-height/view/phase honest single-vote enforcement, mutual exclusion between the first-round order and timeout choices, prepare locks, and higher-prepare-certificate unlock rules.
- Encoded the 20+20 model directly: Byzantine stake must be strictly below 20%; a separate disjoint crash/offline set may be up to 20%.
- Added BLS12-381 signing, individual verification, same-message aggregation, aggregate verification, and proof-of-possession validation to `crypto`.
- Individual votes remain collector-local. Aggregate certificates are serializable and contain the canonical target, signer IDs, exact signed stake, and aggregate signature.
- Added a bounded asynchronous execution worker. It lets consensus order one subsequent block while prior execution runs and preserves Phase 2 canonical execution.
- Added a 20-node deterministic consensus simulator with leader equivocation, vote withholding, crash/offline nodes, link partitions, per-node delay, deterministic message loss, local timeouts, and bounded view changes.
- Added a raw-TCP production coordinator with authenticated proposal relay, a bounded proposal-observation delay, BLS votes/certificates, RocksDB-backed replica safety snapshots, and recovery across process restarts.

## Safety and liveness evidence

- Fast certificates require the ceiling of 80% stake; prepare, commit, and timeout certificates require the ceiling of 60%.
- Certificate tests cover signer sets, signed stake, BLS aggregates, and threshold enforcement.
- Honest replicas refuse same-view double votes, refuse to sign both order and timeout in one view, and refuse conflicting locked proposals.
- A selective-delivery regression builds a valid 80% fast certificate at one honest replica, then proves the remaining stake cannot also form the 60% timeout certificate needed to abandon that view under the strict less-than-20% Byzantine assumption. A separate regression proves that a prior timeout vote blocks a later order vote in the same view.
- The identified same-view attack is an explicit regression test. With 19% Byzantine stake and honest stake split 41/40, the equivocating side can form only one 60% certificate. With exactly 20% Byzantine stake and two 40% honest partitions, both conflicting certificates can form, and the fault-model validator rejects the scenario as outside the safety assumption.
- An exhaustive integer-stake test checks every Byzantine allocation from 0% through 19% and every two-way honest partition, proving two conflicting 60% quorums cannot coexist in that model.
- A killed leader is bypassed in one certified view change.
- A combined 15% Byzantine-withholding plus separate 20% offline scenario retains 65% responsive stake and finalizes through the fallback.
- Testkit combines equivocation, withholding, partitions, delay, and dropped messages and confirms bounded recovery. Its chaos campaign also executes the selective-delivery probe through production `Replica` and certificate code rather than relying only on the simulator's aggregate outcome.
- A five-process socket test covers healthy operation, a killed leader with a corrupt voter, a partitioned leader with a withholding voter, and a Byzantine equivocating leader below 20% stake. Every observed honest process hard-asserts the same finalized block, bounded view changes, and finality below two seconds.
- A cross-crate test finalizes height 2 before polling height 1 execution, then confirms execution results remain in height order. A Loom model checks every explored interleaving of the bounded one-block handoff.

## Latency and benchmark results

Measurements were captured on 2026-07-21 on an arm64 macOS 26.3.1 development machine using Rust 1.91.1.

The 20-validator LAN model uses 4–6 ms one-way latency:

| Scenario | Path | Simulated finality latency | View changes |
| --- | --- | ---: | ---: |
| No injected faults | 80% fast | 12 ms | 0 |
| 15% Byzantine withholding + 20% offline | 60% fallback | 24 ms | 0 |

Run `cargo run -p testkit --example phase_4_report` to reproduce these figures. Phase 3 separately measured 14 ms to reconstruct a propagated block at 80% stake; composing those conservative stage measurements gives a 26 ms local-model proxy. Propagation is therefore the larger modeled stage (14 ms versus a 12 ms fast vote round), although both are far below 500 ms.

`cargo bench -p testkit --bench finality -- --quick` measured 56.51 ms median host runtime for constructing keys/proofs and executing one 20-validator, 256-transaction-order simulation. This is simulator processing time, not protocol latency.

These are deterministic in-process simulations with injected realistic LAN delay, not public-internet measurements. The separate five-process raw-TCP test exercises real sockets, OS scheduling, persistent key loading, and fault recovery, but currently enforces only a coarse sub-two-second bound and does not compose libp2p block propagation or execution. Neither result is methodologically comparable to the cited ~750 ms Aptos Raptr and ~640 ms Sui Mysticeti measurements. No transport or BFT optimization should be justified from the in-process number alone.

## Verification

The Phase 4 workspace passes:

- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`

On the benchmark machine, test and clippy commands require Xcode's library directory in `LIBCLANG_PATH` and `DYLD_LIBRARY_PATH` for the existing RocksDB bindgen dependency.

## Explicitly deferred

- Integrated socket-level block-propagation, consensus, execution, and durable-state measurements, plus public-internet benchmarks.
- Epoch changes, validator reconfiguration, durable application state, and state synchronization. Replica vote/lock safety state itself is persisted in RocksDB.
- Production pacemaker tuning, adaptive timeouts, slashing/equivocation evidence, and peer banning.
- Consensus certificate integration into the persistent block store and production node lifecycle.
- Phase 5 EVM interoperability, fee-market, MEV, and witness work.
