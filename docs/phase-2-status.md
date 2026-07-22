# Phase 2 status

## Implemented

- Preserved `SequentialExecutor` as the deterministic Phase 1 reference implementation.
- Added per-transaction object read/write tracking in `state`, including reads performed by the Move resolver and ownership/version checks.
- Added deterministic state deltas for speculative results without changing Merkle-root or rent semantics. Speculative state now uses one immutable shared block-start version plus transaction-local overlays; ordinary deltas inspect only versioned keys rather than scanning or copying the full state.
- Added a fixed-size parallel executor with versioned speculation, canonical-order validation and commit, conflict aborts, and deterministic re-execution.
- Moved state-root calculation out of transactions and speculative attempts, including resurrection transitions. Both executors compute one canonical root after the final ordered commit and attach that block root to every transaction receipt.
- Added complete static object-reference declarations. Undeclared runtime access is rejected.
- Added structural fast-path owner lanes for transactions touching exclusively single-owner objects owned by their sender. Same-owner work remains ordered; independent owners run concurrently. Ownership changes, references overlapping optimistic work, and the same absent object ID claimed by different owner lanes are excluded. Resurrection ownership is checked against the witness.
- Added scheduler receipts and counters reporting access sets, execution path, attempt count, commits, aborts, and re-executions.
- Made Move change-set commits atomic without cloning the accumulated block overlay: the host validates and prepares a delta before applying it once.
- Reused Move executors per Rayon work partition, added a minimum speculation grain, moved successful deltas into canonical state, and removed redundant declaration/write-set temporaries.
- Kept Merkle calculation once per block while removing the final full-state materialization; roots now iterate the immutable base and overlay directly in canonical key order.

## Correctness tests

- Property tests compare every receipt root and final state root against sequential execution for generated low-contention single-owner workloads. Focused assertions require every receipt to carry the once-computed final block root.
- Property tests cover repeated shared-object conflicts whose later speculative attempts must abort and re-execute.
- Adversarial generated mixes combine shared and single-owner objects, hot keys, mutation, deletion, and recreation.
- Focused tests assert fast-path selection, conflict detection, and re-execution counters. A regression test gives two owners the same absent object ID and hard-asserts that parallel and sequential execution fail at the same canonical transaction with identical retained state.
- State tests prove version overlays isolate their immutable base, tracked key sets are exact, and applying a transaction-local delta reconstructs the candidate root.
- A Loom model explores speculative completion interleavings and proves canonical validation order and conflict decisions do not depend on worker completion order. Its conflict decision calls the production `conflicts_with_committed_writes` predicate, so the model cannot silently retain a duplicated rule. Production workers do not share mutable state; synchronization is confined to Rayon task completion before the single canonical commit pass.
- `realistic_move_equivalence` is a normal integration test, not a logged benchmark check. It executes real Move `Token::transfer` calls through both executors and uses hard `assert_eq!` gates for the execution result root, independently recomputed final-state root, receipt count, and every parallel receipt root. A mismatch fails `cargo test --workspace --all-targets`.

## Performance investigation

Results were captured on 2026-07-21 on an eight-logical-core arm64 macOS 26.3.1 development machine with Rust 1.91.1. These are execution-subsystem measurements, not end-to-end blockchain TPS.

### Unmodified profile

The original 256-transaction low-contention parallel/8 run took 121.64 ms versus 84.28 ms sequentially. A 20-second macOS `sample` profile at 1 ms intervals attributed the critical path as follows:

| Category | Approximate elapsed-time share |
| --- | ---: |
| Merkle/state-root computation | 95.4% |
| Actual transaction work, access tracking, and delta construction | 2.4% |
| Full state snapshot/copy | 1.5% |
| True mutex/lock contention | <0.1% |
| Other orchestration | ~0.6% |

The coordinator spent 24.8% of elapsed time parked in the Rayon join while workers performed speculation. That wait overlaps the worker categories above and is not additive lock overhead. Worker active times were balanced within roughly 3%. Merkle work split into about 20.9% in speculative workers and 74.5% in the serial ordered-commit phase.

Source inspection confirmed 513 complete roots in each 256-transaction parallel block: one per speculative native operation, one after each ordered delta, and one final root. The sequential reference computed 257.

### Isolated root-once change

The first isolated change reduced both executors to one root per block without changing snapshot or scheduler behavior.

| 256 low contention | Before | Root once | Change |
| --- | ---: | ---: | ---: |
| Sequential | 84.28 ms | 0.435 ms | 193.9x faster |
| Parallel / 1 | 195.18 ms | 14.39 ms | 13.6x faster |
| Parallel / 8 | 121.64 ms | 5.60 ms | 21.7x faster |
| 1-to-8 speedup | 1.60x | 2.57x | — |
| Parallel efficiency | 20.1% | 32.1% | +12.0 points |

After removing the repeated roots, the 8-worker path was profiled again for 60 seconds with the former coarse snapshot behavior restored behind a temporary measurement-only feature. A separate control submitted and collected 256 no-op items through the same eight-worker Rayon pool so fixed dispatch cost would not be folded into state-copy cost.

| Category | Approximate elapsed-time share |
| --- | ---: |
| Full state snapshot/copy CPU | 39.5% |
| Full-state delta scanning | 22.5% |
| Scheduler and allocator synchronization/waiting | 20.2% |
| Actual transaction execution | 0.5% |
| Pure thread-pool dispatch | 0.34% |
| One final Merkle root | 7.0% |
| Commit, access tracking, and other orchestration | 9.9% |

The dispatch-only control took 18.53 microseconds, compared with approximately 5.47 ms for the profiled parallel block. The coordinator was parked in the Rayon completion latch for 88.1% of wall time, overlapping worker activity rather than adding another 88.1%. The profile therefore identifies coarse state materialization and full-map delta derivation, together with their allocator/scheduler contention, as the post-Merkle bottleneck; fixed pool dispatch was not dominant.

The requested block-size sweep was then run before changing state versioning. Efficiency is `T1 / (8 * T8)` relative to parallel/1.

| Transactions | Sequential | Parallel / 1 | Parallel / 8 | Parallel / sequential time | 1-to-8 speedup | Parallel efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.492 ms | 14.075 ms | 6.282 ms | 12.78x slower | 2.24x | 28.0% |
| 1,024 | 2.153 ms | 263.76 ms | 90.904 ms | 42.22x slower | 2.90x | 36.3% |
| 4,096 | 10.289 ms | 5.031 s | 2.116 s | 205.70x slower | 2.38x | 29.7% |

The focused 4,096/8 estimate was 2.116 seconds with a 1.888–2.370 second interval. The worsening absolute ratio proves this was an `O(transactions * materialized state size)` defect rather than fixed overhead that larger blocks would amortize.

### Isolated version-overlay change

The second isolated change replaced transaction-sized deep copies and full-map delta scans with immutable shared bases and small version overlays. The root-once change remained in place.

| 256 low contention | Root once/coarse | Version overlays | Change |
| --- | ---: | ---: | ---: |
| Sequential | 0.492 ms | 0.473 ms | about 4% faster/noise |
| Parallel / 1 | 14.075 ms | 0.777 ms | 18.1x faster |
| Parallel / 8 | 6.282 ms | 0.843 ms | 7.5x faster |
| 1-to-8 speedup | 2.24x | 0.92x | — |
| Parallel efficiency | 28.0% | 11.5% | — |

### Block-size scaling after both changes

Command: `cargo bench -p execution --bench parallel_scaling -- low_contention --sample-size 20 --warm-up-time 1 --measurement-time 3 --noplot`.

Efficiency is `T1 / (workers * Tworkers)`, relative to parallel/1.

| Transactions | Sequential | Parallel / 1 | Parallel / 8 | Parallel / sequential time | 1-to-8 speedup | Parallel efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.473 ms | 0.777 ms | 0.843 ms | 1.78x slower | 0.92x | 11.5% |
| 1,024 | 1.885 ms | 3.267 ms | 3.324 ms | 1.76x slower | 0.98x | 12.3% |
| 4,096 | 7.992 ms | 15.818 ms | 14.802 ms | 1.85x slower | 1.07x | 13.4% |

Increasing the block size did not amortize the gap. After removing roots and coarse copies, this micro-workload is dominated by creating and collecting one Rayon task, speculative attempt, delta, and receipt for each extremely cheap native mutation. Parallel execution does not yet meaningfully beat sequential execution, so no execution TPS result is promoted as a headline performance claim.

### Fresh post-overlay profile

The optimized 256-transaction parallel/8 path was profiled again after the version-overlay change. The median benchmark time was 0.831 ms. Worker samples were normalized to the coordinator's speculative critical path; the coordinator's 30.1% Rayon completion-latch wait overlaps the worker categories and is not counted again.

| Category | Approximate elapsed-time share |
| --- | ---: |
| Shared snapshot and transaction-local clone | 2.8% |
| Overlay delta scanning | 4.5% |
| Scheduler and allocator synchronization | 5.3% |
| Native transaction execution | 4.6% |
| Pool dispatch | 2.2% |
| One final Merkle root | 41.1% |
| Commit, tracking, validation, allocation, and teardown | 39.5% |

A fresh 256-item no-op Rayon control measured pool dispatch at 18.63 microseconds. Call stacks further separated synchronization from allocation: Rayon wake-mutex blocking accounted for approximately 1.5% of wall time and allocator lock blocking approximately 1.0%. The remainder is primarily allocation/teardown CPU and serial tracking/commit work, not workers stalled on state locks. A full Block-STM MVMap is therefore not justified by lock contention in this profile.

### Realistic Move workload

The realistic low-contention variant publishes a compiled Move `Token` module and executes its `transfer` entry function for each transaction. Every call resolves the shared module and reads and writes two disjoint Move resource objects. It exercises the actual Move VM and state resolver rather than the native object-mutation shortcut.

Command: `cargo bench -p execution --bench parallel_scaling -- realistic_move --sample-size 20 --warm-up-time 1 --measurement-time 3 --noplot`.

#### Sequential atomic-commit repair

The first realistic sweep exposed a quadratic sequential defect: every Move call cloned and detached the accumulated block overlay to preserve session atomicity. That contaminated the apparent parallel advantage. The isolated repair validates a Move change set into a transaction delta and applies it once only after all operations succeed.

The clean sequential-only rerun, before any subsequent parallel optimization, was:

| Transactions | Defective sequential | Clean sequential | Improvement |
| ---: | ---: | ---: | ---: |
| 256 | 6.1594 ms | 2.2772 ms | 2.70x |
| 1,024 | 72.438 ms | 9.3565 ms | 7.74x |
| 4,096 | 1.0362 s | 39.307 ms | 26.36x |

The clean baseline is approximately linear at 8.9–9.6 microseconds per transaction. The immediately following, otherwise unmodified parallel rerun removed the earlier apparent win:

| Transactions | Clean sequential | Parallel / 1 | Parallel / 8 | Parallel / sequential time | 1-to-8 speedup | Parallel efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 2.2772 ms | 8.0024 ms | 3.3836 ms | 1.49x slower | 2.37x | 29.6% |
| 1,024 | 9.3565 ms | 31.858 ms | 13.129 ms | 1.40x slower | 2.43x | 30.3% |
| 4,096 | 39.307 ms | 130.46 ms | 50.816 ms | 1.29x slower | 2.57x | 32.1% |

#### Root and allocation investigation

Source inspection and profiling confirm that both executors call `StateTree::root` exactly once after canonical ordered commit. There is no per-transaction or per-worker-commit root on either path. Removing the one final full-state materialization, while retaining the full root calculation, changed realistic parallel/8 from 3.3836 to 3.2581 ms at 256 transactions and 13.129 to 12.567 ms at 1,024; the 4,096 result moved from 50.816 to 51.693 ms. The mixed result shows the materialization clone was not the dominant remaining cost.

A fresh sample then identified per-task Move VM construction and small Rayon work units. The implementation now reuses one executor per Rayon partition, sets a minimum grain of approximately `tasks / (4 * workers)`, consumes successful deltas instead of cloning their values, extracts tracked sets without cloning when uniquely owned, and avoids temporary declared/read/write sets during validation. Benchmark setup also compacts the reusable initial state before timing so Criterion's reset clone does not trigger an artificial first-transaction copy of an accumulated overlay.

The final 20-second sample of synthetic parallel/8 attributes the critical path approximately as follows. The coordinator waited for Rayon for 35.8% of wall time; that interval overlaps the worker categories and is not added again.

| Category | Approximate elapsed-time share |
| --- | ---: |
| Shared snapshot / transaction overlay setup | 2.2% |
| Overlay delta scanning | 3.3% |
| Scheduler and allocator synchronization | 4.4% |
| Native transaction execution | 3.1% |
| Pure pool dispatch | 2.4% |
| One final Merkle root | 49.3% |
| Commit, tracking, validation, allocation, and teardown | 35.3% |

The sample still shows allocation/free CPU as the principal speculative overhead; it does not show scheduler lock contention as the dominant problem. An attempted BCS-direct-to-BLAKE3 leaf encoding was rejected: it changed the 256 synthetic parallel/8 median from about 0.795 ms to 1.508 ms because many small `Write` calls outweighed the saved vector allocation. The faster contiguous-buffer encoding was restored.

#### Post-batching results

Final realistic Move medians are:

| Transactions | Sequential | Parallel / 1 | Parallel / 2 | Parallel / 4 | Parallel / 8 | Parallel / sequential time | 1-to-8 speedup | Parallel efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 2.1511 ms | 2.3916 ms | 2.0023 ms | 1.6545 ms | 1.5946 ms | 1.35x faster | 1.50x | 18.7% |
| 1,024 | 8.8761 ms | 9.7317 ms | 7.5651 ms | 5.7667 ms | 5.4620 ms | 1.63x faster | 1.78x | 22.3% |
| 4,096 | 36.522 ms | 41.162 ms | 32.031 ms | 24.087 ms | 22.237 ms | 1.64x faster | 1.85x | 23.1% |

The synthetic native mutation remains unfavorable and therefore keeps the performance promotion gate closed:

| Transactions | Sequential | Parallel / 1 | Parallel / 8 | Parallel / sequential time | 1-to-8 speedup | Parallel efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.4157 ms | 0.5896 ms | 0.6786 ms | 1.63x slower | 0.87x | 10.9% |
| 1,024 | 1.8877 ms | 2.5474 ms | 2.6217 ms | 1.39x slower | 0.97x | 12.1% |
| 4,096 | 7.1982 ms | 10.953 ms | 10.906 ms | 1.52x slower | 1.00x | 12.6% |

The realistic workload now benefits from parallelism on a correct sequential baseline, but scaling efficiency remains low and the near-zero-cost synthetic path is still slower. Consequently no raw execution TPS number is promoted as a headline result, and all figures remain execution-subsystem benchmarks rather than end-to-end blockchain TPS.

### Coordinator-wait and final cost investigation

The reported 35.8% Rayon coordinator wait was investigated as a possible load imbalance. Inclusive speculative samples for the eight workers were 1,987, 1,991, 2,023, 1,846, 1,967, 1,926, 1,924, and 1,921. Their coefficient of variation is 2.66%, with a 9.09% minimum-to-maximum spread. Work is therefore distributed evenly; the coordinator is parked while all workers perform useful speculation, rather than waiting for one straggler after most workers have gone idle. Reducing grain size is not supported by this evidence and would increase task overhead.

Two Merkle alternatives were measured independently. Parallel leaf/subtree hashing using Rayon was rejected because its scheduling and allocation overhead worsened within-run parallel scaling. The retained change reuses one BCS scratch buffer for all leaf encodings in the final root and keeps one leaf-hash vector. It preserves the once-per-block root and canonical tree algorithm. Relative to the immediately preceding run, synthetic parallel/8 time improved by 9.1%, 9.5%, and 8.4% at 256, 1,024, and 4,096 transactions respectively. Realistic results remained favorable but were noisy at the middle size.

An attempt to pool speculative-attempt vectors with a Rayon `fold` was also rejected in isolation. It improved the smallest synthetic case, was neutral at larger synthetic sizes, and regressed the 4,096-transaction realistic parallel/8 median by 7.1%; the changed partition behavior cost more than the saved allocations.

The retained commit/validation change exploits the structural fast-path invariant directly. Fast lanes are already disjoint from optimistic transactions, and transactions belonging to the same owner execute serially inside one lane. Canonical commit therefore no longer scans or populates the optimistic committed-write set for fast-lane attempts. Runtime undeclared-access validation and receipt read/write sets remain hard requirements. All equivalence and Loom tests passed before measuring the change.

Final paired medians after the scratch-buffer and structural-validation changes are below. Efficiency remains `T1 / (8 * T8)` and is intentionally reported ahead of any throughput conversion.

| Synthetic native mutation | Sequential | Parallel / 1 | Parallel / 8 | Parallel / sequential time | 1-to-8 speedup | Parallel efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.3352 ms | 0.4993 ms | 0.5565 ms | 1.66x slower | 0.90x | 11.2% |
| 1,024 | 1.4681 ms | 2.0381 ms | 2.1305 ms | 1.45x slower | 0.96x | 12.0% |
| 4,096 | 6.1103 ms | 9.0290 ms | 8.6847 ms | 1.42x slower | 1.04x | 13.0% |

| Realistic Move `Token::transfer` | Sequential | Parallel / 1 | Parallel / 8 | Parallel / sequential time | 1-to-8 speedup | Parallel efficiency |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 2.1438 ms | 2.3207 ms | 1.4655 ms | 1.46x faster | 1.58x | 19.8% |
| 1,024 | 8.4488 ms | 9.4033 ms | 5.0052 ms | 1.69x faster | 1.88x | 23.5% |
| 4,096 | 35.922 ms | 42.391 ms | 20.143 ms | 1.78x faster | 2.10x | 26.3% |

The final 30-second synthetic 256/8 sample attributes 63.2% of coordinator samples to the Rayon completion latch, overlapping worker execution; 28.6% to the single final Merkle root; and 8.2% to ordered commit, validation, receipt construction, dispatch, and other coordinator work combined. This is a coordinator wall-time view rather than an additive CPU-cost table. It confirms that committed-set validation is no longer a material serial cost. Remaining worker stacks are dominated by transaction-local overlay/access-set allocation and teardown. Incremental Merkle nodes and a genuinely persistent MVMap are the next architectural opportunities, but neither is introduced as an unmeasured late change.

#### Known bounded limitation: very cheap transactions

Very cheap, low-work transactions remain slower under the parallel executor than under the sequential reference at every tested block size: 1.66x slower at 256 transactions, 1.45x slower at 1,024, and 1.42x slower at 4,096. This synthetic native-mutation workload is dominated by transaction-local overlay, access-set, scheduling, receipt, and final-tree bookkeeping rather than application execution. It is intentionally retained as a diagnostic so this regression cannot be hidden by the realistic Move result.

This limitation is documented technical debt, not a Phase 2 sequencing blocker. The remaining credible remedies are incremental Merkle nodes and a genuinely persistent MVMap; both are structural changes and are deferred rather than pursued as further micro-optimization of the current architecture. Phase 2's core performance goal is considered met because realistic Move transactions consistently benefit from parallel execution on the corrected sequential baseline, while hard CI assertions require parallel and sequential execution to produce identical state roots.

Phase 2 is therefore sufficiently complete to proceed to Phase 3 in the engineering build sequence, carrying this bounded limitation forward explicitly. No execution-only TPS figure is promoted. These measurements exclude consensus, networking, durable storage, and RPC, so they are not comparable to blockchain TPS. A legitimate milestone throughput figure must wait for the Phase 3/4 socket-level, multi-process harness.

## Verification

The Phase 2 workspace passes:

- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`

On the benchmark machine, the test and clippy commands require Xcode's library directory in `LIBCLANG_PATH` and `DYLD_LIBRARY_PATH` so the existing RocksDB bindgen dependency can load `libclang.dylib`. This is a local build-environment requirement, not a source change.

## Explicitly deferred

- Incremental Merkle nodes; the current root is computed once per block without materializing a cloned map, but still re-encodes and rebuilds the tree from the full logical state. This is one of the two structural changes expected to address the documented cheap-transaction regression.
- A genuinely persistent MVMap that reduces transaction-local overlay and allocation costs. This is the second structural change expected to address the documented cheap-transaction regression.
- Adaptive fallback that switches a detected hot key or block tail to sequential execution.
- Persistent cross-block speculative versions and reads from already-published lower-index transaction versions.
