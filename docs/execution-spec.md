# Kestrel execution specification

## Phase 1 sequential executor

`SequentialExecutor` applies a block's operations in list order on one thread. Each Move VM session is atomic: VM effects are validated and prepared as a transaction-local delta, and the delta is applied only after every transition succeeds. This avoids cloning the accumulated block overlay for every call. If a later transaction fails, earlier successful transactions remain committed and execution stops at the failing index.

Supported operations are compiled-module publication and Move entry-function calls. Receipts contain the transaction index, event count, compute consumed, access metadata, and the containing block's final state root. The root is computed once after canonical ordered commit and is shared by every receipt in that block.

## Move VM host

Kestrel pins the Apache-2.0 Move reference implementation at revision `c453c90994cba0af30314fe4134acee57fb20d0c`. The host uses 32-byte Move addresses and no chain-specific native functions in Phase 1.

The adapter:

- resolves bytecode modules and resources from Kestrel objects;
- asks the Move VM to verify module publication and entry execution;
- maps Move change sets into object creation, mutation, and deletion;
- assigns a configured initial rent balance to new Move objects;
- materializes signer arguments from the authenticated transaction sender rather than accepting arbitrary signer bytes;
- commits an effect set only after every state transition succeeds.

Type arguments, chain-specific native functions, signed transaction-envelope decoding, and production access-control framework modules are explicitly deferred. The Phase 1 token module is an execution fixture, not a production asset contract.

Gas charging is no longer deferred: `MoveVmHost` meters real Move bytecode against a caller-supplied `gas_limit` using the pinned Move reference implementation's own per-instruction cost schedule (`move-vm-test-utils`'s `GasStatus`/`INITIAL_COST_SCHEDULE`), and native object operations are charged a flat compute cost checked against the same budget. Exceeding the limit aborts the operation atomically with no partial state mutation. `ExecutableTransaction.compute_limit` is the single source of truth for this budget, and consumed compute is reported per transaction through `ExecutionReceipt`/`DeferredExecutionResult`. Converting that consumed-compute number into an actual charged fee credited to a validator (`mempool::FeeLedger::settle`) is not yet wired into the live node pipeline — see `docs/TECH_DEBT.md` TD-011.

Phase 6's node lifecycle now owns one signed-envelope boundary above the Phase 1
VM: it selects the active genesis signature scheme, binds the public key to the
sender address, verifies the signature, enforces the next durable nonce, decodes
the BCS `ExecutableTransaction`, and requires its sender to match the envelope.
Live network/RPC admission now routes through this same boundary (see
`docs/TECH_DEBT.md` TD-002/TD-005); fee settlement is not yet connected to it.

## Determinism requirement

Given identical starting state, genesis state configuration, module bytecode, and ordered calls, execution must produce identical receipts and final roots. Property tests vary mint and transfer values, replay each block against two independent VMs and trees, and assert root equality.

## Phase 2 parallel executor

`ParallelExecutor` speculates transactions on an immutable, shared block-start state version using a fixed Rayon worker pool. Each transaction receives a small private overlay rather than a deep copy of the full object maps. Every state lookup and mutation records an object-key read/write set, and successful speculation publishes the overlay as an exact deterministic state delta; failed speculation is never committed. Overlay differences inspect only versioned keys when views share a base version.

The commit pass visits transactions only in canonical block order. A speculative result is valid when its read and write sets do not intersect keys written by earlier canonical commits. Conflicts are aborted and re-executed against the current canonical state. After the final ordered delta is applied, the executor computes one canonical block state root and attaches it to every receipt; the sequential reference uses the same block-root receipt semantics. The first canonical failure stops the block while retaining earlier commits.

Transactions must declare every object reference and its access mode. Undeclared runtime access rejects the transaction. A transaction whose declared references all name single-owner objects owned by its sender may use the structural fast path. Eligible transactions are partitioned into deterministic owner lanes; each lane executes directly in transaction order while distinct owners execute concurrently. Any object also referenced by a non-fast-path transaction, or the same object ID declared by different owner lanes, disqualifies every transaction touching that object from the fast path. Ownership-changing operations leave the fast path, and resurrection eligibility derives the owner from the authenticated witness rather than trusting only the declaration.

Move bytecode and modules continue to use the Phase 1 VM host. Native object operations exist in the same ordered adapter so scheduler invariants, structural ownership, and contention behavior can be exercised without changing state semantics.

## Phase 5 co-resident EVM

`vm-evm` embeds pinned `revm` 36 in the same process and state transition as native Move objects. It reserves address `0x0000000000000000000000000000000000000f00` as the native-object precompile. Operation `0` reads an object by its 32-byte ID. Operation `1` writes new object bytes using an expected version; writes through `STATICCALL` and stale versions fail.

Each EVM transaction runs with a candidate clone of native state. Native-object effects become visible only when the EVM transaction succeeds, so a revert, halt, validation error, or bridge failure cannot leak partial native writes. A deployed forwarding-contract fixture proves both directions with the actual Move VM: it publishes the Move `Token` module and calls `mint`, EVM bytecode reads and mutates the resulting resource through the precompile, and a subsequent Move `transfer` call observes and mutates that same resource.

Resurrection is also an execution operation. Both sequential and parallel executors consume the same root-free witness transition, including the expired-tree removal, and compute the state root only after canonical block commit. A cross-executor regression test requires identical final roots after resurrection and mutation.

## Phase 6 durable handoff

After successful deferred execution, the worker emits a root-bound
`StateSnapshot` with the block result. `BlockLifecycle` writes the finalized
order and certificate, KestrelCast payload commitment, ordered transaction IDs,
state root, snapshot, and next nonces in one RocksDB batch. RPC state advances
only after that batch succeeds. Restart restores the snapshot and nonce map.
The shipped `node` binary's validator path (`ConsensusCoordinator::bind_with_pipeline`
plus `Stage2Pipeline`) submits its live finalized orders into this handoff
directly; running that composition across real separate machines with fault
injection remains open (see `docs/TECH_DEBT.md` TD-003).
