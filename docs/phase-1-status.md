# Phase 1 status

## Implemented

- Deterministic active and expired Merkle prefix tries.
- Version-checked object creation, mutation, deletion, and transfer.
- Fixed per-object, per-epoch rent accounting with exact expiry transitions.
- Expired-object history retaining the last active root.
- Pinned reference Move VM with 32-byte Kestrel addresses.
- Atomic module publication and entry-function sessions backed by object state.
- Single-threaded ordered block executor and per-transaction receipts.
- A compiled Move token fixture covering publication, minting, resource mutation, and transfer.

## Acceptance evidence

- Move token module deploys, mints two resources, and transfers value end-to-end.
- Property-generated transaction lists replay to identical final state roots.
- Property-generated insertion orders produce identical Merkle roots.
- Rent test confirms an object with balance 5 and rate 2 remains active through epoch 2 and expires at epoch 3.
- Ownership and stale-version checks reject invalid native-object mutations.

## Explicitly deferred

- Persistent state snapshots and state sync.
- Gas schedule and fee charging.
- Standard-library/framework publication and production token access control.
- Transaction-envelope signature and nonce validation in the execution adapter.
- Parallel execution and conflict detection (Phase 2).
- Expired-object witness resurrection (Phase 5).

