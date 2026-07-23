# Kestrel technical-debt register

This is the canonical list of open implementation debt as of 2026-07-22. Phase
status documents remain historical records, but new debt and closure evidence
should be tracked here. This workspace export has no Git metadata, so an exact
introducing commit cannot be recovered; each entry records the introducing
phase and source document instead.

Priority meanings:

- **Safety-critical:** blocks any claim that the relevant safety gate is met.
- **High:** blocks an integrated Stage 2 or production-readiness claim.
- **Medium:** bounded protocol, performance, or maintainability limitation.
- **Low:** portability or operator-experience limitation.

## Open debt

### TD-001 — Byzantine safety campaign cannot observe conflicting node decisions

- **Priority:** Medium; narrowed from High. A genuine per-validator,
  randomized exploration now exists and runs as part of the chaos campaign;
  `ConsensusSimulator`'s original single-aggregate-outcome model (used for the
  broader fault/liveness sweep across all 11 existing call sites) is
  unchanged, since generalizing it fully would have required re-deriving its
  closed-form latency/certificate semantics with real risk of silently
  breaking existing timing assertions across five call sites for no added
  safety value.
- **Introduced:** Phase 4; carried into the Phase 6 chaos campaign; narrowed
  post-Phase-6. Sources: `phase-4-status.md`, `phase-6-status.md`,
  `testkit::ConsensusSimulator`, `testkit::explore_equivocating_proposal_safety`.
- **Why deferred/exposed:** The deterministic simulator still returns one
  aggregate finalization result per height for its main fault/liveness sweep.
  Added alongside it: `explore_equivocating_proposal_safety`, which builds a
  real, independent, production `consensus::Replica` for every honest
  validator in a freshly randomized validator set each trial, has an
  equivocating leader sign two different proposals for the same height/view,
  splits the honest validators into two randomized delivery groups (one per
  proposal), drives every honest validator's vote and finalization decision
  through its own real `Replica` (this function only routes messages — it
  never signs on an honest validator's behalf), and asserts pairwise
  agreement: no two independent honest replicas may finalize different blocks
  at the same height, and the two delivery groups may not both independently
  reach an 80% fast certificate. Verified the detection logic is real, not
  vacuous: temporarily busting the enforced <20% Byzantine budget in this
  function caused it to correctly catch 24 genuine cross-replica
  disagreements out of 200 randomized trials; restored to the correct budget,
  200 trials (each with a fresh random validator count, Byzantine subset, and
  delivery split) find zero violations, run as part of
  `ChaosCampaign::run_simulated` and asserted in
  `chaos::tests::hundred_scenario_campaign_has_no_safety_or_liveness_failure`.
  This generalizes the single hand-crafted `production_cross_view_fast_safety`
  scenario (which tests a related but different property: that a formed fast
  certificate's voters cannot also form a competing timeout certificate) into
  an actual randomized per-node state-space exploration, using real Replica
  code throughout rather than one aggregate outcome.
- **Expected resolution:** Extend per-validator replica modeling to partition
  and message-drop fault dimensions (today's addition covers equivocation
  specifically), and consider whether the main `ConsensusSimulator` sweep
  should eventually move to the same per-replica model — weighed against the
  risk of disturbing its five existing call sites' timing assumptions.
  Continue using independent per-node observations in the real multi-process
  and external chaos harnesses as well.

### TD-002 — Consensus is not wired to the durable application lifecycle

- **Priority:** Medium; narrowed from High. The full production binary now runs
  this composition; what remains is durability/governance hardening, not wiring.
- **Introduced:** Phase 4 deferred-execution scope; substantially closed
  post-Phase-6. Sources: `consensus-spec.md`, `phase-6-status.md`,
  `crates/node/tests/stage2_pipeline_integration.rs`, and
  `crates/node/tests/stage2_node_rpc_integration.rs`.
- **Why deferred/exposed:** `BlockLifecycle` validates signed envelopes and
  canonical nonces, consumes a final certificate plus reconstructed KestrelCast
  payload, runs `DeferredExecutor`, atomically commits the block/certificate and
  root-bound state snapshot, advances RPC state, and restores after restart. The
  shipped `node` binary's validator path (`main.rs`) now constructs a real
  `network::NetworkNode`, `BlockLifecycle`, and `Stage2Pipeline` and calls
  `ConsensusCoordinator::bind_with_pipeline`, replacing the old
  synthetic-proposal-only path entirely — there is no longer a "library code
  exists but the binary doesn't use it" gap. `crates/cli`'s `validator-init` now
  also generates and persists the libp2p gossip identity each validator needs,
  and genesis carries each validator's `gossip_peer_id`/`gossip_address`. A
  process-level integration test spawns the actual compiled `node` binary across
  four separate OS processes and asserts a transaction submitted over public RPC
  is admitted, gossiped, ordered, executed, and committed identically everywhere.
- **Expected resolution:** Remaining work is tracked as its own items: durable
  pre-consensus admission (TD-012/TD-015), epoch/validator-set reconfiguration
  (TD-013), and a real multi-process Stage 2 devnet with fault injection and
  published propagation/finality numbers (TD-003, TD-020).

### TD-003 — Stage 2 transport remains split and lacks an integrated block lifecycle

- **Priority:** High.
- **Introduced:** Phase 3 and Phase 4; partially advanced during Phase 6. Exact
  commit unavailable. Sources: `phase-3-status.md`, `phase-4-status.md`,
  `phase-6-status.md`, and `crates/node/tests/stage2_node_rpc_integration.rs`.
- **Why deferred/exposed:** The shipped `node` binary's validator path now always
  builds a real `network::NetworkNode` (libp2p transaction gossip + `KestrelCast`
  shred request/response) composed with real consensus ordering, deferred
  execution, and durable RocksDB persistence — proven both in-process (four
  nodes, one process, `stage2_pipeline_integration.rs`) and across four separate
  OS processes with a transaction submitted over public RPC
  (`stage2_node_rpc_integration.rs`). `tests/stage_2_processes.rs`'s five-process
  Byzantine-fault suite (killed/partitioned/equivocating leaders, corrupt/withheld
  votes) now also runs this same real pipeline, not the old synthetic path, and
  still passes. The raw-TCP `ConsensusCoordinator` proposal/vote/certificate
  transport remains a deliberately separate channel from libp2p (documented, not
  unified — its own `CoordinatorFaults` fault-injection knobs are unaffected by
  which proposal source feeds it). Still outstanding: this has only run on one
  machine over loopback for seconds at a time; nobody has injected socket-level
  latency/loss/relay death specifically on the libp2p transaction/shred paths (as
  opposed to the raw-TCP consensus messages, which already have this), and nobody
  has measured propagation-to-80%-stake or end-to-end finality under those
  conditions or across real network topology/geography.
- **Expected resolution:** Inject socket-level latency/loss and relay death on
  the libp2p transaction/shred paths specifically, then run a real Stage 2
  private devnet (external of this workspace's automated tests) across real
  machines/geography and record propagation-to-80%-stake and end-to-end finality
  numbers, per §9 Stage 2's exit criteria.

### TD-005 — Signed admission is not connected to network/RPC ingress and the mempool

- **Priority:** Medium; narrowed from High. Both gossip and RPC ingress are now
  proven through the same admission boundary in the production binary; only
  durability remains.
- **Introduced:** Phase 1 explicit deferral; substantially closed post-Phase-6.
  Sources: `phase-1-status.md`, `crates/node/tests/stage2_pipeline_integration.rs`,
  and `crates/node/tests/stage2_node_rpc_integration.rs`.
- **Why deferred/exposed:** `BlockLifecycle` and `Stage2Pipeline` share one
  `TransactionValidator` instantiated from genesis. `rpc::RpcService` now takes
  an optional `TransactionSubmitter` and exposes a `kestrel_submitTransaction`
  JSON-RPC method; the shipped `node` binary wires this to
  `Stage2PipelineHandle`, so a transaction submitted over public RPC is admitted
  through the same validator, gossiped, selected by the real mempool (no
  synthetic transaction IDs), and is the one a leader actually proposes. A
  process-level test proves this by submitting a transaction to one of four real
  `node` processes over RPC and confirming every process (including the three
  that never saw the RPC call) commits the identical result. What remains open:
  there is still no durable pre-consensus admission queue — an accepted-but-not-
  yet-finalized transaction is only held in process memory, so a crash between
  RPC admission and finalization loses it (the sender must resubmit; no double
  execution risk since nonces gate re-admission).
- **Expected resolution:** Persist accepted payloads and nonce reservations
  durably (shared scope with TD-012/TD-015) so a restart does not silently drop
  an admitted, not-yet-finalized transaction.

### TD-006 — Signature-scheme activation has no governance or epoch transition

- **Priority:** High dependency risk; governance itself remains an initial-build
  non-goal.
- **Introduced:** Phase 0 static-genesis stub. Exact commit unavailable. Sources:
  `phase-0-status.md` and `engineering-build-spec.md` section 8.
- **Why deferred:** No governance module or validator-set reconfiguration exists.
  The allowlist is immutable genesis data, and `SchemeRegistry` has no runtime
  activation transition.
- **Expected resolution:** Before any signature-scheme migration, specify an
  authenticated governance/upgrade proposal, activation epoch and delay,
  compatibility window, rollback/emergency policy, state commitment, and
  deterministic registry update applied by every node.

### TD-008 — Parallel execution remains slower for very cheap transactions

- **Priority:** Medium, bounded performance limitation.
- **Introduced:** Phase 2. Exact commit unavailable. Source:
  `phase-2-status.md` lines under “Known bounded limitation”.
- **Why deferred:** Synthetic native mutations do too little application work to
  amortize overlays, access sets, task scheduling, receipts, and the final full
  tree rebuild. Further micro-tuning of the current architecture was rejected
  after measurement.
- **Expected resolution:** Implement incremental Merkle nodes and a genuinely
  persistent Block-STM-style MVMap, then repeat synthetic and realistic
  block-size/core-count sweeps while retaining sequential-root equivalence as a
  hard gate.

### TD-009 — Parallel versioning lacks adaptive and cross-block features

- **Priority:** Medium.
- **Introduced:** Phase 2. Exact commit unavailable. Source:
  `phase-2-status.md` explicit deferrals.
- **Why deferred:** The current shared base plus transaction-local overlay is
  correct and sufficient for the measured realistic workload, but it does not
  publish lower-index transaction versions for speculative readers, persist
  versions across blocks, or switch a hot tail to sequential execution.
- **Expected resolution:** Extend the persistent MVMap with indexed versions,
  deterministic dependency/retry scheduling, version reclamation, cross-block
  lifecycle rules, and an evidence-based adaptive hot-key fallback.

### TD-011 — Move gas metering and fee settlement are not integrated

- **Priority:** Medium; narrowed from High. Gas metering itself is now real,
  deterministic, and enforced everywhere execution runs. Fee settlement
  (charging the metered amount and crediting a validator) is not yet wired
  into the live node pipeline.
- **Introduced:** Phase 1 explicit deferral; Phase 5 added a separate fee
  ledger; gas metering closed post-Phase-6. Sources: `phase-1-status.md`,
  `mempool-spec.md`, `crates/vm-move/src/lib.rs`, `crates/execution/src/lib.rs`.
- **Why deferred/exposed:** `MoveVmHost::publish_module`/`execute_entry_function`
  now meter real Move bytecode via `move-vm-test-utils`'s reference `GasStatus`
  and `INITIAL_COST_SCHEDULE` (the same per-instruction cost table used
  upstream) instead of `UnmeteredGasMeter`, bounded by a caller-supplied
  `gas_limit`; exceeding it aborts atomically with `MoveHostError::OutOfGas`
  and leaves state unchanged (verified by a dedicated test). Native object
  primitives (create/mutate/delete/transfer/resurrect), which bypass the Move
  VM entirely, are charged a flat `NATIVE_OPERATION_COMPUTE_COST` checked
  against the same limit. `ExecutableTransaction.compute_limit` is now the
  single source of truth for this budget (the redundant
  `SignedExecutionPayload.compute_limit` field was removed), and real
  consumed-compute values now flow out through `MoveExecutionResult`,
  `OperationResult`, `ExecutionReceipt`, and `DeferredExecutionResult` —
  available per transaction, not fabricated. The
  parallel-vs-sequential state-root equivalence property (the codebase's most
  important correctness invariant) was re-verified with metering enabled and
  still holds. What remains unwired: `mempool::FeeLedger::settle` (debit payer,
  credit validator, enforce compute-limit conservation) is fully implemented
  but still only exercised by the mempool crate's own unit tests — nothing in
  `BlockLifecycle`/`Stage2Pipeline` calls it with the now-real per-transaction
  `compute_used`, there is no genesis mechanism to fund a payer's `FeeLedger`
  balance, and `FeeLedger` balances are not durably persisted. `vm-evm`'s own
  gas handling (hardcoded `gas_limit`/zero `gas_price`, discarded `gas_used`)
  is unaffected by this and remains a separate, fully decoupled sub-problem
  (see TD-016).
- **Expected resolution:** Retain per-transaction fee-bid metadata from mempool
  selection through to block commit, have `Stage2Pipeline` own a `FeeLedger`
  and call `settle()` for each committed transaction crediting that height's
  leader, add a genesis-level initial-balance mechanism for testability, persist
  `FeeLedger` balances durably (shared scope with TD-012), and ensure a
  settlement failure (e.g. insufficient payer balance) is logged rather than
  rejecting an already-finalized, already-executed block.

### TD-012 — State synchronization, in-flight recovery, and pruning are incomplete

- **Priority:** High; narrowed. Both in-flight recovery gaps identified in the
  original deferral are now closed and tested; snapshot transfer,
  cross-network sync, and pruning remain untouched.
- **Introduced:** Phase 1 explicit deferral; remains a Phase 6 promotion blocker.
  Exact commit unavailable. Sources: `phase-1-status.md`, `state-spec.md`, and
  `phase-6-status.md`.
- **Why deferred/exposed:** The application lifecycle atomically persists a
  finalized block record, certificate, nonces, and root-bound full-state
  snapshot, restores it before execution, and rejects snapshot tampering, and
  the production `node` binary now drives it directly (see TD-002). An
  accepted-but-not-yet-finalized transaction is now durably logged and replayed
  on restart (TD-015). A finalized-but-not-yet-committed block — validated,
  handed to `DeferredExecutor`, but not yet through `poll_commit`'s atomic
  write when a crash occurs — is now also durably recorded before submission
  (`BlockLifecycle::submit_payload` in `crates/node/src/lifecycle.rs`) and
  atomically cleared in the same `WriteBatch` as the commit. On restart,
  `BlockLifecycle::open` replays any leftover record straight back into a
  freshly created executor, requiring strict height contiguity from the last
  committed checkpoint and failing closed (`LifecycleError::PendingReplayGap`)
  on any gap rather than silently continuing — this path no longer depends on
  the network resending a block the node already validated. Covered by
  `finalized_block_submitted_before_a_crash_still_commits_after_restart` in
  `crates/node/tests/block_lifecycle.rs`, verified to fail without the fix.
  There is still no authenticated snapshot transfer to a late-joining node, no
  cross-network state-sync protocol, and no pruning policy — these are
  materially larger, riskier features than the two recovery gaps just closed
  and remain open design questions rather than narrow bugs.
- **Expected resolution:** Design and add authenticated snapshot and
  block-history transfer for late-joining nodes, define a retention/pruning
  policy, define a cross-network state-sync protocol, and measure bootstrap and
  crash-recovery bounds end-to-end.

### TD-013 — Epoch transitions and validator reconfiguration are incomplete

- **Priority:** High; narrowed. Rent-epoch advancement is now real and wired
  into the live pipeline. Validator/stake reconfiguration is untouched and
  remains the harder, higher-risk half of this item.
- **Introduced:** Phase 4; slashing directives added in Phase 6 without the
  applying transition; rent-epoch wiring closed post-Phase-6. Exact commit
  unavailable. Sources: `phase-4-status.md`, `consensus-spec.md`,
  `crates/execution/src/lib.rs`, `crates/node/tests/block_lifecycle.rs`.
- **Why deferred/exposed:** `StateTree::advance_to_epoch` (deterministic
  per-epoch rent decrement and expiry) was fully implemented and tested at the
  `state` crate level but had zero call sites in the live pipeline —
  `blocks_per_epoch` from genesis was validated for nonzero-ness and otherwise
  never read. `DeferredExecutor::new` now takes `blocks_per_epoch` and its
  worker deterministically advances state to `Epoch(height / blocks_per_epoch)`
  before executing each block's transactions, so the reported state root
  already reflects that epoch's rent accounting — every validator computes the
  identical result since it is a pure function of height and a genesis
  constant, not wall-clock time. A new test drives this through the real
  `BlockLifecycle` pipeline (not by calling `advance_to_epoch` directly):
  committing blocks alone, with no transaction touching an object, decrements
  its rent balance every epoch, the decremented balance and epoch survive a
  full `BlockLifecycle` restart (proving durability, since `Epoch` is part of
  the persisted `StateSnapshot`), and continuing far enough expires the object.
  Validator/stake reconfiguration is deliberately untouched: `ValidatorSet` is
  immutable, `Clone`-only data threaded **by value** into every safety check in
  `Replica` (leader election, proposal/vote/certificate verification) and
  independently duplicated inside `ConsensusCoordinator` and `VoteCollector`,
  with no shared handle, no durable representation in `ReplicaSnapshot`, and no
  setter anywhere — confirmed the highest-risk correctness surface in the
  codebase. `SlashingPolicy::adjudicate`/`EvidenceBook::record` fully implement
  evidence verification and directive computation but are exercised only by
  `consensus`'s own unit test; nothing constructs an `EvidenceBook`, submits
  evidence on-chain, or applies a `SlashingDirective` to a validator's stake
  anywhere in `node` or `testkit`.
- **Expected resolution:** Validator reconfiguration needs its own focused pass
  (deliberately deferred rather than rushed alongside rent-epoch wiring): add an
  on-chain mechanism for evidence submission so every honest node agrees on the
  identical set of applied directives (not just its own local observations),
  a durable slash queue applied deterministically at a clean epoch boundary,
  and a safe swap-in point for the resulting `ValidatorSet` in both `Replica`
  and `ConsensusCoordinator` — preserving consensus locks/votes across the
  boundary and adding restart/boundary-fault tests with the same rigor the
  codebase already applies to consensus safety.

### TD-014 — Production peer discovery, reputation, and relay tuning are absent

- **Priority:** High for public deployment; narrowed. Configured bootstrap
  peers and a real ban mechanism now exist; NAT traversal, in-band reputation
  scoring, operator policy, and telemetry-driven tuning do not.
- **Introduced:** Phase 3 explicit deferral; bootstrap peers closed alongside
  TD-002, banning closed while scoping this item. Exact commit unavailable.
  Sources: `networking-spec.md`, `phase-3-status.md`, `crates/network/src/service.rs`.
- **Why deferred/exposed:** Discovery is mDNS-only for LAN devnets, but every
  validator's genesis-configured `gossip_peer_id`/`gossip_address` is now dialed
  explicitly as a `ConfiguredPeer` (closed alongside TD-002/TD-003) — bootstrap
  no longer depends on mDNS for the validator set itself. `network::NetworkHandle`
  now exposes `ban_peer`, backed by a real `libp2p::allow_block_list::Behaviour`:
  banning a peer closes its live connection immediately and blocks all future
  connections in both directions (tested directly: a real two-node swarm, ban,
  then confirm the connection drops and its messages never arrive again — and
  confirmed the test fails without the ban call). `Stage2Pipeline` tracks an
  in-memory offense count per gossip peer and calls `ban_peer` once a peer's
  invalid-message count crosses a configurable threshold (default 8; the
  pure counting/threshold logic has its own unit test). What remains absent:
  bans are in-memory only (not persisted, do not survive a restart, and are not
  shared/gossiped between nodes — a peer banned by one validator is unknown to
  the rest), there is no NAT traversal (AutoNAT/relay/hole-punching) for
  non-LAN/non-preconfigured peers, no operator-facing peer policy (allow/deny
  lists, manual unban), and no telemetry on ban/offense rates or
  relay-selection/replication tuning driven by observed network conditions.
- **Expected resolution:** Persist and gossip ban/offense state so a peer
  banned by one validator does not stay trusted by the rest, add an
  authenticated public discovery/NAT strategy for non-preconfigured peers,
  expose operator controls (manual ban/unban, policy configuration), add
  telemetry for offense/ban events, and drive relay-selection/replication
  tuning from observed network conditions.

### TD-015 — Mempool and application sequencing remain in-memory and disconnected

- **Priority:** Medium; narrowed from High. The durable-admission half is
  closed; application ordering-policy versioning and the TEE/adversarial-load
  scope remain open.
- **Introduced:** Phase 5 explicit deferral; durable admission closed
  post-Phase-6. Sources: `mempool-spec.md`, `phase-5-status.md`,
  `crates/node/src/pipeline.rs`.
- **Why deferred/exposed:** Validated libp2p gossip and RPC ingress are wired
  into the localized mempool via the shared `Stage2Pipeline` admission path
  (see TD-002/TD-005). An admitted-but-not-yet-finalized transaction is now
  also durably logged: `PipelineProposalSource::admit` persists the signed
  transaction to the node's single `RocksDB` store (shared with
  `BlockLifecycle` via `BlockLifecycle::store_handle` rather than opening a
  second store) before touching any in-memory mempool state, so a storage
  failure aborts admission with nothing partial to unwind. `note_committed`
  and `rollback` remove the durable entry once it is no longer needed.
  `Stage2Pipeline::new` replays every persisted entry on startup by calling
  the same `admit` path against the freshly restored nonces, so a
  transaction already finalized before the crash is naturally rejected as
  stale (not silently reinstated) rather than needing separate
  cross-referencing logic. A dedicated test proves this across a real
  drop-and-reopen of every component holding the store (standing in for a
  process crash): admit, tear down without ever finalizing, reopen at the
  same data directory, and confirm the transaction is proposable again with
  no resubmission -- verified to actually depend on the replay logic by
  disabling it and confirming the test fails first. Still open: application
  ordering policies (`OrderingPolicy` registrations) remain immutable
  in-process state, not versioned or persisted; TEE fair ordering, encryption,
  cross-scope policy, and adversarial scheduler infrastructure remain
  explicitly out of the base implementation; and stress-testing localized
  fees under adversarial load (distinct from the durability property closed
  here) has not been done.
- **Expected resolution:** Persist and version `OrderingPolicy` registrations
  (shared next step with TD-012's broader state-sync/pruning scope), stress
  localized fees and hooks under adversarial load, and scope any TEE or
  encrypted ordering design as a separately reviewed subsystem.

### TD-016 — EVM host lacks production envelopes, persistence, and bridge hardening

- **Priority:** High.
- **Introduced:** Phase 5 explicit deferral. Exact commit unavailable. Sources:
  `evm-spec.md` and `phase-5-status.md`.
- **Why deferred:** EVM accounts live in the host instance; there are no signed
  EVM transaction envelopes, durable account commits, Solidity bindings,
  production gas calibration, or defined cross-VM reentrancy rules. Arbitrary
  Move entry functions are not exposed as precompiles.
- **Expected resolution:** Define chain ID/signature/nonce admission, persist EVM
  account and code state atomically with native state, version and audit the
  bridge ABI, meter calls, specify reentrancy/call-depth semantics, and add crash
  and adversarial integration tests.

### TD-017 — Small resurrection proofs still use the binary trie

- **Priority:** Medium.
- **Introduced:** Phase 5 explicit compromise. Exact commit unavailable. Sources:
  `state-spec.md` and `phase-5-status.md`.
- **Why deferred:** The implementation compressed the existing binary-Merkle
  witness rather than changing root semantics to a Verkle/vector commitment.
  Trusted setup/library choice, proof aggregation, and historical-root retention
  were left open.
- **Expected resolution:** Select and audit a commitment scheme, design a
  deterministic root migration and compatibility period, batch/aggregate proofs,
  define setup governance if needed, and set retention/pruning policy.

### TD-018 — Move framework and access-control surface are fixture-grade

- **Priority:** Medium.
- **Introduced:** Phase 1 explicit deferral. Exact commit unavailable. Sources:
  `execution-spec.md` and `phase-1-status.md`.
- **Why deferred:** There is no standard framework publication, production token
  access control, chain-specific native-function set, or type-argument support;
  the Token module is a test fixture.
- **Expected resolution:** Define the genesis framework and upgrade policy,
  implement and meter audited natives, add type arguments, enforce publication
  and capability rules, and replace fixture-only assumptions with conformance
  tests.

### TD-019 — RPC and operations need production hardening

- **Priority:** High for public deployment.
- **Introduced:** Phase 6 explicit deferral. Exact commit unavailable. Source:
  `phase-6-status.md`.
- **Why deferred:** TLS termination, distributed denial-of-service controls,
  archival/indexer APIs, gRPC/WebSocket surfaces, production load tests, and a
  concrete authorized cloud/Kubernetes chaos adapter live outside the current
  local code-readiness pass.
- **Expected resolution:** Define deployment architecture and trust boundaries,
  load/abuse-test RPC, add required subscription/indexing APIs, connect metrics
  and alerts, and implement narrowly scoped external chaos controls in the
  deployment repository.

### TD-020 — Public-testnet Stages 2–5 remain unpassed

- **Priority:** Release gate.
- **Introduced:** Phase 6 staged plan. Exact commit unavailable. Sources:
  `engineering-build-spec.md` section 9 and `phase-6-status.md`.
- **Why deferred:** Stage 2 is not an integrated transaction-processing network;
  Stages 3–4 require external operators, geography, state sync, real epochs,
  sustained campaigns and public measurements; Stage 5 additionally requires a
  separate go-live review and external audit.
- **Expected resolution:** Close TD-001 through the integrated Stage 2 blockers,
  then execute every stage in order and publish the required evidence without
  substituting simulator or subsystem measurements for public-network results.

## Resolved historical deferrals

The following remain mentioned in phase-status documents because those files are
historical snapshots, but they are not open debt in their original form:

- Phase 0's BLS aggregation deferral was implemented in Phase 4.
- Phase 0's VM, execution, networking, consensus, RPC, and node crate stubs were
  populated in later phases; integration gaps are tracked above separately.
- Phase 1 parallel execution/conflict detection was implemented in Phase 2.
- Phase 1 resurrection was implemented in Phase 5.
- Phase 3 consensus/voting deferral was implemented at the protocol-library and
  raw-TCP coordinator levels; integrated transport/execution debt remains TD-002
  and TD-003.
- Phase 4 durable local consensus safety state and crash recovery are now backed
  by RocksDB replica snapshots in the node coordinator; durable block/application
  state remains TD-012.
- Phase 4 slashing/equivocation evidence was implemented in Phase 6; applying its
  directives at an epoch transition remains TD-013.
- Phase 4 multi-process socket consensus exists as a five-process test, but its
  full subsystem integration remains TD-003. The test now covers an equivocating
  leader below the Byzantine bound and compares finalized hashes across nodes.
- TD-004 was closed by compiling and publishing the real Move `Token` fixture,
  calling `mint`, crossing the deployed EVM precompile bridge in both directions,
  and calling Move `transfer` against the EVM-updated resource.
- TD-010 was closed by making resurrection return a root-free transition; the
  block executors remain the sole final-root computation point.
- TD-022 was closed by enforcing mutually exclusive order/timeout first-round
  votes. Selective-delivery production-code and multi-process equivocation tests
  cover the previously unsafe trace under the strict less-than-20% bound.
- TD-023 was closed by excluding IDs claimed by multiple owner lanes, validating
  resurrection owner claims against witnesses, and comparing parallel and
  sequential failure state for duplicate absent IDs.
- TD-024 was closed by extracting the production conflict predicate and invoking
  it directly from the Loom interleaving model.
- TD-025 (found and closed in the same pass, while scoping TD-014): any
  unauthenticated gossip peer could permanently kill a validator's entire
  `Stage2Pipeline` task with one message — a malformed transaction (failed BCS
  decode), or one that failed signature/nonce/mempool-admission validation,
  propagated through the `?` operator in `Stage2Pipeline::run` and terminated
  the task, silently halting all transaction processing on that node until an
  operator restarted it. A single crafted gossip message from a peer with zero
  stake was a full denial of service. Fixed by rejecting and logging
  (`tracing::debug!`) failures on the two untrusted network-ingress arms
  (inbound transactions, inbound shreds) instead of propagating them as fatal;
  the trusted arms (already-certified finalized orders, the lifecycle
  commit/poll path) are unchanged and still fail loudly, since a failure there
  indicates a genuine internal fault rather than adversarial input. A
  regression test spins up a real 4-node gossip mesh plus a fifth
  unauthenticated "attacker" peer, confirms the attack is caught (reproducing
  the original crash when the fix is reverted), and confirms a legitimate
  transaction still commits identically on every node afterward.
- TD-021 was closed by deliberately narrowing scope rather than shipping
  unverifiable code: `cli::write_secret` continues to fail closed on
  non-Unix hosts, now with an explicit message directing operators to run
  `validator-init` on a Unix host (a container or WSL is sufficient) and
  transfer the resulting files if the validator itself runs on Windows.
  Owner-only file-permission/ACL handling is inherently security-sensitive,
  and this workspace has no Windows environment to test a Windows
  implementation against — shipping unverified ACL code for a secret-key file
  would risk a false sense of security rather than a real one, which the
  original tech-debt entry's own "or explicitly narrow the supported
  validator-host matrix" resolution anticipated as an acceptable outcome.
- TD-007 was closed by adding `crypto::AggregateSignatureScheme` (aggregate,
  verify_aggregate, proof_of_possession, verify_proof_of_possession — the
  BLS-specific extension `SignatureScheme` intentionally omits, since Ed25519
  cannot support it) and threading `Arc<dyn AggregateSignatureScheme>`/
  `&dyn AggregateSignatureScheme` through every consensus signing/verification
  call site that previously named `Bls12381Scheme` directly: `ValidatorSet::new`,
  `SignedProposal::sign`/`verify`, `Vote::sign`, `verify_equivocation_evidence`,
  `SlashingPolicy::adjudicate`, `EvidenceBook::record`, `verify_certificate`,
  `VoteCollector`, `AsyncVoteAggregator`, and `Replica` (which now holds the
  scheme as a field, set once at construction/restore). `ConsensusCoordinator`
  and `BlockLifecycle` in `crates/node` each construct one `Bls12381Scheme`
  instance and pass it in; swapping the aggregation implementation now means
  changing that one line, not any consensus-crate logic. Left deliberately
  untouched: `GenesisDocument::validate()`'s one-time bootstrap construction of
  the initial `ValidatorSet` still references `Bls12381Scheme` directly, since
  genesis's own `Validator.proof_of_possession`/`public_key` fields are already
  BLS-specific by construction — that boundary was judged out of scope for
  "swap without touching consensus logic" and changing it would have rippled
  across dozens of unrelated test fixtures for no corresponding benefit. This
  was a mechanical dependency-injection refactor with no behavioral change
  (same BLS implementation throughout); the full consensus test suite
  (including multi-process real-TCP Byzantine-fault scenarios) passes
  unchanged.
