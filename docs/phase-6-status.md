# Phase 6 status

## Implemented code-readiness work

- Replaced the RPC stub with a real Axum HTTP server, JSON-RPC 2.0 validation, per-IP fixed-window rate limiting, request-body and batch limits, canonical error responses, read-only status/object methods, liveness/readiness probes, and Prometheus text metrics.
- Added structured JSON tracing and graceful Ctrl-C shutdown to the validator process. Public RPC binding requires an explicit acknowledgement flag.
- Added deterministic genesis JSON with canonical order-independent hashing, initial state-root construction, rent configuration, signature-scheme activation, 4–500 validator bounds, endpoint uniqueness, BLS proof-of-possession validation, and a genesis-configured equivocation penalty.
- Added validator onboarding that generates OS-random BLS keys, writes private material with mode `0600` on Unix without printing it, and emits a public validator profile.
- Added independently verifiable, replay-protected same-round BLS double-vote evidence and next-epoch slashing directives. Downtime and network faults are explicitly not slashable.
- Added a 100-iteration scripted chaos campaign spanning leader death, leader equivocation, 15% Byzantine withholding plus 20% separate offline stake, 0.05% message loss, and added latency.
- Added a production raw-TCP consensus coordinator with signed-proposal relay, BLS vote/certificate exchange, equivocation observation before first-round voting, local timeouts, and RocksDB-backed replica safety snapshots.
- Added a five-process socket test covering healthy finality, killed and partitioned leaders, corrupt and withheld votes, and a less-than-20%-stake equivocating leader.
- Added a provider-neutral external `ChaosTarget` interface that detects conflicting finalized hashes, enforces a bounded liveness window, and heals injected faults on every return path.
- Added an operator runbook and a usable local node launcher.
- Started the integrated Stage 2 block lifecycle: signed envelopes are bound to active genesis schemes and sender addresses, nonces are enforced in canonical order, KestrelCast payloads are reconstructed and matched to a final BLS certificate, Phase 2 deferred execution runs the actual payload, and the block/certificate/root-bound application checkpoint is committed atomically to RocksDB. RPC object state and the state root advance only after that durable commit.
- Added canonical full-state snapshots with format/genesis/root binding and retained-expiry-witness validation. A node can restore the last committed application state and admission nonces before accepting the next height.
- Proved the `Stage2Pipeline`/`ConsensusCoordinator::bind_with_pipeline` composition end to end with a four-node in-process integration test: one real signed transaction submitted through libp2p transaction gossip is admitted through the shared validator, selected by the localized mempool (not a synthetic ID), erasure-coded and relayed as `KestrelCast` shreds over libp2p request/response, ordered by real consensus, executed by `DeferredExecutor`, and atomically committed — converging to an identical state root and finalized height on every node.
- Wired the shipped `node` binary's validator path to this same composition: it now builds a real `network::NetworkNode`, `BlockLifecycle`, and `Stage2Pipeline` and calls `ConsensusCoordinator::bind_with_pipeline`, replacing the old synthetic-proposal-only path entirely. Genesis now carries each validator's libp2p `gossip_peer_id`/`gossip_address`, and `cli validator-init` generates and persists that validator's libp2p identity alongside its consensus key.
- Added a `kestrel_submitTransaction` JSON-RPC method (`rpc::TransactionSubmitter`) so a signed transaction can be admitted over public RPC through the same validated path as gossip ingress; the method is refused outright when no submitter is configured (read-only/observer nodes never silently accept a transaction).
- Added a process-level integration test that spawns the actual compiled `node` binary across four separate OS processes, submits one transaction over `kestrel_submitTransaction` RPC to a single process, and confirms every process — including the three that never received the RPC call — commits the identical mutated object via `kestrel_getObject`. `tests/stage_2_processes.rs`'s existing five-process Byzantine-fault suite now exercises this same real pipeline (not the old synthetic path) and still passes unchanged.

## Automated evidence

- Genesis hashes and state roots are invariant to validator/object input order and survive JSON round trips.
- Validator onboarding tests verify owner-only secret permissions and the emitted BLS proof of possession.
- RPC tests cover readiness transitions, object reads, oversized body rejection, batch rejection, rate limiting, and metrics output.
- Slashing tests accept two valid conflicting votes, compute the configured penalty, reject replay and non-conflicting evidence, and reject a tampered signature.
- The checked-in 100-scenario chaos campaign finalized all 100 heights with zero safety violations and zero liveness failures. The worst case recovered within four view changes.
- The chaos campaign's safety check includes a production-code selective-delivery probe: after an 80% fast certificate is delivered to one honest replica, the signers' mutually exclusive first-round choice prevents a conflicting 60% timeout certificate.
- The five-process socket test hard-asserts one finalized hash at every observed node, a bounded view change after each leader fault, and finality below two seconds. This is a coarse CI bound, not a published latency benchmark.
- The block-lifecycle integration test reconstructs a signed block from 10 of 20 shreds, rejects a validly signed but out-of-sequence nonce without advancing the head, validates an 80% fast certificate, executes and atomically persists height 1, reopens RocksDB, verifies the RPC/state root, and commits height 2 using the restored nonce and object version. Snapshot tampering is a hard test failure.
- The Stage 2 pipeline integration test runs four full in-process nodes (real libp2p transport, real mempool, real `KestrelCast`, real consensus, real `DeferredExecutor`, real RocksDB) and hard-asserts that a transaction submitted on one node is executed and committed identically — same mutated object data, same state root, same finalized height — on all four within a bounded timeout.
- The node-binary RPC integration test spawns the compiled `node` binary as four separate OS processes, submits a transaction to one process's `kestrel_submitTransaction` RPC method, and hard-asserts every process's `kestrel_getObject` converges on the identical committed data within a bounded timeout.
- The RPC test suite covers `kestrel_submitTransaction` both routing a transaction to a configured submitter and being refused (method not found) with no submitter configured.

Run `cargo run -p testkit --example phase_6_report` to reproduce the campaign report.

## §9 stage gates

| Stage | Status | Evidence or blocker |
| --- | --- | --- |
| 1 — local devnet | Passed in automation | Full workspace gates and scripted Byzantine/chaos scenarios pass. |
| 2 — private 4–20 real-node devnet | Not passed | The production `node` binary now runs the full pipeline — signed KestrelCast payload reconstruction, final-certificate validation, deferred execution, atomic state/certificate commits, RPC root updates, restart restoration, real libp2p transaction gossip/shred relay, and public RPC transaction admission — proven both in one process (four in-process nodes) and across four separate OS processes with an RPC-submitted transaction. The five-process Byzantine-fault suite (`tests/stage_2_processes.rs`) now runs this same real pipeline instead of the old synthetic path. What remains for this gate: injecting socket-level latency/loss/relay death specifically on the libp2p paths (the raw-TCP consensus transport already has this), running across real separate machines/geography rather than one machine over loopback, and recording propagation-to-80%-stake and end-to-end finality measurements under those conditions, per §9's exit criteria (see TD-003). |
| 3 — public 50–100 validator testnet | Not started | Requires external operators, durable checkpoint/genesis state sync with a measured bound, wall-clock epoch operation, and a sustained multi-week deployment. |
| 4 — public 150–200 validator testnet | Not started | Requires real geography, continuous authorized chaos, published public-internet TPS/finality, and participant concentration measurements. |
| 5 — capped mainnet | Blocked by design gate | Requires Stage 2–4 evidence, a separate go-live review, and an external security audit. |

## Verification

The workspace passes:

- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`

## Explicitly deferred

- In-flight admitted-transaction recovery (an accepted-but-not-yet-finalized transaction is only held in process memory, not durably persisted), fee settlement from metered execution, epoch transitions/validator reconfiguration, and the complete operator-run multi-machine process topology remain outstanding.
- Checkpoint/state-sync protocol, snapshot authentication, pruning, and measured late-join bootstrap time.
- TLS termination, distributed denial-of-service protection, archival/indexer APIs, gRPC, WebSocket subscriptions, and production RPC load tests.
- A concrete cloud/Kubernetes chaos adapter and credentials; these belong in the deployment repository for the authorized testnet.
- Every external/elapsed-time Stage 2–5 result. No public network was launched or represented as tested.
