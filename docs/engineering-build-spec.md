# Kestrel L1 — Engineering Build Specification (for AI coding agent / Codex)

## 0. How to use this document

This is an implementation spec, not a research report. It assumes the reader (a coding agent) will build incrementally, phase by phase, with working tests at each stage before moving on. Do not attempt to build the whole system in one pass. Each phase has explicit acceptance criteria — treat them as gates, not suggestions. Where a design decision is underspecified, prefer the simplest correct implementation and leave a `// TODO(design):` comment rather than guessing silently.

**Non-goals for the initial build:** production-grade security audit, mainnet economics/tokenomics finalization, governance module, bridges to other chains, mobile/light-client SDKs, full post-quantum signatures. These come after Phase 6.

---

## 1. System overview

**Kestrel** is a single-shard, general-purpose Layer-1 blockchain with:
- A pipelined, dual-path BFT consensus protocol ("Kestrel-BFT") with off-chain vote aggregation
- Asynchronous deferred execution (consensus orders transactions; execution runs in a pipelined stage after ordering)
- A Move-based virtual machine with an object/resource state model, plus a co-resident EVM-compatible execution environment
- Optimistic parallel execution (Block-STM style) with a structural fast path for single-owner objects
- Single-hop, stake-weighted, erasure-coded block propagation
- Storage rent and epoch-based state expiry from genesis
- A crypto-agile signature/address abstraction (not full PQC, but swappable)

**Language: Rust, end to end.** Client/node binary, consensus engine, networking stack, storage layer, and VM host are all Rust. Rationale: no GC pauses (critical for sub-500ms finality), memory safety without sacrificing performance, mature async ecosystem (tokio), and it's what every serious 2026 high-performance chain (Aptos, Sui, Monad, Sei's newer components) converged on. Smart contracts are written in Move (primary) with an embedded EVM/Solidity environment (secondary, via a `revm`-based interpreter) for interoperability.

**Toolchain baseline:**
- Rust stable (pin exact version in `rust-toolchain.toml`; do not use nightly unless a specific unavoidable feature requires it — document why if so)
- `tokio` for async runtime
- `libp2p` for networking/gossip (or a hand-rolled QUIC-based transport if `libp2p` proves too heavyweight for the propagation layer — decide in Phase 3 based on benchmarks, not up front)
- `RocksDB` (via `rust-rocksdb`) for the storage backend initially; keep the storage layer behind a trait so it can be swapped later
- `move-vm` crates (fork/vendor from the Move language reference implementation) for the Move VM host
- `revm` for the EVM-compatible execution environment
- `blst` or `bls12_381` for BLS signature aggregation
- `criterion` for benchmarking, `proptest`/`quickcheck` for property-based testing, `loom` for concurrency-model testing of the parallel execution engine

---

## 2. Repository structure

```
kestrel/
├── Cargo.toml                     # workspace root
├── rust-toolchain.toml
├── crates/
│   ├── types/            # shared types: Hash, Address, PublicKey, Signature, Slot, Epoch
│   ├── crypto/           # signature abstraction layer, BLS aggregation, hashing
│   ├── consensus/        # Kestrel-BFT: leader election, voting, fast/slow path, timeouts
│   ├── execution/        # deferred execution pipeline, Block-STM scheduler, conflict detection
│   ├── vm-move/          # Move VM host integration, object model, resource types
│   ├── vm-evm/           # EVM-compatible execution environment (revm integration), precompiles
│   ├── state/            # state tree, storage rent accounting, epoch-based expiry, witnesses
│   ├── storage/          # RocksDB-backed persistence layer, behind a storage trait
│   ├── network/          # gossip, erasure-coded block propagation ("KestrelCast"), peer discovery
│   ├── mempool/          # transaction ingestion, fee-market prioritization, MEV scheduler hooks
│   ├── rpc/              # JSON-RPC / gRPC API surface for clients and tooling
│   ├── node/             # the validator binary — wires all crates together
│   ├── cli/              # wallet/CLI tooling: keygen, transfer, deploy, query
│   └── testkit/          # test harnesses: local devnet spinner, chaos/fault injection, simulators
├── docs/
│   ├── consensus-spec.md
│   ├── execution-spec.md
│   ├── state-spec.md
│   └── networking-spec.md
├── scripts/
│   └── devnet-up.sh
└── testnets/
    ├── local/                     # single-process multi-validator config
    └── configs/
```

Each crate should have its own `README.md` restating its scope (1 paragraph) and its own unit tests. Cross-crate behavior (e.g. "a byzantine leader triggers view change") belongs in integration tests under `testkit`, not scattered across unit tests.

---

## 3. Build phases

Work through these in order. Do not start a phase until the previous phase's acceptance criteria pass in CI.

### Phase 0 — Foundations (types, crypto, storage skeleton)

**Scope:**
- `types`: define `Address`, `Hash` (default: BLAKE3), `Slot`, `Epoch`, `Transaction`, `Block`, `AccountState`/`Object` types. Addresses are defined as `hash(scheme_id || public_key)` — **not** the raw public key — to satisfy the crypto-agility requirement from day one (see §8).
- `crypto`: a `SignatureScheme` trait (`sign`, `verify`, `pubkey_size`, `sig_size`, `scheme_id`) with a first concrete implementation (Ed25519). Design the trait so a second implementation (BLS for aggregation, and eventually ML-DSA/Falcon) can be added without touching call sites. A `SchemeRegistry` maps `scheme_id -> impl SignatureScheme`, gated by a governance-controlled activation list (stub the governance gate as a static config for now).
- `storage`: a `KvStore` trait (`get`, `put`, `delete`, `iterate_prefix`, batch writes) with a RocksDB implementation.

**Acceptance criteria:**
- Unit tests: sign/verify round-trip for Ed25519; address derivation is deterministic and collision-resistant against a raw-Ed25519-key address (i.e., you cannot forge a Kestrel address that also happens to be a valid legacy-style raw pubkey — this is the property Solana's own PQ-migration discussion flags as necessary).
- `cargo bench` baseline for hashing and signature verification throughput (record numbers in `docs/`, they'll matter for consensus vote-processing budget later).

### Phase 1 — Single-node execution: Move VM + state

**Scope:**
- `vm-move`: integrate a Move VM host. Define the object/resource state model: every piece of state is an `Object` with an owner (single-owner or shared), a type, and a version. Implement object creation, mutation, deletion, and ownership transfer as VM-level primitives.
- `state`: a Merkle-ized state tree (start with a simple Merkle Patricia-style tree; the verkle-equivalent small-witness structure is a Phase 5 upgrade, not a Phase 1 requirement — get correctness first). Implement storage rent accounting: every object has a rent balance; a per-epoch job decrements it; zero-balance objects move to an "expired" state (not deleted — see §7 for resurrection).
- Single-threaded, single-node transaction execution: apply a list of transactions sequentially against the state tree, no parallelism yet.

**Acceptance criteria:**
- Deploy and execute a simple Move module (e.g., a fungible-token-like resource with mint/transfer) end to end against the state tree, single-threaded.
- State root is deterministic and reproducible from a given transaction list (critical property — write a property test that re-executes the same block twice and asserts identical state roots).
- Storage rent test: create an object, advance epochs without topping up rent, confirm it transitions to expired state at the correct epoch.

### Phase 2 — Parallel execution (Block-STM + structural fast path)

**Scope:**
- `execution`: implement the Block-STM-style optimistic scheduler — execute transactions speculatively in parallel across a thread pool, track read/write sets per transaction, detect conflicts against committed reads, abort and re-execute conflicting transactions, commit in a validated serializable order.
- Implement the **structural fast path**: transactions that touch only single-owner objects (determinable statically from the transaction's declared object references) skip the optimistic scheduler and execute directly, since by construction they cannot conflict with a different owner's transactions.
- Conflict detection must be correct under `loom`-style model checking for the scheduler's concurrency logic — this is the highest-risk correctness area in the whole system; do not skip this testing.

**Acceptance criteria:**
- Parallel execution of a block produces an identical state root to sequential single-threaded execution of the same block, for both adversarial (heavy contention, many shared objects) and benign (mostly single-owner) transaction mixes. This is the core correctness invariant — automate it as a fuzz/property test that runs on every CI build.
- Benchmark: parallel execution throughput scales measurably with core count on a benign (low-contention) workload; document the degradation curve under high contention (expected and fine, per the research spec — Monad and Aptos both show this tradeoff).

### Phase 3 — Networking and propagation

**Scope:**
- `network`: peer discovery and gossip (start with `libp2p`'s gossipsub for simplicity; benchmark against target latency before deciding whether a custom transport is needed). Implement erasure-coded block propagation ("KestrelCast"): a leader splits a block into shreds with Reed-Solomon erasure coding (recoverable from ~50% of shreds), and relays are stake-weighted single-hop (not a multi-hop tree).
- Implement the peer-to-peer transaction gossip path (mempool propagation) separately from block/shred propagation — these have different latency/bandwidth tradeoffs and should not share a queue.

**Acceptance criteria:**
- Local multi-node testnet (`testkit`, ≥20 simulated nodes with injected latency/loss) demonstrates block reconstruction from a subset of shreds when some relays are killed mid-propagation.
- Measure and record propagation time to 80% of stake under simulated realistic latency (target: sub-50ms on a LAN-simulated testnet, as a proxy for the design's public-internet target — do not expect to hit true public-internet numbers until Phase 6's public testnet).

### Phase 4 — Consensus (Kestrel-BFT)

**Scope:**
- `consensus`: implement the dual-path pipelined BFT using the explicit 20+20 resilience model: safety requires strictly less than 20% Byzantine stake, while liveness additionally tolerates a separate disjoint crash/offline stake of up to 20%. Leader rotation; fast path finalizes at ≥80% stake in one round; fallback path finalizes at ≥60% stake in two rounds; local timeouts (not a global cryptographic clock — no Proof-of-History equivalent). Off-chain BLS vote aggregation: votes are signed with the BLS scheme from `crypto`, aggregated off the critical path, and only the aggregate certificate is gossiped/stored on-chain.
- Wire consensus to `execution` via **asynchronous deferred execution**: the consensus layer's job is only to agree on transaction *order*; execution runs in a separate pipelined stage that can lag consensus by up to one block's worth of time without blocking the next round of ordering.
- Byzantine fault injection: leader equivocation, vote withholding, network partition, delayed/dropped messages — all must be exercised in `testkit`.

**Acceptance criteria:**
- Safety: no two conflicting blocks are ever finalized at the same height, under all injected Byzantine scenarios in the test suite (this must hold with mathematical certainty in test — treat any observed safety violation as a release blocker, not a bug to triage later).
- Liveness: the chain continues finalizing blocks after a leader is killed/isolated, within a bounded number of view changes.
- Measure end-to-end finality latency on the local multi-node testnet under realistic injected latency; this is the number to compare against the research report's ~750ms (Aptos Raptr) / ~640ms (Sui Mysticeti) benchmarks — document the gap honestly if the local testnet doesn't hit sub-500ms, and identify the bottleneck (usually propagation, not the BFT algorithm itself) before optimizing blindly.

### Phase 5 — Fee market, MEV scheduling, EVM interoperability, state expiry hardening

**Scope:**
- `mempool`: localized (per-object/per-account) fee market — priority ordering scoped to the objects a transaction touches, not a single global fee auction. Priority fees priced by actual compute consumed, paid to validators (no punitive burn).
- MEV scheduler hooks: a pluggable ordering-policy interface that applications can register against (this is the "application-controlled sequencing" hook from the research spec) — start with a simple interface (a contract can specify an ordering preference for transactions targeting it) rather than the full TEE-based fair-ordering scheduler, which is a larger, security-critical subsystem better scoped as its own follow-on project once the base chain is stable.
- `vm-evm`: integrate `revm` as a co-resident execution environment with precompile access to native Move-object state, so EVM contracts can read/write state the Move side understands (Hyperliquid HyperCore/HyperEVM-style bridge, not a separate parallel chain).
- Harden state expiry: implement resurrection (a witness proof that reconstructs an expired object from its last committed root) and confirm stateless-validator verification works against the small-witness structure.

**Acceptance criteria:**
- A contract deployed via the EVM environment can read state written by a Move transaction and vice versa, through the precompile bridge, with a working end-to-end test.
- Fee market test: a spike of transactions targeting one hot object does not measurably degrade fee/latency for transactions touching unrelated objects in the same block.
- Resurrection test: expire an object, then successfully resurrect and mutate it via a witness proof, with correct state-root continuity.

### Phase 6 — Public testnet readiness

**Scope:** see §9 (Testnet Plan) below in full. This phase is operational as much as it is code: RPC hardening, validator onboarding tooling, monitoring/observability (metrics, tracing), slashing conditions for provable Byzantine behavior, genesis tooling, and a chaos-testing harness that can run continuously against a public (non-devnet) network.

**Acceptance criteria:** see §9's stage-by-stage gates.

---

## 4. Consensus spec summary (detail lives in `docs/consensus-spec.md`)

- Validator set size target: 150–200 at steady state; must function correctly (just with worse latency) from a minimum of 4 up to at least 500 for testing headroom.
- 20+20 stake-weighted resilience: safety under strictly less than 20% Byzantine stake, plus liveness with up to a separate 20% crash/offline stake. The 60% fallback quorum is not safe under a classical <1/3 Byzantine assumption and must never be documented or configured as such.
- Two-path finality: fast path (single round, ≥80% stake) and slow/fallback path (two rounds, ≥60% stake). Both paths must be implemented and tested from Phase 4 — do not defer the fallback path, it is what gives liveness under adversarial/partitioned conditions.
- Fixed block time target: ~300ms, governed by local timeouts, not a global synchronized clock.
- Votes are BLS-signed and aggregated off the consensus critical path; only aggregate certificates go on-chain.

## 5. Execution spec summary (detail lives in `docs/execution-spec.md`)

- Deferred execution: ordering (consensus) and execution are separate pipeline stages; execution is allowed to lag by up to one block period.
- Parallel scheduler: Block-STM-style optimistic concurrency control with conflict detection and re-execution.
- Structural fast path: transactions touching only single-owner objects bypass the optimistic scheduler entirely.
- Determinism is non-negotiable: parallel and sequential execution of the same transaction list must always produce the same state root. This is the single most important correctness property in the codebase.

## 6. State model spec summary (detail lives in `docs/state-spec.md`)

- Object/resource-based, not account-based or UTXO-based. Every stateful item is an `Object { id, owner: Owner::Single(Address) | Owner::Shared, type_tag, version, data, rent_balance }`.
- Storage rent: objects must maintain a nonzero rent balance or transition to `Expired` after N epochs (N is a genesis parameter).
- Expired objects are not deleted from history — they move to a separate expired-state tree, resurrectable via witness proof against the last committed state root before expiry.
- State root: a Merkle structure in Phase 1–4; upgrade to a small-witness (verkle-equivalent) structure in Phase 5, enabling stateless-validator verification.

## 7. Networking spec summary (detail lives in `docs/networking-spec.md`)

- Block propagation: erasure-coded shreds ("KestrelCast"), single-hop stake-weighted relay (not Turbine's multi-hop tree).
- Recoverable from ~50% of shreds.
- Transaction gossip (mempool) is a separate path from block/shred propagation, with independent bandwidth budgets.
- Dedicated-fiber/low-jitter link integration is optional and must degrade gracefully to the public internet path — never a hard requirement to participate as a validator.

## 8. Crypto-agility / quantum-readiness implementation notes

This is a design constraint threaded through Phase 0, not a separate module to bolt on later:

- Addresses are `hash(scheme_id || public_key)`, never a raw public key. This means a future signature scheme (e.g. ML-DSA) can be added and used for new addresses without any change to the address format.
- `SignatureScheme` is a trait with multiple implementations behind a `SchemeRegistry`, activated via a governance-gated allowlist (stub as static genesis config until a governance module exists).
- The BLS vote-aggregation layer used by consensus is implemented behind the same abstraction so it can be swapped without touching the consensus state machine's core logic.
- Do not implement full PQC signature schemes in the initial build — implement the *seam* they will plug into. Add a `// TODO(pqc):` marker at the exact integration point in `crypto` so a future scheme addition is a localized change.
- Optional: reserve an unused `scheme_id` range in the registry for hybrid classical+PQC signatures (a transaction signed by both an Ed25519 key and a PQC key, valid only if both verify) so high-value accounts have an opt-in upgrade path without a protocol fork.

## 9. Testnet plan

Do not skip stages. Each stage's exit criteria must pass before promoting to the next.

**Stage 1 — Local devnet (single process, in-process transport).**
Owner: protocol devs only. Purpose: validate consensus safety/liveness and Move VM correctness with `testkit`'s simulated network (no real sockets). Exit criteria: full Phase 0–4 test suites green; a scripted Byzantine-fault scenario suite runs clean.

**Stage 2 — Private multi-node devnet (4–20 real networked nodes, team-run).**
Purpose: first test over real sockets/real OS scheduling. Inject latency, packet loss, and Byzantine nodes deliberately (kill leaders mid-round, corrupt/withhold votes, partition the network). Validate the dual-path BFT's fast/slow path transition under these conditions, not just happy path. Exit criteria: safety holds under all injected fault scenarios; liveness recovers within a bounded number of view changes after each fault; propagation and finality latency numbers recorded and compared against the research spec's targets.

**Stage 3 — Public incentivized testnet, low validator count (~50–100), external operators.**
Purpose: validate on non-team hardware and non-team network topology (real geo-distribution, real internet jitter). Focus areas: state-sync/bootstrap from genesis for a new node joining late, fee-market behavior under adversarial synthetic load (stress-test bots deliberately spamming hot objects), storage-rent and state-expiry behavior over real wall-clock epochs. Exit criteria: a new validator can sync from genesis and start participating within a defined time bound; no safety violations over a sustained multi-week run; fee market demonstrably isolates congestion to the contended objects.

**Stage 4 — Public testnet at target validator count (150–200).**
Purpose: this is where the headline numbers (finality latency, sustained TPS) are actually measured under conditions resembling the design target — public internet, target validator count, real geographic distribution. Run continuous chaos campaigns: kill leaders mid-round, simulate the ~0.05% message-drop rate that the research spec flags as a known failure mode for DAG-style protocols (confirm Kestrel-BFT is robust to this, per its Raptr-style design lineage). Exit criteria: sustained finality and throughput numbers published (honestly — public-internet-achievable, not lab-only) and match or credibly approach the design targets in §"Target performance" of the research report; no safety violations; Nakamoto coefficient and validator-count targets from the research spec's economic design are being met by the actual participant set.

**Stage 5 — Mainnet-beta / capped mainnet.**
Purpose: real economic value, but capped (TVL cap or capped total stake) before uncapped mainnet, matching how Solana, Sui, and Aptos all effectively staged their launches. Exit criteria: defined by a separate go-live review, not purely by this document — this stage requires a security audit of Phases 0–5 (external, not self-assessed) before proceeding.

**Cross-cutting requirement for all stages 2–5:** state expiry and storage rent must be live and under test from Stage 2 onward, not added later — this is the entire point of designing them in at genesis rather than retrofitting them (see the research report's Ethereum retrofit-pain discussion). Similarly, run the MEV scheduler hooks under adversarial load starting at Stage 3, since fair-ordering/MEV infrastructure has historically been where other chains' early rollouts failed quietly.

---

## 10. Suggested order of work for the coding agent

1. Set up the Cargo workspace and crate skeletons (§2) with empty but compiling crates and CI wired up (`cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`) before writing any real logic.
2. Phase 0 end to end, including the address/signature crypto-agility design — get this right first, since `types` and `crypto` are depended on by everything else and are expensive to change later.
3. Phase 1, single-threaded — prioritize correctness and a deterministic state root over any performance work.
4. Phase 2 — do not proceed to Phase 3 until the parallel-vs-sequential state-root equivalence property test is solid; this is the highest-risk correctness surface in the system.
5. Phase 3 and Phase 4 can be developed somewhat in parallel by separable workstreams (networking vs. consensus logic) but must be integration-tested together before either is considered done, since consensus's real-world latency numbers are meaningless without the propagation layer under them.
6. Phase 5 — EVM interoperability and MEV hooks are additive and can be deprioritized relative to fee-market and state-expiry hardening if time-constrained; the base chain's correctness and performance matter more than early EVM support.
7. Phase 6 is as much operational tooling and staged rollout discipline as it is code — do not treat it as "done" just because the binaries compile and run locally.

At every phase boundary, produce a short written status note (what's implemented, what's tested, what's explicitly deferred) rather than silently carrying incomplete work into the next phase.
