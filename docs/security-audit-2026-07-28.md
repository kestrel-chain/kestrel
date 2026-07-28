# Kestrel internal security audit

**Date:** 2026-07-28
**Audited commit:** `25318b8526b754c5e190c10728ead5ba9a8fcb06` (`origin/main`)
**Audited tree:** `6cbf5fea97c63c45f0809e5f5922d45c84330217`
**Assessment type:** Internal, adversarial pre-audit
**Status:** Testnet blockers found

This review is intended to prepare Kestrel for an independent audit. It is not
a certification, does not claim exhaustive coverage, and must not replace the
external review required before economic launch.

## Executive summary

Kestrel has a notably strong correctness-oriented test suite and several good
security foundations: BLS proofs of possession, recomputed certificate stake,
sorted/unique certificate signers, domain-separated signatures and hashes,
strict transaction signatures and nonces, root-bound durable checkpoints,
atomic RocksDB batches, deterministic parallel/sequential equivalence tests,
and bounded public RPC bodies, batches, and per-IP calls.

The audited revision should nevertheless **not be promoted to a public
testnet**. Two critical liveness failures permit a single adversarial event to
stop progress, and one of them leaves a durable poison block that fails again
after restart. Five high-severity findings expose economic bypasses,
cross-network replay, and remotely reachable resource exhaustion. The private
Stage 2 campaign should be postponed until KST-001 and KST-002 are fixed and
regression-tested; public exposure additionally requires KST-003 through
KST-007.

| Severity | Count |
| --- | ---: |
| Critical | 2 |
| High | 5 |
| Medium | 2 |
| Low / hardening | 2 |

## Scope and method

Reviewed production code in `types`, `crypto`, `consensus`, `state`,
`execution`, `mempool`, `network`, `storage`, `rpc`, `node`, `vm-move`, and
`vm-evm`, plus operator tooling, specifications, CI, incident history, and the
technical-debt register.

The review traced untrusted data from RPC/libp2p/raw TCP through signature
validation, consensus certification, execution, persistence, recovery, and
operator status. It also examined quorum intersections, vote durability,
integer bounds, queue and frame limits, task failure propagation, secret-file
handling, unsafe/panic sites, dependency provenance, and current RustSec
advisories. `cargo audit` used RustSec database commit
`0bfde9d6a469ae503f8a6147c2dd552856cd5999` (updated 2026-07-27) against 679
locked dependencies.

## Verification performed

- `cargo fmt --all -- --check` — passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo test -p consensus --test kestrel_bft` — 12 passed.
- Selected execution suites (`deferred_execution`, `loom_deferred`,
  `loom_scheduler`, `parallel_equivalence`, and `resurrection_execution`) —
  9 passed.
- Testkit consensus-fault and propagation suites — 8 passed.
- `cargo audit` — failed the dependency security gate with three known
  vulnerabilities, recorded in KST-009.

These results confirm the existing gates at the audited commit; they do not
close findings whose attack timing or payload is absent from the current
suite.

## Findings

### KST-001 — A leader can permanently prevent both certification and timeout

**Severity:** Critical
**Class:** Consensus liveness / certificate withholding
**Affected:** `crates/node/src/coordinator.rs:508-549`,
`crates/node/src/coordinator.rs:552-660`,
`crates/consensus/src/lib.rs:932-953`

Followers send order votes only to the current proposer. An honest replica that
has cast an order vote is permanently forbidden from casting a timeout vote in
that height/view. Only the leader can aggregate order votes into a prepare or
fast certificate.

A Byzantine leader with less than 20% stake can collect enough honest order
votes to make total order-voting stake at least 60%, then withhold the
certificate. The non-order-voting stake is below 40%; even if all Byzantine
stake equivocates into a timeout vote, the timeout side remains below 60%.
Neither finalization nor a view change can occur. The same failure can happen
when an honest leader crashes after receiving the votes but before broadcasting
the certificate.

This violates the claimed recovery from killed/withholding leaders. Existing
fault tests kill leaders at other points and do not cover this
post-vote/pre-certificate window.

**Recommendation:** Disseminate individually authenticated order and commit
votes so another validator can aggregate a valid certificate when the leader
fails. Authenticate the transport identity, allow a quorum-authenticated
certificate to be relayed by any admitted validator, and add a deterministic
test that kills the leader after 60% and after 80% of order votes have left
honest replicas but before certificate publication. Re-review the complete
cross-view safety proof after changing vote dissemination.

### KST-002 — A validly signed but failing transaction durably halts execution

**Severity:** Critical
**Class:** Remote denial of service / finalized poison block
**Affected:** `crates/node/src/lifecycle.rs:95-123`,
`crates/node/src/lifecycle.rs:401-489`,
`crates/execution/src/lib.rs:204-251`,
`crates/node/src/pipeline.rs:367-425`

Admission proves signature, sender, nonce, payload encoding, nonzero compute,
and fee-cap shape. It does not establish that the declared operation can
execute against canonical state. For example, a correctly signed mutation can
declare a nonexistent object or stale version and pass public RPC admission.

Consensus orders and finalizes transaction identifiers without execution
feedback. Once the payload reaches `DeferredExecutor`, the deterministic state
error becomes an `ExecutionError`. `BlockLifecycle::poll_commit` returns it as
fatal, `Stage2Pipeline::run` exits, and the coordinator subsequently loses its
finalized-order sink. Every honest node evaluating the same finalized payload
fails.

The block was persisted under `application/pending/v1/` before execution.
Restart replays the same invalid block, so restarting validators does not
recover the network.

**Recommendation:** Define failed-transaction semantics before accepting
public traffic. The conventional safe design is to make transaction failure a
deterministic receipt that consumes nonce and gas while reverting only that
transaction's state effects; it must not invalidate an already-finalized
block. Ensure the executor runs each transaction atomically and continues
through deterministic failures. Add end-to-end RPC tests for nonexistent
objects, stale versions, out-of-gas Move calls, invalid entry functions, and a
failure after an earlier successful transaction, including restart.

### KST-003 — Raw consensus TCP admits unauthenticated, unbounded slow clients

**Severity:** High
**Class:** Remote connection exhaustion / amplification
**Affected:** `crates/node/src/coordinator.rs:362-384`,
`crates/node/src/coordinator.rs:665-820`,
`crates/node/src/coordinator.rs:1260-1285`

Every accepted TCP connection spawns a task. There is no semaphore, per-IP or
global connection limit, peer allowlist, transport authentication, or read
deadline. A client can open sockets and never send the four-byte frame length,
holding tasks and file descriptors indefinitely. The four-megabyte frame limit
is reached only after the length is read and therefore does not mitigate this.

`Envelope.sender` is also only a serialized claim. Inner proposals and votes
are authenticated, but catch-up requests are not. An outsider can claim another
validator ID and repeatedly make a node load retained orders and connect to the
claimed validator's configured address, creating intra-validator traffic
amplification.

**Recommendation:** Put the consensus channel behind mutually authenticated
Noise/TLS or reuse authenticated libp2p streams; bind transport identity to the
genesis validator ID. Until then, enforce an IP/validator allowlist at the
network edge. Add accept concurrency limits, header/body deadlines, idle
timeouts, per-peer request budgets, and bounded catch-up response work.

### KST-004 — Untrusted shreds can amplify traffic and consume gigabytes

**Severity:** High
**Class:** Remote memory/network denial of service
**Affected:** `crates/network/src/service.rs:565-594`,
`crates/node/src/pipeline.rs:454-465`,
`crates/node/src/pipeline.rs:1078-1105`,
`crates/network/src/kestrel_cast.rs:158-203`

The libp2p shred protocol accepts requests from arbitrary connected identities,
not only genesis validators. If `relay_requested` is set, the pipeline clones
and queues the shred to every validator peer **before** validating its metadata,
integrity, payload, or authorization to request relay.

The in-flight limit counts block IDs, not bytes or shreds. One block group may
contain up to 255 distinct, nearly one-megabyte shreds. An attacker can keep 64
groups one shred below reconstruction, consuming roughly 16 GiB without ever
producing a validation error or offense. New libp2p identities also evade the
per-peer eight-offense ban.

**Recommendation:** Accept shred delivery/repair only from configured
validators, authorize `relay_requested` against the deterministic relay plan,
and validate metadata before forwarding. Bound total buffered bytes,
per-block bytes, per-peer bytes, shard size, and shard count. Charge incomplete
groups to a peer budget and rate-limit identity/address churn. Never amplify a
payload before validation.

### KST-005 — Fee settlement fails open and enables unfunded execution

**Severity:** High
**Class:** Economic integrity / fee bypass
**Affected:** `crates/node/src/pipeline.rs:118-133`,
`crates/node/src/pipeline.rs:899-970`,
`crates/node/src/lifecycle.rs:540-589`,
`crates/mempool/src/lib.rs:336-379`

Admission checks that the signed cap covers the quoted per-compute price but
does not check or reserve the payer's balance. At commit, insufficient balance,
fee arithmetic failure, or validator-credit overflow is logged and the block
commit proceeds. The state change and nonce persist while no fee is charged.

The default genesis creator emits an empty `initial_fee_balances` map, and the
Stage 2 process tests likewise use empty balances with the default nonzero base
fee. Their transactions therefore demonstrate fail-open settlement rather than
a paid production path.

Because the mempool has no global transaction/byte bound and attackers can
generate unlimited sender keys, this also removes the economic control on
memory, disk, signature-verification, and block-space spam.

**Recommendation:** Validate and atomically reserve the maximum payable charge
at admission, release/refund the unused portion deterministically, and make
settlement an infallible consequence of a prior reservation. Treat invariant
failures as consensus-critical before certification, never as a warning after
state execution. Add unfunded, balance-race, overflow, restart, and multi-
transaction conservation tests. Add global/per-peer mempool count and byte
limits independently of fees.

### KST-006 — Transactions are replayable across Kestrel networks

**Severity:** High
**Class:** Cross-domain replay
**Affected:** `crates/types/src/lib.rs:132-153`,
`crates/node/src/lifecycle.rs:101-123`

The signed transaction message contains sender, nonce, and payload, but no
chain ID, genesis hash, network ID, protocol version, or transaction domain
prefix. Account addresses are also network-independent. A transaction signed
for one Kestrel network is valid on another network wherever the account nonce
and referenced state permit it.

**Recommendation:** Version the transaction signing envelope and include a
fixed transaction domain plus immutable chain identity (preferably genesis
hash, or a collision-resistant chain ID committed by genesis). Reject the
legacy envelope after an explicit compatibility decision. Add cross-genesis
negative tests for both Ed25519 and BLS transaction schemes.

### KST-007 — Valid blocks can exceed the shred transport's hidden 1 MiB limit

**Severity:** High
**Class:** Deterministic liveness failure / configuration mismatch
**Affected:** `crates/node/src/pipeline.rs:86-108`,
`crates/node/src/pipeline.rs:1035-1075`,
`crates/network/src/service.rs:86-109`,
`crates/network/src/service.rs:380-386`

Kestrel permits 4,096 transactions per block and 512 KiB per gossiped
transaction, and advertises a 2 MiB shred limit. With ten data shards, a valid
payload above roughly 10 MiB creates shreds above one MiB.

The locked `libp2p-request-response` CBOR codec defaults to a 1 MiB inbound
request limit. Kestrel constructs it with `request_response::Config::default()`
and does not customize the codec. Outbound serialization accepts the larger
shred, but the receiver truncates/fails decoding before Kestrel's 2 MiB check.
The leader can still obtain consensus over transaction IDs; followers can
neither receive nor repair the certified payload and eventually stop.

**Recommendation:** Establish one protocol-level maximum encoded block size
and reject proposal construction above it. Configure the codec request limit
from the same constant with framing overhead included, or reduce Kestrel's
shred limit below the codec bound. Test exact boundary sizes end to end through
proposal, relay, repair, execution, and restart.

### KST-008 — Critical task failures leave health/readiness reporting success

**Severity:** Medium
**Class:** Fail-open operations / stale monitoring
**Affected:** `crates/node/src/main.rs:195-212`,
`crates/rpc/src/lib.rs:455-475`

The coordinator and pipeline are detached Tokio tasks. Their errors are logged
but not propagated to `main`, and they do not clear `NodeStatus.ready`. The RPC
server remains alive; `/healthz` always returns 200 and `/readyz` can continue
returning 200 while consensus and execution are stopped.

This delays detection and allows an orchestrator to keep routing traffic to a
dead validator.

**Recommendation:** Supervise critical tasks with structured concurrency.
Any unexpected coordinator, pipeline, network, or execution exit must
atomically clear readiness and either trigger bounded recovery or terminate the
process nonzero. Readiness should also enforce bounded finalized/committed
progress and execution lag.

### KST-009 — Locked dependencies contain known advisories

**Severity:** Medium
**Class:** Supply chain / known-vulnerable dependency
**Affected:** `Cargo.lock`, `crates/network`, CI

`cargo audit` reports:

- `RUSTSEC-2026-0119`: `hickory-proto 0.25.2`, quadratic CPU exhaustion;
- `RUSTSEC-2026-0118`: `hickory-proto 0.25.2`, unbounded NSEC3 loop;
- `RUSTSEC-2025-0055`: `tracing-subscriber 0.2.25`, ANSI log injection.

`hickory-proto` is in the active production graph through
`libp2p-mdns 0.48.0`. The NSEC3 advisory appears to require DNSSEC features not
used by Kestrel, so its direct reachability is unconfirmed; the dependency gate
still fails. The old `tracing-subscriber` package does not appear in the active
target graph and may be stale lockfile residue.

RustSec also reports unmaintained transitive crates and unsound old
`ouroboros`, `atty`, and `rand` versions, primarily inherited from the pinned
legacy Move revision. The `rand 0.7.3` path reaches `move-vm-runtime`, although
the advisory's custom-logger/reseeding preconditions were not observed here.

**Recommendation:** Upgrade libp2p/hickory, regenerate/prune the lockfile, add
`cargo audit` or `cargo deny` as a required CI gate, and plan migration from the
legacy Move revision. Record narrowly justified advisory ignores only after
feature/path reachability analysis.

## Low-severity and hardening observations

### KST-010 — Validator keys remain ordinary cloneable byte vectors

Consensus private keys are read into `Vec<u8>`, cloned into coordinator and
replica state, and never zeroized or memory-locked. The CLI correctly creates
key files with `0600` and refuses overwrite, but the node does not reject
over-permissive key files.

Use a zeroizing secret type with minimized cloning, reject unsafe key
permissions on Unix, and define HSM/remote-signer support before public
operators.

### KST-011 — CI actions use mutable references

`.github/workflows/ci.yml` uses `actions/checkout@v4` and
`dtolnay/rust-toolchain@master`. Pin third-party actions to reviewed full commit
SHAs, enable dependency review and artifact provenance, and add secret and
license scans.

## Positive controls observed

- No project-authored `unsafe` blocks were found in production source.
- BLS validator admission verifies proof of possession before aggregate use.
- Certificate verification recomputes exact signer stake and rejects empty,
  unsorted, or duplicate signer sets.
- Proposal and vote signatures are domain-separated; the certified block ID
  binds transaction order, parent, height, and fee commitment.
- Honest votes are persisted before transmission, and local double voting is
  rejected.
- State snapshots validate format, object uniqueness/history, and full root
  before restoration.
- Final block, checkpoint, fee balances, nonce state, and pending deletion use
  atomic RocksDB batches.
- Parallel execution has sequential-root equivalence and Loom interleaving
  coverage.
- RPC limits request bodies, batch fan-out, method names, and per-IP call rate.
- Validator onboarding uses OS randomness, `create_new`, `0600`, and `fsync`.

## Remediation order and release gates

1. Fix KST-001 and re-review the consensus safety/liveness proof.
2. Fix KST-002 with deterministic transaction-failure receipts and atomic
   per-transaction rollback.
3. Fix KST-005 and add bounded mempool admission.
4. Fix KST-003 and KST-004 before exposing validator transport addresses.
5. Fix KST-006 before signing transactions intended for more than one network.
6. Fix KST-007 and run encoded-size boundary tests.
7. Supervise node tasks and readiness (KST-008).
8. Clear RustSec and CI supply-chain gates (KST-009/KST-011).
9. Re-run the full workspace suite, model tests, fuzz targets, multi-process
   Byzantine suite, load/abuse harness, and a new adversarial regression suite.
10. Only then restart the private multi-machine Stage 2 campaign.

An independent auditor should receive this report, the corresponding fixes and
regression tests, protocol specifications, incident history, threat model,
dependency manifest, and retained Stage 2 evidence. Critical and high findings
must be re-tested by someone other than the implementer before closure.
