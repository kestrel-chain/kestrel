# Kestrel incident log

Permanent record of severe defects found and fixed, kept separate from the
routine phase-status and tech-debt documents. An entry is warranted when a bug
could have caused a total safety or liveness failure, so that an eventual
external audit has a clear, self-contained account of what happened, why, how it
was found, and how it is now guarded against regression.

Each entry records: severity, symptom, root cause, how it was detected, the fix,
the regression guards, and — because a severe bug is rarely unique — the result
of a systematic audit for siblings of the same class.

---

## INC-001 — Deferred-execution backpressure treated as a fatal error

- **Severity:** Critical (total liveness failure under honest sustained load; no
  adversary required).
- **Component:** `crates/node/src/pipeline.rs` (`Stage2Pipeline`), at the
  boundary with `crates/execution` (`DeferredExecutor`).
- **Status:** Fixed.

### Symptom

Under sustained transaction load, validators that had been finalising and
committing normally would abruptly stop. Their logs showed, at the same height:

```
ERROR Stage 2 pipeline stopped: "execution is already one ordered block behind"
ERROR consensus coordinator stopped: "the finalized-order execution sink is closed"
```

The pipeline task exited, which dropped the channel its coordinator feeds, so
the coordinator exited too. With two of four validators gone, the network fell
below quorum and the chain stopped finalising entirely. Restarting a node
recovered it only until load returned.

### Root cause

Kestrel executes deferred: consensus orders a block, and a separate worker
executes it later, connected by a one-block bounded channel. That bound *is* the
backpressure the consensus spec mandates — when execution is one block behind,
`DeferredExecutor::submit` returns `LagLimitReached` (from `TrySendError::Full`).

At high throughput ordering legitimately outruns execution by a block — that is
the normal, expected behaviour of a deferred pipeline. But
`Stage2Pipeline::submit_available_orders` propagated `LagLimitReached` via `?`
as a **fatal** error, terminating the pipeline task. So a validator killed
itself precisely when it was busiest, and two such deaths cost the network its
quorum. The trigger was ordinary load; no malformed input or Byzantine peer was
involved.

### Detection

Found while measuring throughput. The steady-state benchmark showed high
variance at 800 tx/s — some runs committing ~740 tx/s, others zero. The initial
hypothesis (a benchmark-harness startup failure) was **wrong**, and confirming
it from evidence rather than plausibility is what found the bug: a diagnostic
was added to preserve a zero run's node logs, which showed the nodes healthy and
committing to height 13 before the fatal backpressure error — a post-ready crash,
not a cold start.

### Fix

Treat `LagLimitReached` as transient backpressure: leave the order in
`pending_orders`, return, and retry on the next tick, once `poll_commit` has
drained a completed block and freed the executor slot. Every other failure stays
fatal. The pre-existing `maximum_pending_orders` bound still catches execution
that has genuinely *stopped* (as opposed to merely lagging), reporting it rather
than letting the backlog grow without limit.

### Regression guards

- `execution`: `a_full_one_block_buffer_is_recoverable_backpressure_not_a_worker_failure`
  — deterministically fills the one-block buffer and asserts a full buffer is
  reported as the retriable `LagLimitReached`, never confused with the fatal
  `WorkerStopped`, and that draining clears it. Verified to fail if the full
  buffer is reported as `WorkerStopped`.
- `node`: `executor_one_block_behind_is_treated_as_backpressure_not_a_crash`
  — asserts the pipeline classifies `LagLimitReached` as backpressure and every
  other lifecycle error as fatal.
- Behavioural proof: the 800 tx/s steady-state benchmark went from intermittent
  zero-throughput crashes to 10/10 clean reps (~740 tx/s, tight CI) with the
  fix. This end-to-end reproduction depends on release-speed consensus outrunning
  execution and does not occur in a debug `cargo test`, which is why the
  permanent guards live at the executor and classifier boundaries instead.

### Systematic audit for siblings

The bug class is "a bounded-channel-full / backpressure signal misclassified as
a fatal error." Every bounded channel and `try_send`/`try_recv` site was
reviewed:

- **Confirmed safe:** the coordinator's inbound-message queue uses `send().await`
  (it backpressures rather than dropping or erroring); `VoteCollector::add_vote`
  is consumed with `if let Ok(...)`; the network loop's inbound sends use
  `let _ = try_send(...)` (drop-on-full, recovered by gossip/repair).
- **Dead code with a latent trap (no live impact):** `AsyncVoteAggregator`'s
  `AggregationQueueFull` path has no non-test call sites — the live coordinator
  uses the synchronous collector. If it is ever wired in, its caller must treat
  `Full` as transient.
- **One live sibling, fixed here:** in `Stage2Pipeline::handle_shred`, a relay
  send that failed because this node's *own* outbound shred queue was full
  (`GossipError::ShredQueueUnavailable` — pure local backpressure) was run
  through the peer-offense classifier, which did not exclude it. Under load that
  counted honest relaying peers as offenders and eventually banned them —
  durably, since bans are now persisted (TD-014). Fixed by classifying local
  gossip-queue backpressure as never-the-peer's-fault, guarded by
  `routine_gossip_races_are_not_counted_as_peer_offenses`.

---

## INC-002 — A rejoining validator killed itself catching up

- **Severity:** Critical (a validator that restarts can never rejoin; it dies
  during catch-up instead of recovering).
- **Component:** `crates/node/src/pipeline.rs` (`Stage2Pipeline`), at the
  boundary with the catch-up path in `crates/node/src/coordinator.rs`.
- **Status:** Fixed.

### Symptom

On a healthy four-validator network, cleanly stopping one validator and then
restarting it wedged the *restarted* validator within a second of rejoining. Its
log showed catch-up starting correctly and then, mid-burst, a fatal stop:

```
DEBUG caught up to a peer height=801
DEBUG caught up to a peer height=865
WARN  execution has fallen too far behind ordering; stopping this validator …
      pending=65 limit=64 awaiting_height=737
ERROR Stage 2 pipeline stopped: 65 certified orders undelivered to execution (limit 64) …
ERROR consensus coordinator stopped: the finalized-order execution sink is closed
```

The pipeline task exited, dropped the finalized-order channel, and took the
coordinator down with it (the same cascade as INC-001). The validator was left
frozen at the height it had reached, permanently out of the network.

### Root cause

This is a second instance of INC-001's bug class — a transient, expected backlog
misclassified as a fatal error — reached through a path the INC-001 audit did not
cover, and in fact *introduced* by the validator-catch-up feature (commit
"Let a behind validator catch up on finalized orders it missed").

When a restarted validator rejoins, its coordinator catches up by finalizing the
orders it missed, in bursts of up to a full catch-up batch (64) back to back. It
pushes each finalized order into the pipeline's unbounded intake channel. The
pipeline drains that into `pending_orders` and submits them to execution in
height order — but execution of a caught-up height needs that height's *payload*,
which the validator never received while it was down and must now re-fetch via
payload repair. So ordering raced hundreds of blocks ahead while execution waited
on repairs, `pending_orders` blew past `maximum_pending_orders` (64), and the
bound — written to catch execution that had *genuinely stopped* — fired on a
validator that was simply busy catching up.

The INC-001 write-up had explicitly reasoned that `maximum_pending_orders` was
the *legitimate* detector of stopped execution. That was true for the steady
state it was written against; catch-up introduced a new, benign source of large
transient backlog that the instantaneous check could not tell from a real stall.

### Detection

Found by running a real four-node network on one host, cleanly killing a
validator (the remaining three kept finalizing — f=1 tolerance is intact), then
restarting it and watching it die. The node log named the exact bound and height,
which distinguished this from a network-wide fault: the rest of the network was
unaffected; only the rejoining node stopped.

### Fix

Key the failure on *lack of progress*, not on backlog size alone. A backlog over
the bound is fatal only if execution has made **no** forward progress
(`submitted_height` unchanged) for a whole `execution_stall_grace` window
(default 30s, above the 8s payload-repair backoff cap). A validator catching up
keeps advancing `submitted_height` as repaired payloads arrive, so it is never
killed; a genuinely frozen validator advances not at all and is still stopped, as
before — preserving INC-001's guarantee that a validator cannot keep voting on
blocks it will never execute. The per-tick submit was also made to re-run after
`poll_commit` frees the executor slot, so catch-up drains at execution speed
rather than one height per tick.

A separate, real limitation remains and is *by design*, not a bug: catch-up
retains only a bounded window of finalized orders (`CATCHUP_RETENTION`, 1024
heights). A validator that falls further behind than that window cannot be
recovered by catch-up at all and now stops cleanly (no progress ⇒ genuine stall)
rather than freezing silently. Full state sync for that case is future work.

### Regression guards

- `node`: `a_catching_up_backlog_is_not_a_stall_but_a_frozen_one_is` — drives the
  `backlog_is_a_genuine_stall` predicate directly: a backlog within the bound, or
  one over the bound but not yet frozen for the grace window (the catch-up case),
  is never a stall; a backlog over the bound and frozen for the whole window is.
- `node`: `a_late_starting_validator_catches_up_to_the_others` — a real-TCP test
  in which a validator joins after a quorum has finalized past it and must reach
  the stop height purely by catching up. It was previously flaky (see below) and
  is now race-free.
- Behavioural proof: the same kill-and-rejoin on the live devnet that reproduced
  the death now lets the validator catch up and resume finalizing. Like INC-001,
  the end-to-end reproduction depends on release-speed ordering outrunning
  execution and does not occur in a debug `cargo test`, which is why the
  permanent guard lives at the classifier boundary.

### A flaky test, fixed alongside

The catch-up test above originally had every validator, including the late one,
run to a fixed stop height. Catch-up is triggered only by *receiving* a
certificate fresher than one's own height, so if the four peers reached the stop
height and exited before the late validator engaged, it never got a trigger and
hung the full consensus bound. Whether that happened was a wall-clock race, and a
faster machine made it *more* likely (the peers finished sooner) — so it passed
locally and failed in CI. The fix makes the peers run until aborted rather than
to a stop height: a quorum is always live for the late validator to trigger on
and fetch from, on any hardware. The assertion became "the late validator reached
at least the stop height", since a single catch-up batch can carry it several
heights past the quorum in one step.

---

## INC-003 — A restarted leader crashed on its own equivocation guard

- **Severity:** Critical (a validator that restarts at a height it led crashes
  its consensus coordinator; under fault churn this cost the network its quorum).
- **Component:** `crates/node/src/coordinator.rs` (`ConsensusCoordinator`), at the
  boundary with `consensus::Replica`'s vote signing.
- **Status:** Fixed.

### Symptom

A validator that was restarted (crash, upgrade, or fault-injection kill) while it
was the leader for its current height would immediately stop its consensus
coordinator on restart:

```
ERROR consensus coordinator stopped: honest replica refused to double vote
```

Restarting it again just reproduced the crash, because the cause was in its own
persisted state. With a second validator also faulted (an ordinary occurrence
under real churn), losing this one dropped the network below quorum and it
stopped finalizing.

### Root cause

The same class as INC-001/INC-002: a *correct, expected* safety refusal treated
as a fatal error.

`Replica::sign_once` returns `LocalDoubleVote` when the replica is asked to sign
a second, *different* vote at one (height, view, phase) — the equivocation guard.
A leader legitimately trips this across a restart:

1. As leader of `(H, 0)` it builds a block, signs its own order vote for it, and
   persists the vote. The in-memory proposal is not persisted.
2. It restarts. It reloads the persisted vote for `(H, 0, Order) = block_A` but
   has lost the proposal, and is still the leader of `(H, 0)`.
3. Its mempool has changed meanwhile, so it rebuilds a *different* block `block_B`
   for `(H, 0)` and tries to self-vote — `block_B != block_A` ⇒ `LocalDoubleVote`.

The coordinator's `skip_safe_vote_refusal` helper already reclassified the
sibling refusal `ConflictingFirstRoundVote` as a skip, but deliberately kept
`LocalDoubleVote` fatal on the assumption it "could not happen honestly." The
restart-reproposal path above is exactly how it happens honestly.

### Detection

Found by the local chaos-soak harness (`devnet/soak.sh`): four real release-build
validators under continuous transaction load with wall-clock fault churn
(kill/isolate/shred-drop/gossip-delay/consensus-drop), asserting safety (no two
nodes finalize different blocks at one height), liveness (the tip keeps
advancing), and no-crash (a node meant to be up never exits). A `kill` fault on a
current leader reproduced the crash on the first fault; a node-level inbound trace
confirmed the crash fired on the leader's *self*-vote for a rebuilt proposal.

### Fix

Reclassify `LocalDoubleVote` as a safe skip (return no vote, log a warning),
alongside `ConflictingFirstRoundVote`. Refusing to cast the vote is the safe
action, and it fires on the leader's self-vote *before* the rebuilt proposal is
broadcast — so skipping also stops the leader from equivocating on the wire. The
node keeps running and advances that height by catching up instead of crashing.
Genuine consensus faults (`CertificateRoundMismatch`, `SafetyViolation`, …) stay
fatal.

### Regression guards

- `node`: `a_safe_vote_refusal_is_skipped_but_other_failures_stay_fatal` — both
  `ConflictingFirstRoundVote` and `LocalDoubleVote` are skipped (return
  `Ok(None)`); a genuine fault still returns `Err`.
- Behavioural: the soak no longer produces any `refused to double vote` /
  `coordinator stopped` crash; the refusals now appear as the expected
  `declined to sign a second, different vote …` warning and the node stays up.

### Systematic audit for siblings

The two first-round-vote refusals (`ConflictingFirstRoundVote`, `LocalDoubleVote`)
are now both handled as safe skips at the two `vote_for_proposal` call sites.
Every other `ConsensusError` reaching the coordinator stays fatal, which is
correct: `SafetyViolation`, `CertificateRoundMismatch`, and `HeightOverflow`
represent genuine invariant breaks, not routine refusals.

### Known follow-on (not fixed here)

Fixing the crash exposed the *next* layer, which the soak also surfaced and which
is tracked as recovery work under TD-012, not resolved here: a validator that
falls behind under load does not reliably *complete* catch-up. Its consensus
coordinator catches up on finalized orders, but the execution pipeline cannot
execute them without the corresponding `KestrelCast` payloads, and payload repair
does not reliably recover them during a catch-up burst — so the INC-002 progress
guard eventually (and correctly) stops the node. Separately, a behind leader
re-proposes its stale height every tick (now skipped, not fatal) instead of
transitioning cleanly to catching up. These are catch-up/state-sync robustness
gaps, not new fatal-classification bugs.
