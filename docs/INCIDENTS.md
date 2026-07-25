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
