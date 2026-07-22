# Kestrel consensus specification

## Resilience model

Kestrel-BFT uses an explicit 20+20 stake model, not classical 3f+1 resilience:

- safety requires Byzantine stake to be strictly less than 20%;
- liveness additionally permits a separate, disjoint crash/offline set of up to 20%;
- therefore responsive honest stake remains strictly above 60% in the admitted model.

The strict inequality is consensus-critical. Two 60% quorums intersect in at least 20% stake. When Byzantine stake is below 20%, that intersection contains honest stake, and the honest single-vote and locking rules prevent conflicting certificates. At exactly 20%, a Byzantine set can equivocate while two 40% honest partitions each form a conflicting 60% certificate. Exactly 20% Byzantine stake is outside the safety model and is rejected by fault-model validation.

The first-round choices are mutually exclusive: an honest replica signs either an order vote for a proposal or a timeout vote for that height and view, never both. This also closes the selective-delivery fast-certificate case. An 80% fast certificate and a 60% timeout certificate intersect in at least 40% stake; with less than 20% Byzantine stake, forming both would require honest stake to sign both first-round choices. Without the timeout certificate, replicas cannot advance and vote for a conflicting later-view proposal.

## Ordering and finality

Validator order is deterministic and leaders rotate by height plus view. A proposal commits to height, parent, and ordered transaction hashes. Consensus does not compute or vote on execution effects.

The first-round order vote has two certificate interpretations:

- at 80% stake it forms a fast certificate and finalizes in one round;
- at 60% stake it forms a prepare certificate and locks the block.

A prepared block enters the fallback's distinct second round. A 60% commit certificate finalizes it. Honest replicas issue at most one vote per height, view, and phase and refuse to sign both order and timeout in the same view. A locked replica votes for the same block or for a proposal carrying a valid higher-view prepare certificate. Certificate verification recomputes signer stake, enforces sorted unique signer identities, and verifies the BLS aggregate.

Local timers produce timeout votes only when the replica has not already signed an order vote in that view. A 60% timeout certificate advances the view, and deterministic rotation selects a different leader. There is no global cryptographic clock. In the raw-TCP coordinator, authenticated leader proposals are relayed to peers and voting is delayed briefly so a same-view equivocation can be observed before an honest replica chooses its first-round vote.

## Vote authentication and storage

Validator votes use BLS12-381 in the minimum-public-key configuration. Validator admission verifies a proof of possession for every public key, preventing rogue-key attacks against fast aggregate verification. Individual votes are verified and aggregated locally. Only aggregate certificates—with signer identities, signed stake, target, phase, and aggregate signature—are intended for gossip and durable storage.

## Deferred execution

Finality emits an ordered block ID and transaction hashes. `DeferredExecutor` owns state in a separate worker stage and accepts the corresponding executable payloads through a one-block bounded channel. Ordering can finalize the next height without polling or waiting for the previous execution result. Execution remains canonical Phase 2 parallel execution; state roots do not feed back into the ordering vote.

The one-block bound is backpressure, not a consensus dependency. A node coordinator must stop admitting additional ordered payloads if execution falls more than one buffered block behind.

## Phase 6 slashing evidence

Kestrel slashes only behavior that can be proven from authenticated protocol messages. `EquivocationEvidence` contains two individually valid BLS votes from the same admitted validator at the same height, view, and phase, but for different block IDs. Verification checks both signatures against the epoch stake table. Evidence has an order-independent hash and can be recorded only once.

The penalty is a genesis-configured 1–10,000 basis-point fraction of the offender's current stake, rounded up, and is emitted as a `SlashingDirective` for application at an epoch boundary. The Phase 6 evidence book does not mutate an active epoch's immutable validator set.

Downtime, latency, packet loss, partitions, vote withholding, malformed unauthenticated messages, and being on the losing side of a fork are not independently provable Byzantine behavior and are not slashable. Signed proposal equivocation can be added only after proposals themselves gain authenticated envelopes.
