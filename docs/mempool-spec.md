# Kestrel mempool specification

## Localized fee scopes

Every admitted transaction declares one primary fee scope: an object it touches or its sender account. Queue depth raises the base fee only for that scope. An object-scoped transaction must include that object in its declared touched-object set. Admission fixes the local base price; the payer's cap must cover it plus the priority fee.

Block construction visits scopes in canonical `BTreeMap` order and selects them round-robin, subject to a configured per-scope limit. Work needed to reach an unrelated scope is independent of the number of transactions waiting behind a hot object.

## Fee settlement

The compute limit reserves eligibility but does not determine payment. Settlement multiplies actual compute consumed by the admitted local base plus priority fee. The payer is debited and the selected validator receives the entire amount. Kestrel has no fee burn in this phase.

## Application sequencing

An application may register a deterministic `OrderingPolicy` for its own object or account scope. The policy compares admitted transactions and may inspect opaque application policy data. Policy registration is immutable for the running instance; replacement requires an explicit higher-level transition. The default policy sorts priority fee descending, then arrival sequence and transaction ID ascending.

Network/RPC ingress is wired into this admission path in the production node (see `docs/TECH_DEBT.md` TD-002/TD-005), and an admitted-but-not-yet-finalized transaction is durably logged so it survives a restart (TD-015). TEE-based fair ordering, encrypted mempools, cross-scope application policies, and versioned/persisted `OrderingPolicy` registrations remain deferred.
