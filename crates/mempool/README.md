# mempool

Phase 5 transaction admission, localized fee ordering, and application sequencing hooks.

Each transaction selects an object or account fee scope. Congestion changes the admission base price only in that scope, and deterministic round-robin block selection caps each scope's contribution so a hot object cannot starve unrelated traffic. Admission reserves `compute_limit * max_fee_per_compute`; canonical settlement charges actual compute, transfers the full base plus priority fee from payer to validator, and releases the unused reservation. Competing admissions cannot reserve the same balance twice.

The Stage 2 pipeline independently bounds admitted transaction count and encoded bytes globally and per gossip peer. Accounting remains charged until canonical submission or rollback, including across durable admission replay after restart.

Applications can register one deterministic `OrderingPolicy` for their own scope. The default orders higher priority fee first with canonical arrival sequence and transaction ID tie-breakers.
