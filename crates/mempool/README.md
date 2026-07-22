# mempool

Phase 5 transaction admission, localized fee ordering, and application sequencing hooks.

Each transaction selects an object or account fee scope. Congestion changes the admission base price only in that scope, and deterministic round-robin block selection caps each scope's contribution so a hot object cannot starve unrelated traffic. Settlement charges actual compute and transfers the full base plus priority fee from payer to validator; nothing is burned.

Applications can register one deterministic `OrderingPolicy` for their own scope. The default orders higher priority fee first with canonical arrival sequence and transaction ID tie-breakers.
