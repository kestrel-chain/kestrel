# consensus

Kestrel-BFT ordering, rotating leaders, 80% one-round fast finality, 60% two-round fallback finality, local timeout certificates, locking, and proof-of-possession protected BLS aggregate certificates. Safety assumes strictly less than 20% Byzantine stake; liveness additionally tolerates a separate 20% offline stake. Phase 6 adds replay-protected slashing directives only for cryptographically proven same-round double votes; downtime and network faults are never slashable evidence.
