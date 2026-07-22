# cli

Phase 6 validator onboarding and genesis tooling. `validator-init` creates BLS key material with owner-only permissions and a public proof-of-possession profile without printing the secret. `genesis-create` assembles profiles into canonical genesis JSON; `genesis-validate` verifies its hash, initial state root, stake table, endpoints, signature schemes, rent, and slashing parameters.
