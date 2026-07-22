# vm-evm

Phase 5 co-resident `revm` execution and native-object precompiles.

`EvmHost` deploys and calls ordinary EVM bytecode while sharing the canonical `state::StateTree`. The reserved native-object precompile supports reads and version-checked writes. Native changes are transactional with the EVM call and are discarded on failure.
