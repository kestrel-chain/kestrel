# Kestrel EVM interoperability specification

## Co-resident execution

Kestrel embeds `revm` 36 as a second execution environment, not as a separate chain. Its account database and the native object tree participate in one host transaction. Native state is cloned before an EVM call and committed only if that call succeeds.

## Native-object precompile

The reserved address `0x0000000000000000000000000000000000000f00` accepts:

- read: `0x00 || object_id[32]`, returning the object's opaque data;
- write: `0x01 || object_id[32] || expected_version_u64_be || replacement_data`.

Writes increment the normal native object version and fail when the expected version is stale. Writes from `STATICCALL` fail. Precompile gas is a fixed base plus a per-byte input/output charge.

The Phase 5 deployed-contract fixture is deliberately small: its runtime forwards calldata to the precompile and returns the result. The integration test compiles and publishes an actual Move `Token` module, invokes its `mint` entry function, reads and writes the resulting Move resource through deployed EVM code, then invokes Move `transfer` and asserts the EVM-written balance is observed. A stale EVM write is a hard error and leaves the root unchanged.

Solidity ABI bindings, EVM transaction envelopes, persistent EVM account storage, production gas calibration, cross-VM reentrancy rules, and exposing arbitrary Move entry functions as precompiles are deferred.
