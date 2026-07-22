# Kestrel state specification

## Phase 1 model

Every live value is an `Object` keyed by a 32-byte `ObjectId`. Objects have an owner (`Single(Address)` or `Shared`), type tag, monotonically increasing version, opaque bytes, and rent balance. Creation starts at version zero; mutation and transfer increment the version exactly once; deletion requires the caller's expected version.

Move modules and resources use deterministic object IDs:

- module: `BLAKE3("kestrel/move/module/v1" || account || framed_module_name)`
- resource: `BLAKE3("kestrel/move/resource/v1" || account || framed_struct_tag)`

Modules are shared objects. Move resources are single-owner objects whose owner bytes equal the Move account address.

## Merkle commitment

Phase 1 uses a deterministic binary Merkle prefix trie over the bits of each object ID. Leaves commit to a domain separator, complete object ID, and canonical BCS object bytes. Branches commit to their depth and both child hashes. Empty subtrees have depth-specific hashes.

Active and expired objects have separate roots. The public state root commits to both roots, the current epoch, and the rent-rate genesis parameter. BTree-ordered input plus complete key commitments makes the result independent of insertion order and host map randomization.

## Storage rent and expiry

`rent_per_object_per_epoch` is immutable Phase 1 genesis configuration and must be nonzero. Every epoch transition subtracts that rate with saturation. An object reaching zero moves atomically from the active tree into the expired tree. Its history record stores:

- the final object bytes;
- exact expiry epoch;
- active-tree root immediately before the expiry charge;
- the pre-charge object bytes and their compressed binary-Merkle inclusion path.

Advancing multiple epochs executes each transition in order so expiry metadata remains exact. Expired IDs cannot be recreated through ordinary creation.

## Phase 5 resurrection

An expired object can return to the active tree only through a `ResurrectionWitness`. The witness contains the exact pre-expiry object, its historical active root, expiry epoch, and a compressed inclusion path containing only occupied trie branches. `verify_resurrection_witness` is stateless: it reconstructs the leaf and branch hashes from those fields and requires the result to equal the claimed historical root.

Stateful resurrection additionally requires the witness to match the retained expired record, removes that record, increments the object's version, supplies a nonzero fresh rent balance, and inserts it into the current active tree. The resulting current state root therefore commits atomically to both removal from expired state and insertion into active state. A consumed witness cannot be replayed.

The 256-object regression fixture produces fewer than 32 proof steps and a BCS witness below 2 KiB. This is a small witness for the current binary trie and supports stateless validation without changing Phase 1 root semantics. A production Verkle/vector-commitment migration, trusted setup or commitment-library selection, proof batching, and historical-root pruning policy remain deferred.

## Durable application checkpoints

`StateTree::durable_snapshot` materializes active objects, retained expired
records, epoch, rent configuration, format version, and the complete state root
in canonical order. Restoration rejects duplicate or overlapping object IDs,
inconsistent retained-expiry identities, invalid resurrection proofs, unknown
formats, and any root mismatch.

The Phase 6 node lifecycle atomically stores this root-bound snapshot with the
finalized block record, aggregate certificate, and admission nonces. The tree
remains in memory while executing, but a restarted lifecycle restores the last
committed application root before accepting the next height. Authenticated
snapshot transfer, late-join state synchronization, in-flight block recovery,
and pruning remain separate Stage 2/3 work. Parallel and cross-VM execution must
not change state-transition or root semantics.

`DeferredExecutor` now drives epoch advancement automatically: before executing
a block's transactions it advances state to `height / blocks_per_epoch`, so
rent is charged and exhausted objects expire purely from blocks being
committed, with no explicit rent transaction required, and the resulting root
(and epoch) is exactly what gets persisted and restored above. Validator/stake
reconfiguration at an epoch boundary remains separate, higher-risk work (see
`docs/TECH_DEBT.md` TD-013).
