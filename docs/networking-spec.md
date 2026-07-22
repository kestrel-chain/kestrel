# Kestrel networking specification

## Phase 3 transport and discovery

Nodes use libp2p 0.56 over TCP with Noise authentication/encryption and Yamux multiplexing. mDNS supplies local-devnet peer discovery and identify advertises `/kestrel/identify/1`. Every validator's genesis-configured `gossip_peer_id`/`gossip_address` is dialed explicitly as a bootstrap peer, so the validator set does not depend on mDNS; public discovery for non-preconfigured peers and NAT traversal remain deferred (see `docs/TECH_DEBT.md` TD-014).

A peer that repeatedly sends an invalid transaction or shred is banned: `network::NetworkHandle::ban_peer` closes its live connection immediately and blocks all future connections in both directions via `libp2p::allow_block_list`. `Stage2Pipeline` tracks an in-memory per-peer offense count and bans automatically once it crosses a configurable threshold. This ban state is in-memory only — not persisted, not shared with other validators, and never automatically lifted.

Transaction and block propagation are deliberately independent:

- Transactions use signed gossipsub on `kestrel/transactions/1`, with their own bounded inbound/outbound queues and maximum message size.
- Shreds use targeted libp2p request/response streams on `/kestrel/shreds/1`, with separate bounded queues and size limits. Shreds are never automatically regossiped. A leader sends explicitly to selected relays; each relay sends directly to validators once. This enforces one relay layer rather than a multi-hop tree or mesh.

Queue saturation or oversized input on one path does not consume capacity from the other path.

## KestrelCast

The default code is 10 data plus 10 parity shreds. All shreds have equal payload length and commit to the block hash, original byte length, shred index, and coding dimensions. Any 10 distinct valid shreds reconstruct the block. Reconstruction rejects inconsistent metadata, duplicates, out-of-range indices, insufficient subsets, and final block-hash mismatches.

Relay selection is stake-weighted sampling without replacement, seeded by the block ID so every validator derives the same plan. Each shred is assigned to a configurable number of selected relays. Relay selection uses protocol-independent validator IDs; the node layer maps those IDs to discovered libp2p peer IDs.

## Simulation model

`testkit::PropagationSimulator` requires at least 20 validators and deterministically models:

- validator stake;
- one-way latency and bounded jitter;
- independent per-link loss;
- leader-to-relay delivery;
- paced relay-to-validator shred emission;
- relays becoming completely unavailable at configured event times;
- earliest arrival of replicated shreds;
- actual Reed–Solomon reconstruction time per validator;
- time to reconstructed stake thresholds, including 80%.

The model has exactly one relay layer and no consensus behavior. Simulated latency is an acceptance proxy, not a public-internet or kernel/socket measurement.

## Stage 2 application boundary

`node::BlockLifecycle` accepts any sufficient `Shred` subset through the
production KestrelCast decoder, verifies the reconstructed signed payload against
its final consensus certificate and canonical transaction order, and hands the
decoded operations to deferred execution and durable commit. This establishes
the payload-to-application boundary in production code. The shipped `node`
binary's validator path (`Stage2Pipeline` plus
`ConsensusCoordinator::bind_with_pipeline`) composes the running libp2p node,
relay selection, leader mempool, and consensus into one process, proven both
in-process and across real separate OS processes with a transaction submitted
over public RPC; running that composition across real machines/geography with
injected network faults remains open (see `docs/TECH_DEBT.md` TD-003).
