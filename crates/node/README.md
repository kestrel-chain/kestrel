# node

Phase 6 validator process boundary and deterministic genesis validation. `node run --genesis PATH` initializes canonical genesis state, emits structured JSON tracing, serves hardened RPC/metrics with graceful shutdown, and refuses non-loopback RPC binding unless the operator explicitly supplies `--allow-public-rpc`.

Supplying `--validator-id`, `--validator-key`, and `--data-dir` together enables the raw-TCP consensus coordinator. It relays authenticated proposals, exchanges BLS votes and certificates, persists replica vote/lock safety state in RocksDB, and updates finalized RPC status. A five-process fault test covers leader death, partition, equivocation, vote withholding, and corrupt votes.

Stage 2 is still not promoted: the coordinator orders synthetic hashes on a separate transport and is not yet composed with libp2p transaction gossip, KestrelCast block reconstruction, deferred execution, or durable application-state checkpoints.
