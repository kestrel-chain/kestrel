# node

Phase 6 validator process boundary and deterministic genesis validation. `node run --genesis PATH` initializes canonical genesis state, emits structured JSON tracing, serves hardened RPC/metrics with graceful shutdown, and refuses non-loopback RPC binding unless the operator explicitly supplies `--allow-public-rpc`.

Supplying `--validator-id`, `--validator-key`, `--gossip-key`, and `--data-dir`
together enables the full validator pipeline. The raw-TCP coordinator relays
authenticated proposals, exchanges BLS votes and certificates, and persists
replica vote/lock safety state. A separate libp2p service carries signed
transaction gossip and `KestrelCast` shreds. Certified payloads run through
deferred execution and are atomically committed with application state and
certificates in RocksDB. Multi-process tests cover the healthy RPC-submission
path plus leader death, partition, equivocation, vote withholding, and corrupt
votes.

Stage 2 is still not promoted because the integrated pipeline has only been
exercised on one host. The remaining gate is a controlled multi-machine,
multi-region run with real propagation/finality evidence and authorized fault
churn; `scripts/stage2_campaign_monitor.py` and
`scripts/stage2_campaign_report.py` provide the provider-neutral observation
and evidence layer for that run.
