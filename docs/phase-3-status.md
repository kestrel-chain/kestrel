# Phase 3 status

## Implemented

- Added libp2p 0.56 TCP transport with Noise security, Yamux multiplexing, mDNS local discovery, and identify.
- Added signed transaction gossipsub on its own protocol, topic, inbound/outbound queues, and message-size budget.
- Added targeted request/response shred delivery on a distinct protocol and independent queues. The transport does not regossip shreds, so leaders and relays explicitly enforce a single relay layer.
- Added KestrelCast Reed–Solomon encoding with 10 data and 10 parity shreds by default, giving a 50% reconstruction threshold.
- Added metadata validation, duplicate/index checks, insufficient-shred errors, and reconstructed block-hash verification.
- Added deterministic stake-weighted relay selection without replacement and configurable shred replication across relays.
- Added a deterministic local-devnet simulator with validator stake, latency, jitter, loss, scheduled relay death, actual subset reconstruction, and stake-percentile timing.

## Acceptance tests

- Property-generated payloads reconstruct from arbitrary shuffled 10-of-20 shred subsets.
- Corrupted reconstruction fails the block integrity check.
- Relay plans are deterministic and replicate every shred as configured.
- Queue-isolation tests prove a saturated shred path does not block transaction gossip.
- A 25-validator scenario kills four of 16 selected relays at 9 ms during paced emission. The four nodes become unavailable; 21/25 validators (84% of stake) reconstruct, and 80% stake is reached at 14 ms.
- A 32-validator LAN scenario with 4–6 ms base one-way latency, up to 4 ms jitter, and 2% per-link loss reconstructs at all validators and reaches 80% stake at 14 ms.

Run `cargo run -p testkit --example phase_3_report` to reproduce the simulated percentile measurements.

## Benchmarks

`cargo bench -p testkit --bench propagation` measures the deterministic simulator with a 512 KiB block:

| Validators | Median simulator runtime | Processing throughput |
| ---: | ---: | ---: |
| 20 | 46.159 ms | 10.832 MiB/s |
| 50 | 113.81 ms | 4.393 MiB/s |
| 100 | 225.86 ms | 2.214 MiB/s |

These Criterion figures measure the cost of executing the simulation, not simulated network latency. Both modeled protocol latency measurements are 14 ms and below the Phase 3 LAN-simulation target of 50 ms. The simulator does not run libp2p, so it provides no comparative evidence for or against replacing libp2p with a custom transport; that decision remains open pending an integrated socket-level benchmark.

## Verification

The Phase 3 workspace passes:

- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`

On the benchmark machine, the test and clippy commands require Xcode's library directory in `LIBCLANG_PATH` and `DYLD_LIBRARY_PATH` so the existing RocksDB bindgen dependency can load `libclang.dylib`. This is a local build-environment requirement, not a networking source change.

## Explicitly deferred

- Public bootstrap discovery, NAT traversal, persistent peer scoring/bans, and production operator configuration.
- Real multi-process, kernel/socket, geographically distributed, and public-internet measurements.
- Adaptive relay-count/replication tuning from live telemetry.
- Consensus, voting, block ordering, and all Phase 4 work.
