# Kestrel

Kestrel is a single-shard, general-purpose Layer-1 blockchain implemented in Rust. Phases 0–5 provide deterministic object state and Move execution, optimistic parallel execution, erasure-coded propagation, Kestrel-BFT, localized fee markets, co-resident EVM execution, and witnessed state resurrection. Phase 6 adds public-testnet operational tooling while retaining explicit real-deployment promotion gates.

## Development

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
cargo bench --workspace
```

See [`docs/phase-6-status.md`](docs/phase-6-status.md) for the current phase gate and [`docs/testnet-operations.md`](docs/testnet-operations.md) for operator procedures.

`rust-rocksdb` generates bindings with Clang. On macOS Command Line Tools installations where `libclang` is not already discoverable, prefix the first build with:

```sh
DYLD_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib \
LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib \
cargo test --workspace --all-targets
```
