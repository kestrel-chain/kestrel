# storage

Persistence boundary for Kestrel. The `KvStore` trait isolates protocol code from its backend, while Phase 0 provides a RocksDB implementation with deterministic prefix scans and atomic write batches.

