# rpc

Phase 6 public HTTP API with JSON-RPC 2.0, liveness/readiness probes, and Prometheus text metrics. The surface enforces request-body, batch, and per-IP rate limits and exposes only read methods: `kestrel_getStatus` and `kestrel_getObject`. Administrative or validator-control methods are intentionally not exposed.
