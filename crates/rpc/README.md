# rpc

Phase 6 public HTTP API with JSON-RPC 2.0, liveness/readiness probes, and Prometheus text metrics. The surface enforces request-body, batch, and per-IP rate limits; every call in a JSON-RPC batch consumes one rate-limit unit, so batching cannot multiply a client's effective allowance. It exposes the read methods `kestrel_getStatus` and `kestrel_getObject`, plus `kestrel_submitTransaction` only when the node supplies the production admission sink. Administrative or validator-control methods are intentionally not exposed.

Run `scripts/rpc_load_abuse.py` against an authorized node to collect bounded
load and abuse evidence. The harness uses only `kestrel_getStatus`, defaults to
loopback, verifies malformed JSON, batch/body limits, partial-body slow clients,
fixed-window rate limiting, latency/error gates, and Prometheus counter deltas,
and writes an immutable JSON report.
