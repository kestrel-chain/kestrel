#!/usr/bin/env python3
"""Run a bounded, read-only load and abuse campaign against Kestrel JSON-RPC."""

from __future__ import annotations

import argparse
import ipaddress
import json
import math
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class HarnessError(Exception):
    """Invalid input, transport failure, or failed acceptance gate."""


MAXIMUM_SAMPLES = 100_000
MAXIMUM_PROBE_BODY_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class Sample:
    ok: bool
    status: int
    latency_ms: float
    calls: int
    error: str | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_target(url: str, allow_non_loopback: bool) -> urllib.parse.ParseResult:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise HarnessError("RPC URL must be an absolute http:// or https:// URL")
    if parsed.username or parsed.password or parsed.fragment:
        raise HarnessError("RPC URL must not contain credentials or a fragment")
    if allow_non_loopback:
        return parsed
    hostname = parsed.hostname
    is_loopback = hostname == "localhost"
    if not is_loopback:
        try:
            is_loopback = ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            is_loopback = False
    if not is_loopback:
        raise HarnessError(
            "refusing to load a non-loopback target without --allow-non-loopback"
        )
    return parsed


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def rpc_payload(batch_size: int, first_id: int = 1) -> bytes:
    calls = [
        {
            "jsonrpc": "2.0",
            "id": first_id + index,
            "method": "kestrel_getStatus",
        }
        for index in range(batch_size)
    ]
    value: Any = calls[0] if batch_size == 1 else calls
    return json.dumps(value, separators=(",", ":")).encode()


def http_request(
    url: str,
    body: bytes,
    timeout_seconds: float,
) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Connection": "close"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def valid_status_response(payload: Any, batch_size: int) -> bool:
    responses = payload if batch_size > 1 else [payload]
    return (
        isinstance(responses, list)
        and len(responses) == batch_size
        and all(
            isinstance(response, dict)
            and isinstance(response.get("result"), dict)
            and "chainId" in response["result"]
            and "genesisHash" in response["result"]
            for response in responses
        )
    )


def send_sample(
    url: str,
    body: bytes,
    batch_size: int,
    timeout_seconds: float,
    scheduled_at: float,
) -> Sample:
    try:
        status, encoded = http_request(url, body, timeout_seconds)
        latency_ms = (time.monotonic() - scheduled_at) * 1_000
        if status != 200:
            return Sample(False, status, latency_ms, batch_size, f"http_{status}")
        try:
            payload = json.loads(encoded)
        except json.JSONDecodeError:
            return Sample(False, status, latency_ms, batch_size, "invalid_json")
        if not valid_status_response(payload, batch_size):
            return Sample(False, status, latency_ms, batch_size, "invalid_rpc_result")
        return Sample(True, status, latency_ms, batch_size)
    except (OSError, urllib.error.URLError, TimeoutError) as error:
        latency_ms = (time.monotonic() - scheduled_at) * 1_000
        return Sample(False, 0, latency_ms, batch_size, type(error).__name__)


def summarize_samples(
    samples: list[Sample],
    started: float,
    finished: float,
    offered_envelopes_per_second: float,
    batch_size: int,
) -> dict[str, Any]:
    successes = [sample for sample in samples if sample.ok]
    latencies = [sample.latency_ms for sample in successes]
    errors = Counter(
        sample.error or f"http_{sample.status}" for sample in samples if not sample.ok
    )
    elapsed = max(finished - started, 1e-9)
    return {
        "envelopes_attempted": len(samples),
        "calls_attempted": sum(sample.calls for sample in samples),
        "envelopes_succeeded": len(successes),
        "calls_succeeded": sum(sample.calls for sample in successes),
        "success_ratio": len(successes) / len(samples) if samples else 0.0,
        "offered_envelopes_per_second": offered_envelopes_per_second,
        "offered_calls_per_second": offered_envelopes_per_second * batch_size,
        "achieved_envelopes_per_second": len(successes) / elapsed,
        "achieved_calls_per_second": sum(sample.calls for sample in successes)
        / elapsed,
        "elapsed_seconds": elapsed,
        "latency_ms": {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "maximum": max(latencies) if latencies else None,
        },
        "errors": dict(sorted(errors.items())),
    }


def run_load(args: argparse.Namespace) -> dict[str, Any]:
    count = max(1, math.ceil(args.duration_seconds * args.requests_per_second))
    if count > MAXIMUM_SAMPLES:
        raise HarnessError(
            f"refusing {count} samples; maximum is {MAXIMUM_SAMPLES}"
        )
    started = time.monotonic()
    futures = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        for index in range(count):
            scheduled_at = started + index / args.requests_per_second
            remaining = scheduled_at - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            futures.append(
                executor.submit(
                    send_sample,
                    args.url,
                    rpc_payload(args.batch_size, index * args.batch_size + 1),
                    args.batch_size,
                    args.timeout_seconds,
                    scheduled_at,
                )
            )
        samples = [future.result() for future in futures]
    finished = time.monotonic()
    return summarize_samples(
        samples,
        started,
        finished,
        args.requests_per_second,
        args.batch_size,
    )


def json_rpc_error_probe(
    url: str,
    body: bytes,
    timeout_seconds: float,
    expected_code: int,
    expected_message: str,
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        status, encoded = http_request(url, body, timeout_seconds)
        payload = json.loads(encoded)
        error = payload.get("error", {}) if isinstance(payload, dict) else {}
        passed = (
            status == 200
            and error.get("code") == expected_code
            and error.get("message") == expected_message
        )
        return {
            "passed": passed,
            "http_status": status,
            "rpc_error": error,
            "latency_ms": (time.monotonic() - started) * 1_000,
        }
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        return {
            "passed": False,
            "error": str(error),
            "latency_ms": (time.monotonic() - started) * 1_000,
        }


def oversized_body_probe(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    try:
        status, _ = http_request(
            args.url,
            b" " * (args.maximum_body_bytes + 1),
            args.timeout_seconds,
        )
        return {
            "passed": status == 413,
            "http_status": status,
            "bytes": args.maximum_body_bytes + 1,
            "latency_ms": (time.monotonic() - started) * 1_000,
        }
    except (OSError, urllib.error.URLError) as error:
        return {
            "passed": False,
            "error": str(error),
            "latency_ms": (time.monotonic() - started) * 1_000,
        }


def open_slow_client(
    parsed: urllib.parse.ParseResult,
    maximum_body_bytes: int,
    timeout_seconds: float,
) -> socket.socket:
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    connection = socket.create_connection(
        (parsed.hostname, port), timeout=timeout_seconds
    )
    if parsed.scheme == "https":
        context = ssl.create_default_context()
        connection = context.wrap_socket(
            connection, server_hostname=parsed.hostname
        )
    path = parsed.path or "/"
    if parsed.query:
        path += f"?{parsed.query}"
    hostname = (
        f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    )
    host = hostname if parsed.port is None else f"{hostname}:{parsed.port}"
    headers = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {maximum_body_bytes}\r\n"
        "Connection: close\r\n\r\n"
        "{"
    ).encode()
    connection.sendall(headers)
    return connection


def slow_client_probe(
    args: argparse.Namespace,
    parsed: urllib.parse.ParseResult,
) -> dict[str, Any]:
    clients: list[socket.socket] = []
    started = time.monotonic()
    try:
        for _ in range(args.slow_clients):
            clients.append(
                open_slow_client(
                    parsed, args.maximum_body_bytes, args.timeout_seconds
                )
            )
        probe_started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [
                executor.submit(
                    send_sample,
                    args.url,
                    rpc_payload(1, 90_000 + index),
                    1,
                    args.timeout_seconds,
                    probe_started,
                )
                for index in range(args.slow_probe_requests)
            ]
            samples = [future.result() for future in futures]
        held_for = time.monotonic() - started
        if held_for < args.slow_hold_seconds:
            time.sleep(args.slow_hold_seconds - held_for)
        latencies = [sample.latency_ms for sample in samples if sample.ok]
        p95 = percentile(latencies, 0.95)
        return {
            "passed": all(sample.ok for sample in samples)
            and p95 is not None
            and p95 <= args.max_slow_probe_p95_ms,
            "partial_connections": len(clients),
            "healthy_requests": len(samples),
            "healthy_successes": sum(sample.ok for sample in samples),
            "healthy_latency_ms": {
                "p95": p95,
                "maximum": max(latencies) if latencies else None,
            },
            "gate_max_p95_ms": args.max_slow_probe_p95_ms,
            "held_seconds": max(time.monotonic() - started, args.slow_hold_seconds),
        }
    except (OSError, ssl.SSLError) as error:
        return {"passed": False, "error": str(error)}
    finally:
        for client in clients:
            try:
                client.close()
            except OSError:
                pass


def rate_limit_probe(args: argparse.Namespace) -> dict[str, Any]:
    if args.rate_limit_calls == 0:
        return {"passed": True, "skipped": True}
    time.sleep(args.rate_window_seconds + 0.05)
    remaining = args.rate_limit_calls
    envelopes = 0
    failures = []
    next_id = 100_000
    while remaining:
        calls = min(remaining, args.maximum_batch_length)
        started = time.monotonic()
        sample = send_sample(
            args.url,
            rpc_payload(calls, next_id),
            calls,
            args.timeout_seconds,
            started,
        )
        envelopes += 1
        if not sample.ok:
            failures.append(asdict(sample))
        next_id += calls
        remaining -= calls
    try:
        status, _ = http_request(
            args.url, rpc_payload(1, next_id), args.timeout_seconds
        )
    except (OSError, urllib.error.URLError) as error:
        return {
            "passed": False,
            "budgeted_calls": args.rate_limit_calls,
            "budget_envelopes": envelopes,
            "error": str(error),
            "failures": failures,
        }
    return {
        "passed": not failures and status == 429,
        "budgeted_calls": args.rate_limit_calls,
        "budget_envelopes": envelopes,
        "overflow_http_status": status,
        "failures": failures,
    }


METRIC_NAMES = {
    "kestrel_rpc_requests_total",
    "kestrel_rpc_rejected_total",
    "kestrel_rpc_errors_total",
    "kestrel_rpc_latency_microseconds_total",
}


def fetch_metrics(url: str, timeout_seconds: float) -> dict[str, int]:
    parsed = urllib.parse.urlparse(url)
    metrics_url = urllib.parse.urlunparse(
        parsed._replace(path="/metrics", params="", query="", fragment="")
    )
    request = urllib.request.Request(metrics_url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            text = response.read().decode()
    except (OSError, UnicodeDecodeError, urllib.error.URLError) as error:
        raise HarnessError(f"cannot read RPC metrics: {error}") from error
    metrics: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in METRIC_NAMES:
            try:
                metrics[parts[0]] = int(float(parts[1]))
            except ValueError as error:
                raise HarnessError(
                    f"metric {parts[0]} is not numeric"
                ) from error
    missing = METRIC_NAMES - metrics.keys()
    if missing:
        raise HarnessError(f"metrics response is missing: {sorted(missing)}")
    return metrics


def metric_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {name: after[name] - before[name] for name in sorted(METRIC_NAMES)}


def execute(args: argparse.Namespace) -> dict[str, Any]:
    started_at = utc_now()
    parsed = validate_target(args.url, args.allow_non_loopback)
    metrics_before = fetch_metrics(args.url, args.timeout_seconds)
    workload = run_load(args)
    # Isolate the deterministic abuse probes from the final fixed window used
    # by the paced workload. Otherwise a baseline deliberately run near the
    # configured limit can make the first probe observe an unrelated 429.
    time.sleep(args.rate_window_seconds + 0.05)
    abuse: dict[str, Any] = {
        "malformed_json": json_rpc_error_probe(
            args.url, b"{", args.timeout_seconds, -32700, "parse error"
        ),
        "oversized_batch": json_rpc_error_probe(
            args.url,
            rpc_payload(args.maximum_batch_length + 1),
            args.timeout_seconds,
            -32600,
            "batch limit exceeded",
        ),
        "oversized_body": oversized_body_probe(args),
    }
    if args.slow_clients:
        abuse["slow_clients"] = slow_client_probe(args, parsed)
    else:
        abuse["slow_clients"] = {"passed": True, "skipped": True}
    abuse["rate_limit"] = rate_limit_probe(args)
    metrics_after = fetch_metrics(args.url, args.timeout_seconds)
    deltas = metric_delta(metrics_before, metrics_after)
    slow_successes = int(abuse["slow_clients"].get("healthy_successes", 0))
    rate_envelopes = int(abuse["rate_limit"].get("budget_envelopes", 0))
    rate_overflow = int(not abuse["rate_limit"].get("skipped", False))
    expected_metrics = {
        # Successful workload calls definitely reached the handler; failed
        # transports may not have. The two JSON error probes, healthy requests
        # made while slow clients are held, and deterministic limiter envelopes
        # must all be visible as well.
        "kestrel_rpc_requests_total": workload["envelopes_succeeded"]
        + 2
        + slow_successes
        + rate_envelopes
        + rate_overflow,
        "kestrel_rpc_errors_total": 2,
        "kestrel_rpc_rejected_total": 1 + rate_overflow,
    }
    metrics_accounting_passed = all(
        deltas[name] >= minimum for name, minimum in expected_metrics.items()
    ) and deltas["kestrel_rpc_latency_microseconds_total"] > 0

    p95 = workload["latency_ms"]["p95"]
    gates = {
        "minimum_success_ratio": {
            "limit": args.min_success_ratio,
            "observed": workload["success_ratio"],
            "passed": workload["success_ratio"] >= args.min_success_ratio,
        },
        "maximum_p95_ms": {
            "limit": args.max_p95_ms,
            "observed": p95,
            "passed": p95 is not None and p95 <= args.max_p95_ms,
        },
        "abuse_probes": {
            "passed": all(probe.get("passed") is True for probe in abuse.values())
        },
        "metrics_monotonic": {
            "observed": deltas,
            "passed": all(value >= 0 for value in deltas.values()),
        },
        "metrics_accounting": {
            "minimum_expected": expected_metrics,
            "observed": deltas,
            "passed": metrics_accounting_passed,
        },
    }
    passed = all(gate["passed"] for gate in gates.values())
    return {
        "schema_version": 1,
        "started_at": started_at,
        "target": args.url,
        "configuration": {
            "duration_seconds": args.duration_seconds,
            "requests_per_second": args.requests_per_second,
            "batch_size": args.batch_size,
            "concurrency": args.concurrency,
            "timeout_seconds": args.timeout_seconds,
            "maximum_body_bytes": args.maximum_body_bytes,
            "maximum_batch_length": args.maximum_batch_length,
            "slow_clients": args.slow_clients,
            "rate_limit_calls": args.rate_limit_calls,
        },
        "workload": workload,
        "abuse": abuse,
        "metrics": {
            "before": metrics_before,
            "after": metrics_after,
            "delta": deltas,
        },
        "gates": gates,
        "passed": passed,
    }


def run(args: argparse.Namespace) -> int:
    validate_args(args)
    if args.output.exists():
        raise HarnessError("refusing to overwrite existing RPC evidence")
    report = execute(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    workload = report["workload"]
    p95 = workload["latency_ms"]["p95"]
    outcome = "PASS" if report["passed"] else "FAIL"
    print(
        f"{outcome}: success={workload['success_ratio']:.3%}, "
        f"calls/s={workload['achieved_calls_per_second']:.1f}, "
        f"p95={p95 if p95 is not None else 'n/a'}ms"
    )
    if not report["passed"]:
        raise HarnessError("one or more RPC load/abuse gates failed")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8899/")
    parser.add_argument("--duration-seconds", type=float, default=10)
    parser.add_argument("--requests-per-second", type=float, default=250)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--timeout-seconds", type=float, default=2)
    parser.add_argument("--min-success-ratio", type=float, default=0.99)
    parser.add_argument("--max-p95-ms", type=float, default=250)
    parser.add_argument("--maximum-body-bytes", type=int, default=512 * 1024)
    parser.add_argument("--maximum-batch-length", type=int, default=64)
    parser.add_argument("--slow-clients", type=int, default=16)
    parser.add_argument("--slow-probe-requests", type=int, default=16)
    parser.add_argument("--slow-hold-seconds", type=float, default=0.5)
    parser.add_argument("--max-slow-probe-p95-ms", type=float, default=500)
    parser.add_argument("--rate-limit-calls", type=int, default=1_000)
    parser.add_argument("--rate-window-seconds", type=float, default=1)
    parser.add_argument("--allow-non-loopback", action="store_true")
    parser.add_argument("--output", required=True, type=Path)
    return parser


def validate_args(args: argparse.Namespace) -> None:
    invalid = (
        min(
            args.duration_seconds,
            args.requests_per_second,
            args.batch_size,
            args.concurrency,
            args.timeout_seconds,
            args.maximum_body_bytes,
            args.maximum_batch_length,
            args.slow_probe_requests,
            args.slow_hold_seconds,
            args.max_p95_ms,
            args.max_slow_probe_p95_ms,
            args.rate_window_seconds,
        )
        <= 0
        or not 0 <= args.min_success_ratio <= 1
        or not 0 <= args.slow_clients <= 256
        or not 0 <= args.rate_limit_calls <= 10_000
        or args.batch_size > args.maximum_batch_length
        or args.maximum_batch_length > 1_024
        or args.maximum_body_bytes > MAXIMUM_PROBE_BODY_BYTES
        or args.concurrency > 256
        or args.requests_per_second > 10_000
        or args.duration_seconds > 3_600
        or math.ceil(args.duration_seconds * args.requests_per_second)
        > MAXIMUM_SAMPLES
    )
    if invalid:
        raise HarnessError("invalid or unsafe RPC harness limits")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except HarnessError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
