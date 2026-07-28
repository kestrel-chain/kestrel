import importlib.util
import json
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "rpc_load_abuse.py"
SPEC = importlib.util.spec_from_file_location("rpc_load_abuse", SCRIPT)
assert SPEC and SPEC.loader
harness = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = harness
SPEC.loader.exec_module(harness)


class MockKestrelHandler(BaseHTTPRequestHandler):
    maximum_body_bytes = 512
    maximum_batch_length = 4
    requests_per_window = 12
    rate_window_seconds = 0.1
    lock = threading.Lock()
    window_started = time.monotonic()
    window_calls = 0
    metrics = {
        "kestrel_rpc_requests_total": 0,
        "kestrel_rpc_rejected_total": 0,
        "kestrel_rpc_errors_total": 0,
        "kestrel_rpc_latency_microseconds_total": 0,
    }

    def log_message(self, _format, *_args):
        pass

    def send_bytes(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Expected when the harness closes intentionally incomplete slow
            # clients after proving healthy requests remain responsive.
            pass

    def do_GET(self):
        if self.path != "/metrics":
            self.send_bytes(404, b"")
            return
        with self.lock:
            text = "".join(
                f"{name} {value}\n" for name, value in self.metrics.items()
            ).encode()
        self.send_bytes(200, text, "text/plain; version=0.0.4")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > self.maximum_body_bytes:
            self.send_bytes(413, b"body too large", "text/plain")
            return
        body = self.rfile.read(length)
        started = time.monotonic()
        try:
            request = json.loads(body)
        except json.JSONDecodeError:
            request = None
            parse_error = True
        else:
            parse_error = False

        if isinstance(request, list) and request:
            cost = (
                len(request)
                if len(request) <= self.maximum_batch_length
                else 1
            )
        else:
            cost = 1
        with self.lock:
            handler = type(self)
            now = time.monotonic()
            if now - handler.window_started >= handler.rate_window_seconds:
                handler.window_started = now
                handler.window_calls = 0
            self.metrics["kestrel_rpc_requests_total"] += 1
            if handler.window_calls + cost > handler.requests_per_window:
                self.metrics["kestrel_rpc_rejected_total"] += 1
                self.send_bytes(429, b"rate limit exceeded", "text/plain")
                return
            handler.window_calls += cost

        if parse_error:
            response = {
                "jsonrpc": "2.0",
                "error": {"code": -32700, "message": "parse error"},
                "id": None,
            }
            self.record_error(started)
            self.send_json(response)
            return
        if isinstance(request, list) and len(request) > self.maximum_batch_length:
            response = {
                "jsonrpc": "2.0",
                "error": {"code": -32600, "message": "batch limit exceeded"},
                "id": None,
            }
            with self.lock:
                self.metrics["kestrel_rpc_rejected_total"] += 1
            self.record_error(started)
            self.send_json(response)
            return

        calls = request if isinstance(request, list) else [request]
        responses = [
            {
                "jsonrpc": "2.0",
                "result": {
                    "chainId": "mock-kestrel",
                    "genesisHash": "mock-genesis",
                    "finalizedHeight": 7,
                    "finalizedBlock": "block-7",
                },
                "id": call.get("id"),
            }
            for call in calls
        ]
        self.record_latency(started)
        self.send_json(responses if isinstance(request, list) else responses[0])

    def record_latency(self, started):
        elapsed = max(1, int((time.monotonic() - started) * 1_000_000))
        with self.lock:
            self.metrics["kestrel_rpc_latency_microseconds_total"] += elapsed

    def record_error(self, started):
        with self.lock:
            self.metrics["kestrel_rpc_errors_total"] += 1
        self.record_latency(started)

    def send_json(self, payload):
        self.send_bytes(
            200, json.dumps(payload, separators=(",", ":")).encode()
        )


class RpcLoadAbuseTests(unittest.TestCase):
    def test_percentile_uses_linear_interpolation(self):
        self.assertEqual(harness.percentile([1.0, 2.0, 3.0], 0.5), 2.0)
        self.assertEqual(harness.percentile([0.0, 10.0], 0.95), 9.5)
        self.assertIsNone(harness.percentile([], 0.95))

    def test_non_loopback_requires_explicit_authorization(self):
        with self.assertRaisesRegex(harness.HarnessError, "non-loopback"):
            harness.validate_target("https://validator.example/", False)
        parsed = harness.validate_target("https://validator.example/", True)
        self.assertEqual(parsed.hostname, "validator.example")

    def test_unsafe_sample_count_is_rejected_before_network_access(self):
        args = harness.build_parser().parse_args(
            [
                "--duration-seconds",
                "3600",
                "--requests-per-second",
                "10000",
                "--output",
                "unused.json",
            ]
        )
        with self.assertRaisesRegex(harness.HarnessError, "unsafe"):
            harness.validate_args(args)

    def test_failed_gate_still_writes_immutable_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "failed.json"
            args = harness.build_parser().parse_args(["--output", str(output)])
            report = {
                "passed": False,
                "workload": {
                    "success_ratio": 0.5,
                    "achieved_calls_per_second": 1.0,
                    "latency_ms": {"p95": 999.0},
                },
            }
            with mock.patch.object(harness, "execute", return_value=report):
                with self.assertRaisesRegex(harness.HarnessError, "gates failed"):
                    harness.run(args)
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")), report
            )
            with self.assertRaisesRegex(harness.HarnessError, "overwrite"):
                harness.run(args)

    def test_local_campaign_exercises_load_abuse_metrics_and_evidence(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), MockKestrelHandler)
        server.daemon_threads = True
        task = threading.Thread(target=server.serve_forever, daemon=True)
        task.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "rpc-evidence.json"
                args = harness.build_parser().parse_args(
                    [
                        "--url",
                        f"http://127.0.0.1:{server.server_port}/",
                        "--duration-seconds",
                        "0.2",
                        "--requests-per-second",
                        "20",
                        "--batch-size",
                        "1",
                        "--concurrency",
                        "2",
                        "--timeout-seconds",
                        "1",
                        "--min-success-ratio",
                        "1",
                        "--max-p95-ms",
                        "1000",
                        "--maximum-body-bytes",
                        "512",
                        "--maximum-batch-length",
                        "4",
                        "--slow-clients",
                        "2",
                        "--slow-probe-requests",
                        "2",
                        "--slow-hold-seconds",
                        "0.05",
                        "--max-slow-probe-p95-ms",
                        "1000",
                        "--rate-limit-calls",
                        "12",
                        "--rate-window-seconds",
                        "0.1",
                        "--output",
                        str(output),
                    ]
                )
                try:
                    result = harness.run(args)
                except harness.HarnessError:
                    self.fail(output.read_text(encoding="utf-8"))
                self.assertEqual(result, 0)
                report = json.loads(output.read_text(encoding="utf-8"))
                self.assertTrue(report["passed"])
                self.assertEqual(report["workload"]["envelopes_attempted"], 4)
                self.assertTrue(
                    all(probe["passed"] for probe in report["abuse"].values())
                )
                self.assertGreaterEqual(
                    report["metrics"]["delta"]["kestrel_rpc_errors_total"], 2
                )
                self.assertGreaterEqual(
                    report["metrics"]["delta"]["kestrel_rpc_rejected_total"], 2
                )
                with self.assertRaisesRegex(harness.HarnessError, "overwrite"):
                    harness.run(args)
        finally:
            server.shutdown()
            server.server_close()
            task.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
