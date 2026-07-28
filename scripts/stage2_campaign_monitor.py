#!/usr/bin/env python3
"""Continuously monitor safety and liveness on an authorized Stage 2 testnet."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class MonitorError(Exception):
    """Invalid monitor input or an observed protocol invariant failure."""


@dataclass
class CampaignMonitor:
    expected_chain_id: str
    expected_genesis_hash: str
    stall_seconds: float
    canonical_blocks: dict[int, str] = field(default_factory=dict)
    maximum_height: int = 0
    last_progress_at: float | None = None
    observations: int = 0
    rpc_errors: int = 0

    def observe(self, validator: str, status: dict[str, Any], now: float) -> None:
        if status.get("chainId") != self.expected_chain_id:
            raise MonitorError(
                f"{validator} reported chain {status.get('chainId')!r}, "
                f"expected {self.expected_chain_id!r}"
            )
        if status.get("genesisHash") != self.expected_genesis_hash:
            raise MonitorError(
                f"{validator} reported genesis {status.get('genesisHash')!r}, "
                f"expected {self.expected_genesis_hash!r}"
            )
        try:
            height = int(status["finalizedHeight"])
            block = str(status["finalizedBlock"])
        except (KeyError, TypeError, ValueError) as error:
            raise MonitorError(f"{validator} returned an invalid status payload") from error
        known = self.canonical_blocks.get(height)
        if known is not None and known != block:
            raise MonitorError(
                f"safety violation at height {height}: "
                f"{validator}={block}, previously observed={known}"
            )
        self.canonical_blocks[height] = block
        self.observations += 1
        if self.last_progress_at is None or height > self.maximum_height:
            self.maximum_height = height
            self.last_progress_at = now

    def check_liveness(self, now: float) -> None:
        if (
            self.last_progress_at is not None
            and now - self.last_progress_at > self.stall_seconds
        ):
            raise MonitorError(
                f"liveness failure: maximum finalized height {self.maximum_height} "
                f"did not advance for {now - self.last_progress_at:.1f}s"
            )


def load_manifest(path: Path) -> tuple[dict[str, Any], list[dict[str, str]]]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise MonitorError(f"cannot read manifest {path}: {error}") from error
    if manifest.get("schema_version") != 1:
        raise MonitorError("manifest schema_version must be 1")
    if not manifest.get("chain_id") or not manifest.get("genesis_hash"):
        raise MonitorError("manifest requires chain_id and genesis_hash")
    validators = []
    for raw in manifest.get("validators", []):
        if not raw.get("name") or not raw.get("rpc"):
            raise MonitorError("every validator requires name and rpc")
        validators.append({"name": str(raw["name"]), "rpc": str(raw["rpc"])})
    if len(validators) < 4:
        raise MonitorError("manifest must contain at least four RPC validators")
    return manifest, validators


def fetch_status(validator: dict[str, str], timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(
        validator["rpc"],
        data=json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "kestrel_getStatus",
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    if "result" not in payload:
        raise MonitorError(f"{validator['name']} returned JSON-RPC error: {payload!r}")
    return payload["result"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run(args: argparse.Namespace) -> int:
    manifest, validators = load_manifest(args.manifest.resolve())
    if args.output.exists() or args.summary.exists():
        raise MonitorError("refusing to overwrite existing campaign evidence")
    monitor = CampaignMonitor(
        str(manifest["chain_id"]),
        str(manifest["genesis_hash"]),
        args.stall_seconds,
    )
    started = time.monotonic()
    deadline = started + args.duration_seconds
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    failure = None
    try:
        with args.output.open("w", encoding="utf-8") as output:
            with ThreadPoolExecutor(max_workers=len(validators)) as executor:
                while time.monotonic() < deadline:
                    sampled_at = time.monotonic()
                    futures = {
                        validator["name"]: executor.submit(
                            fetch_status, validator, args.rpc_timeout_seconds
                        )
                        for validator in validators
                    }
                    for validator in validators:
                        name = validator["name"]
                        event = {"timestamp": utc_now(), "validator": name}
                        try:
                            status = futures[name].result()
                        except (
                            MonitorError,
                            OSError,
                            urllib.error.URLError,
                        ) as error:
                            monitor.rpc_errors += 1
                            event["error"] = str(error)
                        else:
                            event["status"] = status
                            try:
                                # Invariant failures terminate the campaign,
                                # but the triggering observation is retained.
                                monitor.observe(name, status, sampled_at)
                            except MonitorError as error:
                                event["invariant_failure"] = str(error)
                                output.write(json.dumps(event, sort_keys=True) + "\n")
                                output.flush()
                                raise
                        output.write(json.dumps(event, sort_keys=True) + "\n")
                        output.flush()
                    monitor.check_liveness(time.monotonic())
                    remaining = args.interval_seconds - (
                        time.monotonic() - sampled_at
                    )
                    if remaining > 0:
                        time.sleep(remaining)
    except MonitorError as error:
        failure = str(error)
    summary = {
        "schema_version": 1,
        "campaign": manifest.get("campaign"),
        "duration_seconds": time.monotonic() - started,
        "maximum_finalized_height": monitor.maximum_height,
        "observations": monitor.observations,
        "rpc_errors": monitor.rpc_errors,
        "canonical_heights_observed": len(monitor.canonical_blocks),
        "failure": failure,
        "passed": monitor.observations > 0 and failure is None,
    }
    args.summary.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if not summary["passed"]:
        raise MonitorError(
            failure or "campaign completed without one valid status observation"
        )
    print(
        f"PASS: height={monitor.maximum_height}, observations={monitor.observations}, "
        f"rpc_errors={monitor.rpc_errors}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--duration-seconds", type=float, default=21_600)
    parser.add_argument("--interval-seconds", type=float, default=2)
    parser.add_argument("--stall-seconds", type=float, default=45)
    parser.add_argument("--rpc-timeout-seconds", type=float, default=2)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args(argv)
    if min(
        args.duration_seconds,
        args.interval_seconds,
        args.stall_seconds,
        args.rpc_timeout_seconds,
    ) <= 0:
        parser.error("durations, intervals, and timeouts must be positive")
    try:
        return run(args)
    except MonitorError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
