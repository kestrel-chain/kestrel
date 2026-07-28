#!/usr/bin/env python3
"""Build auditable Stage 2 evidence from Kestrel validator JSON logs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


class CampaignError(Exception):
    """Invalid campaign input."""


@dataclass(frozen=True)
class Validator:
    name: str
    validator_id: str
    stake: int
    log: Path


def timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * quantile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def distribution(values: list[float]) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values) if values else None,
        "mean": statistics.mean(values) if values else None,
    }


def load_manifest(path: Path) -> tuple[dict[str, Any], list[Validator]]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CampaignError(f"cannot read manifest {path}: {error}") from error
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise CampaignError(f"manifest schema_version must be {SCHEMA_VERSION}")
    try:
        genesis_path = (path.parent / str(manifest["genesis"])).resolve()
        genesis_payload = genesis_path.read_bytes()
        genesis = json.loads(genesis_payload)
    except (KeyError, OSError, json.JSONDecodeError) as error:
        raise CampaignError(f"cannot read campaign genesis: {error}") from error
    if genesis.get("chain_id") != manifest.get("chain_id"):
        raise CampaignError("manifest chain_id does not match the genesis document")
    genesis_validators = {}
    try:
        for entry in genesis["validators"]:
            validator_bytes = bytes(entry["validator"]["id"])
            if len(validator_bytes) != 32:
                raise ValueError("validator ID is not 32 bytes")
            validator_id = validator_bytes.hex()
            if validator_id in genesis_validators:
                raise ValueError("duplicate validator ID")
            genesis_validators[validator_id] = int(entry["validator"]["stake"])
    except (KeyError, TypeError, ValueError) as error:
        raise CampaignError("genesis contains an invalid validator entry") from error
    manifest["_genesis_sha256"] = hashlib.sha256(genesis_payload).hexdigest()
    raw_validators = manifest.get("validators")
    if not isinstance(raw_validators, list) or len(raw_validators) < 4:
        raise CampaignError("manifest must contain at least four validators")
    validators = []
    names = set()
    validator_ids = set()
    for raw in raw_validators:
        try:
            name = str(raw["name"])
            validator_id = str(raw["validator_id"]).removeprefix("0x").lower()
            log = (path.parent / str(raw["log"])).resolve()
        except (KeyError, TypeError, ValueError) as error:
            raise CampaignError(f"invalid validator entry: {raw!r}") from error
        if validator_id not in genesis_validators:
            raise CampaignError(f"{name} validator_id is absent from genesis")
        stake = genesis_validators[validator_id]
        try:
            declared_stake = int(raw.get("stake", stake))
        except (TypeError, ValueError) as error:
            raise CampaignError(f"{name} stake is invalid") from error
        if declared_stake != stake:
            raise CampaignError(f"{name} stake does not match genesis")
        if (
            not name
            or not validator_id
            or name in names
            or validator_id in validator_ids
            or stake <= 0
        ):
            raise CampaignError(
                "validator names and IDs must be unique and stake must be positive"
            )
        names.add(name)
        validator_ids.add(validator_id)
        validators.append(Validator(name, validator_id, stake, log))
    configured_ids = {validator.validator_id for validator in validators}
    if configured_ids != set(genesis_validators):
        raise CampaignError("campaign must include every genesis validator exactly once")
    return manifest, validators


def parse_logs(validators: list[Validator]) -> tuple[dict[str, Any], dict[str, str]]:
    admissions: dict[str, dict[str, datetime]] = defaultdict(dict)
    finalizations: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
    commits: dict[str, set[int]] = defaultdict(set)
    genesis_by_validator: dict[str, str] = {}
    id_by_validator: dict[str, str] = {}
    fatal_events = []
    duplicate_finalization_conflicts = []
    checksums = {}
    malformed_lines = {}

    for validator in validators:
        try:
            payload = validator.log.read_bytes()
        except OSError as error:
            raise CampaignError(f"cannot read {validator.log}: {error}") from error
        checksums[validator.name] = hashlib.sha256(payload).hexdigest()
        malformed = 0
        for line_number, raw_line in enumerate(payload.splitlines(), start=1):
            try:
                event = json.loads(raw_line)
                occurred_at = timestamp(event["timestamp"])
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                malformed += 1
                continue
            fields = event.get("fields", {})
            message = fields.get("message")
            target = event.get("target", "")
            if message == "validator RPC ready" and target == "node":
                genesis_by_validator[validator.name] = str(fields.get("genesis", ""))
                id_by_validator[validator.name] = str(fields.get("validator_id", ""))
            elif message == "admitted transaction" and target == "node::pipeline":
                transaction_id = fields.get("transaction_id")
                if transaction_id:
                    admissions[str(transaction_id)].setdefault(
                        validator.name, occurred_at
                    )
            elif message == "finalized height" and target == "node::coordinator":
                try:
                    height = int(fields["height"])
                    finalized = {
                        "block": str(fields["block"]),
                        "view": int(fields.get("view", 0)),
                        "latency_ms": int(fields.get("latency_ms", 0)),
                        "timestamp": occurred_at,
                    }
                    previous = finalizations[height].get(validator.name)
                    if previous is not None and previous["block"] != finalized["block"]:
                        duplicate_finalization_conflicts.append(
                            {
                                "validator": validator.name,
                                "height": height,
                                "first_block": previous["block"],
                                "later_block": finalized["block"],
                            }
                        )
                    else:
                        finalizations[height][validator.name] = finalized
                except (KeyError, TypeError, ValueError):
                    malformed += 1
            elif message == "committed block" and target == "node::lifecycle":
                try:
                    commits[validator.name].add(int(fields["height"]))
                except (KeyError, TypeError, ValueError):
                    malformed += 1
            if (
                event.get("level") in {"ERROR", "FATAL"}
                and message in {"consensus coordinator stopped", "Stage 2 pipeline stopped"}
            ):
                fatal_events.append(
                    {
                        "validator": validator.name,
                        "line": line_number,
                        "timestamp": event.get("timestamp"),
                        "message": message,
                        "error": fields.get("error"),
                    }
                )
        malformed_lines[validator.name] = malformed

    return (
        {
            "admissions": admissions,
            "finalizations": finalizations,
            "commits": commits,
            "genesis": genesis_by_validator,
            "validator_ids": id_by_validator,
            "fatal_events": fatal_events,
            "duplicate_finalization_conflicts": duplicate_finalization_conflicts,
            "malformed_lines": malformed_lines,
        },
        checksums,
    )


def build_report(
    manifest: dict[str, Any],
    validators: list[Validator],
    parsed: dict[str, Any],
    checksums: dict[str, str],
) -> dict[str, Any]:
    stakes = {validator.name: validator.stake for validator in validators}
    total_stake = sum(stakes.values())
    threshold_fraction = float(manifest.get("propagation_stake_threshold", 0.8))
    if not 0 < threshold_fraction <= 1:
        raise CampaignError("propagation_stake_threshold must be in (0, 1]")
    threshold_stake = math.ceil(total_stake * threshold_fraction)
    measured_clock_skew_ms = manifest.get("measured_maximum_clock_skew_ms")
    if parsed["admissions"] and measured_clock_skew_ms is None:
        raise CampaignError(
            "manifest requires measured_maximum_clock_skew_ms when propagation "
            "samples are present"
        )
    if measured_clock_skew_ms is not None:
        measured_clock_skew_ms = float(measured_clock_skew_ms)
        if measured_clock_skew_ms < 0:
            raise CampaignError("measured_maximum_clock_skew_ms cannot be negative")

    propagation_latencies = []
    propagation_incomplete = []
    for transaction_id, observations in parsed["admissions"].items():
        ordered = sorted(observations.items(), key=lambda item: item[1])
        first_seen = ordered[0][1]
        observed_stake = 0
        reached_at = None
        for validator_name, seen_at in ordered:
            observed_stake += stakes[validator_name]
            if observed_stake >= threshold_stake:
                reached_at = seen_at
                break
        if reached_at is None:
            propagation_incomplete.append(
                {
                    "transaction_id": transaction_id,
                    "observed_stake": observed_stake,
                }
            )
        else:
            propagation_latencies.append((reached_at - first_seen).total_seconds() * 1000)

    safety_violations = []
    finality_latencies = []
    finalization_skews = []
    view_changes = []
    highest_finalized = {}
    for height, observations in sorted(parsed["finalizations"].items()):
        blocks = {entry["block"] for entry in observations.values()}
        if len(blocks) > 1:
            safety_violations.append(
                {
                    "height": height,
                    "blocks": {
                        validator: entry["block"]
                        for validator, entry in sorted(observations.items())
                    },
                }
            )
        times = [entry["timestamp"] for entry in observations.values()]
        if len(times) > 1:
            finalization_skews.append((max(times) - min(times)).total_seconds() * 1000)
        for validator, entry in observations.items():
            highest_finalized[validator] = max(height, highest_finalized.get(validator, 0))
            if entry["latency_ms"] > 0:
                finality_latencies.append(float(entry["latency_ms"]))
            view_changes.append(float(entry["view"]))

    validator_progress = {}
    for validator in validators:
        committed = parsed["commits"].get(validator.name, set())
        finalized_height = highest_finalized.get(validator.name, 0)
        committed_height = max(committed, default=0)
        validator_progress[validator.name] = {
            "finalized_height": finalized_height,
            "committed_height": committed_height,
            "execution_lag": max(0, finalized_height - committed_height),
        }

    expected_genesis = manifest.get("genesis_hash")
    genesis_mismatches = []
    validator_id_mismatches = []
    if expected_genesis:
        for validator in validators:
            observed = parsed["genesis"].get(validator.name)
            if observed != expected_genesis:
                genesis_mismatches.append(
                    {
                        "validator": validator.name,
                        "expected": expected_genesis,
                        "observed": observed,
                    }
                )
    for validator in validators:
        observed = parsed["validator_ids"].get(validator.name)
        if observed != validator.validator_id:
            validator_id_mismatches.append(
                {
                    "validator": validator.name,
                    "expected": validator.validator_id,
                    "observed": observed,
                }
            )

    metrics = {
        "propagation_to_stake_ms": distribution(propagation_latencies),
        "propagation_incomplete": propagation_incomplete,
        "incomplete_propagation_count": len(propagation_incomplete),
        "finality_latency_ms": distribution(finality_latencies),
        "cross_validator_finalization_skew_ms": distribution(finalization_skews),
        "view_changes": distribution(view_changes),
        "validator_progress": validator_progress,
        "measured_maximum_clock_skew_ms": measured_clock_skew_ms,
        "malformed_log_lines_total": sum(parsed["malformed_lines"].values()),
    }
    failures = list(safety_violations)
    failures.extend(genesis_mismatches)
    failures.extend(validator_id_mismatches)
    failures.extend(parsed["fatal_events"])
    failures.extend(parsed["duplicate_finalization_conflicts"])
    gates = manifest.get("gates", {})
    gate_results = evaluate_gates(gates, metrics, failures)
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign": manifest.get("campaign"),
        "chain_id": manifest.get("chain_id"),
        "genesis_hash": expected_genesis,
        "validator_count": len(validators),
        "total_stake": total_stake,
        "propagation_stake_threshold": threshold_fraction,
        "propagation_threshold_stake": threshold_stake,
        "inputs": {
            "genesis_sha256": manifest["_genesis_sha256"],
            "sha256": checksums,
            "malformed_log_lines": parsed["malformed_lines"],
        },
        "metrics": metrics,
        "safety_violations": safety_violations,
        "genesis_mismatches": genesis_mismatches,
        "validator_id_mismatches": validator_id_mismatches,
        "fatal_events": parsed["fatal_events"],
        "duplicate_finalization_conflicts": parsed[
            "duplicate_finalization_conflicts"
        ],
        "gates": gate_results,
        "passed": all(result["passed"] for result in gate_results),
    }


def evaluate_gates(
    gates: dict[str, Any], metrics: dict[str, Any], unconditional_failures: list[Any]
) -> list[dict[str, Any]]:
    results = [
        {
            "name": "no_safety_genesis_or_fatal_failure",
            "passed": not unconditional_failures,
            "actual": len(unconditional_failures),
            "expected": 0,
        }
    ]
    checks = [
        ("min_propagation_samples", "propagation_to_stake_ms", "count", lambda a, e: a >= e),
        ("min_finality_samples", "finality_latency_ms", "count", lambda a, e: a >= e),
        ("max_propagation_p95_ms", "propagation_to_stake_ms", "p95", lambda a, e: a <= e),
        ("max_finality_p95_ms", "finality_latency_ms", "p95", lambda a, e: a <= e),
        (
            "max_finalization_skew_p95_ms",
            "cross_validator_finalization_skew_ms",
            "p95",
            lambda a, e: a <= e,
        ),
    ]
    for gate_name, metric_name, statistic, compare in checks:
        if gate_name not in gates:
            continue
        actual = metrics[metric_name][statistic]
        expected = gates[gate_name]
        results.append(
            {
                "name": gate_name,
                "passed": actual is not None and compare(actual, expected),
                "actual": actual,
                "expected": expected,
            }
        )
    if "max_execution_lag" in gates:
        actual = max(
            progress["execution_lag"]
            for progress in metrics["validator_progress"].values()
        )
        expected = gates["max_execution_lag"]
        results.append(
            {
                "name": "max_execution_lag",
                "passed": actual <= expected,
                "actual": actual,
                "expected": expected,
            }
        )
    scalar_checks = [
        ("max_incomplete_propagation", "incomplete_propagation_count"),
        ("max_malformed_log_lines", "malformed_log_lines_total"),
        ("max_clock_skew_ms", "measured_maximum_clock_skew_ms"),
    ]
    for gate_name, metric_name in scalar_checks:
        if gate_name not in gates:
            continue
        actual = metrics[metric_name]
        expected = gates[gate_name]
        results.append(
            {
                "name": gate_name,
                "passed": actual is not None and actual <= expected,
                "actual": actual,
                "expected": expected,
            }
        )
    return results


def markdown(report: dict[str, Any]) -> str:
    metrics = report["metrics"]

    def row(name: str, values: dict[str, Any]) -> str:
        return (
            f"| {name} | {values['count']} | {values['p50']} | "
            f"{values['p95']} | {values['p99']} | {values['max']} |"
        )

    lines = [
        f"# Stage 2 campaign: {report.get('campaign') or 'unnamed'}",
        "",
        f"**Verdict:** {'PASS' if report['passed'] else 'FAIL'}",
        "",
        "| Metric | samples | p50 | p95 | p99 | max |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
        row("Propagation to stake (ms)", metrics["propagation_to_stake_ms"]),
        row("Finality latency (ms)", metrics["finality_latency_ms"]),
        row(
            "Cross-validator finalization skew (ms)",
            metrics["cross_validator_finalization_skew_ms"],
        ),
        row("View changes", metrics["view_changes"]),
        "",
        "## Gates",
        "",
    ]
    for gate in report["gates"]:
        lines.append(
            f"- {'PASS' if gate['passed'] else 'FAIL'} `{gate['name']}`: "
            f"actual={gate['actual']}, expected={gate['expected']}"
        )
    lines.extend(
        [
            "",
            "## Validator progress",
            "",
            "| Validator | finalized | committed | execution lag |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for validator, progress in sorted(metrics["validator_progress"].items()):
        lines.append(
            f"| {validator} | {progress['finalized_height']} | "
            f"{progress['committed_height']} | {progress['execution_lag']} |"
        )
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args(argv)
    try:
        destinations = [args.output, args.markdown]
        if any(path is not None and path.exists() for path in destinations):
            raise CampaignError("refusing to overwrite existing campaign evidence")
        manifest, validators = load_manifest(args.manifest.resolve())
        parsed, checksums = parse_logs(validators)
        report = build_report(manifest, validators, parsed, checksums)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if args.markdown:
            args.markdown.parent.mkdir(parents=True, exist_ok=True)
            args.markdown.write_text(markdown(report), encoding="utf-8")
    except (CampaignError, OSError) as error:
        print(f"stage2 campaign input error: {error}", file=sys.stderr)
        return 2
    print(f"{'PASS' if report['passed'] else 'FAIL'}: wrote {args.output}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
