#!/usr/bin/env python3
import json
import math
import os
import random
import statistics
import sys
from pathlib import Path


RUNS = int(os.environ.get("RUNS", "10"))
PREFIX = os.environ.get("BASELINE_PREFIX", "repro-")
ROOT = Path(os.environ.get("CRITERION_ROOT", "target/criterion"))
CONFIGURATIONS = [
    ("synthetic", 256, "low_contention_256"),
    ("synthetic", 1_024, "low_contention_1024"),
    ("synthetic", 4_096, "low_contention_4096"),
    ("realistic Move", 256, "realistic_move_256"),
    ("realistic Move", 1_024, "realistic_move_1024"),
    ("realistic Move", 4_096, "realistic_move_4096"),
]
METHODS = ["sequential", "parallel/1", "parallel/2", "parallel/4", "parallel/8"]


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * quantile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def median_confidence_interval(values: list[float]) -> tuple[float, float]:
    generator = random.Random(0x4B45_5354_5245_4C)
    medians = [
        statistics.median(generator.choices(values, k=len(values)))
        for _ in range(50_000)
    ]
    return percentile(medians, 0.025), percentile(medians, 0.975)


def summary(values: list[float]) -> tuple[float, float, float, float, float, float]:
    deviation = statistics.stdev(values)
    mean = statistics.mean(values)
    lower, upper = median_confidence_interval(values)
    return (
        statistics.median(values),
        percentile(values, 0.95),
        deviation,
        100.0 * deviation / mean,
        lower,
        upper,
    )


def load_duration(group: str, method: str, run: int) -> float:
    path = ROOT / group / method / f"{PREFIX}{run:02d}" / "estimates.json"
    try:
        estimates = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        sys.exit(f"missing benchmark result: {path}")
    return float(estimates["median"]["point_estimate"])


def fmt_stats(values: list[float], scale: float, suffix: str) -> str:
    median, p95, deviation, cv, lower, upper = summary(
        [value / scale for value in values]
    )
    return (
        f"{median:.3f} [{lower:.3f}, {upper:.3f}] / "
        f"{p95:.3f} / {deviation:.3f} / {cv:.2f}% {suffix}"
    )


durations: dict[tuple[str, str], list[float]] = {}
print("| Workload | Transactions | Executor | time median [95% CI] / p95 / SD / CV | throughput median [95% CI] / p95 / SD / CV |")
print("| --- | ---: | --- | ---: | ---: |")
for workload, transactions, group in CONFIGURATIONS:
    for method in METHODS:
        values = [load_duration(group, method, run) for run in range(1, RUNS + 1)]
        durations[(group, method)] = values
        throughputs = [transactions * 1_000_000_000.0 / value for value in values]
        print(
            f"| {workload} | {transactions:,} | {method} | "
            f"{fmt_stats(values, 1_000_000.0, 'ms')} | "
            f"{fmt_stats(throughputs, 1_000.0, 'ktx/s')} |"
        )

print()
print("| Workload | Transactions | seq/parallel-8 speedup median [95% CI] / p95 / SD / CV | wins | 1-to-8 efficiency median [95% CI] / p95 / SD / CV |")
print("| --- | ---: | ---: | ---: | ---: |")
for workload, transactions, group in CONFIGURATIONS:
    sequential = durations[(group, "sequential")]
    parallel_one = durations[(group, "parallel/1")]
    parallel_eight = durations[(group, "parallel/8")]
    speedups = [seq / par for seq, par in zip(sequential, parallel_eight, strict=True)]
    efficiencies = [
        one / (8.0 * eight)
        for one, eight in zip(parallel_one, parallel_eight, strict=True)
    ]
    print(
        f"| {workload} | {transactions:,} | "
        f"{fmt_stats(speedups, 1.0, 'x')} | "
        f"{sum(speedup > 1.0 for speedup in speedups)}/{RUNS} | "
        f"{fmt_stats(efficiencies, 0.01, '%')} |"
    )
