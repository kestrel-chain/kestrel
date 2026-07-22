#!/usr/bin/env python3
import json
import os
import statistics
import sys
from pathlib import Path


RUNS = int(os.environ.get("RUNS", "10"))
LEFT_ROOT = Path(os.environ.get("LEFT_ROOT", "target/criterion"))
LEFT_PREFIX = os.environ.get("LEFT_PREFIX", "controlled-")
LEFT_LABEL = os.environ.get("LEFT_LABEL", "APFS")
RIGHT_ROOT = Path(
    os.environ.get("RIGHT_ROOT", "target/criterion-ram-2026-07-22.noindex")
)
RIGHT_PREFIX = os.environ.get("RIGHT_PREFIX", "ram-")
RIGHT_LABEL = os.environ.get("RIGHT_LABEL", "RAM/no-index")
CONFIGURATIONS = [
    ("synthetic", 256, "low_contention_256"),
    ("synthetic", 1_024, "low_contention_1024"),
    ("synthetic", 4_096, "low_contention_4096"),
    ("realistic Move", 256, "realistic_move_256"),
    ("realistic Move", 1_024, "realistic_move_1024"),
    ("realistic Move", 4_096, "realistic_move_4096"),
]
METHODS = ["sequential", "parallel/1", "parallel/2", "parallel/4", "parallel/8"]


def load_durations(root: Path, prefix: str, group: str, method: str) -> list[float]:
    values = []
    for run in range(1, RUNS + 1):
        path = root / group / method / f"{prefix}{run:02d}" / "estimates.json"
        try:
            estimates = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            sys.exit(f"missing benchmark result: {path}")
        values.append(float(estimates["median"]["point_estimate"]))
    return values


def coefficient_of_variation(values: list[float]) -> float:
    return 100.0 * statistics.stdev(values) / statistics.mean(values)


def throughput_cv(durations: list[float], transactions: int) -> float:
    throughputs = [transactions * 1_000_000_000.0 / value for value in durations]
    return coefficient_of_variation(throughputs)


rows = []
for workload, transactions, group in CONFIGURATIONS:
    for method in METHODS:
        left = load_durations(LEFT_ROOT, LEFT_PREFIX, group, method)
        right = load_durations(RIGHT_ROOT, RIGHT_PREFIX, group, method)
        left_time_cv = coefficient_of_variation(left)
        right_time_cv = coefficient_of_variation(right)
        left_throughput_cv = throughput_cv(left, transactions)
        right_throughput_cv = throughput_cv(right, transactions)
        rows.append(
            (
                workload,
                transactions,
                method,
                left_time_cv,
                right_time_cv,
                left_throughput_cv,
                right_throughput_cv,
            )
        )

print(
    f"| Workload | Transactions | Executor | {LEFT_LABEL} time CV | "
    f"{RIGHT_LABEL} time CV | delta | {LEFT_LABEL} throughput CV | "
    f"{RIGHT_LABEL} throughput CV | delta |"
)
print("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
for row in rows:
    workload, transactions, method, left_time, right_time, left_tp, right_tp = row
    print(
        f"| {workload} | {transactions:,} | {method} | {left_time:.2f}% | "
        f"{right_time:.2f}% | {right_time - left_time:+.2f} pp | {left_tp:.2f}% | "
        f"{right_tp:.2f}% | {right_tp - left_tp:+.2f} pp |"
    )

left_time_cvs = [row[3] for row in rows]
right_time_cvs = [row[4] for row in rows]
left_throughput_cvs = [row[5] for row in rows]
right_throughput_cvs = [row[6] for row in rows]
print()
print(f"Configurations with lower time CV: {sum(r < l for l, r in zip(left_time_cvs, right_time_cvs, strict=True))}/{len(rows)}")
print(f"Median time CV: {LEFT_LABEL} {statistics.median(left_time_cvs):.2f}%, {RIGHT_LABEL} {statistics.median(right_time_cvs):.2f}%")
print(f"Mean time CV: {LEFT_LABEL} {statistics.mean(left_time_cvs):.2f}%, {RIGHT_LABEL} {statistics.mean(right_time_cvs):.2f}%")
print(f"Configurations with lower throughput CV: {sum(r < l for l, r in zip(left_throughput_cvs, right_throughput_cvs, strict=True))}/{len(rows)}")
print(f"Median throughput CV: {LEFT_LABEL} {statistics.median(left_throughput_cvs):.2f}%, {RIGHT_LABEL} {statistics.median(right_throughput_cvs):.2f}%")
print(f"Mean throughput CV: {LEFT_LABEL} {statistics.mean(left_throughput_cvs):.2f}%, {RIGHT_LABEL} {statistics.mean(right_throughput_cvs):.2f}%")
