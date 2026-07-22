# Phase 2 benchmark reproducibility report

Measured on 2026-07-22. These are execution-subsystem results, not end-to-end
blockchain TPS.

## Methodology and host controls

- Host: fanless MacBook Air, Apple M2, 8 cores (4 performance and 4 efficiency),
  16 GB RAM, Rust 1.91.1, connected to AC power.
- macOS does not expose hard CPU affinity or user-level fixed-frequency/turbo
  control on this host. `powermetrics` frequency sampling requires superuser
  access. Eight-worker measurements therefore include macOS scheduling across
  heterogeneous performance and efficiency cores.
- The host was thermally preconditioned for 180 seconds with the 4,096-transaction
  realistic Move parallel/8 workload. Preconditioning data was discarded. This
  measures sustained thermally constrained throughput instead of a cold burst.
- Chrome, VS Code, Mail, Spotify, System Settings, and Time Machine were absent.
  The harness checked before every workload family and would abort if any
  reappeared. macOS reported no thermal or performance warning during any
  retained repetition.
- Ten independent named Criterion baselines were collected for every
  configuration. Each used 20 samples, a one-second warm-up, a three-second
  measurement target, and no `--quick`. Criterion extended slow configurations
  when needed to obtain all 20 samples.
- Six workload/size families were rotated between repetitions. Executor order
  was sequential-to-parallel/8 on odd runs and reversed on even runs, preventing
  thermal position from systematically favoring one executor.
- Four repetition-boundary snapshots showed material APFS/Spotlight activity;
  six were clean. Those retained OS effects are reflected in p95, standard
  deviation, and CV rather than being selectively discarded.
- Statistics below are computed across the ten independent run-level Criterion
  median estimates. The p95 uses type-7 interpolation; SD is sample standard
  deviation; CV is sample SD divided by the mean. Brackets contain a deterministic
  50,000-resample bootstrap 95% confidence interval for the median.

Reproduce collection with `scripts/benchmark-reproducibility.sh` and aggregation
with `scripts/summarize-benchmark-repro.py`.

## Full results

Each cell is `median [95% CI] / p95 / SD / CV`.

| Workload | Transactions | Executor | Time | Throughput |
| --- | ---: | --- | ---: | ---: |
| Synthetic | 256 | sequential | 0.391 [0.340, 0.421] / 0.442 / 0.044 / 11.32% ms | 656.194 [607.746, 753.996] / 757.664 / 76.006 / 11.29% ktx/s |
| Synthetic | 256 | parallel/1 | 0.594 [0.516, 0.633] / 0.677 / 0.065 / 11.18% ms | 431.239 [404.839, 496.304] / 498.173 / 48.784 / 10.99% ktx/s |
| Synthetic | 256 | parallel/2 | 0.689 [0.574, 0.732] / 0.748 / 0.079 / 11.90% ms | 371.735 [349.763, 446.009] / 447.658 / 47.886 / 12.22% ktx/s |
| Synthetic | 256 | parallel/4 | 0.698 [0.527, 0.774] / 0.907 / 0.156 / 22.83% ms | 366.920 [330.825, 486.031] / 488.882 / 83.242 / 21.32% ktx/s |
| Synthetic | 256 | parallel/8 | 0.803 [0.609, 0.848] / 0.929 / 0.137 / 18.25% ms | 318.801 [302.105, 420.473] / 430.205 / 65.395 / 18.65% ktx/s |
| Synthetic | 1,024 | sequential | 1.552 [1.468, 1.785] / 1.880 / 0.177 / 10.92% ms | 661.184 [573.837, 697.729] / 700.583 / 66.807 / 10.48% ktx/s |
| Synthetic | 1,024 | parallel/1 | 2.291 [2.128, 2.653] / 2.746 / 0.273 / 11.53% ms | 448.433 [385.997, 481.314] / 487.348 / 48.676 / 11.11% ktx/s |
| Synthetic | 1,024 | parallel/2 | 2.529 [2.334, 2.968] / 3.096 / 0.333 / 12.66% ms | 407.085 [345.567, 438.814] / 441.840 / 48.502 / 12.28% ktx/s |
| Synthetic | 1,024 | parallel/4 | 2.405 [2.097, 3.002] / 3.137 / 0.457 / 18.16% ms | 428.711 [341.660, 488.384] / 495.773 / 73.632 / 17.59% ktx/s |
| Synthetic | 1,024 | parallel/8 | 2.524 [2.273, 3.144] / 3.395 / 0.478 / 17.86% ms | 408.841 [327.465, 450.600] / 461.981 / 66.278 / 16.85% ktx/s |
| Synthetic | 4,096 | sequential | 7.503 [6.210, 7.803] / 8.310 / 0.843 / 11.69% ms | 545.989 [525.344, 659.555] / 668.104 / 68.861 / 11.97% ktx/s |
| Synthetic | 4,096 | parallel/1 | 11.010 [10.150, 12.092] / 13.714 / 1.592 / 14.17% ms | 372.107 [339.171, 404.781] / 444.009 / 49.899 / 13.46% ktx/s |
| Synthetic | 4,096 | parallel/2 | 11.838 [10.176, 12.894] / 13.630 / 1.462 / 12.46% ms | 346.010 [317.662, 402.512] / 421.361 / 45.763 / 12.91% ktx/s |
| Synthetic | 4,096 | parallel/4 | 11.106 [8.977, 12.374] / 13.084 / 1.662 / 15.25% ms | 369.016 [331.019, 456.296] / 457.851 / 59.125 / 15.40% ktx/s |
| Synthetic | 4,096 | parallel/8 | 10.848 [9.072, 12.047] / 12.900 / 1.601 / 15.03% ms | 377.610 [340.950, 451.518] / 471.275 / 59.373 / 15.13% ktx/s |
| Realistic Move | 256 | sequential | 2.419 [2.218, 2.855] / 3.298 / 0.443 / 17.55% ms | 105.822 [91.549, 115.919] / 124.008 / 16.234 / 15.61% ktx/s |
| Realistic Move | 256 | parallel/1 | 2.659 [2.453, 2.832] / 3.824 / 0.657 / 23.41% ms | 96.282 [90.432, 104.371] / 111.385 / 15.753 / 16.67% ktx/s |
| Realistic Move | 256 | parallel/2 | 2.487 [2.203, 2.686] / 3.383 / 0.550 / 21.71% ms | 102.954 [95.297, 117.393] / 131.644 / 19.209 / 18.36% ktx/s |
| Realistic Move | 256 | parallel/4 | 2.394 [1.987, 2.863] / 3.349 / 0.621 / 25.54% ms | 106.939 [89.420, 128.864] / 154.082 / 27.574 / 24.78% ktx/s |
| Realistic Move | 256 | parallel/8 | 2.409 [1.956, 2.941] / 3.533 / 0.725 / 29.50% ms | 106.284 [87.036, 133.581] / 164.285 / 32.500 / 28.93% ktx/s |
| Realistic Move | 1,024 | sequential | 10.029 [9.232, 11.572] / 13.093 / 1.608 / 15.48% ms | 102.108 [90.026, 111.647] / 121.604 / 14.646 / 14.55% ktx/s |
| Realistic Move | 1,024 | parallel/1 | 11.040 [10.065, 12.446] / 16.437 / 2.773 / 23.65% ms | 92.762 [82.877, 102.190] / 110.413 / 16.330 / 18.01% ktx/s |
| Realistic Move | 1,024 | parallel/2 | 9.688 [8.568, 11.224] / 13.858 / 2.202 / 21.82% ms | 105.698 [92.415, 120.169] / 133.826 / 20.218 / 19.20% ktx/s |
| Realistic Move | 1,024 | parallel/4 | 8.773 [7.514, 11.153] / 14.143 / 2.805 / 29.88% ms | 116.725 [95.566, 137.289] / 163.675 / 31.872 / 27.17% ktx/s |
| Realistic Move | 1,024 | parallel/8 | 8.138 [6.648, 10.227] / 12.684 / 2.522 / 29.71% ms | 125.842 [101.561, 155.565] / 182.001 / 35.382 / 27.29% ktx/s |
| Realistic Move | 4,096 | sequential | 40.004 [35.255, 44.402] / 45.560 / 4.722 / 11.85% ms | 102.408 [92.248, 116.184] / 119.426 / 12.405 / 11.92% ktx/s |
| Realistic Move | 4,096 | parallel/1 | 45.194 [39.652, 48.985] / 50.069 / 4.738 / 10.70% ms | 90.660 [83.617, 103.304] / 106.570 / 10.149 / 10.86% ktx/s |
| Realistic Move | 4,096 | parallel/2 | 39.457 [32.520, 43.877] / 53.570 / 8.823 / 21.96% ms | 103.828 [93.352, 125.954] / 130.329 / 20.111 / 19.01% ktx/s |
| Realistic Move | 4,096 | parallel/4 | 32.994 [26.204, 40.691] / 43.425 / 7.656 / 22.88% ms | 124.146 [100.660, 156.314] / 175.477 / 31.026 / 24.11% ktx/s |
| Realistic Move | 4,096 | parallel/8 | 30.042 [23.368, 35.374] / 35.708 / 5.710 / 19.28% ms | 136.399 [115.793, 175.285] / 190.495 / 30.585 / 21.30% ktx/s |

## Paired conclusions

Each cell is `median [95% CI] / p95 / SD / CV`. A speedup above 1 means
parallel/8 is faster than sequential. Efficiency is parallel/1 time divided by
eight times parallel/8 time.

| Workload | Transactions | Sequential/parallel-8 speedup | Parallel wins | 1-to-8 efficiency |
| --- | ---: | ---: | ---: | ---: |
| Synthetic | 256 | 0.532 [0.465, 0.557] / 0.572 / 0.049 / 9.52% | 0/10 | 9.674 [8.924, 10.577] / 10.890 / 0.832 / 8.49% |
| Synthetic | 1,024 | 0.617 [0.581, 0.650] / 0.663 / 0.043 / 7.04% | 0/10 | 11.613 [10.283, 11.879] / 11.992 / 0.884 / 7.91% |
| Synthetic | 4,096 | 0.677 [0.620, 0.706] / 0.841 / 0.102 / 14.83% | 0/10 | 12.431 [11.742, 15.134] / 17.176 / 2.236 / 16.74% |
| Realistic Move | 256 | 1.022 [0.942, 1.183] / 1.325 / 0.165 / 15.47% | 6/10 | 14.259 [13.000, 15.825] / 18.472 / 2.263 / 15.41% |
| Realistic Move | 1,024 | 1.232 [1.143, 1.424] / 1.534 / 0.189 / 14.86% | 9/10 | 17.378 [16.047, 19.499] / 20.972 / 2.168 / 12.27% |
| Realistic Move | 4,096 | 1.326 [1.257, 1.503] / 1.645 / 0.164 / 11.99% | 10/10 | 18.324 [17.281, 21.342] / 23.191 / 2.481 / 13.01% |

The prior 256-transaction result of 1.02x slower does not persist: the controlled
median is 1.022x faster, but its confidence interval crosses parity and only six
of ten runs favor parallel execution. The correct conclusion is parity/inconclusive,
not a regression or a win.

The 4,096-transaction Move advantage is repeatable: parallel/8 won all ten paired
runs and the speedup confidence interval remains above one. The controlled median
is 1.326x. The earlier 1.78x point estimate was not reproduced; even the controlled
p95 is 1.645x. The later 1.60x point estimate is plausible as an upper-tail run,
not the central estimate.

The synthetic path lost all ten paired runs at every size, confirming the known
bounded limitation. The maximum-contention benchmark was intentionally not rerun
because reproducibility of the low-contention and realistic suites was the current
priority.

Absolute-time CV remains 10.7–29.9% for realistic configurations, so this host is
not suitable for a headline absolute execution-throughput number. Paired speedup
is more stable (12.0–15.5% CV for realistic workloads), but Phase 2 status and any
Phase 3 benchmarking decision remain unchanged by this report.

The follow-up RAM-disk/Spotlight-isolation experiment is documented in
`phase-2-absolute-throughput-noise.md`. It reduced median throughput CV from
16.14% to 10.45% across all 30 configurations, but residual parallel/8 CV remains
too high for a headline absolute-throughput number.
