# Phase 2 absolute-throughput noise investigation

Measured on 2026-07-22. These are execution-subsystem measurements, not
end-to-end blockchain TPS.

## Question and result

Moving the benchmark executable, Criterion results, and logs from the APFS
workspace to a 512 MiB HFS+ RAM disk with Spotlight disabled substantially
reduced absolute-throughput variance, but did not make this laptop suitable for
a headline absolute-throughput number.

- Throughput CV improved in 29 of 30 configurations. Median CV fell from 16.14%
  to 10.45% and mean CV from 16.98% to 10.61%.
- Time CV improved in 27 of 30 configurations. Median CV fell from 17.71% to
  11.27% and mean CV from 18.00% to 11.89%.
- The only throughput-CV regression was synthetic/256/sequential, from 11.29%
  to 11.83% (+0.54 percentage points). The three time-CV regressions were the
  synthetic/256 sequential, parallel/1, and parallel/2 configurations.
- Residual variance grows with worker count. Across the six workloads, median
  throughput CV on RAM/no-index was 6.67% for sequential, 6.05% for parallel/1,
  10.46% for parallel/2, 15.94% for parallel/4, and 14.34% for parallel/8.
- Realistic Move parallel/8 throughput CV remains 17.79%, 16.12%, and 15.04% at
  256, 1,024, and 4,096 transactions respectively. Those values are still too
  high for a trustworthy absolute-throughput milestone.

The directory relocation therefore helped materially, but it did not eliminate
system-wide scheduling noise.

## Experimental controls

- The same fanless Apple M2 MacBook Air, benchmark binary, workloads, Criterion
  settings, workload rotation, alternating executor order, and process/Time
  Machine guards were used as in `phase-2-benchmark-reproducibility.md`.
- The benchmark binary, Criterion home, and benchmark logs were copied to
  `/Volumes/KestrelBench/kestrel.noindex` on a temporary 512 MiB HFS+ RAM disk.
  No timed benchmark artifact was written to APFS.
- The volume contained `.metadata_never_index`, the benchmark directory used a
  `.noindex` suffix, `mdutil` reported indexing disabled, and `mdfind` returned
  zero benchmark `estimates.json` artifacts.
- Every configuration had ten independent run-level Criterion medians. Each
  median used 20 samples, one second of warm-up, and a three-second measurement
  target. The host was thermally preconditioned for 180 seconds.
- Mail relaunched during the first attempt at repetition 5, so the guard aborted
  it. After Mail and Chrome were closed, repetition 5 was fully overwritten
  after another 180-second precondition. No partial repetition-5 result is in
  the analysis.
- macOS reported AC power and no thermal or performance warning throughout.
  User-level CPU frequency locking and CPU affinity are unavailable on this M2
  host.

The 300 RAM-disk result files were counted before and after post-measurement
archival. The preserved raw data is under
`target/criterion-ram-2026-07-22.noindex`. Collection uses
`scripts/benchmark-reproducibility.sh`; summaries use
`scripts/summarize-benchmark-repro.py`; the paired variance comparison uses
`scripts/compare-benchmark-noise.py`.

## Residual host activity

Disabling indexing on the benchmark directory worked, but it did not stop
unrelated macOS services from using CPU. Repetition-boundary snapshots observed
material activity in eight of ten repetitions, including `apfsd` (about 48–69%
CPU), `triald` (about 91%), `duetexpertd` (about 87%),
`spotlightknowledged` (about 89%), `textunderstandingd` (about 71%), and
`mediaanalysisd` (about 91%). Repetitions 9 and 10 had comparatively quiet
boundaries.

These are boundary snapshots, not continuous attribution samples, so they do
not prove which service delayed a particular Criterion sample. They do prove
that a RAM-backed benchmark directory is not the same as a host without APFS or
other macOS background activity. A local Docker VM would still share this host's
CPU scheduler and background daemons; the stronger control requires a dedicated
Linux or bare-metal benchmark runner.

As a descriptive cross-check, the geometric mean of each repetition's
configuration time normalized to the configuration median had 12.17% CV in the
APFS dataset and 9.84% CV in the RAM/no-index dataset. This aggregate is not an
independent inferential test because the 30 configurations share each run's host
conditions.

## Full RAM/no-index absolute-throughput results

Each cell is `median [bootstrap 95% CI] / p95 / sample SD / CV`, calculated
across ten independent run-level Criterion medians. `ktx/s` is thousands of
execution-subsystem transactions per second and must not be presented as
blockchain TPS.

| Workload | Transactions | Executor | Throughput |
| --- | ---: | --- | ---: |
| Synthetic | 256 | sequential | 752.048 [689.085, 759.181] / 759.893 / 84.180 / 11.83% ktx/s |
| Synthetic | 256 | parallel/1 | 491.178 [451.484, 495.792] / 498.200 / 50.305 / 10.74% ktx/s |
| Synthetic | 256 | parallel/2 | 440.727 [394.763, 445.100] / 450.085 / 49.141 / 11.82% ktx/s |
| Synthetic | 256 | parallel/4 | 483.928 [398.090, 486.617] / 490.975 / 62.199 / 14.01% ktx/s |
| Synthetic | 256 | parallel/8 | 424.850 [347.334, 446.616] / 453.131 / 54.839 / 13.65% ktx/s |
| Synthetic | 1,024 | sequential | 691.675 [636.553, 702.004] / 703.940 / 50.207 / 7.50% ktx/s |
| Synthetic | 1,024 | parallel/1 | 478.302 [418.864, 485.521] / 487.806 / 39.912 / 8.80% ktx/s |
| Synthetic | 1,024 | parallel/2 | 435.167 [383.002, 438.649] / 439.708 / 40.527 / 9.87% ktx/s |
| Synthetic | 1,024 | parallel/4 | 489.072 [378.879, 497.368] / 498.621 / 69.896 / 15.75% ktx/s |
| Synthetic | 1,024 | parallel/8 | 449.456 [370.320, 462.851] / 463.493 / 50.548 / 12.00% ktx/s |
| Synthetic | 4,096 | sequential | 662.382 [641.384, 669.569] / 673.620 / 26.969 / 4.13% ktx/s |
| Synthetic | 4,096 | parallel/1 | 426.421 [412.809, 454.626] / 456.586 / 22.419 / 5.22% ktx/s |
| Synthetic | 4,096 | parallel/2 | 408.859 [379.935, 427.217] / 431.371 / 27.096 / 6.73% ktx/s |
| Synthetic | 4,096 | parallel/4 | 461.331 [396.097, 483.927] / 484.430 / 45.389 / 10.17% ktx/s |
| Synthetic | 4,096 | parallel/8 | 446.370 [389.789, 466.102] / 470.223 / 42.626 / 9.86% ktx/s |
| Realistic Move | 256 | sequential | 124.497 [116.838, 125.826] / 126.791 / 7.094 / 5.84% ktx/s |
| Realistic Move | 256 | parallel/1 | 111.979 [105.207, 112.994] / 113.618 / 4.243 / 3.86% ktx/s |
| Realistic Move | 256 | parallel/2 | 132.536 [115.286, 134.695] / 135.895 / 10.302 / 8.10% ktx/s |
| Realistic Move | 256 | parallel/4 | 165.680 [121.276, 169.817] / 170.017 / 24.166 / 16.13% ktx/s |
| Realistic Move | 256 | parallel/8 | 164.315 [120.982, 170.902] / 173.314 / 26.466 / 17.79% ktx/s |
| Realistic Move | 1,024 | sequential | 117.224 [111.165, 122.811] / 123.147 / 6.005 / 5.15% ktx/s |
| Realistic Move | 1,024 | parallel/1 | 109.026 [101.524, 110.733] / 111.734 / 5.363 / 5.04% ktx/s |
| Realistic Move | 1,024 | parallel/2 | 129.073 [114.923, 138.959] / 144.169 / 14.073 / 11.05% ktx/s |
| Realistic Move | 1,024 | parallel/4 | 172.576 [131.367, 186.294] / 186.872 / 28.677 / 17.90% ktx/s |
| Realistic Move | 1,024 | parallel/8 | 186.549 [144.635, 199.823] / 201.955 / 28.109 / 16.12% ktx/s |
| Realistic Move | 4,096 | sequential | 115.489 [103.853, 120.815] / 121.544 / 8.719 / 7.73% ktx/s |
| Realistic Move | 4,096 | parallel/1 | 101.926 [95.946, 107.718] / 108.007 / 6.948 / 6.87% ktx/s |
| Realistic Move | 4,096 | parallel/2 | 128.156 [111.136, 136.024] / 137.018 / 14.842 / 12.04% ktx/s |
| Realistic Move | 4,096 | parallel/4 | 173.768 [132.668, 180.430] / 184.068 / 27.920 / 17.55% ktx/s |
| Realistic Move | 4,096 | parallel/8 | 192.167 [163.674, 203.319] / 206.447 / 27.513 / 15.04% ktx/s |

## APFS versus RAM/no-index throughput CV

| Workload | Transactions | Executor | APFS CV | RAM/no-index CV | Delta |
| --- | ---: | --- | ---: | ---: | ---: |
| Synthetic | 256 | sequential | 11.29% | 11.83% | +0.54 pp |
| Synthetic | 256 | parallel/1 | 10.99% | 10.74% | -0.25 pp |
| Synthetic | 256 | parallel/2 | 12.22% | 11.82% | -0.40 pp |
| Synthetic | 256 | parallel/4 | 21.32% | 14.01% | -7.32 pp |
| Synthetic | 256 | parallel/8 | 18.65% | 13.65% | -5.00 pp |
| Synthetic | 1,024 | sequential | 10.48% | 7.50% | -2.97 pp |
| Synthetic | 1,024 | parallel/1 | 11.11% | 8.80% | -2.32 pp |
| Synthetic | 1,024 | parallel/2 | 12.28% | 9.87% | -2.41 pp |
| Synthetic | 1,024 | parallel/4 | 17.59% | 15.75% | -1.85 pp |
| Synthetic | 1,024 | parallel/8 | 16.85% | 12.00% | -4.85 pp |
| Synthetic | 4,096 | sequential | 11.97% | 4.13% | -7.84 pp |
| Synthetic | 4,096 | parallel/1 | 13.46% | 5.22% | -8.23 pp |
| Synthetic | 4,096 | parallel/2 | 12.91% | 6.73% | -6.18 pp |
| Synthetic | 4,096 | parallel/4 | 15.40% | 10.17% | -5.23 pp |
| Synthetic | 4,096 | parallel/8 | 15.13% | 9.86% | -5.26 pp |
| Realistic Move | 256 | sequential | 15.61% | 5.84% | -9.77 pp |
| Realistic Move | 256 | parallel/1 | 16.67% | 3.86% | -12.81 pp |
| Realistic Move | 256 | parallel/2 | 18.36% | 8.10% | -10.25 pp |
| Realistic Move | 256 | parallel/4 | 24.78% | 16.13% | -8.65 pp |
| Realistic Move | 256 | parallel/8 | 28.93% | 17.79% | -11.14 pp |
| Realistic Move | 1,024 | sequential | 14.55% | 5.15% | -9.40 pp |
| Realistic Move | 1,024 | parallel/1 | 18.01% | 5.04% | -12.97 pp |
| Realistic Move | 1,024 | parallel/2 | 19.20% | 11.05% | -8.15 pp |
| Realistic Move | 1,024 | parallel/4 | 27.17% | 17.90% | -9.27 pp |
| Realistic Move | 1,024 | parallel/8 | 27.29% | 16.12% | -11.17 pp |
| Realistic Move | 4,096 | sequential | 11.92% | 7.73% | -4.19 pp |
| Realistic Move | 4,096 | parallel/1 | 10.86% | 6.87% | -3.98 pp |
| Realistic Move | 4,096 | parallel/2 | 19.01% | 12.04% | -6.96 pp |
| Realistic Move | 4,096 | parallel/4 | 24.11% | 17.55% | -6.56 pp |
| Realistic Move | 4,096 | parallel/8 | 21.30% | 15.04% | -6.26 pp |

## Decision

Use RAM/no-index storage for future local diagnostic runs because it removes a
measurable source of variance. Continue using paired speedup and efficiency as
the primary local metrics. Do not promote an absolute execution-throughput
number from this laptop. Before publishing an absolute number, rerun the same
harness on a dedicated non-APFS Linux or bare-metal host with fixed performance
governor/affinity and continuous CPU/background-process telemetry.
