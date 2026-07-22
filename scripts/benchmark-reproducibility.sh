#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 BENCHMARK_BINARY" >&2
  exit 64
fi

benchmark_binary=$1
runs=${RUNS:-10}
start_run=${START_RUN:-1}
sample_size=${SAMPLE_SIZE:-20}
warm_up_seconds=${WARM_UP_SECONDS:-1}
measurement_seconds=${MEASUREMENT_SECONDS:-3}
cooldown_seconds=${COOLDOWN_SECONDS:-30}
baseline_prefix=${BASELINE_PREFIX:-repro-}
precondition_seconds=${PRECONDITION_SECONDS:-0}
log_directory=${LOG_DIRECTORY:-/private/tmp/kestrel-benchmark-repro}
reject_process_pattern=${REJECT_PROCESS_PATTERN:-}

if (( runs < 10 )); then
  echo "RUNS must be at least 10" >&2
  exit 64
fi
if (( start_run < 1 || start_run > runs )); then
  echo "START_RUN must be between 1 and RUNS" >&2
  exit 64
fi
if [[ ! -x $benchmark_binary ]]; then
  echo "benchmark binary is not executable: $benchmark_binary" >&2
  exit 66
fi
mkdir -p "$log_directory"

families=(
  low_contention/256
  low_contention/1024
  low_contention/4096
  realistic_move/256
  realistic_move/1024
  realistic_move/4096
)
forward_methods=(sequential parallel/1 parallel/2 parallel/4 parallel/8)
reverse_methods=(parallel/8 parallel/4 parallel/2 parallel/1 sequential)

reject_competing_processes() {
  if [[ -n $reject_process_pattern ]] \
    && ps -Ao comm= | grep -E -q "$reject_process_pattern"
  then
    echo "aborting: a rejected competing process is running" >&2
    ps -Ao pid,%cpu,comm -r | sed -n '1,12p' >&2
    exit 75
  fi
  if command -v tmutil >/dev/null 2>&1 \
    && tmutil status | grep -q 'Running = 1'
  then
    echo "aborting: Time Machine is running" >&2
    exit 75
  fi
}

if (( precondition_seconds > 0 )); then
  reject_competing_processes
  echo "thermal preconditioning for ${precondition_seconds}s"
  "$benchmark_binary" \
    --bench realistic_move/4096/parallel/8 \
    --profile-time "$precondition_seconds" \
    --noplot \
    --quiet \
    >"$log_directory/precondition.log" 2>&1
  if command -v pmset >/dev/null 2>&1; then
    pmset -g therm
  fi
  uptime
  echo "thermal preconditioning complete"
fi

for (( run = start_run; run <= runs; run++ )); do
  baseline=$(printf '%s%02d' "$baseline_prefix" "$run")
  run_log="$log_directory/$baseline.log"
  : >"$run_log"
  echo "benchmark repetition $run/$runs baseline=$baseline"

  # Rotate workload/size order and reverse executor order every other run so
  # thermal position cannot systematically favor one configuration.
  family_offset=$(( ((run - 1) * 5) % ${#families[@]} ))
  for (( position = 0; position < ${#families[@]}; position++ )); do
    reject_competing_processes
    family_index=$(( (family_offset + position) % ${#families[@]} ))
    family=${families[$family_index]}
    if (( run % 2 == 1 )); then
      methods=("${forward_methods[@]}")
    else
      methods=("${reverse_methods[@]}")
    fi
    for method in "${methods[@]}"; do
      "$benchmark_binary" \
        --bench "$family/$method" \
        --sample-size "$sample_size" \
        --warm-up-time "$warm_up_seconds" \
        --measurement-time "$measurement_seconds" \
        --noplot \
        --quiet \
        --save-baseline "$baseline" \
        >>"$run_log" 2>&1
    done
  done

  if command -v pmset >/dev/null 2>&1; then
    pmset -g therm
  fi
  uptime
  if command -v ps >/dev/null 2>&1; then
    ps -Ao pid,%cpu,comm -r | sed -n '1,8p'
  fi
  echo "completed repetition $run/$runs"
  if (( run < runs && cooldown_seconds > 0 )); then
    sleep "$cooldown_seconds"
  fi
done
