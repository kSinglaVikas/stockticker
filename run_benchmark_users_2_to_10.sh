#!/usr/bin/env bash
set -euo pipefail

MINUTES=1
WAIT_SECONDS=2

echo "Running benchmark with --minutes ${MINUTES} --users 2"
python3 parallel_query_benchmark.py --minutes "${MINUTES}" --users 2

for users in {3..10}; do
  echo "Running benchmark with --minutes ${MINUTES} --users ${users}"
  python3 parallel_query_benchmark.py --minutes "${MINUTES}" --users "${users}"
  sleep "${WAIT_SECONDS}"
done

echo "Benchmark sweep complete (users: 2 through 10)."