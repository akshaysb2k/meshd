#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bench
pkill -f 'bin/backend' 2>/dev/null || true
pkill -f 'bin/meshd'   2>/dev/null || true
sleep 0.3

./bin/backend -addr :9001 -name b1 -latency-ms 4 -jitter-ms 2 >/dev/null 2>&1 &
./bin/backend -addr :9002 -name b2 -latency-ms 4 -jitter-ms 2 >/dev/null 2>&1 &
./bin/backend -addr :9003 -name b3 -latency-ms 4 -jitter-ms 2 >/dev/null 2>&1 &
sleep 0.5
./bin/meshd -config examples/config.json -addr :8080 -admin-addr :9901 -log-level warn >bench/meshd.log 2>&1 &
sleep 1.5

echo "--- steady state ---"
curl -s localhost:9901/clusters | head -20

(
  sleep 6
  echo ">>> t=6s  b2 starts returning 503 for every request"
  curl -s "localhost:9002/_control?error_rate=1.0" >/dev/null
  sleep 7
  echo ">>> t=13s b2 recovers"
  curl -s "localhost:9002/_control?error_rate=0" >/dev/null
) &

./bin/loadgen -url http://localhost:8080/api/users -qps 300 -duration 20s -csv bench/failover.csv

echo
echo "--- endpoint state after the incident ---"
curl -s localhost:9901/clusters
echo
echo "--- key metrics ---"
curl -s localhost:9901/metrics | grep -E '^meshd_(retries_total|outlier_ejections_total|requests_total)' | head -20

pkill -f 'bin/backend' 2>/dev/null || true
pkill -f 'bin/meshd'   2>/dev/null || true
