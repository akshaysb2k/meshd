#!/usr/bin/env bash
# Measure proxy overhead against a direct-to-backend baseline.
# Needs more than one core to be meaningful.
set -euo pipefail
cd "$(dirname "$0")/.."
QPS=${QPS:-1000}
DUR=${DUR:-15s}
mkdir -p bench
pkill -f 'bin/backend' 2>/dev/null || true
pkill -f 'bin/meshd' 2>/dev/null || true
sleep 0.5
for p in 9001 9002 9003; do ./bin/backend -addr ":$p" -name "b$p" -latency-ms 0 -jitter-ms 0 >/dev/null 2>&1 & done
sleep 0.5
./bin/meshd -config examples/config.json -addr :8080 -admin-addr :9901 -log-level error >/dev/null 2>&1 &
sleep 1.5
echo "=== through meshd ==="
./bin/loadgen -url http://localhost:8080/api/x -qps "$QPS" -duration "$DUR" -csv bench/proxied.csv
echo
echo "=== direct to one backend (baseline) ==="
./bin/loadgen -url http://localhost:9001/x -qps "$QPS" -duration "$DUR" -csv bench/direct.csv
pkill -f 'bin/backend' 2>/dev/null || true
pkill -f 'bin/meshd' 2>/dev/null || true
