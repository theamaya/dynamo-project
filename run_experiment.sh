#!/usr/bin/env bash
set -euo pipefail

# Config (adjust as needed)
NODES="${NODES:-5}"
BASE_PORT="${BASE_PORT:-60000}"
WORKLOAD="${WORKLOAD:-A}"
DURATION="${DURATION:-60}"
CONCURRENCY="${CONCURRENCY:-10}"
DIST="${DIST:-uniform}"
KEYSPACE="${KEYSPACE:-1000}"
FAILURE_MODE="${FAILURE_MODE:-none}"   # none|crash|slow|single_partition
FAILURE_DURATION="${FAILURE_DURATION:-20}"
CRASH_COUNT="${CRASH_COUNT:-1}"
SLOW_MS="${SLOW_MS:-200}"
PARTITION_MS="${PARTITION_MS:-60000}"   # only for single_partition
COMPUTE_METRICS="${COMPUTE_METRICS:---compute_metrics}"  # set to "" to skip
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
READ_QUORUM_R="${READ_QUORUM_R:-2}"
WRITE_QUORUM_W="${WRITE_QUORUM_W:-2}"

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

cleanup() {
  echo "[script] cleaning up cluster"
  if [[ -f cluster_procs.json ]]; then
    python - <<'PY'
import json, os, signal
with open("cluster_procs.json") as f:
    procs = json.load(f)
for pid in [v["pid"] for v in procs.values()]:
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
PY
  fi
}
trap cleanup EXIT

echo "[script] launching cluster..."
python run_cluster.py \
  --nodes "$NODES" \
  --base_port "$BASE_PORT" \
  --replication_factor "$REPLICATION_FACTOR" \
  --read_quorum_r "$READ_QUORUM_R" \
  --write_quorum_w "$WRITE_QUORUM_W" &
launcher_pid=$!

# Wait for cluster_procs.json to appear
for _ in {1..20}; do
  [[ -f cluster_procs.json ]] && break
  sleep 0.2
done

echo "[script] running experiment..."
python experiments/experiment_driver.py \
  --cluster_procs cluster_procs.json \
  --workload "$WORKLOAD" \
  --duration "$DURATION" \
  --concurrency "$CONCURRENCY" \
  --dist "$DIST" \
  --keyspace "$KEYSPACE" \
  --replication_factor "$REPLICATION_FACTOR" \
  --read_quorum_r "$READ_QUORUM_R" \
  --write_quorum_w "$WRITE_QUORUM_W" \
  --warmup 10 \
  --failure_mode "$FAILURE_MODE" \
  --failure_duration "$FAILURE_DURATION" \
  --crash_count "$CRASH_COUNT" \
  --slow_ms "$SLOW_MS" \
  --partition_ms "$PARTITION_MS" \
  $COMPUTE_METRICS

echo "[script] experiment done; stopping cluster"
cleanup
wait $launcher_pid 2>/dev/null || true