#!/usr/bin/env bash
set -euo pipefail

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --nodes)
      NODES="$2"
      shift 2
      ;;
    --replication-factor)
      REPLICATION_FACTOR="$2"
      shift 2
      ;;
    --read-quorum-r)
      READ_QUORUM_R="$2"
      shift 2
      ;;
    --write-quorum-w)
      WRITE_QUORUM_W="$2"
      shift 2
      ;;
    --failure-mode)
      FAILURE_MODE="$2"
      shift 2
      ;;
    --crash-count)
      CRASH_COUNT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--nodes N] [--replication-factor N] [--read-quorum-r N] [--write-quorum-w N] [--failure-mode MODE] [--crash-count N] [--output-dir DIR] [--base-port PORT]"
      exit 1
      ;;
  esac
done

# Config (adjust as needed, can be overridden by command-line args or env vars)
NODES="${NODES:-20}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
READ_QUORUM_R="${READ_QUORUM_R:-2}"
WRITE_QUORUM_W="${WRITE_QUORUM_W:-2}"
FAILURE_MODE="${FAILURE_MODE:-none}"   # none|crash|slow|single_partition

BASE_PORT="${BASE_PORT:-60000}"
WORKLOAD="${WORKLOAD:-A}"
DURATION="${DURATION:-60}"
CONCURRENCY="${CONCURRENCY:-100}"
DIST="${DIST:-uniform}"
KEYSPACE="${KEYSPACE:-200}"
FAILURE_DURATION="${FAILURE_DURATION:-50}"
CRASH_COUNT="${CRASH_COUNT:-1}"
SLOW_MS="${SLOW_MS:-1000}"
PARTITION_MS="${PARTITION_MS:-120000}"   # only for single_partition
COMPUTE_METRICS="${COMPUTE_METRICS:---compute_metrics}"  # set to "" to skip


ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# Determine cluster_procs.json path based on output directory
if [[ -n "${OUTPUT_DIR:-}" ]]; then
  # Ensure output directory exists before launching cluster
  mkdir -p "$OUTPUT_DIR"
  # Use absolute path to avoid working directory issues
  if [[ "$OUTPUT_DIR" != /* ]]; then
    OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
  fi
  CLUSTER_PROCS_PATH="$OUTPUT_DIR/cluster_procs.json"
else
  CLUSTER_PROCS_PATH="$(pwd)/cluster_procs.json"
fi

cleanup() {
  echo "[script] cleaning up cluster"
  
  # Kill the launcher process if it's still running
  if [[ -n "${launcher_pid:-}" ]]; then
    kill "$launcher_pid" 2>/dev/null || true
    wait "$launcher_pid" 2>/dev/null || true
  fi
  
  # Kill all node processes from cluster_procs.json
  if [[ -f "$CLUSTER_PROCS_PATH" ]]; then
    python - <<PY
import json, os, signal, time

def is_process_alive(pid):
    """Check if a process is still alive"""
    try:
        os.kill(pid, 0)  # Signal 0 doesn't kill, just checks if process exists
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except Exception:
        return False

def kill_process(pid, sig):
    """Try to kill a process with the given signal"""
    try:
        os.kill(pid, sig)
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except Exception:
        return False

try:
    with open("$CLUSTER_PROCS_PATH") as f:
        procs = json.load(f)
    
    pids = [v["pid"] for v in procs.values()]
    print(f"[cleanup] Found {len(pids)} node processes to clean up")
    
    # First, send SIGTERM to all processes for graceful shutdown
    for pid in pids:
        if is_process_alive(pid):
            kill_process(pid, signal.SIGTERM)
    
    # Wait a bit for graceful shutdown
    time.sleep(3)
    
    # Check which processes are still alive and force kill them
    alive_pids = [pid for pid in pids if is_process_alive(pid)]
    
    if alive_pids:
        print(f"[cleanup] Force killing {len(alive_pids)} processes that didn't respond to SIGTERM")
        for pid in alive_pids:
            kill_process(pid, signal.SIGKILL)
        
        # Wait longer to ensure ports are released
        time.sleep(2)
        
        # Final check
        still_alive = [pid for pid in alive_pids if is_process_alive(pid)]
        
        if still_alive:
            print(f"[cleanup] WARNING: {len(still_alive)} processes may still be running: {still_alive}")
        else:
            print("[cleanup] All processes terminated successfully")
    else:
        print("[cleanup] All processes terminated gracefully")
except Exception as e:
    print(f"[cleanup] Error during cleanup: {e}")
PY
    
    # Additional wait to ensure ports are fully released by the OS
    sleep 2
    
    # Check if any processes are still using the ports we need
    # Note: This is just a warning check, don't fail if ports are still in use
    # as they might be in TIME_WAIT state which is normal
    if [[ -n "${BASE_PORT:-}" && -n "${NODES:-}" ]]; then
        python - <<PY
import socket

base_port = $BASE_PORT
num_nodes = $NODES
ports_in_use = []

for i in range(num_nodes):
    port = base_port + i
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.1)
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        if result == 0:
            ports_in_use.append(port)
    except Exception:
        pass

if ports_in_use:
    print(f"[cleanup] WARNING: Ports still in use: {ports_in_use}")
    print("[cleanup] This may cause issues in the next experiment")
    print("[cleanup] Waiting additional 3 seconds for ports to be released...")
else:
    print(f"[cleanup] All ports {base_port}-{base_port + num_nodes - 1} are free")
PY
        # If ports are still in use, wait a bit more
        if python - <<PY
import socket
base_port = $BASE_PORT
num_nodes = $NODES
for i in range(num_nodes):
    port = base_port + i
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.1)
        if sock.connect_ex(('127.0.0.1', port)) == 0:
            sock.close()
            exit(1)
        sock.close()
    except: pass
exit(0)
PY
        then
            sleep 3
        fi
    fi
  else
    echo "[cleanup] cluster_procs.json not found, skipping process cleanup"
  fi
}
trap cleanup EXIT

echo "[script] launching cluster..."
CLUSTER_ARGS=(
  --nodes "$NODES"
  --base_port "$BASE_PORT"
  --replication_factor "$REPLICATION_FACTOR"
  --read_quorum_r "$READ_QUORUM_R"
  --write_quorum_w "$WRITE_QUORUM_W"
)

if [[ -n "${OUTPUT_DIR:-}" ]]; then
  # Use the absolute path we computed earlier
  CLUSTER_ARGS+=(--output_dir "$OUTPUT_DIR")
fi

python run_cluster.py "${CLUSTER_ARGS[@]}" &
launcher_pid=$!

# Wait for cluster_procs.json to appear (increase timeout for larger clusters)
# With 30 nodes, it takes longer to start all nodes
MAX_WAIT_ATTEMPTS=$((20 + NODES / 2))  # Scale with number of nodes
for i in $(seq 1 $MAX_WAIT_ATTEMPTS); do
  if [[ -f "$CLUSTER_PROCS_PATH" ]]; then
    echo "[script] cluster_procs.json found after ${i} attempts"
    break
  fi
  if [[ $i -eq $MAX_WAIT_ATTEMPTS ]]; then
    echo "[script] ERROR: cluster_procs.json not found after $MAX_WAIT_ATTEMPTS attempts"
    echo "[script] Expected path: $CLUSTER_PROCS_PATH"
    exit 1
  fi
  sleep 0.2
done

# Additional wait after cluster_procs.json appears to give nodes time to fully start
# With many nodes, they need time to bind to ports and initialize
echo "[script] waiting additional ${NODES} seconds for all nodes to fully start..."
sleep $NODES

echo "[script] running experiment..."
EXPERIMENT_DRIVER_ARGS=(
  --cluster_procs "$CLUSTER_PROCS_PATH"
  --nodes "$NODES"
  --workload "$WORKLOAD"
  --duration "$DURATION"
  --concurrency "$CONCURRENCY"
  --dist "$DIST"
  --keyspace "$KEYSPACE"
  --replication_factor "$REPLICATION_FACTOR"
  --read_quorum_r "$READ_QUORUM_R"
  --write_quorum_w "$WRITE_QUORUM_W"
  --warmup 10
  --failure_mode "$FAILURE_MODE"
  --failure_duration "$FAILURE_DURATION"
  --crash_count "$CRASH_COUNT"
  --slow_ms "$SLOW_MS"
  --partition_ms "$PARTITION_MS"
)

if [[ -n "${OUTPUT_DIR:-}" ]]; then
  EXPERIMENT_DRIVER_ARGS+=(--output_dir "$OUTPUT_DIR")
fi

if [[ -n "$COMPUTE_METRICS" ]]; then
  EXPERIMENT_DRIVER_ARGS+=(--compute_metrics)
fi

python experiments/experiment_driver.py "${EXPERIMENT_DRIVER_ARGS[@]}"

echo "[script] experiment done; stopping cluster"
cleanup
wait $launcher_pid 2>/dev/null || true