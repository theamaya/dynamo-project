# experiments/experiment_driver.py
import argparse
import asyncio
import csv
import json
import os
import signal
import time
import random
import httpx
from datetime import datetime

from collections import defaultdict


def compute_metrics_from_workload(csv_path, warmup, failure_duration, exclude_nodes=None):
    """
    Robust metrics computation that correctly handles:
      - multi-version reads (list of {value, vc, ts})
      - Read-Your-Write: pass if any returned sibling equals client's last write
      - Monotonic Reads: uses max returned ts to check monotonicity
      - Monotonic Writes: basic check using last write tracking
      - Time-windowed availability
    """

    import csv, json, ast, time
    from datetime import datetime
    from collections import defaultdict

    events = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:

            # -------------------------------
            # 1. TIMESTAMP PARSING (FIXED)
            # -------------------------------
            ts_raw = row.get("timestamp") or row.get("ts")
            if not ts_raw:
                # skip rows without a usable timestamp
                continue
            ts = None
            # try float first
            try:
                ts = float(ts_raw)
            except Exception:
                # try ISO 8601
                try:
                    ts = datetime.fromisoformat(ts_raw).timestamp()
                except Exception:
                    try:
                        ts = datetime.fromisoformat(ts_raw.replace(" ", "T")).timestamp()
                    except Exception:
                        # give up on this row
                        continue

            client = row.get("client") or row.get("worker")
            op = (row.get("op") or "").upper()
            key = row.get("key")
            success_field = str(row.get("success")).lower()
            success = success_field == "true"
            node = row.get("node")
            raw_value = row.get("value")
            if raw_value in (None, ""):
                raw_value = row.get("versions_or_vc")

            # -----------------------------------
            # 2. JSON / LIST PARSING FIX
            # -----------------------------------
            parsed = None
            if raw_value is None:
                parsed = None
            else:
                cleaned = raw_value.replace('""', '"')  # CSV escapes
                try:
                    parsed = json.loads(cleaned)
                except Exception:
                    try:
                        parsed = ast.literal_eval(cleaned)
                    except Exception:
                        parsed = raw_value  # fallback

            # -------------------------------
            # NORMALIZE READ PAYLOADS
            # -------------------------------
            read_values = None
            read_max_ts = None
            is_multi_version = False
            best_read_value = None
            best_read_ts = None

            if isinstance(parsed, list):
                vals = []
                max_ts = None
                max_ts_value = None
                for item in parsed:
                    if isinstance(item, dict):
                        v = item.get("value")
                        vals.append(v)
                        try:
                            t = float(item.get("ts", 0))
                            if max_ts is None or t > max_ts:
                                max_ts = t
                                max_ts_value = v
                        except Exception:
                            pass
                    else:
                        vals.append(item)

                read_values = set(vals)
                read_max_ts = max_ts
                best_read_value = max_ts_value if max_ts_value is not None else (vals[-1] if vals else None)
                best_read_ts = max_ts
                is_multi_version = len(vals) > 1

            elif isinstance(parsed, dict) and "value" in parsed:
                read_values = {parsed.get("value")}
                try:
                    read_max_ts = float(parsed.get("ts", 0))
                except Exception:
                    read_max_ts = None
                best_read_value = parsed.get("value")
                best_read_ts = read_max_ts

            elif parsed is None or parsed == []:
                read_values = set()
                read_max_ts = None
                best_read_value = None
                best_read_ts = None

            else:
                # Hashable fallback: serialize dicts to JSON for set membership
                if isinstance(parsed, dict):
                    read_values = {json.dumps(parsed, sort_keys=True)}
                else:
                    read_values = {parsed}
                read_max_ts = None
                best_read_value = parsed
                best_read_ts = None

            events.append({
                "ts": ts,
                "client": client,
                "op": op,
                "key": key,
                "success": success,
                "node": node,
                "raw_value": raw_value,
                "parsed": parsed,
                "read_values": read_values,
                "read_max_ts": read_max_ts,
                "is_multi_version": is_multi_version,
                "best_read_value": best_read_value,
                "best_read_ts": best_read_ts
            })

    if not events:
        return {}

    # -------------------------------
    # WINDOW BOUNDARIES
    # -------------------------------
    t0 = events[0]["ts"]
    warmup_end = t0 + warmup
    failure_end = warmup_end + failure_duration

    windows = {
        "warmup":  {"start": t0, "end": warmup_end},
        "failure": {"start": warmup_end, "end": failure_end},
        "recovery": {"start": failure_end, "end": float("inf")}
    }

    total_ops = 0
    ok_ops = 0
    read_ops = write_ops = 0
    read_ok = write_ok = 0

    window_ops = {w: 0 for w in windows}
    window_ok = {w: 0 for w in windows}
    window_reads = {w: 0 for w in windows}
    window_read_ok = {w: 0 for w in windows}
    window_writes = {w: 0 for w in windows}
    window_write_ok = {w: 0 for w in windows}

    exclude_nodes = set(exclude_nodes or [])

    last_write_value = defaultdict(lambda: None)
    last_write_ts = defaultdict(lambda: None)
    last_write_value_global = defaultdict(lambda: None)  # latest written value per key
    last_write_ts_global = defaultdict(lambda: None)     # latest write ts per key (any client)
    last_read_max_ts = defaultdict(lambda: None)

    # Staleness tracking (value-based): does read return latest written value?
    stale_reads = 0
    stale_reads_checked = 0

    empty_reads = 0
    multi_version_reads = 0
    ryw_violations = 0
    monotonic_read_violations = 0
    monotonic_write_violations = 0

    # -------------------------------------
    # MAIN LOOP
    # -------------------------------------
    for ev in events:
        total_ops += 1
        ts = ev["ts"]
        client = ev["client"]
        key = ev["key"]
        op = ev["op"]
        success = ev["success"]
        node = ev.get("node")
        read_values = ev["read_values"]
        read_max_ts = ev["read_max_ts"]
        is_multi = ev["is_multi_version"]

        excluded_node = node in exclude_nodes

        # classify window
        w = None
        for window_name, bounds in windows.items():
            if bounds["start"] <= ts < bounds["end"]:
                w = window_name
                break
        if w is None:
            w = "recovery"

        if not excluded_node:
            window_ops[w] += 1

            if op == "READ":
                read_ops += 1
                window_reads[w] += 1
            else:
                write_ops += 1
                window_writes[w] += 1

            if success:
                ok_ops += 1
                window_ok[w] += 1

                if op == "READ":
                    read_ok += 1
                    window_read_ok[w] += 1
                else:
                    write_ok += 1
                    window_write_ok[w] += 1

        # -------------------------------
        # CONSISTENCY METRICS
        # -------------------------------
        if op == "READ" and success:

            if not read_values:
                empty_reads += 1

            if is_multi:
                multi_version_reads += 1

            # RYW
            prev_written = last_write_value[(client, key)]
            if prev_written is not None:
                if prev_written not in read_values:
                    ryw_violations += 1

            # Monotonic Reads
            if read_max_ts is not None:
                prev_max = last_read_max_ts[(client, key)]
                if prev_max is not None and read_max_ts < prev_max:
                    monotonic_read_violations += 1
                last_read_max_ts[(client, key)] = read_max_ts

            # Read staleness (value-based): compare chosen latest read value to latest written value
            latest_written_val = last_write_value_global.get(key)
            if latest_written_val is not None:
                stale_reads_checked += 1
                chosen_read_val = ev.get("best_read_value")
                if chosen_read_val != latest_written_val:
                    stale_reads += 1

        elif op == "WRITE" and success:
            raw_val = ev.get("parsed")
            if isinstance(raw_val, (str, int, float)):
                written = raw_val
            elif isinstance(raw_val, dict) and "value" in raw_val:
                written = raw_val["value"]
            else:
                written = ev.get("raw_value")

            prev_written = last_write_value[(client, key)]
            if prev_written is not None and written is not None and written != prev_written:
                monotonic_write_violations += 1

            last_write_value[(client, key)] = written
            last_write_ts[(client, key)] = ts
            last_write_value_global[key] = written
            last_write_ts_global[key] = ts

    availability = {
        "overall": ok_ops / total_ops if total_ops else 0.0,
        "reads": read_ok / read_ops if read_ops else 0.0,
        "writes": write_ok / write_ops if write_ops else 0.0
    }

    windowed_availability = {}
    for wname in windows:
        win_total = window_ops[wname]
        windowed_availability[wname] = {
            "overall": window_ok[wname] / win_total if win_total else 0.0,
            "reads": window_read_ok[wname] / window_reads[wname] if window_reads[wname] else 0.0,
            "writes": window_write_ok[wname] / window_writes[wname] if window_writes[wname] else 0.0,
            "ops": win_total
        }

    consistency = {
        "empty_reads": empty_reads,
        "multi_version_reads": multi_version_reads,
        "ryw_violations": ryw_violations,
        "monotonic_read_violations": monotonic_read_violations,
        "monotonic_write_violations": monotonic_write_violations,
        "stale_reads": stale_reads,
        "stale_read_rate": (stale_reads / stale_reads_checked) if stale_reads_checked else 0.0,
        "stale_reads_checked": stale_reads_checked
    }


    return {
        "availability": availability,
        "availability_by_window": windowed_availability,
        "consistency": consistency,
        "total_ops": total_ops
    }

# helper to read cluster_procs.json
def read_cluster_procs(path="cluster_procs.json"):
    with open(path) as f:
        return json.load(f)  # node_id -> {"pid":..., "port":...}

async def inject_delay(node_addr, delay_ms):
    async with httpx.AsyncClient() as client:
        await client.post(f"http://{node_addr}/control/delay", json={"delay_ms": delay_ms})

async def clear_delay(node_addr):
    async with httpx.AsyncClient() as client:
        await client.post(f"http://{node_addr}/control/clear_delay")

def kill_node(pid):
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception as e:
        print("kill error", e)

def start_workload_cmd(args, out_path):
    # returns subprocess.Popen
    import subprocess
    cmd = [
        "python", "experiments/workload.py",
        "--nodes_file", "all_nodes.txt",
        "--workload", args.workload,
        "--duration", str(args.duration),
        "--concurrency", str(args.concurrency),
        "--dist", args.dist,
        "--keyspace", str(args.keyspace),
        "--out", out_path
    ]
    return subprocess.Popen(cmd)

async def run_repair_rounds(sample_keys, replicas_of_key_fn_async, max_rounds=10):
    """
    Perform synchronous rounds: for each round,
      - for each node that is a replica for the key, call repair_once on that node for that key
      - after the round, check how many replicas have same single version
    Return dictionary key -> hops_to_converge (or None if not converged)
    Note: replicas_of_key_fn_async(key) should be an async function returning list of node_addrs (host:port)
    """
    results = {}
    for key in sample_keys:
        converged = False
        for r in range(1, max_rounds+1):
            # for each replica, call /repair_once/{key}
            tasks = []
            try:
                replicas = await replicas_of_key_fn_async(key)
            except Exception as e:
                print(f"[repair] error fetching replicas for key {key}: {e}")
                replicas = []

            if not replicas:
                # can't repair if we don't know replicas
                results[key] = None
                break

            async with httpx.AsyncClient() as client:
                for node in replicas:
                    tasks.append(client.post(f"http://{node}/repair_once/{key}", timeout=5.0))
                # run all
                await asyncio.gather(*tasks, return_exceptions=True)

            # after round, check convergence: fetch versions from all replicas
            async with httpx.AsyncClient() as client:
                versions_list = []
                for node in replicas:
                    try:
                        r = await client.get(f"http://{node}/get_local/{key}", timeout=3.0)
                        if r.status_code == 200:
                            versions_list.append(r.json().get("versions", []))
                        else:
                            versions_list.append([])
                    except Exception:
                        versions_list.append([])

            # check if all replicas have single identical version (compare vc)
            single_vcs = []
            for vlist in versions_list:
                if len(vlist) == 1:
                    single_vcs.append(json.dumps(vlist[0]["vc"], sort_keys=True))
                else:
                    single_vcs.append(None)
            if all(s is not None for s in single_vcs) and len(set(single_vcs)) == 1:
                results[key] = r
                converged = True
                break
        if not converged:
            results[key] = None
    return results

def replicas_from_ring(key, all_nodes, N):
    """
    Given the key, compute replica list using same consistent hashing logic as nodes,
    but as an approximation here we pick N nodes by hashing key modulo len(all_nodes) offsets.
    For accurate mapping, you may expose a /ring_lookup endpoint from a node to get true replicas.
    """
    import hashlib
    h = int(hashlib.sha1(key.encode()).hexdigest(), 16)
    idx = h % len(all_nodes)
    out = []
    for i in range(N):
        out.append(all_nodes[(idx + i) % len(all_nodes)])
    return out

async def prepopulate_cluster(all_nodes, keyspace, initial_value="INIT"):
    """
    Write an initial value to every key in the keyspace.
    Only a single coordinator write is needed per key.
    This function retries failed writes a few times to improve robustness.
    """
    print(f"[driver] prepopulating {keyspace} keys...")

    async with httpx.AsyncClient(timeout=10.0) as client:
        batch = []
        for k in range(1, keyspace + 1):
            key = str(k)
            coord = random.choice(all_nodes)
            payload = {"value": initial_value}
            batch.append(client.put(f"http://{coord}/put/{key}", json=payload))

            # throttle large bursts
            if len(batch) >= 200:
                await asyncio.gather(*batch, return_exceptions=True)
                batch = []

        if batch:
            await asyncio.gather(*batch, return_exceptions=True)

    print("[driver] prepopulation complete.")

# -------------------------
# Helper: ask a coordinator for the true replica set for a key
# -------------------------
async def get_replicas_from_server(all_nodes, key, coordinator_index=0):
    """
    Query a coordinator node for /replicas_for_key/{key}.
    all_nodes: list of node addrs (host:port)
    coordinator_index: which node in all_nodes to query (default 0)
    Returns: list of replica addrs or [] on error.
    """
    if not all_nodes:
        return []
    coord = all_nodes[coordinator_index]
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"http://{coord}/replicas_for_key/{key}")
            if r.status_code == 200:
                js = r.json()
                return js.get("replicas", [])
    except Exception as e:
        print(f"[driver] get_replicas_from_server error contacting {coord} for key {key}: {e}")
    return []

async def main(args):
    cluster = read_cluster_procs(args.cluster_procs)
    all_nodes = [f"127.0.0.1:{cluster[n]['port']}" for n in cluster.keys()]
    # write all_nodes.txt for workload script
    with open("all_nodes.txt", "w") as f:
        f.write(",".join(all_nodes))

    # -------------------------
    # Create experiment result directory
    # -------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    exp_dir = f"results/exp_{timestamp}"
    os.makedirs(exp_dir, exist_ok=True)

    # Save experiment config
    config_path = os.path.join(exp_dir, "experiment_config.json")
    with open(config_path, "w") as f:
        json.dump(vars(args), f, indent=2)

    print(f"[driver] created experiment directory: {exp_dir}")


    # PREPOPULATE KEYSPACE
    await prepopulate_cluster(all_nodes, args.keyspace)

    # start workload subprocess
    out_csv = os.path.join(exp_dir, "workload.csv")
    p = start_workload_cmd(args, out_csv)
    print("[driver] workload started, pid", p.pid)
    # schedule failure injection after warmup
    await asyncio.sleep(args.warmup)

    # inject failure
    crashed_nodes = []
    if args.failure_mode == "crash":
        # choose nodes to kill
        crash_count = getattr(args, 'crash_count', 1)
        available_nodes = list(cluster.keys())
        crash_count = min(crash_count, len(available_nodes))  # Don't crash more than available
        
        victims = random.sample(available_nodes, crash_count)
        for victim in victims:
            pid = cluster[victim]["pid"]
            print(f"[driver] killing {victim} (pid {pid})")
            kill_node(pid)
            crashed_nodes.append(victim)
    elif args.failure_mode == "slow":
        # choose node and inject delay
        victim = random.choice(all_nodes)
        print("[driver] injecting delay on", victim)
        await inject_delay(victim, args.slow_ms)
    elif args.failure_mode == "single_partition":
        victim = random.choice(all_nodes)
        print("[driver] PARTITIONING node", victim)
        # For a strong partition, 60s delay makes node effectively unreachable
        await inject_delay(victim, args.partition_ms)
    else:
        print("[driver] no failure injected")

    # let workload run during failure interval
    await asyncio.sleep(args.failure_duration)

    # heal: if slow, clear delay; if crash, cannot auto-restart here (user must restart)
    if args.failure_mode == "slow":
        await clear_delay(victim)
    elif args.failure_mode == "crash":
        if len(crashed_nodes) == 1:
            print(f"[driver] node {crashed_nodes[0]} was crashed; please restart cluster if you want to measure post-recovery repair.")
        else:
            print(f"[driver] nodes {', '.join(crashed_nodes)} were crashed; please restart cluster if you want to measure post-recovery repair.")
    elif args.failure_mode == "single_partition":
        # print("[driver] healing partition on", victim)
        # await clear_delay(victim)
        print("[driver] we decided not to heal the partition on", victim)

    # wait for workload to finish
    p.wait()
    print("[driver] workload finished. saved to", out_csv)

    # now sample keys and run repair rounds if cluster is healed
    if args.post_repair and args.failure_mode != "crash":
        # pick sample hot keys
        sample_keys = [str(i) for i in random.sample(range(args.keyspace), min(50, args.keyspace))]

        # define async replica mapping function that queries the server
        async def replicas_fn_async(k):
            return await get_replicas_from_server(all_nodes, k, coordinator_index=0)

        print("[driver] running repair rounds for sample keys...")
        res = await run_repair_rounds(sample_keys, replicas_fn_async, max_rounds=args.max_repair_rounds)
        # write repair results
        repair_path = os.path.join(exp_dir, "repair_results.json")
        with open(repair_path, "w") as f:
            json.dump(res, f, indent=2)
        print(f"[driver] repair results stored in {repair_path}")

    if args.compute_metrics:
        print("[driver] computing consistency + availability metrics...")
        exclude_nodes = []
        try:
            # For crash mode, exclude all crashed nodes
            if args.failure_mode == "crash" and crashed_nodes:
                for crashed_node in crashed_nodes:
                    if crashed_node in cluster:
                        victim_addr = f"127.0.0.1:{cluster[crashed_node]['port']}"
                        # exclude_nodes.append(victim_addr)
        except Exception:
            pass

        metrics = compute_metrics_from_workload(
            out_csv,
            warmup=args.warmup,
            failure_duration=args.failure_duration,
            exclude_nodes=exclude_nodes
        )

        metrics_path = os.path.join(exp_dir, "computed_metrics.json")
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=2)

        print(f"[driver] metrics saved to {metrics_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster_procs", type=str, default="cluster_procs.json")
    parser.add_argument("--workload", choices=["A","B"], default="A")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--dist", choices=["uniform","zipf"], default="uniform")
    parser.add_argument("--keyspace", type=int, default=1000)
    parser.add_argument("--replication_factor", type=int, default=3)
    parser.add_argument("--read_quorum_r", type=int, default=2)
    parser.add_argument("--write_quorum_w", type=int, default=2)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--failure_mode", choices=["none","crash","slow","single_partition"], default="none")
    parser.add_argument("--failure_duration", type=int, default=20)
    parser.add_argument("--crash_count", type=int, default=1, help="Number of nodes to crash (only applies to crash failure_mode)")
    parser.add_argument("--slow_ms", type=int, default=200)
    parser.add_argument("--post_repair", action="store_true")
    parser.add_argument("--max_repair_rounds", type=int, default=10)
    parser.add_argument("--N", type=int, default=3)
    parser.add_argument("--partition_ms", type=int, default=60000)   # 60 seconds
    parser.add_argument("--compute_metrics", action="store_true")


    args = parser.parse_args()

    asyncio.run(main(args))
