# experiments/workload.py
import argparse
import asyncio
import csv
import random
import json
from datetime import datetime
import httpx
from numpy.random import zipf

# read nodes from file or CLI
def load_nodes(args):
    if args.nodes_file:
        with open(args.nodes_file) as f:
            return f.read().strip().split(",")
    elif args.nodes:
        return args.nodes.split(",")
    else:
        raise RuntimeError("No nodes provided")

def pick_key(distribution, keyspace):
    if distribution == "uniform":
        return str(random.randint(1, keyspace))
    elif distribution == "zipf":
        while True:
            k = zipf(1.3)
            if k <= keyspace:
                return str(k)

async def do_get(client, node_url, key):
    try:
        r = await client.get(f"http://{node_url}/get/{key}", timeout=5.0)
        if r.status_code == 200:
            body = r.json()
            # resolved_versions: list of {value, vc, ts}
            versions = body.get("resolved_versions", [])
            return True, versions
    except Exception:
        pass
    return False, []

async def do_put(client, node_url, key, value):
    try:
        payload = {"value": value}
        r = await client.put(f"http://{node_url}/put/{key}", json=payload, timeout=5.0)
        if r.status_code == 200:
            body = r.json()
            # If your PUT returns extra info, capture it; otherwise defaults to None
            used_vc = body.get("used_vc") if isinstance(body, dict) else None
            stored_version = body.get("stored_version") if isinstance(body, dict) else None
            return True, used_vc, stored_version
    except Exception:
        pass
    return False, None, None

async def worker(worker_id, nodes, op_ratio, duration, dist, keyspace, csv_writer):
    end = asyncio.get_event_loop().time() + duration
    async with httpx.AsyncClient() as client:
        while asyncio.get_event_loop().time() < end:
            node = random.choice(nodes)
            key = pick_key(dist, keyspace)
            if random.random() < op_ratio:  # read
                success, versions = await do_get(client, node, key)
                csv_writer.writerow([
                    datetime.now().isoformat(), worker_id, "READ", key, node, success, json.dumps(versions)
                ])
            else:
                value = str(random.randint(1, 10**9))
                success, used_vc, stored_version = await do_put(client, node, key, value)
                csv_writer.writerow([
                    datetime.now().isoformat(), worker_id, "WRITE", key, node, success, json.dumps(stored_version)
                ])

async def run(args):
    nodes = load_nodes(args)
    # create output csv
    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts","worker","op","key","node","success","versions_or_vc"])
        workers = []
        read_ratio = 0.95 if args.workload=="B" else 0.5
        for i in range(args.concurrency):
            workers.append(worker(i, nodes, read_ratio, args.duration, args.dist, args.keyspace, writer))
        await asyncio.gather(*workers)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--nodes_file", type=str, help="File with comma-separated list of nodes")
    p.add_argument("--nodes", type=str, help="Comma-separated list of nodes host:port")
    p.add_argument("--workload", choices=["A","B"], default="A")
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--dist", choices=["uniform","zipf"], default="uniform")
    p.add_argument("--keyspace", type=int, default=1000)
    p.add_argument("--out", type=str, default="workload_out.csv")
    args = p.parse_args()
    asyncio.run(run(args))
