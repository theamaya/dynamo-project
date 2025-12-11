import asyncio
import httpx
import hashlib
import json

async def get_server_replicas(node, key):
    async with httpx.AsyncClient(timeout=3.0) as client:
        r = await client.get(f"http://{node}/replicas_for_key/{key}")
        return r.json().get("replicas", [])

def approx_replicas_from_ring(key, nodes, N):
    h = int(hashlib.sha1(key.encode()).hexdigest(), 16)
    idx = h % len(nodes)
    return [nodes[(idx + i) % len(nodes)] for i in range(N)]

async def main():
    with open("cluster_procs.json") as f:
        cluster = json.load(f)

    all_nodes = [f"127.0.0.1:{cluster[n]['port']}" for n in cluster.keys()]

    sample_keys = ["123", "456", "789", "42", "900"]
    coordinator = all_nodes[0]
    N = 3

    for key in sample_keys:
        server_reps = await get_server_replicas(coordinator, key)
        approx_reps = approx_replicas_from_ring(key, all_nodes, N)
        print("\nKEY:", key)
        print("SERVER:", server_reps)
        print("APPROX:", approx_reps)
        mismatch = server_reps != approx_reps
        print("MISMATCH:", mismatch)

asyncio.run(main())
