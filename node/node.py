# node/node.py
import os
import asyncio
from typing import List, Dict, Any
from fastapi import FastAPI, Request, HTTPException, Body
import uvicorn
import httpx
import argparse

# local module imports (assumed present in node/ package)
from hash_ring import HashRing
from membership import MembershipService
from vector_clock import VectorClock
from storage import put_local, get_local_versions, merge_remote_versions, overwrite_local_versions
from replication import quorum_write, quorum_read

def parse_args():
    parser = argparse.ArgumentParser()

    # Required (identity)
    parser.add_argument("--node_id", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--all_nodes", type=str, required=True)

    # Optional (use defaults)
    parser.add_argument("--replication_factor", type=int, default=3)
    parser.add_argument("--read_quorum_r", type=int, default=2)
    parser.add_argument("--write_quorum_w", type=int, default=2)
    parser.add_argument("--vnodes_per_node", type=int, default=100)

    parser.add_argument("--heartbeat_interval", type=float, default=1.0)
    parser.add_argument("--ping_timeout", type=float, default=1.5)

    parser.add_argument("--replication_timeout", type=float, default=1.0)
    parser.add_argument("--read_timeout", type=float, default=1.0)

    parser.add_argument("--debug", action="store_true")

    return parser.parse_args()

args = parse_args()
NODE_ID = args.node_id
PORT = args.port
ALL_NODES = args.all_nodes.split(",")
REPLICATION_FACTOR = args.replication_factor
READ_QUORUM_R = args.read_quorum_r
WRITE_QUORUM_W = args.write_quorum_w
VNODES_PER_NODE = args.vnodes_per_node

# ---------- Globals ----------
app = FastAPI(title=f"dynamo-node-{NODE_ID}")

# instantiate ring and membership service
global_ring = HashRing(nodes=ALL_NODES, vnodes=VNODES_PER_NODE)
membership_service = MembershipService(self_id=NODE_ID, peers=ALL_NODES)

# node-level variable for artificial delay (ms)
_REQUEST_DELAY_MS = 0  # 0 = no delay

# ---------- Helper utilities ----------
def _maybe_sleep():
    """Return coroutine that sleeps if delay set."""
    if _REQUEST_DELAY_MS and _REQUEST_DELAY_MS > 0:
        return asyncio.sleep(_REQUEST_DELAY_MS / 1000.0)
    return asyncio.sleep(0)  # no-op awaitable

def _latest_local_vc(key: str):
    """
    Return the VC dict of the newest version stored locally for 'key',
    or None if none exist.
    """
    versions = get_local_versions(key)
    if not versions:
        return None
    # pick version with max ts
    latest = max(versions, key=lambda v: v.get("ts", 0))
    return latest.get("vc")

# ---------- Health endpoint ----------
@app.get("/ping")
async def ping():
    """Simple liveness probe used by failure detector."""
    return {"status": "ok", "node": NODE_ID}

# ---------- Debug endpoint ----------
@app.get("/ring_snapshot")
def ring_snapshot():
    """Return ring snapshot and known alive nodes for debugging."""
    alive = membership_service.alive_nodes() if callable(getattr(membership_service, "alive_nodes", None)) else membership_service.alive_nodes
    return {
        "node": NODE_ID,
        "ring": global_ring.ring_snapshot(),
        "all_nodes": global_ring.all_nodes(),
        "alive_nodes": sorted(list(alive))
    }

# ---------- New: ring lookup endpoint ----------
@app.get("/ring_lookup/{key}")
def ring_lookup(key: str):
    """
    Return the preference list (replica nodes) for a key using the current ring.
    This does not filter by alive nodes (use alive_nodes if desired).
    """
    replicas = global_ring.get_replicas(key, N=REPLICATION_FACTOR, alive_nodes=None)
    return {"key": key, "replicas": replicas, "N": REPLICATION_FACTOR}

# ---------- Internal RPC: replicate (accept a version) ----------
@app.put("/replicate")
async def replicate_endpoint(req: Request):
    """
    Accept replicate RPC from peers.
    Payload expected: {"key": <str>, "value": <str>, "vc": {node->count}}
    Stores version locally (append) and returns 200.
    """
    await _maybe_sleep()
    body = await req.json()
    key = body.get("key")
    value = body.get("value")
    vc_dict = body.get("vc", {})
    if key is None or value is None:
        raise HTTPException(status_code=400, detail="key & value required")

    vc = VectorClock.from_dict(vc_dict)
    # store locally
    put_local(key, value, vc)
    return {"status": "ok", "node": NODE_ID}

# ---------- Internal RPC: get local versions ----------
@app.get("/get_local/{key}")
async def get_local_endpoint(key: str):
    """
    Return local versions for a key in the form:
    {"versions": [ { "value": ..., "vc": {...}, "ts": ... }, ... ] }
    """
    versions = get_local_versions(key)
    return {"versions": versions}

# ---------- Client-facing API: PUT (coordination + quorum write) ----------
@app.put("/put/{key}")
async def put_handler(key: str, req: Request):
    await _maybe_sleep()
    body = await req.json()
    value = body.get("value")

    if value is None:
        raise HTTPException(status_code=400, detail="value required")

    # Determine replication set
    alive = (
        membership_service.alive_nodes()
        if callable(getattr(membership_service, "alive_nodes", None))
        else membership_service.alive_nodes
    )
    candidates = global_ring.get_replicas(
        key, N=REPLICATION_FACTOR, alive_nodes=alive
    )

    # --------------------------------------------------------------
    # 1. QUORUM READ TO FETCH PARENT VERSIONS
    # --------------------------------------------------------------
    ok, parent_versions, responders = await quorum_read(
        key=key,
        candidates=candidates,
        R=READ_QUORUM_R,
        do_read_repair=True   # automatically keeps replicas synced
    )

    # If no quorum read, treat as empty parent set
    if not ok:
        parent_versions = []

    # --------------------------------------------------------------
    # 2. MERGE VECTOR CLOCKS FROM PARENT VERSIONS
    # --------------------------------------------------------------
    merged = {}
    for pv in parent_versions:
        vc = pv.get("vc", {})
        for n, c in vc.items():
            merged[n] = max(merged.get(n, 0), c)

    parent_vc = VectorClock.from_dict(merged)

    # --------------------------------------------------------------
    # 3. INCREMENT COORDINATOR’S VECTOR CLOCK ENTRY
    # --------------------------------------------------------------
    parent_vc.increment(NODE_ID)

    # --------------------------------------------------------------
    # 4. QUORUM WRITE WITH NEW VEC CLOCK
    # --------------------------------------------------------------
    success, succeeded, failed, stored_version = await quorum_write(
        key=key,
        value=value,
        candidates=candidates,
        W=WRITE_QUORUM_W,
        local_node_id=NODE_ID,
        parent_vc=parent_vc
    )

    return {
        "success": success,
        "requested_replicas": candidates,
        "responded_to_parent_read": responders,
        "succeeded": succeeded,
        "failed": failed,
        "used_vc": stored_version.get("vc") if stored_version else None,
        "stored_version": stored_version
    }


# ---------- Client-facing API: GET (coordination + quorum read) ----------
@app.get("/get/{key}")
async def get_handler(key: str):
    """
    Dynamo-style GET:
      - returns all non-dominated versions (siblings)
      - coordinator performs quorum read + optional read-repair
      - no vector clocks created or manipulated here
    """
    await _maybe_sleep()

    alive = (
        membership_service.alive_nodes()
        if callable(getattr(membership_service, "alive_nodes", None))
        else membership_service.alive_nodes
    )

    candidates = global_ring.get_replicas(
        key, N=REPLICATION_FACTOR, alive_nodes=alive
    )

    ok, resolved_versions, responders = await quorum_read(
        key=key,
        candidates=candidates,
        R=READ_QUORUM_R,
        do_read_repair=True
    )

    if not ok:
        raise HTTPException(
            status_code=503,
            detail="Not enough replicas responded"
        )

    return {
        "resolved_versions": resolved_versions,   # siblings as-is
        "responded_nodes": responders
    }



# ---------- Startup / Shutdown events ----------
@app.on_event("startup")
async def startup():
    # start membership/gossip background task
    asyncio.create_task(membership_service.run())
    # (optional) print startup info
    print(f"[{NODE_ID}] node starting. Ring nodes: {global_ring.all_nodes()}")

@app.on_event("shutdown")
async def shutdown():
    membership_service.stop()
    await asyncio.sleep(0.1)

# ---------- Gossip ----------
@app.post("/gossip")
async def gossip(remote_table: dict):
    membership_service._merge(remote_table)
    return membership_service.get_membership()

# ---- control endpoints: inject artificial delay per-request ----
@app.post("/control/delay")
async def control_delay(payload: Dict = Body(...)):
    """
    payload: {"delay_ms": 200}  -> sets artificial delay in ms
    """
    global _REQUEST_DELAY_MS
    _REQUEST_DELAY_MS = int(payload.get("delay_ms", 0))
    return {"status": "ok", "delay_ms": _REQUEST_DELAY_MS}

@app.post("/control/clear_delay")
async def control_clear_delay():
    global _REQUEST_DELAY_MS
    _REQUEST_DELAY_MS = 0
    return {"status": "ok"}

@app.get("/replicas_for_key/{key}")
async def replicas_for_key(key: str):
    """
    Return the actual replica set for this key according to this node’s ring view.
    """
    try:
        replicas = global_ring.get_replicas(key, N=REPLICATION_FACTOR, alive_nodes=None)
        return {"replicas": replicas}
    except Exception as e:
        return {"error": str(e)}


# ---- repair_once endpoint ----
@app.post("/repair_once/{key}")
async def repair_once_endpoint(key: str):
    """
    One-hop repair for the key:
      - fetch local versions
      - choose a single peer from the ring's preference list (next replica)
      - fetch that peer's versions
      - merge (drop dominated) locally and push merged set to the peer
    Returns summary about versions and whether push occurred.
    """
    # determine full replica list (use full N so we know peers)
    candidates = global_ring.get_replicas(key, N=REPLICATION_FACTOR, alive_nodes=None)
    if not candidates:
        return {"ok": False, "reason": "no candidates"}

    # pick next peer as the repair target (the first candidate that is not self)
    try:
        my_index = candidates.index(NODE_ID)
        target = candidates[(my_index + 1) % len(candidates)]
    except ValueError:
        target = candidates[0] if candidates else None

    local_versions = get_local_versions(key)

    # fetch remote versions from target
    remote_versions = []
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"http://{target}/get_local/{key}")
            if r.status_code == 200:
                remote_versions = r.json().get("versions", [])
    except Exception:
        # target unreachable
        return {"ok": False, "reason": "target_unreachable", "target": target}

    # compute merged set (non-dominated)
    all_versions = local_versions + remote_versions
    keep = []
    for v in all_versions:
        vvc = VectorClock.from_dict(v["vc"])
        dominated = False
        for w in all_versions:
            if v is w:
                continue
            wvc = VectorClock.from_dict(w["vc"])
            cmp = VectorClock.compare(vvc, wvc)
            if cmp == -1:
                dominated = True
                break
        if not dominated:
            keep.append(v)

    # overwrite local versions with merged set
    overwrite_local_versions(key, keep)

    # push merged versions to target (one hop)
    pushed = 0
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            for v in keep:
                payload = {"key": key, "value": v["value"], "vc": v["vc"]}
                await client.put(f"http://{target}/replicate", json=payload)
                pushed += 1
    except Exception:
        pass

    return {"ok": True, "local_before": len(local_versions), "remote_before": len(remote_versions), "merged": len(keep), "pushed_to": target, "pushed": pushed}

# ---------- Main run helper ----------
if __name__ == "__main__":
    uvicorn.run("node:app", host="127.0.0.1", port=PORT, log_level="warning",
        access_log=False
    )