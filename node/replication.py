# node/replication.py
import asyncio
from typing import List, Tuple, Dict, Any
import httpx
from vector_clock import VectorClock
from storage import put_local, get_local_versions, merge_remote_versions, overwrite_local_versions
import time

# timeouts
_RPC_TIMEOUT = 2.0

async def _rpc_put(node: str, payload: Dict[str, Any]) -> Tuple[str, bool]:
    """
    Send replicate RPC to node. Returns (node, success).
    """
    try:
        async with httpx.AsyncClient(timeout=_RPC_TIMEOUT) as client:
            r = await client.put(f"http://{node}/replicate", json=payload)
            return node, r.status_code == 200
    except Exception:
        return node, False

async def _rpc_get_local(node: str, key: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Fetch local versions from a replica (used by quorum_read).
    Returns (node, versions_list) or (node, []) on error.
    """
    try:
        async with httpx.AsyncClient(timeout=_RPC_TIMEOUT) as client:
            r = await client.get(f"http://{node}/get_local/{key}")
            if r.status_code == 200:
                return node, r.json().get("versions", [])
    except Exception:
        pass
    return node, []

def _merge_version_lists(all_versions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Dominance-based merge of a flat list of version dicts.
    Returns the list of non-dominated unique versions.
    """
    # reuse logic similar to storage.merge_remote_versions
    keep = []
    for v in all_versions:
        v_vc = VectorClock.from_dict(v["vc"])
        dominated = False
        for w in all_versions:
            if v is w:
                continue
            w_vc = VectorClock.from_dict(w["vc"])
            if VectorClock.compare(v_vc, w_vc) == -1:
                dominated = True
                break
        if not dominated:
            keep.append(v)

    # dedupe identical
    uniq = []
    seen = set()
    for v in keep:
        sig = (v["value"], tuple(sorted(v["vc"].items())))
        if sig not in seen:
            seen.add(sig)
            uniq.append(v)
    return uniq

async def quorum_write(
    key: str,
    value: str,
    candidates: List[str],
    W: int,
    local_node_id: str,
    parent_vc: VectorClock = None
) -> Tuple[bool, List[str], List[str], Dict[str, Any]]:

    # --- VC generation (Dynamo-style) ---
    if parent_vc is None:
        # blind write
        vc = VectorClock()
    else:
        # merge-write case (copy before increment!)
        vc = parent_vc.copy()

    vc.increment(local_node_id)

    # --- local store (canonical merge) ---
    stored_version = put_local(key, value, vc)

    # --- replication payload ---
    payload = {"key": key, "value": value, "vc": vc.to_dict()}

    rpc_targets = [n for n in candidates if n != local_node_id]
    tasks = [_rpc_put(node, payload) for node in rpc_targets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    succeeded = []
    failed = []

    for node, res in zip(rpc_targets, results):
        ok = False
        if isinstance(res, tuple):
            ok = res[1]
        elif isinstance(res, Exception):
            ok = False
        else:
            ok = bool(res)
        if ok:
            succeeded.append(node)
        else:
            failed.append(node)

    succeeded.append(local_node_id)
    success = len(succeeded) >= W

    return success, succeeded, failed, stored_version


async def quorum_read(
    key: str,
    candidates: List[str],
    R: int,
    do_read_repair: bool = False
) -> Tuple[bool, List[Dict[str, Any]], List[str]]:
    """
    Read from candidates in parallel, collect versions, merge using dominance rules.
    If do_read_repair=True, asynchronously push merged canonical versions to replicas that are stale.
    Returns (ok_enough_responses, merged_versions, responders_list)
    """
    # Fire parallel local fetches
    tasks = [_rpc_get_local(node, key) for node in candidates]
    results = await asyncio.gather(*tasks)

    responders = [node for node, vers in results if vers is not None]
    # Count successes (non-empty return counts as response; we allow empty lists)
    success_count = sum(1 for node, vers in results if vers is not None)

    ok = success_count >= R
    if not ok:
        return False, [], responders

    # Flatten versions
    flat = []
    node_versions_map = {}
    for node, vers in results:
        v = vers or []
        node_versions_map[node] = v
        flat.extend(v)

    merged = _merge_version_lists(flat)
    overwrite_local_versions(key, merged)

    # Optionally perform async read-repair: push merged to replicas that lack some versions
    if do_read_repair and merged:
        async def _repair_target(node_addr: str, merged_versions: List[Dict[str, Any]]):
            # For each merged version push via replicate RPC
            async with httpx.AsyncClient(timeout=_RPC_TIMEOUT) as client:
                for ver in merged_versions:
                    payload = {"key": key, "value": ver["value"], "vc": ver["vc"]}
                    try:
                        await client.put(f"http://{node_addr}/replicate", json=payload)
                    except Exception:
                        pass

        # Launch repair tasks in background
        for node, existing in node_versions_map.items():
            # check if node lacks any merged version signature
            existing_sigs = set((v["value"], tuple(sorted(v["vc"].items()))) for v in (existing or []))
            merged_sigs = set((v["value"], tuple(sorted(v["vc"].items()))) for v in merged)
            if not merged_sigs.issubset(existing_sigs):
                # kick off repair (don't await)
                asyncio.create_task(_repair_target(node, merged))

    return True, merged, responders
