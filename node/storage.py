# node/storage.py
import time
from typing import Dict, List, Any, Tuple
from vector_clock import VectorClock

# Each stored version is a dict:
# { "value": <str>, "vc": <Dict[str,int]>, "ts": <float> }
_STORE: Dict[str, List[Dict[str, Any]]] = {}

def store(key, value):
    _STORE[key] = value

def get_value(key):
    return _STORE.get(key, None)

def now_ts() -> float:
    return time.time()

def _signature(v: Dict[str, Any]) -> Tuple:
    """
    Deterministic signature to dedupe identical versions.
    """
    vc_items = tuple(sorted(v["vc"].items()))
    return (v["value"], vc_items)

def put_local(key: str, value: str, vc: VectorClock) -> Dict[str, Any]:
    """
    Store a new version for key: merge into existing versions,
    drop dominated versions and deduplicate identical ones.
    Returns the stored version dict (the canonical record appended/kept).
    """
    versions = _STORE.setdefault(key, [])
    candidate = {
        "value": value,
        "vc": vc.to_dict(),
        "ts": now_ts()
    }

    # Combine candidate with existing, then run dominance/dedupe
    all_versions = versions + [candidate]

    keep: List[Dict[str, Any]] = []
    for v in all_versions:
        v_vc = VectorClock.from_dict(v["vc"])
        dominated = False
        for w in all_versions:
            if v is w:
                continue
            w_vc = VectorClock.from_dict(w["vc"])
            if VectorClock.compare(v_vc, w_vc) == -1:  # v < w
                dominated = True
                break
        if not dominated:
            keep.append(v)

    # Deduplicate exact identical (value + vc)
    unique = []
    seen = set()
    for v in keep:
        sig = _signature(v)
        if sig not in seen:
            seen.add(sig)
            unique.append(v)

    _STORE[key] = unique
    # Return the canonical stored version corresponding to candidate's signature
    cand_sig = (candidate["value"], tuple(sorted(candidate["vc"].items())))
    for v in unique:
        if (v["value"], tuple(sorted(v["vc"].items()))) == cand_sig:
            return v
    # If candidate was dominated, return whichever entry dominates (best effort)
    return unique[-1] if unique else candidate

def get_local_versions(key: str) -> List[Dict[str, Any]]:
    """
    Return list of versions for a key (may be empty).
    Each version is a dict with keys: value, vc(dict), ts.
    """
    return _STORE.get(key, []).copy()

def overwrite_local_versions(key: str, versions: List[Dict[str, Any]]):
    """
    Replace versions list for a key (useful for repair).
    """
    _STORE[key] = versions.copy()

def merge_remote_versions(key: str, remote_versions: List[Dict[str, Any]]):
    """
    Merge remote versions into local store, keeping all non-dominated versions.
    """
    all_versions = get_local_versions(key) + remote_versions
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
    unique = []
    seen = set()
    for v in keep:
        sig = (v["value"], tuple(sorted(v["vc"].items())))
        if sig not in seen:
            seen.add(sig)
            unique.append(v)

    overwrite_local_versions(key, unique)
    return unique
