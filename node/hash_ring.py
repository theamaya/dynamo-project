# node/hash_ring.py
import hashlib
import bisect
from typing import List, Dict, Tuple, Optional, Set


def _hash_fn(key: str) -> int:
    """Return a stable integer hash for a string key (sha1 -> int)."""
    h = hashlib.sha1()
    h.update(key.encode("utf-8"))
    return int(h.hexdigest(), 16)


class HashRing:
    """
    Simple consistent hashing ring with virtual nodes.

    Usage:
      ring = HashRing(nodes=["node1:8000","node2:8000"], vnodes=1)
      replicas = ring.get_replicas("mykey", N=3)
    """

    def __init__(self, nodes: List[str], vnodes: int = 100):
        self.vnodes = vnodes
        self._ring: List[int] = []                # sorted list of vnode positions (ints)
        self._vnode_map: Dict[int, str] = {}      # position -> vnode_id (e.g. "node1#42")
        self._vnode_to_node: Dict[str, str] = {}  # vnode_id -> physical node id
        self._nodes: Set[str] = set()
        for n in nodes:
            self.add_node(n)

    def add_node(self, node_id: str):
        """Add a physical node with `vnodes` virtual nodes."""
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for i in range(self.vnodes):
            vnode_id = f"{node_id}#{i}"
            pos = _hash_fn(vnode_id)
            # ensure uniqueness of pos (very unlikely collision with sha1)
            while pos in self._vnode_map:
                # perturb vnode_id and rehash (extremely unlikely)
                vnode_id = vnode_id + "_"
                pos = _hash_fn(vnode_id)
            bisect.insort(self._ring, pos)
            self._vnode_map[pos] = vnode_id
            self._vnode_to_node[vnode_id] = node_id

    def remove_node(self, node_id: str):
        """Remove a physical node and all its vnodes."""
        if node_id not in self._nodes:
            return
        self._nodes.remove(node_id)
        # remove vnodes
        to_remove = [pos for pos, vnode in self._vnode_map.items()
                     if self._vnode_to_node.get(vnode) == node_id]
        for pos in to_remove:
            # remove from ring list and maps
            idx = bisect.bisect_left(self._ring, pos)
            if idx < len(self._ring) and self._ring[idx] == pos:
                self._ring.pop(idx)
            self._vnode_map.pop(pos, None)
            vnode = f"{node_id}#"  # not used further

        # remove vnode->node entries
        self._vnode_to_node = {v: n for v, n in self._vnode_to_node.items() if n != node_id}

    def get_replicas(self, key: str, N: int = 3, alive_nodes: Optional[Set[str]] = None) -> List[str]:
        """
        Return up to N distinct physical node_ids that are the replicas for `key`.
        If alive_nodes is provided, only return nodes in that set (skip dead ones).
        """
        if not self._ring:
            return []

        desired = N
        res: List[str] = []
        seen_nodes: Set[str] = set()
        key_pos = _hash_fn(key)
        # find insertion point
        idx = bisect.bisect(self._ring, key_pos)
        ring_len = len(self._ring)

        # Walk clockwise collecting distinct physical nodes
        i = idx
        while len(res) < desired and ring_len > 0:
            pos = self._ring[i % ring_len]
            vnode_id = self._vnode_map[pos]
            node = self._vnode_to_node[vnode_id]
            i += 1
            # skip duplicates (vnode -> same physical node)
            if node in seen_nodes:
                if i - idx >= ring_len:
                    break  # we've looped entire ring
                continue
            # if alive_nodes provided, skip nodes not alive
            if alive_nodes is not None and node not in alive_nodes:
                if i - idx >= ring_len:
                    break
                continue
            res.append(node)
            seen_nodes.add(node)
            if i - idx >= ring_len:
                break

        return res

    def all_nodes(self) -> List[str]:
        return list(self._nodes)

    def ring_snapshot(self) -> List[Tuple[int, str]]:
        """Return a snapshot list of (position, vnode_id) useful for debugging."""
        return [(pos, self._vnode_map[pos]) for pos in self._ring]
