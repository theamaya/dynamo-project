# node/vector_clock.py
from typing import Dict, Tuple, Any
import json


class VectorClock:
    """
    Simple vector clock implementation.
    - Represented as a dict node_id -> counter (int).
    - compare(a,b) returns:
        -1 if a < b (a happened-before b)
         0 if a == b
         1 if a > b (a happened-after b)
         2 if concurrent
    """

    def __init__(self, clock: Dict[str, int] = None):
        self.clock = dict(clock) if clock else {}

    def increment(self, node_id: str):
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def update(self, other: "VectorClock"):
        """Merge other's counters (take max)."""
        for k, v in other.clock.items():
            self.clock[k] = max(self.clock.get(k, 0), v)

    def copy(self) -> "VectorClock":
        return VectorClock(dict(self.clock))

    def to_dict(self) -> Dict[str, int]:
        return dict(self.clock)

    @staticmethod
    def from_dict(d: Dict[str, int]) -> "VectorClock":
        return VectorClock(dict(d or {}))

    def serialize(self) -> str:
        return json.dumps(self.clock, sort_keys=True)

    @staticmethod
    def deserialize(s: str) -> "VectorClock":
        return VectorClock.from_dict(json.loads(s))

    @staticmethod
    def compare(a: "VectorClock", b: "VectorClock") -> int:
        """
        Compare two vector clocks.
        Return:
         -1 if a < b (a happens before b)
          0 if a == b
          1 if a > b
          2 if concurrent
        """
        keys = set(a.clock.keys()) | set(b.clock.keys())
        a_less = False
        b_less = False
        for k in keys:
            av = a.clock.get(k, 0)
            bv = b.clock.get(k, 0)
            if av < bv:
                a_less = True
            elif av > bv:
                b_less = True
        if a_less and not b_less:
            return -1
        if b_less and not a_less:
            return 1
        if not a_less and not b_less:
            return 0
        return 2  # concurrent

    def __repr__(self) -> str:
        return f"VC({self.clock})"

