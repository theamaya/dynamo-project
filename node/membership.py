# node/membership.py

import asyncio
import aiohttp
import random
import time
import logging
from typing import Dict, Set, List

HEARTBEAT_INTERVAL = 1.0
FAIL_THRESHOLD = 3
HTTP_TIMEOUT = 0.4

GOSSIP_INTERVAL = 1.5
MEMBERSHIP_TTL = 10.0

# ----------------------------------------
# Logger for this module
# ----------------------------------------
membership_logger = logging.getLogger("membership")


class MembershipService:
    """
    Handles:
      ✔ Heartbeat failure detection
      ✔ Gossip-based membership convergence
    """

    def __init__(self, self_id: str, peers: List[str]):
        self.self_id = self_id
        self.peers = [p for p in peers if p != self_id]

        now = time.time()
        self.membership: Dict[str, dict] = {
            self_id: {
                "status": "alive",
                "incarnation": 1,
                "timestamp": now,
            }
        }

        # initialize peers as "alive"
        for p in self.peers:
            self.membership[p] = {
                "status": "alive",
                "incarnation": 1,
                "timestamp": now,
            }

        self.fail_counts = {p: 0 for p in self.peers}
        self._running = False

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------

    def alive_nodes(self) -> Set[str]:
        return {
            node for node, data in self.membership.items()
            if data["status"] == "alive"
        }

    def get_membership(self):
        return self.membership

    # ---------------------------------------------------------
    # Failure Detector
    # ---------------------------------------------------------

    async def _probe_once(self, session, peer: str):
        url = f"http://{peer}/ping"

        try:
            async with session.get(url, timeout=HTTP_TIMEOUT) as resp:
                if resp.status == 200:
                    # reset failures
                    if self.fail_counts[peer] > 0:
                        membership_logger.debug(f"{peer} responded again (reset fail count).")
                    self.fail_counts[peer] = 0
                    self._mark_alive(peer)
                    return
        except Exception:
            pass

        # failure
        self.fail_counts[peer] += 1
        membership_logger.debug(f"{peer} ping failed (count={self.fail_counts[peer]}).")

        if self.fail_counts[peer] >= FAIL_THRESHOLD:
            if self.membership[peer]["status"] != "dead":
                membership_logger.debug(f"{peer} marked DEAD after {FAIL_THRESHOLD} failures.")
            self._mark_dead(peer)

    def _mark_dead(self, node: str):
        entry = self.membership[node]
        entry["status"] = "dead"
        entry["timestamp"] = time.time()

    def _mark_alive(self, node: str):
        entry = self.membership[node]
        # log only if transitioning from dead → alive
        if entry["status"] == "dead":
            membership_logger.debug(f"{node} recovered → marked ALIVE again.")
        entry["status"] = "alive"
        entry["timestamp"] = time.time()

    # ---------------------------------------------------------
    # Gossip
    # ---------------------------------------------------------

    async def _gossip_once(self, session):
        peers = list(self.alive_nodes() - {self.self_id})
        if not peers:
            return

        peer = random.choice(peers)
        url = f"http://{peer}/gossip"

        try:
            async with session.post(url, json=self.membership, timeout=HTTP_TIMEOUT) as resp:
                if resp.status == 200:
                    remote_view = await resp.json()
                    membership_logger.debug(f"Gossip with {peer} succeeded.")
                    self._merge(remote_view)
        except Exception:
            membership_logger.debug(f"Gossip to {peer} failed.")

    # ---------------------------------------------------------
    # Merge membership tables
    # ---------------------------------------------------------

    def _merge(self, remote_table: Dict[str, dict]):
        for node, remote_data in remote_table.items():

            if node not in self.membership:
                membership_logger.debug(f"New node {node} discovered through gossip.")
                self.membership[node] = remote_data
                continue

            local_data = self.membership[node]

            # incarnation takes priority
            if remote_data["incarnation"] > local_data["incarnation"]:
                membership_logger.debug(f"Update {node}: higher incarnation received.")
                self.membership[node] = remote_data

            elif remote_data["incarnation"] == local_data["incarnation"]:
                # break ties using timestamp
                if remote_data["timestamp"] > local_data["timestamp"]:
                    membership_logger.debug(f"Update {node}: newer timestamp received.")
                    self.membership[node] = remote_data

    # ---------------------------------------------------------
    # Background tasks
    # ---------------------------------------------------------

    async def run(self):
        """Main loop for pings + gossip."""
        self._running = True

        async with aiohttp.ClientSession() as session:
            while self._running:

                # 1. Ping peers
                await asyncio.gather(
                    *(self._probe_once(session, peer) for peer in self.peers)
                )

                # 2. Gossip
                await self._gossip_once(session)

                await asyncio.sleep(HEARTBEAT_INTERVAL)

    def stop(self):
        self._running = False
