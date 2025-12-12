# node/config.py

import argparse
from typing import List


# -----------------------------------------------------
# Parse CLI arguments (instead of Docker env variables)
# -----------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--node_id", type=str, required=True,
                        help="Unique node identifier (e.g., node3)")
    parser.add_argument("--port", type=int, required=True,
                        help="Port this node listens on")
    parser.add_argument("--all_nodes", type=str, required=True,
                        help="Comma-separated list of all nodes: host:port")

    # Optional overrides
    parser.add_argument("--replication_factor", type=int, default=3)
    parser.add_argument("--read_quorum", type=int, default=2)
    parser.add_argument("--write_quorum", type=int, default=2)
    parser.add_argument("--vnodes_per_node", type=int, default=20)

    parser.add_argument("--heartbeat_interval", type=float, default=2.0)
    parser.add_argument("--ping_timeout", type=float, default=2.0)

    parser.add_argument("--replication_timeout", type=float, default=0.7)
    parser.add_argument("--read_timeout", type=float, default=1.0)

    parser.add_argument("--debug", action="store_true")

    return parser.parse_args()


# Load CLI args once
args = parse_args()


# -----------------------------------------------------
# Node identity
# -----------------------------------------------------

NODE_ID: str = args.node_id       # e.g., "node3"
PORT: int = args.port             # e.g., 8002

# Full cluster membership list
ALL_NODES: List[str] = args.all_nodes.split(",")


# -----------------------------------------------------
# Replication & quorum parameters
# -----------------------------------------------------

REPLICATION_FACTOR: int = args.replication_factor
READ_QUORUM_R: int = args.read_quorum
WRITE_QUORUM_W: int = args.write_quorum
VNODES_PER_NODE: int = args.vnodes_per_node


# -----------------------------------------------------
# Failure detector parameters
# -----------------------------------------------------

HEARTBEAT_INTERVAL: float = args.heartbeat_interval
PING_TIMEOUT: float = args.ping_timeout


# -----------------------------------------------------
# RPC timeouts
# -----------------------------------------------------

REPLICATION_TIMEOUT: float = args.replication_timeout
READ_TIMEOUT: float = args.read_timeout


# -----------------------------------------------------
# Debug flag
# -----------------------------------------------------

DEBUG: bool = args.debug


# -----------------------------------------------------
# Utility function
# -----------------------------------------------------

def dump_config() -> dict:
    return {
        "NODE_ID": NODE_ID,
        "PORT": PORT,
        "ALL_NODES": ALL_NODES,
        "REPLICATION_FACTOR": REPLICATION_FACTOR,
        "READ_QUORUM_R": READ_QUORUM_R,
        "WRITE_QUORUM_W": WRITE_QUORUM_W,
        "VNODES_PER_NODE": VNODES_PER_NODE,
        "HEARTBEAT_INTERVAL": HEARTBEAT_INTERVAL,
        "PING_TIMEOUT": PING_TIMEOUT,
        "REPLICATION_TIMEOUT": REPLICATION_TIMEOUT,
        "READ_TIMEOUT": READ_TIMEOUT,
        "DEBUG": DEBUG
    }
