# run_cluster.py
import subprocess
import time
import argparse
import json
import os

def launch_cluster(
    n_nodes,
    base_port=60000,
    stagger=0.15,
    replication_factor=3,
    read_quorum_r=2,
    write_quorum_w=2,
    output_dir=None,
):
    ports = [base_port + i for i in range(n_nodes)]
    all_nodes = [f"127.0.0.1:{p}" for p in ports]
    all_nodes_str = ",".join(all_nodes)

    procs = {}
    processes = []

    for i, port in enumerate(ports):
        node_id = f"node{i}"
        cmd = [
            "python", "node/node.py",
            "--node_id", node_id,
            "--port", str(port),
            "--all_nodes", all_nodes_str,
            "--replication_factor", str(replication_factor),
            "--read_quorum_r", str(read_quorum_r),
            "--write_quorum_w", str(write_quorum_w),
        ]
        # Optional: pass debug flags or other overrides
        print(f"[launcher] starting {node_id} on {port}")
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        procs[node_id] = {"pid": proc.pid, "port": port}
        time.sleep(stagger)

    # write mapping file
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        cluster_procs_path = os.path.join(output_dir, "cluster_procs.json")
    else:
        cluster_procs_path = "cluster_procs.json"
    
    with open(cluster_procs_path, "w") as f:
        json.dump(procs, f, indent=2)

    print(f"[launcher] cluster started with {n_nodes} nodes; procs written to {cluster_procs_path}")
    return processes, cluster_procs_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--base_port", type=int, default=60000)
    parser.add_argument("--replication_factor", type=int, default=3)
    parser.add_argument("--read_quorum_r", type=int, default=2)
    parser.add_argument("--write_quorum_w", type=int, default=2)
    parser.add_argument("--output_dir", type=str, default=None)
    args = parser.parse_args()

    procs, cluster_procs_path = launch_cluster(
        args.nodes,
        base_port=args.base_port,
        replication_factor=args.replication_factor,
        read_quorum_r=args.read_quorum_r,
        write_quorum_w=args.write_quorum_w,
        output_dir=args.output_dir,
    )
    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        print("[launcher] terminating cluster processes")
        for p in procs:
            p.terminate()
