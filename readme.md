# 📘 **Dynamo Experiments — README**

This document explains how to run the full evaluation pipeline:

* start a multi-node cluster
* run workloads with configurable distribution & concurrency
* inject failures (crash, slow node, partition)
* gather workload results
* optionally run post-repair convergence rounds

The goal is to reproduce the evaluation matrix for your Dynamo-style system.

---

# 🚀 1. **Cluster Setup**

### **Start your cluster**

Make sure each node is launched with:

```bash
python node.py --port <PORT> --id <NODE_ID>
```

You can automate this with a small startup script or a Procfile.

```bash
python run_cluster --nodes 100
```

Each node will write its metadata to:

```
cluster_procs.json
```

Format:

```json
{
  "node1": {"pid": 12345, "port": 60000},
  "node2": {"pid": 12346, "port": 60001},
  "node3": {"pid": 12347, "port": 60002},
  "node4": {"pid": 12348, "port": 60003}
}
```

This is consumed by the experiment driver.

---

# 🎛️ 2. **Experiment Parameters**

You invoke all experiments using:

```bash
python experiments/experiment_driver.py [flags...]
```

Below are all supported flags.

---

## **🔑 Core Parameters**

| Flag              | Description                                | Default              |
| ----------------- | ------------------------------------------ | -------------------- |
| `--cluster_procs` | Path to JSON file describing running nodes | `cluster_procs.json` |
| `--workload`      | Workload type (`A` or `B`)                 | `A`                  |
| `--duration`      | Duration of workload (seconds)             | `60`                 |
| `--concurrency`   | Number of parallel clients                 | `10`                 |
| `--dist`          | Key distribution (`uniform` or `zipf`)     | `uniform`            |
| `--keyspace`      | Number of keys used in workload            | `1000`               |
| `--N`             | Replication factor                         | `3`                  |

---

## **🧪 Failure Injection Parameters**

| Failure Mode        | Meaning                                                        |
| ------------------- | -------------------------------------------------------------- |
| `none`              | No failure; control run                                        |
| `crash`             | Kill a random node during workload                             |
| `slow`              | Inject artificial latency into one random node                 |
| `single_partition`  | Simulate a single-node network partition by forcing huge delay |

### Flags

| Flag                 | Description                                    | Default |
| -------------------- | ---------------------------------------------- | ------- |
| `--failure_mode`     | `none`, `crash`, `slow`, `single_partition`    | `none`  |
| `--warmup`           | Time before injecting failure                  | `10`    |
| `--failure_duration` | How long failure lasts                         | `20`    |
| `--slow_ms`          | Delay for slow-node failure                    | `200`   |
| `--partition_ms`     | Delay for partition mode (≥ 60000 recommended) | `60000` |

---

<!-- ## **🔧 Post-Repair Evaluation**

After the workload & failures, you can run anti-entropy repair rounds.

| Flag                  | Description                       | Default |
| --------------------- | --------------------------------- | ------- |
| `--post_repair`       | Run repair rounds                 | *off*   |
| `--max_repair_rounds` | Max synchronous rounds to attempt | `10`    |

This produces:

```
repair_results.json
```

mapping each tested key to the number of rounds needed to converge. -->

---

# 📊 3. **Running Experiments**

## **A. Control Run (No Failures)**

```bash
python experiments/experiment_driver.py --failure_mode none --workload A
```

Produces:

* `workload_<timestamp>.csv`

---

## **B. Crash a Node**

```bash
python experiments/experiment_driver.py \
    --failure_mode crash \
    --workload A \
    --warmup 10 \
    --failure_duration 20
```

Driver will:

1. Run workload
2. Kill one random node
3. Let workload continue under failure
4. Finish

---

## **C. Slow Node**

```bash
python experiments/experiment_driver.py \
    --failure_mode slow \
    --slow_ms 500 \
    --failure_duration 15
```

This simulates a soft failure under load.

---

## **D. Single-Node Partition**

Uses large artificial delay to simulate the node being unreachable:

```bash
python experiments/experiment_driver.py \
    --failure_mode partition \
    --partition_ms 60000 \
    --failure_duration 20
```

During this time the node cannot participate in quorum.

---

## **E. Run With Post-Repair**

Use this when you want to test convergence of replicas after healing:

```bash
python experiments/experiment_driver.py \
    --failure_mode slow \
    --post_repair \
    --max_repair_rounds 10
```

Produces:

```
repair_results.json
```

---

# 📈 4. **Output Files**

### **Workload Results**

The workload writes a CSV:

```
workload_<timestamp>.csv
```

This includes:

* latencies
* timestamps
* which node served the request
* whether read/writes succeeded
* optional staleness metrics (if instrumented)

### **Repair Results**

If post-repair is enabled:

```
repair_results.json
```

Maps:

```
key → number of anti-entropy rounds needed to converge
```

---

# 🧪 5. **Evaluation Matrix**

Typical matrix you may want to run:

| Workload | Distribution | Failure Mode | Concurrency | Notes                |
| -------- | ------------ | ------------ | ----------- | -------------------- |
| A        | uniform      | none         | 1–256       | baseline             |
| A        | zipf         | slow         | 1–64        | hotspot behavior     |
| A        | uniform      | partition    | 16–128      | divergence & repair  |
| B        | uniform      | crash        | 1–64        | write conflict rates |
| B        | zipf         | partition    | 32          | vector clock growth  |

You run each row by adjusting flags accordingly.

