#!/usr/bin/env bash
set -euo pipefail

# Configuration: Define the values to test for each parameter
NODES_VALUES=(30)
REPLICATION_FACTOR_VALUES=(3)
READ_QUORUM_R_VALUES=(2)
WRITE_QUORUM_W_VALUES=(2)
FAILURE_MODE_VALUES=(crash) # single_partition)
CRASH_COUNT_VALUES=(1 2 3 4 5)

# Output directory for results
RESULTS_DIR="${RESULTS_DIR:-results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUITE_DIR="$RESULTS_DIR/suite_$TIMESTAMP"
mkdir -p "$SUITE_DIR"

echo "Starting experiment suite at $(date)"
echo "Results will be saved to: $SUITE_DIR"
echo ""

# Counter for experiment number
EXP_NUM=0
TOTAL_EXPS=$((${#NODES_VALUES[@]} * ${#REPLICATION_FACTOR_VALUES[@]} * ${#READ_QUORUM_R_VALUES[@]} * ${#WRITE_QUORUM_W_VALUES[@]} * ${#FAILURE_MODE_VALUES[@]} * ${#CRASH_COUNT_VALUES[@]}))

echo "Total experiments to run: $TOTAL_EXPS"
echo ""

# Run experiments with all combinations
for NODES in "${NODES_VALUES[@]}"; do
  for REPLICATION_FACTOR in "${REPLICATION_FACTOR_VALUES[@]}"; do
    for READ_QUORUM_R in "${READ_QUORUM_R_VALUES[@]}"; do
      for WRITE_QUORUM_W in "${WRITE_QUORUM_W_VALUES[@]}"; do
        for FAILURE_MODE in "${FAILURE_MODE_VALUES[@]}"; do
          for CRASH_COUNT in "${CRASH_COUNT_VALUES[@]}"; do
            EXP_NUM=$((EXP_NUM + 1))
            
            # Create a descriptive name for this experiment
            EXP_NAME="nodes${NODES}_rf${REPLICATION_FACTOR}_r${READ_QUORUM_R}_w${WRITE_QUORUM_W}_${FAILURE_MODE}_cc${CRASH_COUNT}"
            EXP_DIR="$SUITE_DIR/$EXP_NAME"
            mkdir -p "$EXP_DIR"
            
            # Calculate unique base port for this experiment to avoid port conflicts
            # Each experiment gets 100 ports (enough for up to 100 nodes)
            PORT_RANGE=0
            BASE_PORT=$((50000 + (EXP_NUM - 1) * PORT_RANGE))
            
            echo "[$EXP_NUM/$TOTAL_EXPS] Running experiment: $EXP_NAME"
            echo "  Parameters:"
            echo "    NODES=$NODES"
            echo "    REPLICATION_FACTOR=$REPLICATION_FACTOR"
            echo "    READ_QUORUM_R=$READ_QUORUM_R"
            echo "    WRITE_QUORUM_W=$WRITE_QUORUM_W"
            echo "    FAILURE_MODE=$FAILURE_MODE"
            echo "    CRASH_COUNT=$CRASH_COUNT"
            echo "    BASE_PORT=$BASE_PORT"
            
            # Run the experiment and capture output
            if ./run_experiment.sh \
              --nodes "$NODES" \
              --replication-factor "$REPLICATION_FACTOR" \
              --read-quorum-r "$READ_QUORUM_R" \
              --write-quorum-w "$WRITE_QUORUM_W" \
              --failure-mode "$FAILURE_MODE" \
              --crash-count "$CRASH_COUNT" \
              --output-dir "$EXP_DIR" \
              --base-port "$BASE_PORT" \
              > "$EXP_DIR/stdout.log" 2> "$EXP_DIR/stderr.log"; then
              echo "  ✓ Experiment completed successfully"
            else
              echo "  ✗ Experiment failed (check logs in $EXP_DIR)"
            fi
            
            # Wait a bit between experiments to ensure cleanup completes
            # and ports are fully released
            if [[ $EXP_NUM -lt $TOTAL_EXPS ]]; then
              echo "  [waiting 3 seconds before next experiment...]"
              sleep 3
            fi
            
            echo ""
          done
        done
      done
    done
  done
done

echo "Experiment suite completed at $(date)"
echo "All results saved to: $SUITE_DIR"

