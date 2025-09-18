#!/bin/bash

# Test script for multi-field performance testing
# This script demonstrates how to use the performance test example

echo "=== Multi-Field Performance Test ==="
echo

# Usage: ./examples/test_performance.sh [index|query|both|analyze] [max_docs]
MODE=${1:-both}
MAX_DOCS=${2:-1000000}

# Check if data file exists
DATA_FILE="/Users/jin/Desktop/big-ann-benchmarks/hdfs_log_data/hdfs-logs-multitenants.json"
if [ ! -f "$DATA_FILE" ]; then
    echo "Error: Data file not found at $DATA_FILE"
    echo "Please ensure the data file exists before running this test"
    exit 1
fi

echo "Data file found: $DATA_FILE"
echo

INDEX_DIR="./hdfs_logs_index"

if [ "$MODE" = "index" ] || [ "$MODE" = "both" ]; then
  echo "Step 1: Building index (max_docs=$MAX_DOCS)..."
  rm -rf "$INDEX_DIR"
  mkdir -p "$INDEX_DIR"
  cargo run --example multi_field_performance_test -- --mode index --data-path "$DATA_FILE" --index-path "$INDEX_DIR" --max-docs "$MAX_DOCS"
  if [ $? -eq 0 ]; then
      echo "Index built successfully!"
      echo
  else
      echo "Error: Failed to build index"
      exit 1
  fi
fi

if [ "$MODE" = "query" ] || [ "$MODE" = "both" ]; then
  echo "Step 2: Running performance queries..."
  cargo run --example multi_field_performance_test -- --mode query --index-path "$INDEX_DIR"
  if [ $? -eq 0 ]; then
      echo "Queries completed successfully!"
      echo
  else
      echo "Error: Failed to run queries"
      exit 1
  fi
fi

if [ "$MODE" = "analyze" ]; then
  echo "Analyzing dataset (first $MAX_DOCS docs) to propose query specs..."
  cargo run --example multi_field_performance_test -- --mode analyze --data-path "$DATA_FILE" --max-docs "$MAX_DOCS"
fi

echo "=== Test completed ==="
