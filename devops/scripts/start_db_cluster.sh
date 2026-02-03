#!/bin/bash

set -euo pipefail

# Check if cluster ID is provided
if [ -z "${1:-}" ]; then
    echo "Error: Cluster ID parameter is required"
    echo "Usage: $0 <cluster-id> [databricks-target]"
    exit 1
fi

CLUSTER_ID="$1"
DATABRICKS_TARGET="${2:-DEV}" # default to DEV if not provided
MAX_WAIT_TIME=600
POLL_INTERVAL=5

echo "Databricks target: $DATABRICKS_TARGET"
echo "Starting cluster: $CLUSTER_ID"
# Verify Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found"
    exit 1
fi

# Verify Databricks credentials are available (either via env vars or config file)
if [ ! -f ~/.databrickscfg ]; then
    # No config file, check environment variables
    if [ -z "${DATABRICKS_HOST:-}" ] || [ -z "${DATABRICKS_CLIENT_ID:-}" ] || [ -z "${DATABRICKS_CLIENT_SECRET:-}" ]; then
        echo "Error: Databricks credentials not set"
        exit 1
    fi
    echo "Using Databricks credentials from environment variables"
else
    echo "Using Databricks credentials from ~/.databrickscfg"
fi

# Check if cluster exists
echo "Checking if cluster $CLUSTER_ID exists..."
if ! databricks clusters get "$CLUSTER_ID" --target "$DATABRICKS_TARGET" &> /dev/null; then
    echo "Error: Cluster $CLUSTER_ID does not exist"
    echo "Available clusters:"
    databricks clusters list --target "$DATABRICKS_TARGET" --output json || true
    exit 1
fi

# Get current cluster status
CLUSTER_STATE=$(databricks clusters get "$CLUSTER_ID" --target "$DATABRICKS_TARGET" | jq -r '.state')
echo "Current cluster state: $CLUSTER_STATE"

# Start cluster if not running
if [ "$CLUSTER_STATE" != "RUNNING" ]; then
    if [ "$CLUSTER_STATE" = "TERMINATED" ] || [ "$CLUSTER_STATE" = "TERMINATING" ]; then
        echo "Starting cluster $CLUSTER_ID..."
        databricks clusters start "$CLUSTER_ID" --target "$DATABRICKS_TARGET"
    else
        echo "Cluster is in state: $CLUSTER_STATE"
        exit 0
    fi
fi

# Wait for cluster to be available
echo "Waiting for cluster to reach RUNNING state..."
ELAPSED=0

while [ $ELAPSED -lt $MAX_WAIT_TIME ]; do
    CURRENT_STATE=$(databricks clusters get "$CLUSTER_ID" --target "$DATABRICKS_TARGET" | jq -r '.state')
    
    if [ "$CURRENT_STATE" = "RUNNING" ]; then
        echo "Success: Cluster $CLUSTER_ID is now RUNNING"
        exit 0
    elif [ "$CURRENT_STATE" = "ERROR" ]; then
        echo "Error: Cluster entered ERROR state"
        exit 1
    fi
    
    echo "Current state: $CURRENT_STATE (waited ${ELAPSED}s)"
    sleep $POLL_INTERVAL
    ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

echo "Error: Timeout waiting for cluster to be RUNNING"
exit 1