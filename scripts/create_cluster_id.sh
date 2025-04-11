#!/bin/bash
set -euo pipefail  # Strict error handling

# Configuration
CLUSTER_DIR="/tmp/clusterID"
CLUSTER_FILE="${CLUSTER_DIR}/clusterID"
MAX_RETRIES=3
RETRY_DELAY=2

# Ensure directory exists with proper permissions
mkdir -p "$CLUSTER_DIR"
chmod 777 "$CLUSTER_DIR"

# Function to generate cluster ID with retries
generate_cluster_id() {
  local attempt=0
  while [ $attempt -lt $MAX_RETRIES ]; do
    if /bin/kafka-storage random-uuid > "$CLUSTER_FILE"; then
      chmod 666 "$CLUSTER_FILE"
      echo "Cluster ID successfully generated"
      return 0
    fi
    
    attempt=$((attempt + 1))
    echo "Attempt $attempt failed, retrying in ${RETRY_DELAY}s..." >&2
    sleep $RETRY_DELAY
  done
  
  echo "Failed to generate cluster ID after $MAX_RETRIES attempts" >&2
  return 1
}

# Main execution
if [ -f "$CLUSTER_FILE" ]; then
  echo "Using existing cluster ID: $(cat "$CLUSTER_FILE")"
  exit 0
else
  echo "Generating new cluster ID..."
  if generate_cluster_id; then
    echo "Cluster ID created: $(cat "$CLUSTER_FILE")"
    exit 0
  else
    exit 1
  fi
fi