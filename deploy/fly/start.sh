#!/bin/sh
# Fly.io entrypoint for Loveliness.
# Dynamic config dependent on FLY_PRIVATE_IP and FLY_MACHINE_ID goes here.
# Static config (shard count, bind addresses, discovery mode) is in fly.toml [env].

echo "Starting Loveliness on Fly.io..."

# Debug: print environment variables
echo "FLY_MACHINE_ID: ${FLY_MACHINE_ID}"
echo "FLY_PRIVATE_IP: ${FLY_PRIVATE_IP}"
echo "FLY_ALLOC_ID: ${FLY_ALLOC_ID}"

# Export dynamic configuration
# IPv6 addresses need to be wrapped in square brackets when including a port
export LOVELINESS_NODE_ID="${FLY_MACHINE_ID}"
export LOVELINESS_RAFT_ADDR="[${FLY_PRIVATE_IP}]:9000"
export LOVELINESS_GRPC_ADDR="[${FLY_PRIVATE_IP}]:9001"
export LOVELINESS_DATA_DIR="/data"
export LOVELINESS_EXPECTED_NODES="${LOVELINESS_EXPECTED_NODES:-3}"

# Debug: print Loveliness config
echo "LOVELINESS_NODE_ID: ${LOVELINESS_NODE_ID}"
echo "LOVELINESS_RAFT_ADDR: ${LOVELINESS_RAFT_ADDR}"
echo "LOVELINESS_GRPC_ADDR: ${LOVELINESS_GRPC_ADDR}"
echo "LOVELINESS_DATA_DIR: ${LOVELINESS_DATA_DIR}"
echo "LOVELINESS_EXPECTED_NODES: ${LOVELINESS_EXPECTED_NODES}"

# Ensure data directory exists and is writable
mkdir -p "${LOVELINESS_DATA_DIR}"
chmod 755 "${LOVELINESS_DATA_DIR}"

echo "Starting loveliness serve..."
exec /usr/local/bin/loveliness serve
