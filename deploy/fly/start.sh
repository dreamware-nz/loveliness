#!/bin/sh
# Fly.io entrypoint for Loveliness.
# Dynamic config dependent on FLY_PRIVATE_IP and FLY_MACHINE_ID goes here.
# Static config (shard count, bind addresses, discovery mode) is in fly.toml [env].

export LOVELINESS_NODE_ID="${FLY_MACHINE_ID}"
export LOVELINESS_RAFT_ADDR="[${FLY_PRIVATE_IP}]:9000"
export LOVELINESS_GRPC_ADDR="[${FLY_PRIVATE_IP}]:9001"
export LOVELINESS_DATA_DIR="/data"
export LOVELINESS_EXPECTED_NODES="${LOVELINESS_EXPECTED_NODES:-3}"

exec /usr/local/bin/loveliness serve
