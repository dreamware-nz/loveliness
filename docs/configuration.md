# Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|---|---|---|
| `LOVELINESS_NODE_ID` | `node-1` | Unique node identifier |
| `LOVELINESS_BIND_ADDR` | `:8080` | HTTP API listen address |
| `LOVELINESS_RAFT_ADDR` | `:9000` | Raft consensus address |
| `LOVELINESS_GRPC_ADDR` | `:9001` | TCP transport address (msgpack) |
| `LOVELINESS_BOLT_ADDR` | `:7687` | Neo4j Bolt protocol listen address (empty to disable) |
| `LOVELINESS_DATA_DIR` | `./data` | Base directory for shard data and Raft state |
| `LOVELINESS_SHARD_COUNT` | `3` | Total number of shards |
| `LOVELINESS_BOOTSTRAP` | `false` | Bootstrap a new cluster (first node only) |
| `LOVELINESS_PEERS` | *(empty)* | Comma-separated list of peer Raft addresses |
| `LOVELINESS_MAX_CONCURRENT_QUERIES` | `16` | Max concurrent CGo calls per shard |
| `LOVELINESS_QUERY_TIMEOUT_MS` | `30000` | Per-shard query timeout in milliseconds |
| `LOVELINESS_S3_BUCKET` | *(empty)* | S3 bucket for backup storage |
| `LOVELINESS_S3_REGION` | *(empty)* | AWS region for S3 |
| `LOVELINESS_S3_PREFIX` | *(empty)* | Key prefix within the S3 bucket |
| `LOVELINESS_S3_ENDPOINT` | *(empty)* | Custom S3 endpoint (MinIO, R2, etc.) |
| `LOVELINESS_BACKUP_INTERVAL_MIN` | `0` | Minutes between scheduled backups (0 = disabled) |
| `LOVELINESS_BACKUP_RETENTION` | `3` | Number of backups to retain |
| `LOVELINESS_BACKUP_DIR` | *(empty)* | Local directory for backups (when S3 is not configured) |
| `LOVELINESS_TLS_CERT` | *(empty)* | Path to server TLS certificate |
| `LOVELINESS_TLS_KEY` | *(empty)* | Path to server TLS private key |
| `LOVELINESS_TLS_CA` | *(empty)* | Path to CA certificate (enables mTLS for inter-node traffic) |
| `LOVELINESS_TLS_MODE` | `off` | `required` (all TLS), `optional` (TLS available, plaintext accepted), `off` |
| `LOVELINESS_TLS_CLIENT_AUTH` | `require` | mTLS client auth: `require`, `request`, `none` |

## TLS

Set `LOVELINESS_TLS_CERT` + `LOVELINESS_TLS_KEY` and `LOVELINESS_TLS_MODE=required` to enable TLS on all listeners (HTTP, Bolt, inter-node TCP).

**Client-facing (HTTP + Bolt):** standard TLS — server proves identity, clients verify.

**Inter-node (TCP transport):** mTLS when `LOVELINESS_TLS_CA` is set. Both sides present certs signed by the cluster CA. Connections from unknown certs are rejected.

See [issue #2](https://github.com/dreamware-nz/loveliness/issues/2) for the full trust boundary design.

## Choosing Shard Count

Shards are fixed at cluster creation — you can't reshard later without rebuilding. Overprovision:

| Data scale | Recommended shards |
|---|---|
| < 10M nodes | 16 |
| 10M–100M | 64 |
| 100M+ | 128–256 |

Rule: your shard count is the maximum number of nodes you can ever use. 16 shards = up to 16 nodes.
