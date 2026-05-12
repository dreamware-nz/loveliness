# Configuration

Most configuration is via environment variables. A few options also expose CLI flags on the `serve` subcommand; flags take precedence over env vars.

## CLI flags

```
loveliness serve [flags]

  --max-memory-per-shard MB   Per-shard LadybugDB buffer pool cap in MB.
                              0 = use LOVELINESS_SHARD_BUFFER_MB env or
                              auto-derived default.
```

Precedence for `max-memory-per-shard`: **flag > env > auto** (`host_ram × 0.7 / shard_count`).

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
| `LOVELINESS_AUTH_TOKEN` | *(empty)* | Shared API token for HTTP and Bolt auth (empty = no auth) |
| `LOVELINESS_TLS_CERT` | *(empty)* | Path to server TLS certificate |
| `LOVELINESS_TLS_KEY` | *(empty)* | Path to server TLS private key |
| `LOVELINESS_TLS_CA` | *(empty)* | Path to CA certificate (enables mTLS for inter-node traffic) |
| `LOVELINESS_TLS_MODE` | `off` | `required` (all TLS), `optional` (TLS available, plaintext accepted), `off` |
| `LOVELINESS_TLS_CLIENT_AUTH` | `require` | mTLS client auth: `require`, `request`, `none` |
| `LOVELINESS_DISCOVER` | *(empty)* | Discovery mode: `dns` to enable DNS-based peer discovery |
| `LOVELINESS_DISCOVER_ADDR` | *(empty)* | DNS name to resolve for peer discovery (e.g., `loveliness.internal`) |
| `LOVELINESS_DISCOVER_INTERVAL` | `5` | Seconds between DNS discovery attempts |
| `LOVELINESS_EXPECTED_NODES` | `0` | Expected node count for quorum-gated auto-bootstrap (0 = no expectation) |
| `LOVELINESS_SHARD_BUFFER_MB` | *(auto)* | Per-shard LadybugDB buffer pool cap in MB. Default: `(host_ram × 0.7) / shard_count`. Set explicitly to override. The CLI flag `--max-memory-per-shard` overrides this. |
| `LOVELINESS_ALLOW_ALL_SHORTEST_UNSAFE` | `false` | Opt in to forwarding `ALL SHORTEST` path queries despite the known LadybugDB segfault (see [Unsafe queries](#unsafe-queries)). |
| `LOVELINESS_REPLICATION_FACTOR` | `1` | Desired replication factor for shard placement at cluster bootstrap (1 = primary only, 2 = primary + 1 replica, etc.). Clamped to the number of nodes; shards beyond that count are reported as under-replicated rather than refused. |

## Unsafe queries

The router rejects `ALL SHORTEST` variable-length path queries by default with an `UNSAFE_QUERY` error:

```
ALL SHORTEST variable-length path queries are disabled — they segfault the LadybugDB native layer (see github.com/johnjansen/loveliness#1). Use SHORTEST (single shortest path) instead.
```

This is a defensive gate: the segfault originates in LadybugDB's CGo/C++ shortest-path implementation and bypasses Go's `recover()`, so a single query can kill the entire node. Until worker-process isolation lands, the only safe response is to refuse the query.

Workarounds:

- **Use `SHORTEST` instead** — single shortest path works reliably on the same data.
- **Set `LOVELINESS_ALLOW_ALL_SHORTEST_UNSAFE=true`** to accept the risk on a per-node basis. The node logs a warning at startup and forwards the query as-is. Do this only when you have an external supervisor that can restart the process.

The detector strips Cypher comments and string literals before matching, so a query like `MATCH (n {note: 'use ALL SHORTEST instead'}) RETURN n` is not rejected.

## Memory sizing

LadybugDB uses mmap for storage; during bulk `COPY FROM` the resident write footprint can spike well above the steady-state read footprint. By default each LadybugDB handle would claim 80% of host RAM as its buffer pool — with N shards on one host, that's N×80%, a guaranteed OOM.

Loveliness caps each shard's buffer pool to keep total usage safe:

- **Auto default**: `host_ram × 0.7 / shard_count`. Host RAM is read from `/proc/meminfo` on Linux and `sysctl hw.memsize` on macOS. On other platforms a 2 GiB assumption is used and a warning is logged — set the flag or env var explicitly there.
- **Floor**: 256 MiB per shard. If the resolved value is below that, it is clamped and a warning is logged. Below this floor, warm reads will thrash the page cache.
- **Recommended floor for production**: 1 GiB per shard. Below 1 GiB, expect frequent eviction under any non-trivial working set.
- **Recommended ceiling**: leave at least 15% of host RAM unallocated for the OS, Raft Bolt store, and Go runtime overhead. The 0.7 factor in the default already accounts for this.

### Sample sizings

| Host RAM | Shards | Auto cap per shard | Notes |
|---|---|---|---|
| 8 GiB | 3 | ~1.87 GiB | Comfortable for development. |
| 16 GiB | 4 | ~2.8 GiB | Good for small production workloads. |
| 48 GiB | 6 | ~5.6 GiB | This is the host that originally OOMed at 23M nodes — the cap fixes it. |
| 128 GiB | 8 | ~11.2 GiB | Large production node. |

### Startup logging

The resolved cap is logged at startup with the source it came from:

```
buffer pool configured per_shard_mb=5734 shards=6 source=auto
buffer pool configured per_shard_mb=4096 shards=6 source=flag
buffer pool configured per_shard_mb=2048 shards=6 source=env
```

## Authentication

Set `LOVELINESS_AUTH_TOKEN` to enable token authentication across HTTP and Bolt.

**HTTP API:** all endpoints except `/health` require `Authorization: Bearer <token>`.

```bash
# Authenticated request
LOVELINESS_AUTH_TOKEN=my-secret ./loveliness &

# Works
curl -s -H "Authorization: Bearer my-secret" localhost:8080/cypher -d "MATCH (n) RETURN n"

# 401 Unauthorized
curl -s localhost:8080/cypher -d "MATCH (n) RETURN n"

# Health is always public (for load balancer probes)
curl -s localhost:8080/health
```

**Bolt protocol:** pass the token as the `credentials` field in the driver auth:

```python
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "my-secret"))
```

The username is ignored — only the password (credentials) is checked against the token.

**Disabled by default:** when `LOVELINESS_AUTH_TOKEN` is empty, all endpoints are open (dev mode).

### Secure Cluster Join

When auth is enabled, joining a cluster requires a single-use, time-limited join token:

```bash
# 1. On the leader: generate a join token (valid 10 minutes, single-use)
TOKEN=$(curl -s -H "Authorization: Bearer $AUTH_TOKEN" -X POST leader:8080/join-token | jq -r .token)

# 2. On the new node: join using the token
curl -s -H "Authorization: Bearer $AUTH_TOKEN" -X POST leader:8080/join -d '{
  "node_id": "node-4",
  "raft_addr": "node4:9000",
  "grpc_addr": "node4:9001",
  "http_addr": "node4:8080",
  "bolt_addr": "node4:7687",
  "join_token": "'"$TOKEN"'"
}'
```

- Tokens are **single-use** — consumed on successful join
- Tokens are **time-limited** — expire after 10 minutes
- All join attempts (success, rejection) are **audit logged** with node ID and source IP
- Without auth enabled, join tokens are not required (dev mode)

## TLS

Set `LOVELINESS_TLS_CERT` + `LOVELINESS_TLS_KEY` and `LOVELINESS_TLS_MODE=required` to enable TLS on all listeners (HTTP, Bolt, inter-node TCP).

**Client-facing (HTTP + Bolt):** standard TLS — server proves identity, clients verify.

**Inter-node (TCP transport):** mTLS when `LOVELINESS_TLS_CA` is set. Both sides present certs signed by the cluster CA. Connections from unknown certs are rejected at the TLS handshake — this is exercised end-to-end by `TestMTLS_RejectsForeignCA` in `pkg/transport/mtls_test.go` so a regression that silently accepted foreign-CA certs would fail CI.

See [issue #2](https://github.com/dreamware-nz/loveliness/issues/2) for the full trust boundary design.

### Generating certs for dev

The cluster CA signs every node's leaf cert. A minimal dev setup uses one self-signed CA and one leaf per node. Helpers using `openssl`:

```
# 1. Cluster CA (one per cluster).
openssl ecparam -genkey -name prime256v1 -noout -out ca.key
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
    -subj "/CN=loveliness-cluster-ca"

# 2. Per-node leaf (one per node; repeat for each node).
openssl ecparam -genkey -name prime256v1 -noout -out node-1.key
openssl req -new -key node-1.key -out node-1.csr \
    -subj "/CN=node-1" \
    -addext "subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:node-1.cluster.local"
openssl x509 -req -in node-1.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out node-1.crt -days 365 \
    -extfile <(printf "subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:node-1.cluster.local")
```

Boot the node with:

```
LOVELINESS_TLS_CERT=./node-1.crt
LOVELINESS_TLS_KEY=./node-1.key
LOVELINESS_TLS_CA=./ca.crt
LOVELINESS_TLS_MODE=required
```

### Rotating a node cert

Cert rotation in the current release **requires a restart of the affected node** — `tls.Config` is captured on startup and there is no SIGHUP-style reload hook. The rolling-restart sequence is:

1. Generate the new leaf cert + key (signed by the same cluster CA, same CN/SANs as the old one).
2. Place them in the same paths as the old cert (`LOVELINESS_TLS_CERT`, `LOVELINESS_TLS_KEY`) — atomically replace (write to a temp file and rename).
3. Drain the node (stop accepting new connections — `loveliness drain` if available, otherwise remove from the load balancer).
4. Restart the node. Existing peer connections from other nodes drop and reconnect on the next RPC (or on the next keepalive eviction, per #87).
5. Verify in `/health` that the node rejoins; smoke-check an inter-node query.

Cluster CA rotation is a longer ceremony (you need both the old and new CA in the trust pool for the cross-over window). That's out of scope for this doc; file a separate issue if you need it.

### Revoking a compromised cert

The cluster currently has no CRL/OCSP wiring. If a node's leaf is compromised:

1. Generate a new cluster CA. The old CA is now invalid for *all* nodes.
2. Issue new leaves for every node from the new CA.
3. Roll-restart every node onto the new CA per the rotation sequence above.

A future enhancement may add CRL distribution; see `pkg/tlsutil` for where it would hook in. Production callers who need fast revocation today are best served by short-lived leaves (e.g. 24h validity) and a re-issue cadence.

## Choosing Shard Count

Shards are fixed at cluster creation — you can't reshard later without rebuilding. Overprovision:

| Data scale | Recommended shards |
|---|---|
| < 10M nodes | 16 |
| 10M–100M | 64 |
| 100M+ | 128–256 |

Rule: your shard count is the maximum number of nodes you can ever use. 16 shards = up to 16 nodes.
