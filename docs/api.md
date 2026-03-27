# HTTP API Reference

All endpoints are on the HTTP port (default `:8080`).

## Query Endpoint

`POST /cypher` — send raw Cypher as the request body, get JSON results back.

## Schema

LadybugDB requires schema before inserting data. The `PRIMARY KEY` becomes the **shard key**.

```bash
# Create a node table — sharded on 'name'
curl -s localhost:8080/cypher -d "CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))"

# Create a relationship table
curl -s localhost:8080/cypher -d "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)"
```

Schema DDL is automatically broadcast to all shards.

## Queries

**Point lookup (single shard via Bloom filter):**
```bash
curl -s localhost:8080/cypher -d "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age, p.city"
```

**Scan (scatter-gather):**
```bash
curl -s localhost:8080/cypher -d "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age LIMIT 10"
```

**Traversal:**
```bash
curl -s localhost:8080/cypher -d "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name"
```

**Response format:**
```json
{
  "columns": ["p.name", "p.age", "p.city"],
  "rows": [{"p.name": "Alice", "p.age": 30, "p.city": "Auckland"}],
  "stats": {"compile_time_ms": 0.12, "exec_time_ms": 0.45}
}
```

## Writes

```bash
# Create a node
curl -s localhost:8080/cypher -d "CREATE (p:Person {name: 'Alice', age: 30, city: 'Auckland'})"

# Create an edge (routed to source node's shard)
curl -s localhost:8080/cypher -d "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2024}]->(b)"
```

## Bulk Loading

**Synchronous bulk load:**
```bash
# Load nodes from CSV
curl -s localhost:8080/bulk/nodes \
  -H "Content-Type: text/csv" \
  -H "X-Table: Person" \
  --data-binary @persons.csv

# Two-pass edge loading for cross-shard performance
# Pass 1: Create reference nodes
curl -s localhost:8080/bulk/edges \
  -H "X-Rel-Table: KNOWS" -H "X-From-Table: Person" -H "X-To-Table: Person" \
  -H "X-Refs-Only: true" \
  --data-binary @edges.csv

# Pass 2: Load edges (refs already exist)
curl -s localhost:8080/bulk/edges \
  -H "X-Rel-Table: KNOWS" -H "X-From-Table: Person" -H "X-To-Table: Person" \
  -H "X-Skip-Refs: true" \
  --data-binary @edges.csv
```

**Async ingest queue:**
```bash
# Submit — returns immediately with job ID
curl -s -X POST localhost:8080/ingest/nodes \
  -H "X-Table: Person" \
  --data-binary @persons.csv
# → {"job_id": "20260327-120000-abc123", "status": "pending"}

# Poll status
curl -s localhost:8080/ingest/jobs/20260327-120000-abc123
# → {"status": "completed", "loaded": 5000000, ...}

# List all jobs
curl -s localhost:8080/ingest/jobs
```

The ingest queue spools the CSV to disk, returns 202 Accepted, and processes jobs sequentially in the background. Jobs survive server restarts.

## Disaster Recovery

**Backup and restore:**
```bash
curl -s localhost:8080/backup -o backup.tar.gz
curl -s -X POST localhost:8080/restore --data-binary @backup.tar.gz
curl -s localhost:8080/wal/status
```

**CSV export:**
```bash
curl -s localhost:8080/export/Person
curl -s localhost:8080/export/Person/edges/KNOWS
```

S3 scheduled backups are configured via environment variables (see [Configuration](configuration.md)).

## Write Consistency

| Level | Behavior | Use case |
|---|---|---|
| `ONE` | Ack after primary write, async replicate | Fast writes, acceptable loss window |
| `QUORUM` | Ack after primary + 1 replica (default) | Safe default for most workloads |
| `ALL` | Ack after all replicas confirm | Maximum durability |

## Cluster Management

```bash
# Health check (always public, no auth required)
curl -s localhost:8080/health | jq

# Cluster status
curl -s localhost:8080/cluster | jq

# Generate a join token (leader only, single-use, 10 min TTL)
curl -s -X POST localhost:8080/join-token
# → {"token": "a1b2c3...", "expires_at": "2026-03-27T12:10:00Z"}

# Add a node with join token
curl -s localhost:8080/join -d '{
  "node_id": "node-4",
  "raft_addr": "node4:9000",
  "grpc_addr": "node4:9001",
  "http_addr": "node4:8080",
  "join_token": "a1b2c3..."
}'
```
