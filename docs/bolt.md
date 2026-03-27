# Neo4j Bolt Protocol Compatibility

Loveliness speaks the **Neo4j Bolt protocol** (v4.x) on port 7687. Existing Neo4j drivers work with minimal changes — just change the connection URL.

## Driver Examples

**Python:**
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

with driver.session() as session:
    session.run("CREATE NODE TABLE IF NOT EXISTS Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))")

    session.run("CREATE (p:Person {name: $name, age: $age, city: $city})",
                name="Alice", age=30, city="Auckland")

    result = session.run("MATCH (p:Person {name: $name}) RETURN p.name, p.age", name="Alice")
    for record in result:
        print(record["p.name"], record["p.age"])

    result = session.run("MATCH (a:Person {name: $name})-[:KNOWS]->(b) RETURN b.name", name="Alice")
    for record in result:
        print("Alice knows", record["b.name"])
```

**JavaScript:**
```javascript
const neo4j = require('neo4j-driver');
const driver = neo4j.driver('bolt://localhost:7687');
const session = driver.session();
const result = await session.run('MATCH (p:Person) RETURN p.name LIMIT 10');
result.records.forEach(r => console.log(r.get('p.name')));
```

**Go:**
```go
driver, _ := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.NoAuth())
session := driver.NewSession(ctx, neo4j.SessionConfig{})
result, _ := session.Run(ctx, "MATCH (p:Person {name: $name}) RETURN p", map[string]any{"name": "Alice"})
```

## What Works

- Schema DDL (`CREATE NODE TABLE`, `CREATE REL TABLE`, with `IF NOT EXISTS`)
- RUN + PULL with batched streaming (including `PULL {n: 100}` for pagination)
- Parameter binding (`$name`, `$age`, `$active`, `$score`, `NULL`) — interpolated into Cypher automatically
- All data types: integers (tiny to int64), floats, booleans, strings (including unicode and 10KB+), null
- Explicit transactions (BEGIN/COMMIT/ROLLBACK)
- RESET for connection recovery after errors
- ROUTE message for driver routing table discovery
- GOODBYE for clean disconnect
- 10+ concurrent driver connections with interleaved sessions
- Rapid-fire queries (100+ per session without reconnecting)

## Recently Added

- **Node/Relationship graph type wrapping** — results with `_label`/`_labels` keys are returned as Bolt Node structs (tag `0x4E`); results with `_src`/`_dst`/`_type` keys are returned as Bolt Relationship structs (tag `0x52`). Synthetic IDs are generated via FNV-64a hash for stable identity.
- **Bolt v5.x message support** — LOGOFF (re-authentication) and TELEMETRY (driver metrics) messages are accepted.
- **Multi-database parameter** — `db` parameter accepted in RUN extra map and BEGIN extra map (single-database: value is ignored but drivers won't error).
- **Bookmark tracking** — PULL SUCCESS returns a `bookmark` string; BEGIN and RUN accept `bookmarks` in extra map for causal consistency signaling.

## Limitations

- No real multi-database support (single database, `db` param is accepted but ignored)
- Bookmarks are connection-scoped sequence numbers, not cluster-wide WAL positions
- Path graph type not yet returned (paths come back as individual nodes/relationships)

## Configuration

- Default port: `:7687` (standard Neo4j Bolt port)
- Set `LOVELINESS_BOLT_ADDR=:7688` to change, or empty string to disable

## Test Results

Verified with 72 exhaustive tests using the official `neo4j` Python driver (v6.1.0):

| Category | Tests | Status |
|---|---|---|
| Connection & Handshake | 4 | All pass (basic, no-auth, sequential, reuse) |
| Schema DDL | 5 | All pass (node tables, rel tables, IF NOT EXISTS) |
| Node CRUD | 6 | All pass (create, read, update, delete, all types) |
| Edge CRUD | 3 | All pass (with props, cross-table, 1-hop read) |
| Parameter Binding | 7 | All pass (string, int, bool, float, null, multi, special chars) |
| Query Patterns | 11 | All pass (literals, ORDER BY, LIMIT, COUNT, SUM, MIN/MAX, DISTINCT) |
| Traversal Patterns | 6 | All pass (1-hop, 2-hop, cross-table, reverse, bidirectional) |
| Explicit Transactions | 4 | All pass (BEGIN/COMMIT, ROLLBACK, multi-statement) |
| Error Handling | 3 | All pass (invalid Cypher, recovery after error, missing table) |
| Concurrent Connections | 3 | All pass (10 sessions, 10 drivers, mixed reads+writes) |
| Result Streaming | 4 | All pass (large results, consume, iterate, column index) |
| Session Management | 3 | All pass (reopen, 20 queries/session, clean close) |
| Data Type Round-trips | 9 | All pass (ints, large int, float, bool, null, unicode, empty, 10KB string) |
| Edge Cases | 4 | All pass (write-only, alias, interleaved sessions, 100 rapid-fire) |

**72/72 tests pass.** Zero Bolt protocol failures.
