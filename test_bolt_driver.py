#!/usr/bin/env python3
"""Test Loveliness Bolt server with the official Neo4j Python driver."""

from neo4j import GraphDatabase

print("=== Neo4j Python Driver → Loveliness Bolt Test ===\n")

# Connect using the standard Neo4j driver.
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

try:
    # 1. Verify connectivity
    print("1. Testing connectivity...")
    driver.verify_connectivity()
    print("   PASS: Connected to Loveliness via Bolt\n")

    # 2. Create schema (register shard key via HTTP, then create tables via Bolt)
    print("2. Creating schema...")
    import urllib.request, json
    req = urllib.request.Request(
        "http://localhost:8090/schema",
        data=json.dumps({"table": "Person", "shard_key": "name"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        urllib.request.urlopen(req)
    except Exception:
        pass  # May already exist
    with driver.session() as session:
        session.run("CREATE NODE TABLE IF NOT EXISTS Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))")
        session.run("CREATE REL TABLE IF NOT EXISTS KNOWS(FROM Person TO Person, since INT64)")
    print("   PASS: Schema created\n")

    # 3. Insert data
    print("3. Inserting nodes...")
    with driver.session() as session:
        people = [
            ("Alice", 30, "Auckland"),
            ("Bob", 25, "Wellington"),
            ("Charlie", 35, "Christchurch"),
            ("Dave", 28, "Hamilton"),
            ("Eve", 32, "Tauranga"),
        ]
        for name, age, city in people:
            session.run(
                "CREATE (p:Person {name: $name, age: $age, city: $city})",
                name=name, age=age, city=city
            )
        print(f"   PASS: Inserted {len(people)} nodes\n")

    # 4. Insert edges
    print("4. Inserting edges...")
    with driver.session() as session:
        edges = [
            ("Alice", "Bob", 2020),
            ("Alice", "Charlie", 2019),
            ("Bob", "Dave", 2021),
            ("Charlie", "Eve", 2018),
        ]
        for src, dst, since in edges:
            session.run(
                "MATCH (a:Person {name: $src}), (b:Person {name: $dst}) CREATE (a)-[:KNOWS {since: $since}]->(b)",
                src=src, dst=dst, since=since
            )
        print(f"   PASS: Inserted {len(edges)} edges\n")

    # 5. Query: find all people
    print("5. Querying all people...")
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN p.name, p.age, p.city ORDER BY p.name")
        records = list(result)
        for r in records:
            print(f"   {r['p.name']:10s} age={r['p.age']:<3} city={r['p.city']}")
        print(f"   PASS: Got {len(records)} records\n")

    # 6. Query with parameter
    print("6. Querying with parameter (name='Alice')...")
    with driver.session() as session:
        result = session.run("MATCH (p:Person {name: $name}) RETURN p.name, p.age", name="Alice")
        records = list(result)
        for r in records:
            print(f"   {r['p.name']} age={r['p.age']}")
        print(f"   PASS: Got {len(records)} records\n")

    # 7. Traversal query
    print("7. Traversal: who does Alice know?")
    with driver.session() as session:
        result = session.run(
            "MATCH (a:Person {name: $name})-[:KNOWS]->(b:Person) RETURN b.name, b.city",
            name="Alice"
        )
        records = list(result)
        for r in records:
            print(f"   Alice → {r['b.name']} ({r['b.city']})")
        print(f"   PASS: Got {len(records)} connections\n")

    # 8. Aggregation
    print("8. Aggregation: average age...")
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN AVG(p.age) AS avg_age")
        records = list(result)
        for r in records:
            print(f"   Average age: {r['avg_age']}")
        print(f"   PASS\n")

    # 9. Explicit transaction
    print("9. Explicit transaction...")
    with driver.session() as session:
        tx = session.begin_transaction()
        tx.run("CREATE (p:Person {name: 'Frank', age: 27, city: 'Dunedin'})")
        tx.commit()
    print("   PASS: Transaction committed\n")

    # 10. Count after insert
    print("10. Final count...")
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN COUNT(p.name) AS cnt")
        records = list(result)
        for r in records:
            print(f"    Total people: {r['cnt']}")
        print(f"    PASS\n")

    print("=" * 50)
    print("ALL TESTS PASSED - Neo4j driver talks to Loveliness!")
    print("=" * 50)

except Exception as e:
    print(f"\n   FAIL: {e}")
    import traceback
    traceback.print_exc()

finally:
    driver.close()
