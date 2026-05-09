# Cypher → Arrow type mapping

This document is the contract between the `/cypher` server and any
Arrow-aware client (DuckDB-WASM, pyarrow, polars, Cosmograph, MCP
clients). It pins down how every Cypher value type lands in the Arrow
schema so client code can declare exactly what it expects to read
back.

The mapping is the same for both IPC variants — `application/vnd.apache.arrow.stream`
and `application/vnd.apache.arrow.file`. They differ only in the
container, not in the schema.

> Status — the **current encoder** implements scalar types, NODE,
> RELATIONSHIP, LIST<T>, MAP<utf8, V>, and the JSON-fallback row in
> the table below. PATH is part of issue [#27](https://github.com/dreamware-nz/loveliness/issues/27)
> and ships as a separate slice; until it lands, path values arrive
> as JSON-encoded `utf8` so clients still receive a parseable result.
> The "Status" column flags which rows are live today vs. landing
> later — the schema contract itself does not change when those
> slices ship; clients can rely on the documented Arrow type from
> the moment they target a release that lists the row as live.

## Scalar types

| Cypher value | Arrow type | Status |
|---|---|---|
| `BOOLEAN` | `bool` | live |
| `INTEGER` (any width) | `int64` | live |
| `FLOAT` | `float64` | live |
| `STRING` | `utf8` | live |
| `NULL` | null bitmap on the column | live |
| `BYTES` | `binary` | follow-up |
| `DATE` | `date32` (days since epoch) | follow-up |
| `LOCAL_TIME` | `time64[ns]` | follow-up |
| `LOCAL_DATETIME` | `timestamp[ns]` (no tz) | follow-up |
| `DATETIME` (zoned) | `timestamp[ns, UTC]` | follow-up |
| `DURATION` | `struct{months: int32, days: int32, nanos: int64}` | follow-up |

Numeric promotion within a single column: if a column produces a mix
of integers and floats across rows, the column is encoded as
`float64` for the entire batch. Boolean mixed with anything else
falls into the JSON fallback row below — booleans are not silently
coerced to 0/1.

## Composite types

| Cypher value | Arrow type | Status |
|---|---|---|
| `LIST<T>` | `list<T'>` where `T'` is `T`'s row above | live |
| `MAP<utf8, V>` | `map<utf8, V'>` (keys always `utf8`) | live |
| `NODE` | `struct{ id: internal_id, labels: list<utf8>, properties: utf8 (JSON) }` | live |
| `RELATIONSHIP` | `struct{ id: internal_id, start_id: internal_id, end_id: internal_id, label: utf8, properties: utf8 (JSON) }` | live |
| `PATH` | `struct{ nodes: list<NODE>, relationships: list<RELATIONSHIP>, length: int64 }` | follow-up |
| any other / heterogeneous column | `utf8` (JSON-encoded value per cell) | live |

### MAP<utf8, V> classification

A column lands as `map<utf8, V'>` when every cell is a slice of
`{Key, Value}` entries (i.e. the Cypher MAP wire shape — `[]MapItem`
in the LadybugDB binding) **and** every entry's key resolves to a Go
`string`. The value type `V'` is unified across every entry's value
under the same rules as scalar columns: `INTEGER` + `FLOAT` promotes
to `float64`, anything else heterogeneous degrades the entire column
to JSON `utf8`. A non-string key in any row also forces the JSON
fallback — Arrow `map<K, V>` requires a single concrete key type and
the wire contract pins it to `utf8`.

A column that mixes maps with non-maps (scalars or lists) across
rows uses the JSON fallback for the same reason `LIST<T>` does — Arrow
has no "sometimes a map" type without a union. Note that we do
**not** recognize Go `map[string]any` as a Cypher MAP: Cypher MAPs
are ordered key/value sequences and we won't silently reorder them.

### LIST<T> classification

The element type `T'` is resolved by walking every list value in the
column and unifying each element's kind under the same rules used for
scalar columns: `INTEGER` + `FLOAT` promotes to `float64`, anything
else heterogeneous degrades the entire column to JSON `utf8`. A
column that contains both list values and non-list values in
different rows is also expressed via the JSON fallback (Arrow has no
"sometimes a list" type without a union). A list column whose every
list is empty or null defaults to `list<utf8>` so the schema is still
concrete. Null elements inside a list ride the list's child null
mask — clients can distinguish `[1, null, 3]` from `[1, 3]`.

`internal_id` is the Arrow struct `struct{ table_id: int64, offset:
int64 }`. It is a faithful representation of the LadybugDB 128-bit
internal identifier, which would not fit in a single `int64` if a
deployment ever allocated IDs near the upper end of the range. Two
int64s round-trip every legal value losslessly today and stay in
range for ID schemes other backends might adopt.

`properties` lands as `utf8` with the property bag JSON-encoded into
the string value. This keeps the schema flat and stable across rows
that have different property sets — every node in a graph has its
own property bag, and trying to express that as a strongly-typed
struct would mean either schema-per-row (which Arrow forbids inside
a single batch) or a sparse schema with one column per property name
(which explodes on real graphs). A follow-up slice promotes
`properties` to `map<utf8, utf8>` (keys utf8, values still
JSON-encoded utf8 strings); the `loveliness.arrow_mapping_version`
metadata key bumps when that lands so clients can detect the change.

## Heterogeneous columns

When a single column produces values of multiple Cypher kinds across
rows that are not numerically compatible (`INTEGER`+`FLOAT` is
compatible, everything else is not), the encoder falls back to
`utf8` and writes each cell as a JSON-encoded string. Schema
metadata records the decision so clients can warn users that a
column degraded:

```
loveliness.column.<name>.fallback = "json"
```

This rule is conservative — Arrow `union` types would express the
mix more precisely but every downstream consumer (DuckDB-WASM,
Cosmograph, polars on its current release) parses unions
inconsistently or not at all. The fallback is also what JSON
already does (a JSON column can hold mixed types), so the contract
across the JSON and Arrow paths stays equivalent.

## Schema metadata

Every Arrow payload carries schema-level metadata that mirrors the
JSON envelope's top-level fields:

| Key | Value | Meaning |
|---|---|---|
| `loveliness.partial` | `"true"` / `"false"` | At least one shard returned an error; rows present are partial |
| `loveliness.errors` | JSON array | Per-shard errors when `partial=true` |
| `loveliness.column.<name>.fallback` | `"json"` | Column degraded to JSON-encoded `utf8` |

Clients should **always** check `loveliness.partial` before treating
the result as authoritative. The wire format does not surface
partial-ness any other way (HTTP status stays 200 because the user
got rows back).

## Mid-stream errors

The Arrow stream format flushes the schema message before the first
record batch, so there is no way to return a 4xx/5xx HTTP status
once a batch has been written. The Loveliness encoder surfaces
errors as schema-level metadata so a single read of the schema
metadata tells a client whether the result is authoritative:

| Key | Value |
|---|---|
| `loveliness.error` | `"true"` |
| `loveliness.error.code` | `SHARD_ERROR` (one shard failed) or `SHARD_ERRORS` (multiple) |
| `loveliness.error.message` | human-readable detail; for multi-shard cases prefixed with the failed-shard count |

Clients **must** check for `loveliness.error == "true"` and surface
the error to the user — silent truncation would otherwise look like
an empty result. The structured per-shard list still rides along on
`loveliness.errors` (JSON array) when clients want machine-readable
detail; the flat keys above exist so a client can do one metadata
lookup. The same keys are emitted on both the stream and file
formats so client code can share the check.

Today the encoder is fully buffered, so all errors are observed
before any byte is written and the metadata lands on the initial
schema message. A future truly-incremental encoder that flushes
record batches as shards reply will need a sentinel terminal-batch
mechanism (likely a zero-row batch with column-level metadata) to
signal errors discovered after the schema is on the wire; the wire
contract for clients does not change — they still check
`loveliness.error` after the last batch.

## Versioning

The mapping is versioned via the schema metadata key
`loveliness.arrow_mapping_version` (string, currently `"1"`). Bumping
this is reserved for a breaking change to a row above — adding a new
row (e.g. when `DURATION` lands) keeps the version unchanged. Clients
that need to be conservative can refuse to parse a payload whose
version is higher than they were built against.

## Reference

- Arrow IPC format: <https://arrow.apache.org/docs/format/Columnar.html#format-ipc>
- HTTP API content negotiation: [`docs/api.md`](api.md#apache-arrow-output)
- Server encoder: `pkg/api/arrow.go`
- Issue tracking remaining slices: [#27](https://github.com/dreamware-nz/loveliness/issues/27)
