# Spike Findings — cross-node transport gaps (#76)

**Verdict:** the polish series got us to "good loopback"; three gaps stop us shipping to anything multi-host (deadlines, retry amplification, teardown leak), three more are cheap wins worth doing soon, and a v2 frame is unavoidable once we want streaming or cancel. Drop in the recommendations below and we have a credible cross-node transport without leaving the TCP+msgpack architecture.

## Inputs

- Issue: [#76](https://github.com/dreamware-nz/loveliness/issues/76)
- Polish series that surfaced these gaps: PRs #61, #65, #68, #73, #75
- Survey of prior art: [SURVEY.md](./SURVEY.md) (Neo4j Fabric, DGraph, MemGraph, CockroachDB)
- Code under discussion: `pkg/transport/tcp.go`, `pkg/transport/pool.go`, `pkg/router/`

## Categorisation

| # | Gap | Category | Follow-up |
|---|---|---|---|
| 1 | `TCPServer.handleConn` blocks teardown on 60s idle | **must-fix-pre-prod** | new issue (S) |
| 2 | TCPPool sizing vs scatter cap uncoordinated | **should-fix-soon** | new issue (M) |
| 3 | No deadline propagation into remote shard | **must-fix-pre-prod** | new issue (M) |
| 4 | No retry budget for the whole query | **must-fix-pre-prod** | new issue (M) |
| 5 | No streaming / chunked responses | **frame-v2 dependent — defer** | bundled into v2 frame issue |
| 6 | No cancel / partial-result signalling | **frame-v2 dependent — defer** | bundled into v2 frame issue |
| 7 | No per-RPC correlation ID | **should-fix-soon** | new issue (S) — see frame-v1-additive note |
| 8 | mTLS not exercised in tests | **won't-fix-here** (test/docs gap) | new issue (S), `testing` label |
| 9 | No admission control / QoS | **won't-fix-here** (policy layer, not transport) | new roadmap issue |
| 10 | Pool eviction is reactive only | **should-fix-soon** | new issue (S) |

Counts: 3 must-fix, 3 should-fix, 2 frame-v2 dependent, 2 won't-fix-here.

## Rationale per item

### 1. Teardown blocked on idle deadline — must-fix
The 60s read deadline means a clean `Stop()` waits up to 60s per connection. The bench saw 120s wallclock. In production this means a rolling restart of a 16-node cluster takes 16 minutes of wallclock at minimum and surfaces as alert noise that's purely a transport bug. Prior-art consensus is unanimous: gracefully close active sessions on shutdown, do not wait on idle deadlines. Fix is mechanical (`SetReadDeadline(now)` on every live conn when `stopCh` fires), test is deterministic. Single-PR-size.

### 2. Pool vs scatter mismatch — should-fix
At 16 shards on one peer with 4 pool slots, scatter serialises 4-at-a-time over TCP. Not a correctness bug; a throughput bug under fan-out. Fix has at least three flavours — adaptive pool growth, per-peer scatter cap, or multiplexing on a single connection — and the right answer probably depends on what we decide for items 5 and 7 (streaming/correlation make multiplexing more attractive). Schedule **after** the frame-v2 decision so we don't reimplement multiplexing on top of v2 streaming.

### 3. Deadline propagation — must-fix
`router.timeout` only gates the caller. A slow shard keeps burning CPU and (worse, for writes) keeps making mutations after the caller has timed out. This is the single biggest hidden-cost gap on the list. Survey is unanimous: deadline on the wire, server-side honors it. We **must** do this; frame already has room (the `MsgQuery` envelope can carry a `deadline_ns` field without rev'ing the frame major).

### 4. Whole-query retry budget — must-fix
Per-shard retries × shard count is multiplicative. A 32-shard query with 2 retries each is 96 RPCs for one user query — that's a self-DOS waiting for a bad day. CockroachDB's AC is the gold-standard reference; we can ship a coarser version (`max_attempts_per_query`, `max_retry_wallclock`) without the full AC framework. Co-design with item 3: retries that fire after the propagated deadline must not be attempted.

### 5. Streaming responses — frame-v2 dependent
Universal in prior art (Bolt `PULL`, gRPC server-streaming, DistSQL flows). Our `MsgResult` is single-shot. Adding streaming **requires** mid-stream framing (chunk + final-chunk markers, or end-of-stream signal). That's a v2 frame whether we want to admit it or not — msgpack tolerance for unknown fields is not enough because the wire-level reader expects one envelope per RPC. **Bundle with item 6**: streaming without cancel is half-built.

### 6. Cancel / partial signalling — frame-v2 dependent
Same shape. Server needs to recognise an in-band control frame ("client cancelled") or we leak compute on every router timeout. v2 frame is the natural place to add this — once we have streaming, the protocol already has a notion of mid-RPC server state, and adding a `MsgCancel` opcode is a small extension. Pinning this to v2 keeps the v1 frame stable until we commit to the migration.

### 7. Per-RPC correlation ID — should-fix
This is the **only** v1-additive change in the list. A `request_id` (uint64) field added to `MsgQuery`/`MsgResult`/`MsgError` is backwards-compatible: old peers that don't read the field still work; the field is just unused on legacy ends. Big debugging win for ~50 lines of code. Should not wait for v2.

### 8. mTLS test gap — won't-fix-here (this layer)
mTLS code paths exist (`SetTLS` on both client and pool). The gap is `:test:`-shaped: no integration test asserts "bad cert is rejected", and no doc covers cert rotation. This is real, but it doesn't change transport design; it's a CI/test work item with the `testing` label. Ship the follow-up as a small test-and-docs PR, don't bundle it with the design-shaped items.

### 9. Admission control / QoS — won't-fix-here (this layer)
Two priority classes + per-class rate limits is a router/policy concern, not a transport concern. The transport's job is to deliver bytes; the router decides which queries get bytes. CockroachDB is the only system in the survey with a real AC framework, and theirs lives in the SQL layer above the transport. File as a roadmap issue scoped at the router; revisit after items 3 and 4 land (since they shape what the policy layer sees).

### 10. Proactive heartbeat — should-fix
`MsgPing/MsgPong` already exists in the wire format. A keepalive loop in the pool that sends a `MsgPing` per idle connection every N seconds, and evicts the conn on no `MsgPong` within timeout, is single-file work. Catches half-dead TCP connections (FIN dropped, NAT timeout) that today linger until the next real RPC fails.

## Frame-format decision

**Recommend: v1-additive for #7 now, v2 frame for #5 + #6 together when scheduled.**

The v1 frame is otherwise stable; we shouldn't churn it just because we want better debugging logs. Item #7 fits the additive model perfectly — old peers ignore the field, new peers benefit immediately, no migration story needed.

Items #5 and #6 force a v2 frame because:
- Streaming changes the reader contract: instead of "one envelope, then ready for next request", the reader has to handle "chunk frames followed by a final marker". You cannot retrofit this onto a single-envelope reader without flag day.
- Cancel needs a server-side control opcode the v1 frame never reserved opcodes for.
- A v2 frame buys us room for other things we don't know we need yet (cancel reasons, partial-result indicators, server-side ack-of-cancel).

Migration shape (sketch, lands with the v2 frame issue):
- Handshake byte advertises supported frame versions. Peers negotiate down to the highest common version.
- v1 stays in tree until rolling-upgrade is confirmed working in a real deployment; remove it in the release after.
- The v1 connection path stays the default until v2 is feature-complete + benched.

Track the v2 frame as its own issue (will be filed alongside items 5 + 6 — see below). Cancellation needs the v2 frame, so its issue depends on the frame issue.

## Co-design constraints

- **Item 4 depends on item 3.** Whole-query retry budgets are meaningful only if remote work has actually stopped — otherwise the retry is racing the abandoned attempt's late completion. Both issues should explicitly call out the dependency.
- **Item 2 depends on the v2 frame outcome.** If we go to streaming, multiplexing on one connection is much more useful than growing the pool. Park #2 until the v2 frame issue lands a decision.
- **Items 5 and 6 must ship together.** Streaming without cancel leaks compute; cancel without streaming is awkward to express. One v2-frame issue covers both; do not split.

## Open questions / parked

- **Wire-format version negotiation byte:** do we add it now (as a v1 cleanup) or as part of the v2 frame issue? Recommend the latter — adding negotiation to v1 in isolation just adds a no-op handshake.
- **Mixed-version cluster rolling upgrade:** the v2 frame issue must answer this. Sketch above is one paragraph; the issue gets a section.
- **Out-of-tree consumers of the wire frame:** nothing currently outside `pkg/transport/` reaches into the frame. If we ever add a non-Go client, that's a much larger conversation that this spike doesn't touch.

## Named follow-ups (filed against this spike)

1. [#83](https://github.com/dreamware-nz/loveliness/issues/83) `fix(transport): unblock TCPServer teardown on stop` — must-fix, S, no wire change.
2. [#88](https://github.com/dreamware-nz/loveliness/issues/88) `feat(transport): adaptive TCP pool sizing or per-peer scatter cap` — should-fix, M, parked behind v2 frame (#89).
3. [#84](https://github.com/dreamware-nz/loveliness/issues/84) `feat(transport): deadline propagation to remote shard` — must-fix, M, v1-additive (`deadline_ns` field).
4. [#85](https://github.com/dreamware-nz/loveliness/issues/85) `feat(router): whole-query retry budget` — must-fix, M, co-designed with #84.
5. [#89](https://github.com/dreamware-nz/loveliness/issues/89) `spec: v2 wire frame (streaming + cancel)` — frame-v2 design issue, L; bundles items 5 + 6 (streaming + cancel) implementation work.
6. [#86](https://github.com/dreamware-nz/loveliness/issues/86) `feat(transport): per-RPC correlation ID (v1-additive)` — should-fix, S, no v2 dependency.
7. [#87](https://github.com/dreamware-nz/loveliness/issues/87) `feat(transport): proactive ping/pong keepalive` — should-fix, S, no wire change.
8. [#90](https://github.com/dreamware-nz/loveliness/issues/90) `test(transport): mTLS rejection + cert-rotation smoke tests` — testing, S.
9. [#91](https://github.com/dreamware-nz/loveliness/issues/91) `roadmap(router): admission control / QoS` — won't-fix-in-transport-layer; router-layer roadmap discussion.

## Out of scope (recorded for the record)

- Implementing any of items 1–10 directly in this PR. Each gets its own issue and PR.
- Replacing the transport with gRPC, HTTP/2, or QUIC. That's the question for [#64](https://github.com/dreamware-nz/loveliness/issues/64) (transport architecture review), not this spike.
- Frame-format change for #7 alone. Additive only; no negotiation byte, no version bump.

## Re-read in 6 months

This file plus `docs/architecture.md`'s "Cross-node transport gaps" section is the source of truth. When you ship a must-fix, edit the architecture.md table cell to read "done in #N" rather than recreating decision records elsewhere.
