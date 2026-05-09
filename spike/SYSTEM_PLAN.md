# Loveliness viz — full system plan

A consolidation of every spike result in `spike/` into a single executable design. Cross-references to the spike-level docs are listed in [§9](#9-cross-references).

---

## TL;DR

The complete loveliness-viz system shape, after four spikes:

- **Backend:** the existing Go daemon (`cmd/loveliness`, LadybugDB-backed) is the source of truth. The viz reaches it through a thin Go binary (`cmd/loveliness-viz`) that proxies HTTP/JSON cypher reads and serves bulk-export snapshots.
- **Bulk transport:** server emits a flat edge buffer (Apache Arrow IPC over HTTP, gzip-on-the-wire) per dataset/snapshot. Pre-computed Leiden labels and a circle-pack layout are baked into the same payload. The browser **does not** ingest into LadybugDB-WASM (ladybug-wasm-poc found this path blocked on missing ingest API).
- **Render engine:** **Sigma 3 + graphology** in the browser. Bake-off across Sigma, bare Cosmos, and Cosmograph picked Sigma on the strength of correctness, smallest payload, native edge events, and the "we already know cluster centroids" framing.
- **Layout strategy:** *layout once, freeze, render*. Static circle-pack positions on the server (or Web Worker), seeded by Leiden communities. No live force simulation in v1 — the cosmos spike showed that's the wrong shape on M-class hardware and our cluster centroids are known.
- **Community detection:** Leiden via `@aflsolutions/graphology-communities-leiden` (pure JS) is the v1 default — meets the perf bar at 200K nodes. The Rust+WASM path (`leiden-rs` 0.8 + `wasm-rayon-coarse`) is the **production interactive path**: 1.4–2× speedup at 100K and a clean γ-sweep API for live γ-slider exploration.
- **Frontend host:** static web app served behind a small Go binary. **Not Wails, not Tauri** — the desktop wrapper isn't pulling its weight. SharedArrayBuffer + WASM threads work fine in a browser tab if the server emits COOP/COEP headers, which the Go binary does.
- **Stack:** TypeScript, React, Tailwind, Sigma for the graph viewport, **d3fc** for analytical side-panels (timelines, distributions, γ-curve plots) — *not* for the graph itself.

This is opinionated. Reasoning per decision is in §5–§6. Phasing in §7.

---

## 0. The picks (one-glance)

For each spike — what we keep, what we discard, what we shelve.

| Concern | Pick | Discard | Shelf |
|---|---|---|---|
| #1 Render engines | **Sigma 3 + graphology** | Cosmos (camera bug on static positions), Cosmograph (4 MB + watermark) | Cosmograph if we later need built-in DuckDB filtering |
| #2 Ladybug-WASM | **Server-side snapshot via Arrow IPC** — no browser ingest | Async-variant `UNWIND-CREATE` path, COPY-FROM main-thread workaround | Revisit when upstream ships `registerFileBuffer(name, bytes)` (#34) |
| #3 Leiden JS | **`@aflsolutions/graphology-communities-leiden` for default labels** (server precompute + cold paths) | nothing — it meets the perf bar | — |
| #4 Leiden Rust+WASM | **`wasm-rayon-coarse` for live γ-slider + plateau scan** (1.4–2× speedup, bit-exact) | rayon-fine-grained (`Atomics.wait` overhead → 35× slowdown) | Worker fan-out kept as fallback when `crossOriginIsolated` is false |
| #5 Cosmos deep | **Layout-once-then-freeze architecture** | Live force simulation in v1 | — |
| Frontend host | **Static SPA served by a small Go binary** with COOP/COEP/CORP headers | Wails (Go desktop), Tauri (Rust desktop) | Tauri later if a desktop bundle becomes required |
| Analytical UI | **d3fc for side panels** (Q(γ) curve, distributions, plateaus) | d3fc-webgl as the graph engine | — |
| Layout | **Server-side `d3-hierarchy.pack` seeded by Leiden communities**, baked into snapshot | Client-side force layout | — |
| Transport | **Apache Arrow IPC** (nodes + edges + community labels + (x,y) + per-γ levels) | Per-row JSON, GraphML, custom binary | gRPC-Web only if streaming diffs become a Phase 5 need |

### New surface area introduced

- `cmd/loveliness-viz/` — small Go binary, serves the SPA + proxies snapshots, emits SAB headers (COOP/COEP/CORP).
- Browser SPA — TS + React + Tailwind + Sigma (viewport) + d3fc (side panels) + leiden-rs.wasm (interactive γ).
- `cmd/loveliness` gains a `/snapshot/{dataset}/{ts}.arrow` endpoint with precomputed Leiden labels and circle-pack `(x, y)`.

---

## 1. Spike inventory

Five spikes ran. Four delivered actionable results, one was a tool comparison.

| # | Spike | Doc | Verdict |
|---|---|---|---|
| 1 | Render engine bake-off (Sigma vs Cosmos vs Cosmograph) | `ENGINE_COMPARISON.md` | **Sigma** |
| 2 | Ladybug-WASM in browser | `ladybug-wasm-poc/README.md` | **Blocked** on ingest API; descope from v1 |
| 3 | Leiden community detection (JS) | `leiden-poc/README.md` | **Pure JS already meets the bar** at 200K |
| 4 | Leiden Rust+WASM rayon | this session, captured below | **rayon-coarse 1.4–2× over serial; matches worker fan-out** |
| 5 | Cosmos PoC (deeper than the bake-off) | `cosmos-poc/SPIKE_FINDINGS.md` | **Layout-once architecture confirmed**, live force sim ruled out |

---

## 2. Spike #1 — Render engine bake-off

Three engines, same workload, three scales.

### Headline numbers (all M-class Mac, Chromium 148, 1200×756 viewport)

| Scenario | Sigma | Cosmos (bare) | Cosmograph |
|---|---:|---:|---:|
| Static 10K (steady-state FPS) | 51.2 | 60.0 | 45.1 |
| Static 50K (steady-state FPS) | 5.5 | 60.0 | not run |
| Circle-pack 10K (FPS) | 51.2 | 46.8 (1st run only) | 59.0 |
| Circle-pack 50K (FPS) | 5.5 (clean) | 44.8 (broken on 2nd run) | 33.2 (clean) |
| Bundle (gzipped est.) | ~150 KB | ~600 KB | ~4 MB |
| First render @ 10K | ~120 ms | ~10 ms | ~750 ms |
| Setup | ESM CDN | ESM CDN | npm + esbuild required |
| Watermark | none | none | "Visualized by Cosmograph.app" |

### Why Sigma wins for our shape

1. **Loveliness graphs are unlikely to exceed ~25K visible nodes** at any zoom level. Beyond that, no human eye can parse the ball — the answer is hierarchical zoom-in (a different viz), not a bigger ball. Sigma handles 25K with edges visible.
2. **Edge events + label control come free** via graphology adjacency. Cosmos requires hand-building an adjacency map for hover-edge / click-edge.
3. **Smallest payload** (~150 KB vs ~600 KB vs ~4 MB), no DuckDB-WASM startup, no watermark.
4. **The cosmos circle-pack camera bug is a serious blocker.** Static positions are *the* dominant case for us (we know cluster centroids). Cosmos handles this badly enough that we'd be fighting it.
5. **Cosmograph's polish is real but the price is steep**: 4 MB bundle, npm/bundler dependency, watermark, DuckDB-WASM warmup.

### When we'd revisit

- Real loveliness graphs that genuinely demand >50K visible at once.
- Need DuckDB-style filtering / search / aggregation built in (cosmograph offers this).

---

## 3. Spike #2 — Ladybug-WASM in browser

Goal: validate `@ladybugdb/wasm-core` as the in-browser graph engine.

### Verdict

**The engine is great. The async variant's data-ingest API is the gap.**

| Bulk-load path | 5K | 25K | 100K | 250K |
|---|---:|---:|---:|---:|
| `UNWIND-CREATE` (async, production-shape) | 3 s | — | did not finish | — |
| `COPY FROM /tmp/x.csv` (sync, MEMFS) | 253 ms | 1.0 s | 4.7 s | 11.6 s |

The COPY-FROM path is **12× faster**. But:

- Async (Worker-backed) variant returns `lbug.FS = {}`. The Worker owns MEMFS; main thread can't reach it. So we can't use COPY-FROM there.
- Sync variant returns a real FS object — but only `createPath` + `createDataFile`, no `writeFile` / `mkdir`. Sync variant blocks the main thread, so it isn't a production target.
- Emscripten build was not compiled with HTTP file-source support — `LOAD FROM 'http://…'` returns "no such file or directory".
- No `registerFileBuffer(name, bytes)` analogue.

Tracked as **dreamware-nz/loveliness#34**.

Query latency at 250K nodes / 1.5M edges, in-memory: point-by-PK warm 1.4 ms, 1-hop warm 3.7 ms, 2-hop warm 10.5 ms, shortest-path warm 124 ms. So query is fine; ingest is the wall.

### What this means for the system

**Don't ingest into LadybugDB-WASM in v1.** The Go daemon already holds the canonical graph; have it emit a Loveliness-friendly snapshot (Arrow IPC) and let the browser render it directly. Re-open the WASM-engine path only if we need browser-local Cypher *and* upstream ships a `registerFileBuffer` API.

---

## 4. Spike #3 — Leiden community detection (pure JS)

Goal: does Leiden run fast enough in the browser to be a v1 feature?

### Headline

**Yes, comfortably.**

| n | edges | full γ-sweep (6 γ) | best single γ | mean Q | ARI(micro) |
|---:|---:|---:|---:|---:|---:|
| 5,000 | 40K | 333 ms | 33 ms | 0.610 | 0.96 |
| 25,000 | 202K | 2.3 s | 302 ms | 0.616 | 0.97 |
| 100,000 | 807K | 14 s | 1.66 s | 0.618 | 0.96 |
| 200,000 | 1.6M | 39 s | 5.7 s | 0.619 | 0.96 |

Library: `@aflsolutions/graphology-communities-leiden` 1.1.1. Returns `{communities, count, modularity, dendrogram, moves, …}`. **Hierarchy comes free** via the `dendrogram` field — no manual recursion.

### What this means

1. **Pure JS Leiden is the v1 default.** We don't need Rust+WASM or WebGPU to ship.
2. **Multi-resolution = γ-sweep**, not a separate algorithm. Six γ values cover the useful range.
3. **Dendrogram height stays in 3–7 levels** even at 200K — comfortably enough for an LOD viz.

### Where we still need Rust+WASM

- Interactive γ-slider with sub-second response.
- Plateau scan for auto-γ recommendation (35–40 γ, every page-load is too slow in JS at 100K+).
- Memory pressure if the user opens multiple datasets in tabs.

That's where Spike #4 lives.

---

## 5. Spike #4 — Leiden Rust+WASM (this session)

Goal: get parallel Leiden working in WASM at 100K+ to support live γ exploration.

### Three paths tested

| Path | What it is | 100K 3L · 35 γ wall | Notes |
|---|---|---:|---|
| **Serial WASM** | `wasm-pack build --features wasm`, single thread | ~33.5 s | baseline, ~700ms/run |
| **Worker fan-out** | N Web Workers, 1 WASM instance each, N γ in parallel via postMessage | 18.5 s · 1.81× | requires per-worker WASM init + N-copy edge buffer |
| **rayon-fine-grained** | `wasm-parallel` feature, leiden-rs's internal `par_iter` everywhere | **22–27 s for ONE run** | catastrophic — `Atomics.wait` cost dwarfs gains |
| **rayon-coarse** | `wasm-rayon-coarse` feature, internal serial, outer `par_iter` over γ | **17.8 s · 1.96×** | 1 WASM instance, 1 SAB-shared edge buffer |

### Direct microbench, 100K 3L, 8 threads — rayon-coarse vs sequential

| γ count | serial ms | rayon-coarse ms | speedup |
|---:|---:|---:|---:|
| 2 | 1687 | 1162 | **1.45×** |
| 3 | 2497 | 1771 | **1.41×** |
| 4 | 3336 | 2415 | **1.38×** |
| 8 | 6681 | 4077 | **1.64×** |
| 35 (plateau scan) | ~33.5 s | 17.8 s | **1.96×** |

Modularities and community counts match serial **bit-for-bit at every γ** — correctness verified.

### Why the speedup caps at ~2×, not 8×

Amdahl, not a WASM bug. Leiden runtime varies 5–10× across γ values (high γ → many tiny communities → slow agglomeration). With 8 threads and 3 γ values, 5 threads sit idle. Wall = max(γ-task time), not avg. With 35 γ values, work-stealing spreads the variance better → closer to 2×.

### Why coarse beats fine-grained

`wasm-bindgen-rayon` uses `Atomics.wait` / `Atomics.notify` over SharedArrayBuffer for inter-thread sync. Native futexes are ~hundreds of nanoseconds; SAB atomics are ~hundreds of microseconds. leiden-rs's internal `par_iter` calls fire thousands of small tasks per Leiden run, so the sync cost dominates. Move parallelism to the *outer* γ-sweep level — one task per γ, ~700 ms each — and the sync cost amortizes to nothing.

### Build pipeline (now stable)

- Nightly Rust + `[unstable] build-std = ["panic_abort", "std"]`
- `+atomics,+bulk-memory,+mutable-globals` target features
- `--shared-memory --import-memory --max-memory=4294967296` linker flags
- Explicit `--export=__wasm_init_tls,__tls_size,__tls_align,__tls_base`
- `rayon::current_num_threads()` exported from Rust to keep TLS use alive through DCE/LTO
- Post-build patch on `pkg/snippets/wasm-bindgen-rayon-*/src/workerHelpers.js`: `'../../..'` → `'../../../leiden_rs.js'` (browsers don't resolve directory imports via package.json `main`)
- COOP `same-origin` + COEP `require-corp` on the static server

### What this means

- **rayon-coarse is the default for `loveliness-viz`'s WASM build.**
- **Worker fan-out stays as a fallback** for environments without `crossOriginIsolated` (rare, but happens with embedded contexts and some CDNs).
- **rayon-fine-grained (`wasm-parallel` feature)** stays in the codebase as a research artifact — not on the default build path.

---

## 6. Spike #5 — Cosmos deeper findings

The bake-off settled which engine to pick. The cosmos spike went deeper on *why* — and produced architectural decisions that apply regardless of engine.

### Key carry-forward

- **Pass:** render-only @ 50K-100K (sim paused) holds 35–60 FPS on M1 in Chromium.
- **Fail:** sim-active @ 100K (13.7 FPS), convergence-≤10s @ 100K (didn't settle in 20s), streaming-chunk-≤16ms (39–195 ms / chunk).
- **Conclusion:** *layout once or pre-compute, then freeze and render at vsync.* The "live 1M-node force-directed simulation" demo is not what we're shipping.

This conclusion is engine-agnostic. It applies equally to Sigma — and Sigma is happy with this shape because we hand it static positions per node.

---

## 7. System architecture

### 7.1 Component map

```
┌──────────────────────────────────────────────────────────────────┐
│  loveliness/ (Go daemon, LadybugDB)                              │
│  - canonical graph storage (Raft-clustered)                      │
│  - Cypher reads / writes (HTTP/JSON)                             │
│  - bulk-export API (NEW): Arrow IPC snapshot per (dataset, ts)   │
└──────────────────────────────────────────────────────────────────┘
                                       ▲
                                       │ Cypher (point + 1-2 hop)
                                       │ Arrow IPC bulk-export
                                       │ + Leiden labels
                                       │ + circle-pack positions
                                       │
                            ┌──────────────────────────────────────┐
                            │ loveliness-viz/ (NEW Go binary)      │
                            │  - HTTPS + COOP/COEP + CORP          │
                            │  - serves static SPA bundle          │
                            │  - proxies cypher reads to daemon    │
                            │  - proxies bulk-export from daemon   │
                            │  - precomputes Leiden + layout once  │
                            │    per (dataset, ts) and caches it   │
                            └──────────────────────────────────────┘
                                              ▲
                                              │ HTTP/JSON for app shell
                                              │ HTTP/Arrow for graph payload
                                              │ HTTP/JSON for live cypher
                                              │
                            ┌──────────────────────────────────────┐
                            │ Browser SPA (TS + React + Tailwind)  │
                            │ ┌─────────────────────────────────┐  │
                            │ │ Sigma graph viewport            │  │
                            │ │  - static positions (server)    │  │
                            │ │  - color by Leiden community    │  │
                            │ │  - hover/click halos            │  │
                            │ └─────────────────────────────────┘  │
                            │ ┌─────────────────────────────────┐  │
                            │ │ d3fc analytical panels           │  │
                            │ │  - γ-curve, Q-curve, plateaus    │  │
                            │ │  - distribution / timeline       │  │
                            │ │  - filter chips                  │  │
                            │ └─────────────────────────────────┘  │
                            │ ┌─────────────────────────────────┐  │
                            │ │ leiden-rs.wasm (rayon-coarse)    │  │
                            │ │  - on-demand γ-slider re-cluster │  │
                            │ │  - plateau scan for auto-γ        │  │
                            │ └─────────────────────────────────┘  │
                            └──────────────────────────────────────┘
```

### 7.2 Data flow per page-load

1. SPA boots, queries `loveliness-viz` for the dataset list.
2. User picks a dataset → SPA requests `/snapshot/{dataset}/{ts}.arrow`.
3. `loveliness-viz` returns a single Arrow IPC payload:
   - `nodes`: id, label, attributes, **leiden_community_id** (default γ), **x**, **y** (circle-pack)
   - `edges`: src, dst, weight
   - `levels`: per-γ community arrays (precomputed plateau sweep, ~5–7 levels)
4. SPA loads it into graphology (browser-side adjacency), hands node positions + colors to Sigma.
5. User drags γ-slider → SPA calls `leiden_run_unweighted_multi_gamma(edges, n, [γ], seed)` in WASM. ~1.5 s for a fresh γ at 100K. Recolor.
6. User clicks node → Sigma fires `clickNode`, SPA either uses local adjacency for 1-hop or sends a cypher query through `loveliness-viz` for deeper structure.

### 7.3 Layout strategy

**Server-side circle-pack, seeded by Leiden communities.** Step-by-step on the server, once per snapshot:

1. Run Leiden at γ=1.0 → community ids per node.
2. Run Leiden over the plateau range (γ ∈ [0.05, 80], 35 points) → recommended γ-levels (saneLevels filter).
3. For each level, group nodes by community → cluster centroids.
4. Apply `d3-hierarchy.pack()` to clusters using community size as weight.
5. Within each cluster, place nodes in a sub-pack or ring.
6. Bake (x, y) per node into the snapshot Arrow.

This is the "we already know cluster centroids" framing the engine bake-off was decided on. It moves layout cost off the user's machine and onto cold compute.

### 7.4 What lives on the client vs server

| Concern | Server | Client |
|---|---|---|
| Canonical graph | ✓ (LadybugDB) | snapshot only |
| Live writes | ✓ | — |
| Read queries (point, 1-hop, 2-hop) | via cypher HTTP | also local for in-snapshot adjacency |
| Default Leiden labels | ✓ (precomputed per snapshot) | — |
| Layout (x, y) | ✓ | — |
| Ad-hoc γ exploration | — | **leiden-rs.wasm rayon-coarse** |
| Plateau scan auto-γ | precomputed once | re-run on demand |
| Render | — | **Sigma** |
| Analytical charts | — | **d3fc** |

---

## 8. Frontend stack — final decisions

### 8.1 Render engine: Sigma 3 + graphology

Locked in by the bake-off (§2). Use `@sigma/edge-curve` for curved edges only at low zoom levels; flat lines otherwise.

### 8.2 NOT d3fc-webgl for the graph

The earlier proposal mentioned `d3fc-webgl`. d3fc is a financial-charting library — series, axes, scales, accelerated by WebGL. Excellent at *that*. For graphs you'd be hand-rolling camera, picking, hover, edge events, halos, hit-testing — i.e. opting *out* of an engine and *into* a low-level toolkit. Sigma already gives all of this.

### 8.3 d3fc DOES belong, just not in the viewport

Around the graph viewport we want analytical panels: γ-vs-Q curve, plateau highlight, community-size distribution, time-series of partition stability if we ever add temporal data. This is exactly d3fc's wheelhouse — D3 idioms with WebGL acceleration for thousands of points without tearing.

```
┌────────────┬───────────────────────────────────────────┐
│ Filter /   │                                           │
│ Search     │            Sigma viewport                 │
│ chips      │       (graph render, hover, click)        │
│            │                                           │
│            ├───────────────────────────────────────────┤
│ Selection  │  γ-slider + d3fc Q(γ) curve + plateaus    │
│ details    │                                           │
│            │  d3fc community-size distribution         │
└────────────┴───────────────────────────────────────────┘
```

### 8.4 NOT Wails, NOT Tauri — static web app

The original proposal asked about Wails (Go) for a desktop binary. The case for it:
- single-exe distribution
- native menus / file dialogs
- avoid browser CORS / COOP setup

The case against it for *us*:
- COOP/COEP / SAB already work in a browser tab — we proved it in this session
- All five spikes ran as browser-served static SPAs — no friction
- Wails adds GUI lifecycle, IPC, and packaging complexity
- The Go daemon already needs to exist (graphdb); adding a desktop wrapper duplicates plumbing
- Cross-platform desktop binaries are a maintenance liability

**Ship a static SPA hosted by a small Go binary.** That binary:
- serves the SPA bundle
- serves snapshots from `loveliness/` over HTTP
- emits COOP/COEP/CORP headers for SAB
- proxies live cypher reads to the loveliness daemon

If we need a desktop one day, wrap *that* in Tauri (Rust → fits the leiden-rs side better than Go → Wails) without rewriting anything.

### 8.5 TypeScript + React + Tailwind

Standard, low-risk. Two notes:

- **Sigma + React:** use `@react-sigma/core` only for the wrapper / lifecycle — direct `useSigma()` hooks for everything else. Don't try to drive Sigma's per-frame state from React renders.
- **Bundle:** Vite or esbuild. Match the existing PoCs (esbuild) unless someone wants HMR ergonomics, in which case Vite.

### 8.6 Build / WASM glue

- `leiden-rs/pkg/` is the artifact. Built with `wasm-pack build --target web --release --no-default-features --features wasm-rayon-coarse` (nightly toolchain pinned via `rust-toolchain.toml`).
- Post-build patch on `workerHelpers.js` is automated in a build script (sed step).
- Probe `initThreadPool` at boot — if `crossOriginIsolated` is false, fall back to worker fan-out path.

---

## 9. Cross-references

- `spike/ENGINE_COMPARISON.md` — render engine bake-off methodology and raw FPS numbers
- `spike/ladybug-wasm-poc/README.md` — full ingest-API gap analysis
- `spike/leiden-poc/README.md` — pure-JS Leiden numbers, dendrogram structure
- `spike/leiden-poc/leiden-rs/` — Rust+WASM Leiden, all four feature paths
- `spike/cosmos-poc/SPIKE_FINDINGS.md` — render-vs-sim split and the layout-once decision
- `cmd/loveliness/main.go` — graphdb daemon (existing)

---

## 10. Phasing

### Phase 0 — repo layout + app shell (1 day)

- Create `cmd/loveliness-viz/`. Go binary scaffolding.
- TS+React+Tailwind+esbuild app shell, served by the Go binary.
- Stub Sigma viewport rendering a synthetic 1K-node graph from `spike/leiden-poc/scenarios.js`.
- COOP/COEP headers on the Go server.

**Done when:** local browser shows Sigma rendering a fixed graph.

### Phase 1 — server-side snapshot pipeline (2–3 days)

- Add `cmd/loveliness` snapshot endpoint: dataset → Arrow IPC of (nodes, edges, leiden_community_id, x, y).
- Server runs Leiden at γ=1.0 + plateau scan once per snapshot, caches result on disk.
- `loveliness-viz` proxies this endpoint and serves it to the SPA.
- SPA fetches → graphology → Sigma. Pre-baked colors, pre-baked positions.

**Done when:** real loveliness data renders in the browser at 25K and 100K with no client-side compute.

### Phase 2 — interactive γ-slider (2 days)

- Wire `leiden-rs/pkg/` (rayon-coarse build) into the SPA bundle.
- Build the slider UI; on slider change, call `leiden_run_unweighted_multi_gamma` with one γ and re-color.
- Show plateau strip from precomputed sweep underneath the slider so user knows where the stable γ ranges are.

**Done when:** at 100K the γ-slider responds in <2s wall-clock.

### Phase 3 — analytical panels (2 days)

- d3fc panels: Q(γ) curve, community-size distribution.
- Hover synchronization between graph and panels (graphology adjacency).

**Done when:** clicking a community in the graph highlights it in the size distribution.

### Phase 4 — hierarchy / LOD (later)

- Use the precomputed dendrogram levels: zoom out → coarser community grouping → fewer visible nodes.
- This is the answer to "what about >25K visible at once" — not a bigger ball, hierarchical zoom.

**Done when:** zoom-out renders meta-nodes at 1.5× the zoom level Sigma would otherwise stutter at.

### Phase 5 — live writes (later)

- Subscribe to dataset change events from `loveliness/`.
- Incremental graphology + Sigma updates without full reload.
- This requires a streaming snapshot diff — not designed yet, defer.

---

## 11. Open questions

1. **Real loveliness graphs** — degree distribution, weights, directionality. Synthetic results (`leiden-poc`) used a planted hierarchy with mean degree 8 and weak macro signal. Real data may have different community structure that surfaces edge cases we haven't measured.
2. **Layout sensitivity to community count** — d3-hierarchy pack works well for small-K. If a γ-level produces 5K communities, sub-pack collapses to dust and we need a fallback (e.g. coarse community grid + per-cluster local force).
3. **Snapshot freshness** — how stale is acceptable? Daily? Per-write? This affects the precompute pipeline's cost profile on the server side.
4. **Multi-tenant snapshots** — if loveliness gets a multi-user shape, snapshot cache key needs (tenant, dataset, ts).
5. **Mobile / tablet** — out of scope for v1, but Sigma + react work fine there if we ever need it. WASM threading does *not* — fall back to fan-out or serial.

---

## 12. What we're explicitly NOT building in v1

- **In-browser Cypher** (LadybugDB-WASM ingest blocked, see §3).
- **Live force simulation** (cosmos spike ruled it out, §6).
- **Cosmograph-style timelines + DuckDB filtering** (over-budget on bundle weight; revisit if d3fc panels are insufficient).
- **A desktop binary** (Wails / Tauri) — ship a static SPA, wrap later if needed.
- **WebGPU Leiden** — pure JS already meets the bar, Rust+WASM exceeds it.
- **>50K visible nodes at once** — solved by hierarchy / LOD instead.
