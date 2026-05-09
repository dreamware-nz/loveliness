# Grafana dashboards

`loveliness-overview.json` is the operational overview dashboard for a
Loveliness cluster. It panels:

- Raft state (one-hot per node)
- Per-shard health (0/1)
- Query QPS by `query_type`
- Query error rate by `status` (everything except `ok`)
- Query latency p50/p95/p99 from the duration histogram
- Replication lag in seconds and entries, per shard × replica
- Bulk-load rows/sec by table
- Go runtime memory (alloc / in-use / sys)
- Goroutines and GC pause/sec

## Importing

1. In Grafana, **Dashboards → New → Import**, then upload
   `loveliness-overview.json` (or paste its contents).
2. When prompted, point the `DS_PROMETHEUS` variable at the Prometheus
   datasource scraping the cluster.
3. The `node` template variable is populated from
   `label_values(loveliness_local_shards, node_id)`. Note that
   `loveliness_local_shards` is currently emitted without a `node_id`
   label; the variable will fall back to `All` until a per-node label
   is added at the scrape config (e.g. via `relabel_configs` /
   `external_labels`).

## Prometheus scrape config (example)

```yaml
scrape_configs:
  - job_name: loveliness
    metrics_path: /metrics
    static_configs:
      - targets:
          - loveliness-0.example.internal:8080
          - loveliness-1.example.internal:8080
          - loveliness-2.example.internal:8080
    relabel_configs:
      - source_labels: [__address__]
        target_label: node_id
        regex: '([^.]+)\..*'
        replacement: '$1'
```

## Schema

- Targets Grafana **schemaVersion 39** (Grafana 10.0+). It will import
  cleanly into newer versions; older versions may need a re-save to
  fill in defaults.
- All series referenced by panels are defined in
  [`docs/metrics.md`](../../docs/metrics.md). Adding a new series
  doesn't require dashboard changes; adding a new *panel* does — keep
  the catalogue in `docs/metrics.md` and the panels here in lock-step.

## Validation

`pkg/api/grafana_dashboard_test.go` does two things on every CI run:

1. Parses the JSON to ensure it stays well-formed.
2. Asserts every metric name listed in `expectedMetrics` appears in at
   least one panel's `expr`. That catches accidental renames or panels
   pointing at metrics we no longer emit.
