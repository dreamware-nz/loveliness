# Kubernetes Deployment

## Prerequisites

- Kubernetes 1.26+
- A StorageClass that supports `ReadWriteOnce` PVCs
- `dreamwarenz/loveliness` image available (Docker Hub or your registry)

## Quick Deploy

```bash
kubectl apply -f deploy/k8s/namespace.yml
kubectl apply -f deploy/k8s/service.yml
kubectl apply -f deploy/k8s/statefulset.yml
```

## Architecture

The deployment uses a **StatefulSet** with a headless service for stable DNS names:

```
loveliness-0.loveliness.loveliness.svc.cluster.local
loveliness-1.loveliness.loveliness.svc.cluster.local
loveliness-2.loveliness.loveliness.svc.cluster.local
```

Each pod gets:
- A persistent volume for shard data + WAL + Raft state
- A unique node ID derived from the pod name
- Raft and transport addresses based on the headless service DNS

## Bootstrap

The first pod (`loveliness-0`) must bootstrap the Raft cluster. Two options:

**Option A — One-time env override:**
```bash
kubectl set env statefulset/loveliness LOVELINESS_BOOTSTRAP=true -n loveliness
# Wait for pod-0 to start, then disable bootstrap
kubectl set env statefulset/loveliness LOVELINESS_BOOTSTRAP=false -n loveliness
```

**Option B — Init container or job:**
```bash
kubectl apply -f deploy/k8s/bootstrap-job.yml
```

After bootstrap, other pods join automatically via `LOVELINESS_PEERS`.

## Scaling

```bash
kubectl scale statefulset loveliness --replicas=5 -n loveliness
```

New pods join the cluster via the peers list. Shard count is fixed at cluster creation (default 16), so you can scale up to 16 nodes maximum with default settings. See [Configuration](configuration.md) for shard count guidance.

## Exposed Services

| Service | Type | Ports |
|---|---|---|
| `loveliness` (headless) | ClusterIP: None | 8080 (HTTP), 7687 (Bolt), 9000 (Raft), 9001 (Transport) |
| `loveliness-lb` | LoadBalancer | 8080 (HTTP), 7687 (Bolt) |

The LoadBalancer service exposes HTTP and Bolt to external clients. Raft and transport ports are internal only.

## Resource Sizing

| Cluster Size | CPU (per pod) | Memory (per pod) | Storage |
|---|---|---|---|
| Dev / < 1M nodes | 500m | 1Gi | 10Gi |
| Small / 1–10M nodes | 1 | 2Gi | 50Gi |
| Medium / 10–100M nodes | 2 | 4Gi | 200Gi |
| Large / 100M+ nodes | 4 | 8Gi | 500Gi |

## Backup to S3

Set environment variables on the StatefulSet:

```yaml
env:
  - name: LOVELINESS_S3_BUCKET
    value: my-backup-bucket
  - name: LOVELINESS_S3_REGION
    value: us-east-1
  - name: LOVELINESS_BACKUP_INTERVAL_MIN
    value: "60"
  - name: LOVELINESS_BACKUP_RETENTION
    value: "5"
```

Or use IRSA/pod identity for credentials instead of access keys.

## Monitoring

- `GET /health` — readiness and liveness probe endpoint
- Logs are structured JSON (parseable by Datadog, Loki, CloudWatch, etc.)
