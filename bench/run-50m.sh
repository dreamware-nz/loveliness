#!/usr/bin/env bash
#
# 50M-scale benchmark on Fly.io: 5 nodes, 6 shards, 50M nodes, 50M edges.
#
# Deploys a dedicated Fly app (loveliness-bench-50m), scales to 5 machines,
# seeds data via parallel HTTP bulk endpoints, runs benchmarks, tears down.
#
# Usage:
#   ./bench/run-50m.sh                       # full: deploy + seed + bench + teardown
#   ./bench/run-50m.sh --skip-deploy         # cluster already running
#   ./bench/run-50m.sh --skip-seed           # data already loaded
#   ./bench/run-50m.sh --seed-only           # seed only, keep cluster up
#   ./bench/run-50m.sh --skip-teardown       # keep cluster after benchmarks
#   ./bench/run-50m.sh --nodes=1000000       # smaller test run
#
set -euo pipefail

# Ensure fly CLI is in PATH.
export PATH="$HOME/.fly/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RESULTS_DIR="$SCRIPT_DIR/results/50m-$TIMESTAMP"
FLY_TOML="$SCRIPT_DIR/fly-50m.toml"
APP_NAME="loveliness-bench-50m"

NODES=50000000
EDGES=50000000
ITERS=200
WORKERS=16
BATCH_SIZE=200000
SEED_WORKERS=2
MACHINE_COUNT=4

SKIP_DEPLOY=false
SKIP_SEED=false
SEED_ONLY=false
SKIP_TEARDOWN=false

for arg in "$@"; do
  case "$arg" in
    --skip-deploy)    SKIP_DEPLOY=true ;;
    --skip-seed)      SKIP_SEED=true ;;
    --seed-only)      SEED_ONLY=true; SKIP_TEARDOWN=true ;;
    --skip-teardown)  SKIP_TEARDOWN=true ;;
    --nodes=*)        NODES="${arg#*=}" ;;
    --edges=*)        EDGES="${arg#*=}" ;;
    --iters=*)        ITERS="${arg#*=}" ;;
    --batch-size=*)   BATCH_SIZE="${arg#*=}" ;;
    --seed-workers=*) SEED_WORKERS="${arg#*=}" ;;
    --machines=*)     MACHINE_COUNT="${arg#*=}" ;;
  esac
done

mkdir -p "$RESULTS_DIR"

echo "╔═══════════════════════════════════════════════════════╗"
echo "║    Loveliness 50M Benchmark (Fly.io)                 ║"
echo "║    app:      $APP_NAME                      ║"
printf "║    cluster:  %d nodes, 12 shards                      ║\n" "$MACHINE_COUNT"
printf "║    nodes: %-12s  edges: %-12s      ║\n" "$NODES" "$EDGES"
printf "║    batch: %-12s  seed-workers: %-5s        ║\n" "$BATCH_SIZE" "$SEED_WORKERS"
printf "║    iters: %-12s  query-workers: %-5s       ║\n" "$ITERS" "$WORKERS"
echo "║    output: $RESULTS_DIR"
echo "╚═══════════════════════════════════════════════════════╝"
echo

# Build the benchmark binary (runs locally against Fly endpoints).
echo "Building benchmark binary..."
cd "$PROJECT_DIR"
CGO_ENABLED=1 go build -o "$SCRIPT_DIR/benchmark-runner" ./cmd/benchmark
echo "  done"
echo

# ─── Deploy ───
if [ "$SKIP_DEPLOY" = false ]; then
  echo "━━━ Deploying $APP_NAME to Fly.io ━━━"

  # Create the app if it doesn't exist.
  if ! fly apps list --json 2>/dev/null | grep -q "\"$APP_NAME\""; then
    echo "  creating app..."
    fly apps create "$APP_NAME" --org personal 2>/dev/null || true
  fi

  # Deploy (builds and pushes image, creates first machine).
  echo "  deploying..."
  fly deploy --config "$FLY_TOML" --app "$APP_NAME" --strategy immediate --yes

  # Scale to target machine count.
  CURRENT_COUNT=$(fly machines list --app "$APP_NAME" --json 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
  if [ "$CURRENT_COUNT" -lt "$MACHINE_COUNT" ]; then
    echo "  scaling from $CURRENT_COUNT to $MACHINE_COUNT machines..."
    fly scale count "$MACHINE_COUNT" --app "$APP_NAME" --yes
  fi

  echo "  deployed ($MACHINE_COUNT machines)"
  echo
fi

# ─── Wait for health ───
echo "━━━ Waiting for cluster health ━━━"
APP_URL="https://${APP_NAME}.fly.dev"
TIMEOUT=300
ELAPSED=0
echo -n "  waiting for $APP_URL/health..."
while [ $ELAPSED -lt $TIMEOUT ]; do
  if curl -sf "$APP_URL/health" > /dev/null 2>&1; then
    echo " ready (${ELAPSED}s)"
    break
  fi
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done
if [ $ELAPSED -ge $TIMEOUT ]; then
  echo " TIMEOUT after ${TIMEOUT}s"
  echo "  check: fly logs --app $APP_NAME"
  exit 1
fi

# Wait for all machines to join the cluster via /discovery.
echo -n "  waiting for $MACHINE_COUNT nodes in cluster..."
ELAPSED=0
while [ $ELAPSED -lt 120 ]; do
  NODE_COUNT=$(curl -sf "$APP_URL/discovery" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('nodes',d.get('peers',[]))))" 2>/dev/null || echo "0")
  if [ "$NODE_COUNT" -ge "$MACHINE_COUNT" ]; then
    echo " $NODE_COUNT nodes ready (${ELAPSED}s)"
    break
  fi
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done
if [ "$NODE_COUNT" -lt "$MACHINE_COUNT" ]; then
  echo " only $NODE_COUNT/$MACHINE_COUNT nodes (continuing anyway)"
fi
echo

# Capture machine info for the results.
fly machines list --app "$APP_NAME" --json > "$RESULTS_DIR/machines.json" 2>/dev/null || true

# For Fly.io, all requests go through the single anycast endpoint.
# Fly's proxy load-balances across machines, so parallel seeding works
# naturally — each request may land on a different node.
ENDPOINT="$APP_URL"

# ─── Seed ───
if [ "$SKIP_SEED" = false ]; then
  echo "━━━ Seeding: schema + ${NODES} nodes + ${EDGES} edges ━━━"
  echo "  endpoint: $ENDPOINT"
  echo "  parallel: $SEED_WORKERS workers, batch size $BATCH_SIZE"
  echo
  "$SCRIPT_DIR/benchmark-runner" \
    -endpoint "$ENDPOINT" \
    -nodes "$NODES" \
    -edges "$EDGES" \
    -batch-size "$BATCH_SIZE" \
    -seed-workers "$SEED_WORKERS" \
    -iters 0 \
    -json-out "$RESULTS_DIR/seed.json"
  echo
fi

if [ "$SEED_ONLY" = true ]; then
  echo "Seed-only mode; cluster is still running."
  echo "  app: $APP_NAME"
  echo "  url: $APP_URL"
  echo "  teardown later: fly apps destroy $APP_NAME --yes"
  exit 0
fi

# ─── Benchmark ───
echo "━━━ Running benchmarks ━━━"
echo "  endpoint: $ENDPOINT"
echo
"$SCRIPT_DIR/benchmark-runner" \
  -endpoint "$ENDPOINT" \
  -nodes "$NODES" \
  -edges "$EDGES" \
  -iters "$ITERS" \
  -workers "$WORKERS" \
  -skip-seed \
  -json-out "$RESULTS_DIR/50m-cluster.json"
echo

# ─── Resource snapshot ───
echo "━━━ Resource snapshot ━━━"
fly machines list --app "$APP_NAME" 2>/dev/null || true
echo
# Grab per-machine metrics if available.
for mid in $(fly machines list --app "$APP_NAME" --json 2>/dev/null | python3 -c "import sys,json; [print(m['id']) for m in json.load(sys.stdin)]" 2>/dev/null); do
  echo "  machine $mid:"
  fly machines status "$mid" --app "$APP_NAME" 2>/dev/null | grep -E "(Memory|CPU|State)" || true
done
echo

# ─── Teardown ───
if [ "$SKIP_TEARDOWN" = false ]; then
  echo "━━━ Tearing down ━━━"
  echo "  destroying app $APP_NAME..."
  fly apps destroy "$APP_NAME" --yes 2>/dev/null || true
  echo "  done"
else
  echo "Cluster still running (--skip-teardown)."
  echo "  app: $APP_NAME"
  echo "  url: $APP_URL"
  echo "  teardown: fly apps destroy $APP_NAME --yes"
fi

echo
echo "Done. Results in: $RESULTS_DIR"
ls -la "$RESULTS_DIR/"
