#!/usr/bin/env bash
#
# Benchmark orchestrator: runs Loveliness (single, 3-node) and Neo4j
# side-by-side with identical data, collects latency + resource metrics.
#
# Usage:
#   ./bench/run.sh              # full comparison (all three configs)
#   ./bench/run.sh --quick      # single-node Loveliness only (fast check)
#   ./bench/run.sh --neo4j-only # Neo4j only
#
# Output: bench/results/<timestamp>/ with JSON per config + comparison report.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RESULTS_DIR="$SCRIPT_DIR/results/$TIMESTAMP"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
NODES=50000
EDGES=50000
ITERS=200
WORKERS=8

mkdir -p "$RESULTS_DIR"

# Parse args.
QUICK=false
NEO4J_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=true ;;
    --neo4j-only) NEO4J_ONLY=true ;;
    --nodes=*) NODES="${arg#*=}" ;;
    --edges=*) EDGES="${arg#*=}" ;;
    --iters=*) ITERS="${arg#*=}" ;;
  esac
done

echo "╔══════════════════════════════════════════════╗"
echo "║    Loveliness Benchmark Comparison           ║"
echo "║    nodes: $NODES  edges: $EDGES              ║"
echo "║    iters: $ITERS  workers: $WORKERS          ║"
echo "║    output: $RESULTS_DIR                      ║"
echo "╚══════════════════════════════════════════════╝"
echo

# Build the benchmark binary.
echo "Building benchmark binary..."
cd "$PROJECT_DIR"
CGO_ENABLED=1 go build -o "$SCRIPT_DIR/benchmark-runner" ./cmd/benchmark
echo "  done"
echo

# Collect docker stats for a container, writing peak RSS to a file.
# Usage: collect_stats <container_name> <output_file> &
collect_stats() {
  local container="$1"
  local outfile="$2"
  local peak_rss=0
  local samples=0
  local cpu_sum=0

  while true; do
    local stats
    stats=$(docker stats --no-stream --format '{{.MemUsage}}|{{.CPUPerc}}' "$container" 2>/dev/null) || break
    local mem_str="${stats%%|*}"
    local cpu_str="${stats##*|}"

    # Parse memory (e.g., "123.4MiB / 2GiB" -> extract first number in MiB).
    local mem_val
    mem_val=$(echo "$mem_str" | awk '{
      val = $1+0;
      unit = $1; gsub(/[0-9.]/, "", unit);
      if (unit == "GiB") val = val * 1024;
      if (unit == "KiB") val = val / 1024;
      printf "%.1f", val
    }')

    local cpu_val
    cpu_val=$(echo "$cpu_str" | tr -d '%')

    local mem_int=${mem_val%.*}
    if [ "$mem_int" -gt "$peak_rss" ] 2>/dev/null; then
      peak_rss=$mem_int
    fi

    cpu_sum=$(echo "$cpu_sum + $cpu_val" | bc 2>/dev/null || echo "$cpu_sum")
    samples=$((samples + 1))

    sleep 1
  done

  local avg_cpu=0
  if [ "$samples" -gt 0 ]; then
    avg_cpu=$(echo "scale=1; $cpu_sum / $samples" | bc 2>/dev/null || echo "0")
  fi

  echo "{\"peak_rss_mib\": $peak_rss, \"avg_cpu_pct\": $avg_cpu, \"samples\": $samples}" > "$outfile"
}

# Wait for a health endpoint to return 200.
wait_healthy() {
  local url="$1"
  local name="$2"
  local timeout="${3:-120}"
  local elapsed=0

  echo -n "  waiting for $name..."
  while [ $elapsed -lt $timeout ]; do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo " ready (${elapsed}s)"
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo " TIMEOUT after ${timeout}s"
  return 1
}

# Run benchmark against a target.
run_benchmark() {
  local name="$1"
  local endpoint="$2"
  local target="${3:-loveliness}"
  local extra_flags="${4:-}"

  echo "── Benchmarking: $name ──"
  echo "  endpoint: $endpoint"
  echo "  target:   $target"

  "$SCRIPT_DIR/benchmark-runner" \
    -endpoint "$endpoint" \
    -target "$target" \
    -nodes "$NODES" \
    -edges "$EDGES" \
    -iters "$ITERS" \
    -workers "$WORKERS" \
    -json-out "$RESULTS_DIR/${name}.json" \
    $extra_flags

  echo "  results: $RESULTS_DIR/${name}.json"
  echo
}

# Cleanup helper.
cleanup_config() {
  local config="$1"
  echo "  tearing down $config..."
  docker compose -f "$COMPOSE_FILE" -p "bench-$config" down -v --remove-orphans 2>/dev/null || true
}

# ─── Single-node Loveliness ───
if [ "$NEO4J_ONLY" = false ]; then
  echo "━━━ Configuration: Loveliness (single node) ━━━"
  cleanup_config "single"
  docker compose -f "$COMPOSE_FILE" -p "bench-single" up -d loveliness-single
  wait_healthy "http://localhost:18080/health" "loveliness-single"

  # Start resource collection in background.
  CONTAINER_SINGLE=$(docker compose -f "$COMPOSE_FILE" -p "bench-single" ps -q loveliness-single)
  collect_stats "$CONTAINER_SINGLE" "$RESULTS_DIR/loveliness-single-stats.json" &
  STATS_PID_SINGLE=$!

  run_benchmark "loveliness-single" "http://localhost:18080"

  kill $STATS_PID_SINGLE 2>/dev/null; wait $STATS_PID_SINGLE 2>/dev/null || true
  cleanup_config "single"
  echo
fi

# ─── 3-node Loveliness cluster ───
if [ "$NEO4J_ONLY" = false ] && [ "$QUICK" = false ]; then
  echo "━━━ Configuration: Loveliness (3-node cluster) ━━━"
  cleanup_config "cluster"
  docker compose -f "$COMPOSE_FILE" -p "bench-cluster" up -d loveliness-c1 loveliness-c2 loveliness-c3
  wait_healthy "http://localhost:28080/health" "loveliness-cluster"

  # Collect stats for all three nodes.
  C1=$(docker compose -f "$COMPOSE_FILE" -p "bench-cluster" ps -q loveliness-c1)
  C2=$(docker compose -f "$COMPOSE_FILE" -p "bench-cluster" ps -q loveliness-c2)
  C3=$(docker compose -f "$COMPOSE_FILE" -p "bench-cluster" ps -q loveliness-c3)
  collect_stats "$C1" "$RESULTS_DIR/loveliness-cluster-c1-stats.json" &
  STATS_PID_C1=$!
  collect_stats "$C2" "$RESULTS_DIR/loveliness-cluster-c2-stats.json" &
  STATS_PID_C2=$!
  collect_stats "$C3" "$RESULTS_DIR/loveliness-cluster-c3-stats.json" &
  STATS_PID_C3=$!

  run_benchmark "loveliness-cluster" "http://localhost:28080"

  kill $STATS_PID_C1 $STATS_PID_C2 $STATS_PID_C3 2>/dev/null
  wait $STATS_PID_C1 $STATS_PID_C2 $STATS_PID_C3 2>/dev/null || true
  cleanup_config "cluster"
  echo
fi

# ─── Neo4j Community Edition ───
if [ "$QUICK" = false ]; then
  echo "━━━ Configuration: Neo4j Community Edition ━━━"
  cleanup_config "neo4j"
  docker compose -f "$COMPOSE_FILE" -p "bench-neo4j" up -d neo4j
  wait_healthy "http://localhost:37474" "neo4j" 120

  CONTAINER_NEO4J=$(docker compose -f "$COMPOSE_FILE" -p "bench-neo4j" ps -q neo4j)
  collect_stats "$CONTAINER_NEO4J" "$RESULTS_DIR/neo4j-stats.json" &
  STATS_PID_NEO4J=$!

  run_benchmark "neo4j" "http://localhost:37474" "neo4j"

  kill $STATS_PID_NEO4J 2>/dev/null; wait $STATS_PID_NEO4J 2>/dev/null || true
  cleanup_config "neo4j"
  echo
fi

# ─── Generate comparison ───
echo "━━━ Generating comparison report ━━━"
cd "$PROJECT_DIR"
CGO_ENABLED=1 go run ./cmd/benchmark-charts \
  -results-dir "$RESULTS_DIR" \
  -output-dir "$RESULTS_DIR"

echo
echo "Done. Results in: $RESULTS_DIR"
echo "  JSON:   $RESULTS_DIR/*.json"
echo "  Charts: $RESULTS_DIR/*.svg"
echo "  Report: $RESULTS_DIR/comparison.md"
