package api

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

// handleMetrics serves Prometheus text-format metrics. We hand-roll the
// exposition format (one line per series, "# HELP" + "# TYPE" headers) to
// avoid dragging in the prometheus client_golang dependency just to publish
// a handful of gauges.
//
// Series exposed (issue #7 acceptance criteria):
//
//	loveliness_replication_lag_seconds{shard_id, replica_id}  gauge
//	loveliness_replication_lag_bytes{shard_id, replica_id}    gauge
//	loveliness_replication_lag_entries{shard_id, replica_id}  gauge
//	loveliness_wal_head_sequence{shard_id}                    gauge
//	loveliness_wal_global_sequence                            gauge
//	loveliness_local_shards                                   gauge
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writeMetrics(w, s)
}

// metricsWAL is the slice of *replication.WAL the metrics handler needs. An
// interface lets tests inject lightweight fakes without spinning up a real
// WAL on disk.
type metricsWAL interface {
	LastSequence() uint64
	ShardSequence(shardID int) uint64
	HeadTimestamp(shardID int) time.Time
	LagBytes(shardID int, afterSeq uint64) int64
}

// metricsReplicaState is the slice of *replication.ReplicaState the metrics
// handler needs.
type metricsReplicaState interface {
	GetPosition(shardID int, nodeID string) uint64
	GetTimestamp(shardID int, nodeID string) time.Time
}

// metricsCluster is the slice of *cluster.Cluster the metrics handler
// needs for raft_state. Kept narrow so tests inject fakes without
// standing up a real Raft.
type metricsCluster interface {
	NodeID() string
	Role() string
}

// raftStates is the set of one-hot label values emitted by
// loveliness_raft_state. Listed explicitly so the metric shape doesn't
// drift if we ever add more raft.RaftState values upstream — all known
// states get a 0/1 value at every scrape, alerts can pick any one.
var raftStates = []string{"leader", "follower", "candidate", "shutdown", "unknown"}

// replicaLagPair identifies a (shard, replica) pair the handler should emit.
type replicaLagPair struct {
	ShardID int
	Replica string
}

func writeMetrics(w io.Writer, s *Server) {
	emitHelp(w, "loveliness_local_shards", "Number of shards opened on this node.", "gauge")
	fprintf(w, "loveliness_local_shards %d\n", len(s.shards))

	if s.cluster != nil {
		writeRaftStateMetric(w, s.cluster)
	}

	if s.queryCounters != nil {
		writeQueryCounterMetrics(w, s.queryCounters.Snapshot())
	}

	if s.queryHistogram != nil {
		writeQueryHistogramMetrics(w, s.queryHistogram.Snapshot())
	}

	if s.bulkLoadCounters != nil {
		writeBulkLoadMetrics(w, s.bulkLoadCounters.Snapshot())
	}

	if s.dr == nil || s.dr.WAL == nil {
		return
	}

	shardIDs := make([]int, 0, len(s.shards))
	for _, sh := range s.shards {
		shardIDs = append(shardIDs, sh.ID)
	}

	var pairs []replicaLagPair
	if s.dr.ReplicaState != nil && s.cluster != nil {
		sm := s.cluster.GetShardMap()
		for shardID, assignment := range sm.Assignments {
			for _, r := range assignment.Replicas {
				if r == "" {
					continue
				}
				pairs = append(pairs, replicaLagPair{ShardID: shardID, Replica: r})
			}
		}
	}

	writeWALMetrics(w, s.dr.WAL, shardIDs, s.dr.ReplicaState, pairs)
}

// writeQueryHistogramMetrics emits loveliness_query_duration_seconds in
// the standard Prometheus histogram shape: one `_bucket{...,le="..."}`
// series per cumulative bucket, plus `_sum` and `_count`. Exposition is
// per-(query_type, status) series in stable order.
func writeQueryHistogramMetrics(w io.Writer, snaps []histogramSnapshot) {
	if len(snaps) == 0 {
		return
	}
	emitHelp(w, "loveliness_query_duration_seconds",
		"/cypher request latency in seconds, bucketed by query type and status.", "histogram")
	for _, h := range snaps {
		for _, p := range h.Buckets {
			fprintf(w,
				"loveliness_query_duration_seconds_bucket{query_type=%q,status=%q,le=%q} %d\n",
				h.QueryType, h.Status, formatBucketBound(p.UpperBound), p.Count)
		}
		fprintf(w,
			"loveliness_query_duration_seconds_sum{query_type=%q,status=%q} %s\n",
			h.QueryType, h.Status, formatGaugeFloat(h.Sum))
		fprintf(w,
			"loveliness_query_duration_seconds_count{query_type=%q,status=%q} %d\n",
			h.QueryType, h.Status, h.Count)
	}
}

// formatBucketBound renders a bucket upper-bound exactly the way
// Prometheus expects: positive infinity as "+Inf", everything else as
// a trimmed decimal so dashboards built against the standard exposition
// (`le="0.005"`, `le="+Inf"`) keep working.
func formatBucketBound(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return formatGaugeFloat(v)
}

// writeBulkLoadMetrics emits loveliness_bulk_load_rows_total{table} from
// a deterministic snapshot. Empty snapshots emit nothing — a series only
// appears once a /bulk/* request has loaded at least one row for the table.
func writeBulkLoadMetrics(w io.Writer, samples []bulkLoadSample) {
	if len(samples) == 0 {
		return
	}
	emitHelp(w, "loveliness_bulk_load_rows_total",
		"Cumulative rows loaded via /bulk/* endpoints, by table.", "counter")
	for _, s := range samples {
		fprintf(w, "loveliness_bulk_load_rows_total{table=%q} %d\n", s.Table, s.Count)
	}
}

// writeQueryCounterMetrics emits loveliness_query_total{query_type, status}
// from a deterministic snapshot. Counters that have never observed a
// value still emit nothing — a series only appears once it's been hit
// at least once, matching client_golang's lazy-creation semantics.
func writeQueryCounterMetrics(w io.Writer, samples []queryCounterSample) {
	if len(samples) == 0 {
		return
	}
	emitHelp(w, "loveliness_query_total",
		"Number of /cypher requests by query type and status bucket.", "counter")
	for _, s := range samples {
		fprintf(w, "loveliness_query_total{query_type=%q,status=%q} %d\n",
			s.QueryType, s.Status, s.Count)
	}
}

// writeRaftStateMetric emits loveliness_raft_state as a one-hot gauge:
// one series per known state, value=1 for the active one, 0 for the rest.
// One-hot is the standard Prometheus shape for enum-like metrics — it
// lets dashboards alert on any single state without having to map an int.
func writeRaftStateMetric(w io.Writer, c metricsCluster) {
	emitHelp(w, "loveliness_raft_state",
		"Current Raft role for this node, one-hot encoded across known states.", "gauge")
	current := c.Role()
	nodeID := c.NodeID()
	for _, st := range raftStates {
		v := 0
		if st == current {
			v = 1
		}
		fprintf(w, "loveliness_raft_state{node_id=%q,state=%q} %d\n", nodeID, st, v)
	}
}

// writeWALMetrics emits the WAL- and replica-related series. It's split out
// so tests can drive it with fakes instead of a full Server + cluster.
func writeWALMetrics(w io.Writer, wal metricsWAL, shardIDs []int, rs metricsReplicaState, pairs []replicaLagPair) {
	emitHelp(w, "loveliness_wal_global_sequence", "Monotonic global WAL sequence across all shards.", "gauge")
	fprintf(w, "loveliness_wal_global_sequence %d\n", wal.LastSequence())

	emitHelp(w, "loveliness_wal_head_sequence", "Highest WAL sequence written for a shard.", "gauge")
	sortedShards := append([]int(nil), shardIDs...)
	sort.Ints(sortedShards)
	for _, sid := range sortedShards {
		fprintf(w, "loveliness_wal_head_sequence{shard_id=\"%d\"} %d\n",
			sid, wal.ShardSequence(sid))
	}

	if rs == nil || len(pairs) == 0 {
		return
	}

	sortedPairs := append([]replicaLagPair(nil), pairs...)
	sort.Slice(sortedPairs, func(i, j int) bool {
		if sortedPairs[i].ShardID != sortedPairs[j].ShardID {
			return sortedPairs[i].ShardID < sortedPairs[j].ShardID
		}
		return sortedPairs[i].Replica < sortedPairs[j].Replica
	})

	emitHelp(w, "loveliness_replication_lag_entries", "Replica lag in WAL entries (head_seq - applied_seq).", "gauge")
	for _, p := range sortedPairs {
		head := wal.ShardSequence(p.ShardID)
		pos := rs.GetPosition(p.ShardID, p.Replica)
		var lag uint64
		if head > pos {
			lag = head - pos
		}
		fprintf(w, "loveliness_replication_lag_entries{shard_id=\"%d\",replica_id=%q} %d\n",
			p.ShardID, p.Replica, lag)
	}

	emitHelp(w, "loveliness_replication_lag_bytes", "Replica lag in WAL bytes (sum of unapplied entry sizes on disk).", "gauge")
	for _, p := range sortedPairs {
		pos := rs.GetPosition(p.ShardID, p.Replica)
		fprintf(w, "loveliness_replication_lag_bytes{shard_id=\"%d\",replica_id=%q} %d\n",
			p.ShardID, p.Replica, wal.LagBytes(p.ShardID, pos))
	}

	emitHelp(w, "loveliness_replication_lag_seconds", "Replica lag in seconds (WAL head timestamp - replica's last-applied entry timestamp).", "gauge")
	for _, p := range sortedPairs {
		head := wal.HeadTimestamp(p.ShardID)
		applied := rs.GetTimestamp(p.ShardID, p.Replica)
		pos := rs.GetPosition(p.ShardID, p.Replica)
		seconds := computeLagSeconds(head, applied, pos)
		fprintf(w, "loveliness_replication_lag_seconds{shard_id=\"%d\",replica_id=%q} %s\n",
			p.ShardID, p.Replica, formatGaugeFloat(seconds))
	}
}

// computeLagSeconds mirrors the API JSON formula: when both the head and the
// applied timestamp are known, it's the difference between them; when the
// replica has applied nothing yet but the WAL has data, it's the wall-clock
// age of the WAL head; otherwise zero.
func computeLagSeconds(head, applied time.Time, pos uint64) float64 {
	if !head.IsZero() && !applied.IsZero() {
		return head.Sub(applied).Seconds()
	}
	if !head.IsZero() && pos == 0 {
		return time.Since(head).Seconds()
	}
	return 0.0
}

func emitHelp(w io.Writer, name, help, typ string) {
	fprintf(w, "# HELP %s %s\n", name, help)
	fprintf(w, "# TYPE %s %s\n", name, typ)
}

// fprintf wraps fmt.Fprintf for the metrics writer. We deliberately drop the
// error: the writer is an http.ResponseWriter and there's nothing useful to do
// if the client disconnected mid-stream.
func fprintf(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}

// formatGaugeFloat avoids scientific notation, which the Prometheus text
// parser accepts but downstream tools sometimes mishandle.
func formatGaugeFloat(v float64) string {
	s := fmt.Sprintf("%.6f", v)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" || s == "-" {
		s = "0"
	}
	return s
}
