package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeWAL implements metricsWAL with hand-set values.
type fakeWAL struct {
	last     uint64
	shardSeq map[int]uint64
	headTS   map[int]time.Time
	lagBytes map[int]int64
}

func (f *fakeWAL) LastSequence() uint64                        { return f.last }
func (f *fakeWAL) ShardSequence(shardID int) uint64            { return f.shardSeq[shardID] }
func (f *fakeWAL) HeadTimestamp(shardID int) time.Time         { return f.headTS[shardID] }
func (f *fakeWAL) LagBytes(shardID int, afterSeq uint64) int64 { return f.lagBytes[shardID] }

// fakeReplicaState implements metricsReplicaState.
type fakeReplicaState struct {
	pos map[string]uint64
	ts  map[string]time.Time
}

func key(shard int, node string) string {
	return node + "/" + string(rune('0'+shard))
}
func (f *fakeReplicaState) GetPosition(shardID int, nodeID string) uint64 {
	return f.pos[key(shardID, nodeID)]
}
func (f *fakeReplicaState) GetTimestamp(shardID int, nodeID string) time.Time {
	return f.ts[key(shardID, nodeID)]
}

func TestWriteWALMetrics_BasicShape(t *testing.T) {
	headTS := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	appliedTS := headTS.Add(-2 * time.Second)

	wal := &fakeWAL{
		last:     17,
		shardSeq: map[int]uint64{0: 10, 1: 7},
		headTS:   map[int]time.Time{0: headTS, 1: headTS},
		lagBytes: map[int]int64{0: 256, 1: 0},
	}
	rs := &fakeReplicaState{
		pos: map[string]uint64{key(0, "node-2"): 8, key(1, "node-2"): 7},
		ts:  map[string]time.Time{key(0, "node-2"): appliedTS, key(1, "node-2"): headTS},
	}
	pairs := []replicaLagPair{
		{ShardID: 0, Replica: "node-2"},
		{ShardID: 1, Replica: "node-2"},
	}

	var buf bytes.Buffer
	writeWALMetrics(&buf, wal, []int{0, 1}, rs, pairs)
	out := buf.String()

	// Headers and series must be present.
	mustContain := []string{
		"# HELP loveliness_wal_global_sequence",
		"# TYPE loveliness_wal_global_sequence gauge",
		"loveliness_wal_global_sequence 17",
		`loveliness_wal_head_sequence{shard_id="0"} 10`,
		`loveliness_wal_head_sequence{shard_id="1"} 7`,
		"# HELP loveliness_replication_lag_entries",
		`loveliness_replication_lag_entries{shard_id="0",replica_id="node-2"} 2`,
		`loveliness_replication_lag_entries{shard_id="1",replica_id="node-2"} 0`,
		`loveliness_replication_lag_bytes{shard_id="0",replica_id="node-2"} 256`,
		`loveliness_replication_lag_bytes{shard_id="1",replica_id="node-2"} 0`,
		`loveliness_replication_lag_seconds{shard_id="0",replica_id="node-2"} 2`,
		`loveliness_replication_lag_seconds{shard_id="1",replica_id="node-2"} 0`,
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("output missing %q\nfull output:\n%s", s, out)
		}
	}
}

func TestWriteWALMetrics_NoReplicas(t *testing.T) {
	wal := &fakeWAL{
		last:     5,
		shardSeq: map[int]uint64{0: 5},
		headTS:   map[int]time.Time{0: time.Now()},
	}
	var buf bytes.Buffer
	writeWALMetrics(&buf, wal, []int{0}, nil, nil)
	out := buf.String()
	if !strings.Contains(out, "loveliness_wal_global_sequence 5") {
		t.Error("expected wal_global_sequence")
	}
	if strings.Contains(out, "loveliness_replication_lag_entries") {
		t.Errorf("must not emit lag series with no replicas; got:\n%s", out)
	}
}

func TestWriteWALMetrics_StableOrder(t *testing.T) {
	wal := &fakeWAL{
		shardSeq: map[int]uint64{2: 1, 0: 1, 1: 1},
		headTS:   map[int]time.Time{0: {}, 1: {}, 2: {}},
	}
	rs := &fakeReplicaState{}
	// Pass shards out of order to confirm sort.
	pairs := []replicaLagPair{
		{ShardID: 1, Replica: "z"},
		{ShardID: 0, Replica: "b"},
		{ShardID: 0, Replica: "a"},
	}
	var buf bytes.Buffer
	writeWALMetrics(&buf, wal, []int{2, 0, 1}, rs, pairs)
	out := buf.String()

	idxA := strings.Index(out, `replica_id="a"`)
	idxB := strings.Index(out, `replica_id="b"`)
	idxZ := strings.Index(out, `replica_id="z"`)
	if idxA < 0 || idxA >= idxB || idxB >= idxZ {
		t.Errorf("expected stable shard/replica ordering a<b<z; got positions a=%d b=%d z=%d in:\n%s",
			idxA, idxB, idxZ, out)
	}

	// Shard ordering for head_sequence series.
	idx0 := strings.Index(out, `loveliness_wal_head_sequence{shard_id="0"}`)
	idx1 := strings.Index(out, `loveliness_wal_head_sequence{shard_id="1"}`)
	idx2 := strings.Index(out, `loveliness_wal_head_sequence{shard_id="2"}`)
	if idx0 < 0 || idx0 >= idx1 || idx1 >= idx2 {
		t.Errorf("expected stable shard ordering 0<1<2 in head_sequence; got %d %d %d", idx0, idx1, idx2)
	}
}

func TestComputeLagSeconds(t *testing.T) {
	zero := time.Time{}
	head := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	applied := head.Add(-3 * time.Second)

	cases := []struct {
		name    string
		head    time.Time
		applied time.Time
		pos     uint64
		check   func(t *testing.T, got float64)
	}{
		{
			name:    "both timestamps known",
			head:    head,
			applied: applied,
			pos:     5,
			check: func(t *testing.T, got float64) {
				if got != 3.0 {
					t.Errorf("expected 3, got %v", got)
				}
			},
		},
		{
			name:    "replica fresh, head set",
			head:    time.Now().Add(-time.Second),
			applied: zero,
			pos:     0,
			check: func(t *testing.T, got float64) {
				if got <= 0 {
					t.Errorf("expected positive lag for fresh replica, got %v", got)
				}
			},
		},
		{
			name:    "no head",
			head:    zero,
			applied: zero,
			pos:     0,
			check: func(t *testing.T, got float64) {
				if got != 0 {
					t.Errorf("expected 0, got %v", got)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, computeLagSeconds(tc.head, tc.applied, tc.pos))
		})
	}
}

func TestFormatGaugeFloat(t *testing.T) {
	cases := map[float64]string{
		0:        "0",
		1:        "1",
		2.5:      "2.5",
		0.000001: "0.000001",
	}
	for in, want := range cases {
		if got := formatGaugeFloat(in); got != want {
			t.Errorf("formatGaugeFloat(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestMetricsHandler_RoutesAndContentType(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("expected text/plain content type, got %q", ct)
	}
	if !strings.Contains(w.Body.String(), "loveliness_local_shards") {
		t.Errorf("expected loveliness_local_shards in output:\n%s", w.Body.String())
	}
}
