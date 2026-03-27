package ingest

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockRouter implements ShardRouter for tests.
type mockRouter struct {
	shards int
}

func (m *mockRouter) ResolveShardForKey(key string) int {
	// Simple hash: first byte mod shard count.
	if len(key) == 0 {
		return 0
	}
	return int(key[0]) % m.shards
}

func (m *mockRouter) ShardCount() int { return m.shards }

// mockWriter implements ShardWriter for tests, tracking what was written.
type mockWriter struct {
	mu      sync.Mutex
	calls   []copyCall
	failFor map[int]bool // shardIDs that should fail
}

type copyCall struct {
	ShardID   int
	TableName string
	CSVPath   string
}

func (m *mockWriter) CopyFrom(shardID int, tableName, csvPath string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, copyCall{shardID, tableName, csvPath})
	if m.failFor != nil && m.failFor[shardID] {
		return 0, fmt.Errorf("mock error for shard %d", shardID)
	}
	// Count lines in the CSV (minus header).
	data, err := os.ReadFile(csvPath)
	if err != nil {
		return 0, err
	}
	lines := strings.Count(string(data), "\n") - 1 // subtract header
	if lines < 0 {
		lines = 0
	}
	return int64(lines), nil
}

func TestWorkerProcessesNodeJob(t *testing.T) {
	dir := t.TempDir()
	q, _ := NewQueue(dir)
	router := &mockRouter{shards: 2}
	writer := &mockWriter{}
	w := NewWorker(q, router, writer, 1000)
	w.Start()
	defer w.Stop()

	csv := "name,age\nalice,30\nbob,25\ncharlie,35\n"
	dataPath := writeTestCSV(t, csv)
	headers := map[string]string{"X-Table": "Person", "X-Shard-Key": "name"}
	id, err := q.Enqueue(JobTypeNodesStream, headers, dataPath)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for processing.
	deadline := time.After(5 * time.Second)
	for {
		job, _ := q.Get(id)
		if job.Status == StatusCompleted || job.Status == StatusFailed {
			if job.Status == StatusFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job to complete")
		case <-time.After(50 * time.Millisecond):
		}
	}

	writer.mu.Lock()
	callCount := len(writer.calls)
	writer.mu.Unlock()

	if callCount == 0 {
		t.Error("expected at least one CopyFrom call")
	}
}

func TestWorkerProcessesEdgeJob(t *testing.T) {
	dir := t.TempDir()
	q, _ := NewQueue(dir)
	router := &mockRouter{shards: 2}
	writer := &mockWriter{}
	w := NewWorker(q, router, writer, 1000)
	w.Start()
	defer w.Stop()

	csv := "from,to,weight\nalice,bob,1\nbob,charlie,2\n"
	dataPath := writeTestCSV(t, csv)
	headers := map[string]string{"X-Rel-Table": "KNOWS"}
	id, err := q.Enqueue(JobTypeEdgesStream, headers, dataPath)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.After(5 * time.Second)
	for {
		job, _ := q.Get(id)
		if job.Status == StatusCompleted || job.Status == StatusFailed {
			if job.Status == StatusFailed {
				t.Fatalf("job failed: %s", job.Error)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job to complete")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestWorkerHandlesMissingHeaders(t *testing.T) {
	dir := t.TempDir()
	q, _ := NewQueue(dir)
	router := &mockRouter{shards: 2}
	writer := &mockWriter{}
	w := NewWorker(q, router, writer, 1000)
	w.Start()
	defer w.Stop()

	// Node job without required headers should fail.
	dataPath := writeTestCSV(t, "name\nalice\n")
	id, _ := q.Enqueue(JobTypeNodesStream, map[string]string{}, dataPath)

	deadline := time.After(5 * time.Second)
	for {
		job, _ := q.Get(id)
		if job.Status == StatusFailed {
			if !strings.Contains(job.Error, "missing") {
				t.Errorf("expected 'missing' in error, got: %s", job.Error)
			}
			break
		}
		if job.Status == StatusCompleted {
			t.Fatal("expected job to fail, but it completed")
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job to fail")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestWorkerSequentialProcessing(t *testing.T) {
	dir := t.TempDir()
	q, _ := NewQueue(dir)
	router := &mockRouter{shards: 1}
	writer := &mockWriter{}
	w := NewWorker(q, router, writer, 1000)
	w.Start()
	defer w.Stop()

	// Enqueue 3 jobs.
	var ids []string
	for i := 0; i < 3; i++ {
		csv := fmt.Sprintf("name\nuser-%d\n", i)
		dataPath := writeTestCSV(t, csv)
		headers := map[string]string{"X-Table": "Person", "X-Shard-Key": "name"}
		id, _ := q.Enqueue(JobTypeNodesStream, headers, dataPath)
		ids = append(ids, id)
	}

	// Wait for all to complete.
	deadline := time.After(10 * time.Second)
	for {
		allDone := true
		for _, id := range ids {
			job, _ := q.Get(id)
			if job.Status != StatusCompleted && job.Status != StatusFailed {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for all jobs")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// All should be completed.
	for _, id := range ids {
		job, _ := q.Get(id)
		if job.Status != StatusCompleted {
			t.Errorf("job %s: expected completed, got %s (error: %s)", id, job.Status, job.Error)
		}
	}
}
