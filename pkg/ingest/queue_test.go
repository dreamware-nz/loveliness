package ingest

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestCSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "ingest-test-*.csv")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(content)
	f.Close()
	return f.Name()
}

func TestQueueEnqueueAndGet(t *testing.T) {
	q, err := NewQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	dataPath := writeTestCSV(t, "name,age\nalice,30\nbob,25\n")
	headers := map[string]string{"X-Table": "Person", "X-Shard-Key": "name"}

	id, err := q.Enqueue(JobTypeNodesStream, headers, dataPath)
	if err != nil {
		t.Fatal("Enqueue:", err)
	}
	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	job, err := q.Get(id)
	if err != nil {
		t.Fatal("Get:", err)
	}
	if job.Status != StatusPending {
		t.Errorf("expected pending, got %s", job.Status)
	}
	if job.Type != JobTypeNodesStream {
		t.Errorf("expected nodes_stream, got %s", job.Type)
	}
	if job.Headers["X-Table"] != "Person" {
		t.Errorf("expected Person, got %s", job.Headers["X-Table"])
	}

	// Data file should have been moved into the job directory.
	if _, err := os.Stat(job.DataFile); os.IsNotExist(err) {
		t.Error("data file not found at", job.DataFile)
	}
}

func TestQueueList(t *testing.T) {
	q, err := NewQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		dataPath := writeTestCSV(t, "col\nval\n")
		q.Enqueue(JobTypeNodesStream, map[string]string{"X-Table": "T"}, dataPath)
		time.Sleep(5 * time.Millisecond)
	}

	jobs, err := q.List()
	if err != nil {
		t.Fatal("List:", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}
	// Newest first.
	if jobs[0].CreatedAt.Before(jobs[2].CreatedAt) {
		t.Error("expected newest first ordering")
	}
}

func TestQueuePending(t *testing.T) {
	q, err := NewQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Empty queue returns nil.
	if job := q.Pending(); job != nil {
		t.Error("expected nil for empty queue")
	}

	dataPath := writeTestCSV(t, "col\nval\n")
	id, _ := q.Enqueue(JobTypeNodesStream, map[string]string{}, dataPath)

	job := q.Pending()
	if job == nil {
		t.Fatal("expected a pending job")
	}
	if job.ID != id {
		t.Errorf("expected job %s, got %s", id, job.ID)
	}

	// Mark as running — should no longer appear as pending.
	q.UpdateStatus(id, StatusRunning, 0, nil, "")
	if job := q.Pending(); job != nil {
		t.Error("running job should not appear as pending")
	}
}

func TestQueueUpdateStatus(t *testing.T) {
	q, err := NewQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	dataPath := writeTestCSV(t, "col\nval\n")
	id, _ := q.Enqueue(JobTypeEdgesStream, map[string]string{"X-Rel-Table": "KNOWS"}, dataPath)

	q.UpdateStatus(id, StatusRunning, 0, nil, "")
	job, _ := q.Get(id)
	if job.Status != StatusRunning {
		t.Errorf("expected running, got %s", job.Status)
	}
	if job.StartedAt == nil {
		t.Error("expected StartedAt to be set")
	}

	q.UpdateStatus(id, StatusCompleted, 1000, nil, "")
	job, _ = q.Get(id)
	if job.Status != StatusCompleted {
		t.Errorf("expected completed, got %s", job.Status)
	}
	if job.Loaded != 1000 {
		t.Errorf("expected 1000 loaded, got %d", job.Loaded)
	}
	if job.DoneAt == nil {
		t.Error("expected DoneAt to be set")
	}
}

func TestQueueCleanup(t *testing.T) {
	q, err := NewQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Create 3 jobs, complete them all.
	for i := 0; i < 3; i++ {
		dataPath := writeTestCSV(t, "col\nval\n")
		id, _ := q.Enqueue(JobTypeNodesStream, map[string]string{}, dataPath)
		q.UpdateStatus(id, StatusCompleted, 10, nil, "")
	}

	// Cleanup with 0 maxAge should remove all completed jobs.
	removed, err := q.Cleanup(0)
	if err != nil {
		t.Fatal("Cleanup:", err)
	}
	if removed != 3 {
		t.Errorf("expected 3 removed, got %d", removed)
	}

	jobs, _ := q.List()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs after cleanup, got %d", len(jobs))
	}
}

func TestQueueNotify(t *testing.T) {
	q, err := NewQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	dataPath := writeTestCSV(t, "col\nval\n")
	q.Enqueue(JobTypeNodesStream, map[string]string{}, dataPath)

	// Should be able to receive notification without blocking.
	select {
	case <-q.NotifyCh():
		// good
	case <-time.After(100 * time.Millisecond):
		t.Error("expected notification on enqueue")
	}
}

func TestQueueDurability(t *testing.T) {
	dir := t.TempDir()

	// Create a queue and enqueue a job.
	q1, _ := NewQueue(dir)
	dataPath := writeTestCSV(t, "name\nalice\n")
	id, _ := q1.Enqueue(JobTypeNodesStream, map[string]string{"X-Table": "Person"}, dataPath)

	// Create a new queue instance from the same directory (simulates restart).
	q2, _ := NewQueue(dir)
	job, err := q2.Get(id)
	if err != nil {
		t.Fatal("durable Get after restart:", err)
	}
	if job.Status != StatusPending {
		t.Errorf("expected pending after restart, got %s", job.Status)
	}

	// Data file should still exist.
	expectedData := filepath.Join(dir, id, "data.csv")
	if _, err := os.Stat(expectedData); os.IsNotExist(err) {
		t.Error("data file missing after restart")
	}
}
