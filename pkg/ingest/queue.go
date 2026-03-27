package ingest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// JobStatus represents the lifecycle of an ingest job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
)

// JobType identifies what kind of bulk operation a job performs.
type JobType string

const (
	JobTypeNodes       JobType = "nodes"
	JobTypeEdges       JobType = "edges"
	JobTypeNodesStream JobType = "nodes_stream"
	JobTypeEdgesStream JobType = "edges_stream"
)

// Job is a queued ingest operation with durable state on disk.
type Job struct {
	ID        string            `json:"id"`
	Type      JobType           `json:"type"`
	Status    JobStatus         `json:"status"`
	Headers   map[string]string `json:"headers"`
	DataFile  string            `json:"data_file"`
	CreatedAt time.Time         `json:"created_at"`
	StartedAt *time.Time        `json:"started_at,omitempty"`
	DoneAt    *time.Time        `json:"done_at,omitempty"`
	Loaded    int64             `json:"loaded"`
	Errors    []string          `json:"errors,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// Queue is a durable, log-backed ingest queue. Each job is a directory under
// the queue root containing meta.json (state) and data.csv (payload). Jobs
// are processed sequentially to respect LadybugDB's single-writer constraint.
type Queue struct {
	dir string
	mu  sync.RWMutex
	// notify signals the worker that a new job is available.
	notify chan struct{}
}

// NewQueue creates a queue backed by the given directory.
func NewQueue(dir string) (*Queue, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create queue dir: %w", err)
	}
	return &Queue{
		dir:    dir,
		notify: make(chan struct{}, 1),
	}, nil
}

// Enqueue writes a new job to disk and returns its ID. The CSV data has
// already been written to dataPath by the caller (the API handler writes
// the request body to a temp file before enqueuing).
func (q *Queue) Enqueue(jobType JobType, headers map[string]string, dataPath string) (string, error) {
	id := generateJobID()
	jobDir := filepath.Join(q.dir, id)
	if err := os.MkdirAll(jobDir, 0750); err != nil {
		return "", err
	}

	// Move the data file into the job directory for co-location.
	destData := filepath.Join(jobDir, "data.csv")
	if err := os.Rename(dataPath, destData); err != nil {
		// Rename fails across filesystems — fall back to copy.
		if err := copyFile(dataPath, destData); err != nil {
			return "", fmt.Errorf("move data file: %w", err)
		}
		os.Remove(dataPath)
	}

	job := &Job{
		ID:        id,
		Type:      jobType,
		Status:    StatusPending,
		Headers:   headers,
		DataFile:  destData,
		CreatedAt: time.Now(),
	}

	if err := q.writeMeta(job); err != nil {
		return "", err
	}

	// Non-blocking signal to the worker.
	select {
	case q.notify <- struct{}{}:
	default:
	}

	return id, nil
}

// Get returns the current state of a job.
func (q *Queue) Get(id string) (*Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.readMeta(id)
}

// List returns all jobs ordered by creation time (newest first).
func (q *Queue) List() ([]*Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, err
	}

	var jobs []*Job
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		job, err := q.readMeta(e.Name())
		if err != nil {
			continue
		}
		jobs = append(jobs, job)
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	return jobs, nil
}

// Pending returns the next pending job (oldest first), or nil.
func (q *Queue) Pending() *Job {
	q.mu.RLock()
	defer q.mu.RUnlock()

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil
	}

	var pending []*Job
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		job, err := q.readMeta(e.Name())
		if err != nil || job.Status != StatusPending {
			continue
		}
		pending = append(pending, job)
	}

	if len(pending) == 0 {
		return nil
	}

	sort.Slice(pending, func(i, j int) bool {
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})
	return pending[0]
}

// UpdateStatus atomically updates a job's status and result fields.
func (q *Queue) UpdateStatus(id string, status JobStatus, loaded int64, errors []string, errMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, err := q.readMeta(id)
	if err != nil {
		return err
	}

	now := time.Now()
	job.Status = status
	job.Loaded = loaded
	job.Errors = errors
	job.Error = errMsg

	switch status {
	case StatusRunning:
		job.StartedAt = &now
	case StatusCompleted, StatusFailed:
		job.DoneAt = &now
	}

	return q.writeMeta(job)
}

// Cleanup removes completed/failed jobs older than the given age.
func (q *Queue) Cleanup(maxAge time.Duration) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return 0, err
	}

	cutoff := time.Now().Add(-maxAge)
	removed := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		job, err := q.readMeta(e.Name())
		if err != nil {
			continue
		}
		if (job.Status == StatusCompleted || job.Status == StatusFailed) && job.CreatedAt.Before(cutoff) {
			os.RemoveAll(filepath.Join(q.dir, e.Name()))
			removed++
		}
	}
	return removed, nil
}

// NotifyCh returns the channel signalled when new jobs arrive.
func (q *Queue) NotifyCh() <-chan struct{} {
	return q.notify
}

func (q *Queue) readMeta(id string) (*Job, error) {
	data, err := os.ReadFile(filepath.Join(q.dir, id, "meta.json"))
	if err != nil {
		return nil, err
	}
	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (q *Queue) writeMeta(job *Job) error {
	data, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(q.dir, job.ID, "meta.json"), data, 0640)
}

func generateJobID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102-150405"), hex.EncodeToString(b))
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0640)
}
