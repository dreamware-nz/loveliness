package ingest

import (
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ShardRouter resolves a key to a shard ID and returns the shard count.
type ShardRouter interface {
	ResolveShardForKey(key string) int
	ShardCount() int
}

// ShardWriter executes a COPY FROM on a shard.
type ShardWriter interface {
	CopyFrom(shardID int, tableName, csvPath string) (int64, error)
}

// Worker processes ingest jobs from the queue sequentially. Sequential
// processing respects LadybugDB's single-writer constraint and avoids
// lock contention on the CGo boundary.
type Worker struct {
	queue      *Queue
	router     ShardRouter
	writer     ShardWriter
	flushSize  int
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// NewWorker creates an ingest worker.
func NewWorker(queue *Queue, router ShardRouter, writer ShardWriter, flushSize int) *Worker {
	if flushSize < 1000 {
		flushSize = 50_000
	}
	return &Worker{
		queue:     queue,
		router:    router,
		writer:    writer,
		flushSize: flushSize,
		stopCh:    make(chan struct{}),
	}
}

// Start begins processing the queue. Call Stop() for graceful shutdown.
func (w *Worker) Start() {
	w.wg.Add(1)
	go w.loop()
	slog.Info("ingest worker started", "flush_size", w.flushSize)
}

// Stop waits for the current job to finish, then exits.
func (w *Worker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
}

func (w *Worker) loop() {
	defer w.wg.Done()

	// Drain any pending jobs on startup.
	w.processAll()

	for {
		select {
		case <-w.stopCh:
			return
		case <-w.queue.NotifyCh():
			w.processAll()
		}
	}
}

func (w *Worker) processAll() {
	for {
		job := w.queue.Pending()
		if job == nil {
			return
		}

		select {
		case <-w.stopCh:
			return
		default:
		}

		w.processJob(job)
	}
}

func (w *Worker) processJob(job *Job) {
	slog.Info("ingest: processing job", "id", job.ID, "type", job.Type)
	_ = w.queue.UpdateStatus(job.ID, StatusRunning, 0, nil, "")

	start := time.Now()
	var loaded int64
	var errs []string
	var errMsg string

	switch job.Type {
	case JobTypeNodes, JobTypeNodesStream:
		loaded, errs, errMsg = w.processNodes(job)
	case JobTypeEdges, JobTypeEdgesStream:
		loaded, errs, errMsg = w.processEdges(job)
	default:
		errMsg = fmt.Sprintf("unknown job type: %s", job.Type)
	}

	status := StatusCompleted
	if errMsg != "" {
		status = StatusFailed
	}
	_ = w.queue.UpdateStatus(job.ID, status, loaded, errs, errMsg)

	slog.Info("ingest: job done",
		"id", job.ID,
		"status", status,
		"loaded", loaded,
		"errors", len(errs),
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

func (w *Worker) processNodes(job *Job) (int64, []string, string) {
	tableName := job.Headers["X-Table"]
	shardKey := job.Headers["X-Shard-Key"]
	if tableName == "" || shardKey == "" {
		return 0, nil, "missing X-Table or X-Shard-Key header"
	}

	f, err := os.Open(job.DataFile)
	if err != nil {
		return 0, nil, fmt.Sprintf("open data file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return 0, nil, fmt.Sprintf("read CSV header: %v", err)
	}

	keyIdx := findCol(header, shardKey)
	if keyIdx == -1 {
		return 0, nil, fmt.Sprintf("shard key column '%s' not found in header %v", shardKey, header)
	}

	shardCount := w.router.ShardCount()
	headerLine := encodeCSV(header)

	buckets := make([][]string, shardCount)
	var totalLoaded atomic.Int64
	var allErrors []string
	var errMu sync.Mutex

	flush := func(sid int) {
		if len(buckets[sid]) == 0 {
			return
		}
		rows := buckets[sid]
		buckets[sid] = nil

		loaded, errs := w.flushToShard(tableName, headerLine, sid, rows)
		totalLoaded.Add(loaded)
		if len(errs) > 0 {
			errMu.Lock()
			allErrors = append(allErrors, errs...)
			errMu.Unlock()
		}
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(row) != len(header) {
			continue
		}

		sid := w.router.ResolveShardForKey(row[keyIdx])
		buckets[sid] = append(buckets[sid], encodeCSV(row))

		if len(buckets[sid]) >= w.flushSize {
			flush(sid)
		}
	}

	for i := 0; i < shardCount; i++ {
		flush(i)
	}

	return totalLoaded.Load(), allErrors, ""
}

func (w *Worker) processEdges(job *Job) (int64, []string, string) {
	relTable := job.Headers["X-Rel-Table"]
	if relTable == "" {
		return 0, nil, "missing X-Rel-Table header"
	}

	f, err := os.Open(job.DataFile)
	if err != nil {
		return 0, nil, fmt.Sprintf("open data file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return 0, nil, fmt.Sprintf("read CSV header: %v", err)
	}
	if len(header) < 2 {
		return 0, nil, "edge CSV must have at least 2 columns"
	}

	shardCount := w.router.ShardCount()
	headerLine := encodeCSV(header)

	buckets := make([][]string, shardCount)
	var totalLoaded atomic.Int64
	var allErrors []string
	var errMu sync.Mutex

	flush := func(sid int) {
		if len(buckets[sid]) == 0 {
			return
		}
		rows := buckets[sid]
		buckets[sid] = nil

		loaded, errs := w.flushToShard(relTable, headerLine, sid, rows)
		totalLoaded.Add(loaded)
		if len(errs) > 0 {
			errMu.Lock()
			allErrors = append(allErrors, errs...)
			errMu.Unlock()
		}
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(row) < 2 {
			continue
		}

		fromShard := w.router.ResolveShardForKey(row[0])
		buckets[fromShard] = append(buckets[fromShard], encodeCSV(row))

		if len(buckets[fromShard]) >= w.flushSize {
			flush(fromShard)
		}
	}

	for i := 0; i < shardCount; i++ {
		flush(i)
	}

	return totalLoaded.Load(), allErrors, ""
}

func (w *Worker) flushToShard(tableName, headerLine string, shardID int, rows []string) (int64, []string) {
	f, err := os.CreateTemp("", fmt.Sprintf("loveliness-ingest-%s-s%d-*.csv", tableName, shardID))
	if err != nil {
		return 0, []string{fmt.Sprintf("shard %d: temp file: %v", shardID, err)}
	}
	f.WriteString(headerLine)
	for _, row := range rows {
		f.WriteString(row)
	}
	tmpPath := f.Name()
	f.Close()
	defer os.Remove(tmpPath)

	loaded, err := w.writer.CopyFrom(shardID, tableName, tmpPath)
	if err != nil {
		return 0, []string{fmt.Sprintf("shard %d: %v", shardID, err)}
	}
	return loaded, nil
}

func findCol(header []string, name string) int {
	for i, h := range header {
		if strings.EqualFold(h, name) {
			return i
		}
	}
	return -1
}

func encodeCSV(record []string) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	w.Write(record)
	w.Flush()
	return b.String()
}
