package backup

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// Scheduler runs periodic backups and enforces retention policy.
type Scheduler struct {
	mgr      *Manager
	store    BackupStore
	shards   []*shard.Shard
	walSeqFn func() uint64 // returns current WAL sequence

	interval  time.Duration
	retention int // number of backups to keep

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewScheduler creates a backup scheduler.
func NewScheduler(mgr *Manager, store BackupStore, shards []*shard.Shard, walSeqFn func() uint64, interval time.Duration, retention int) *Scheduler {
	if retention < 1 {
		retention = 3
	}
	return &Scheduler{
		mgr:       mgr,
		store:     store,
		shards:    shards,
		walSeqFn:  walSeqFn,
		interval:  interval,
		retention: retention,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the periodic backup loop.
func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.loop()
	slog.Info("backup scheduler started", "interval", s.interval, "retention", s.retention)
}

// Stop signals the scheduler to stop and waits for the current backup to finish.
func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *Scheduler) loop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runBackup()
		}
	}
}

func (s *Scheduler) runBackup() {
	start := time.Now()
	key := fmt.Sprintf("backup-%s.tar.gz", start.UTC().Format("20060102-150405"))

	walSeq := s.walSeqFn()

	var buf bytes.Buffer
	manifest, err := s.mgr.CreateBackup(&buf, s.shards, walSeq)
	if err != nil {
		slog.Error("scheduled backup failed", "err", err)
		return
	}

	if err := s.store.Put(key, &buf); err != nil {
		slog.Error("scheduled backup upload failed", "key", key, "err", err)
		return
	}

	slog.Info("scheduled backup complete",
		"key", key,
		"shards", manifest.ShardCount,
		"wal_seq", manifest.WALSequence,
		"size_mb", buf.Len()/(1024*1024),
		"duration", time.Since(start).Round(time.Millisecond),
	)

	s.enforceRetention()
}

func (s *Scheduler) enforceRetention() {
	metas, err := s.store.List()
	if err != nil {
		slog.Warn("retention: list backups failed", "err", err)
		return
	}

	if len(metas) <= s.retention {
		return
	}

	// Delete oldest backups beyond retention count.
	for _, m := range metas[s.retention:] {
		if err := s.store.Delete(m.Key); err != nil {
			slog.Warn("retention: delete failed", "key", m.Key, "err", err)
		} else {
			slog.Info("retention: deleted old backup", "key", m.Key)
		}
	}
}

// RunNow triggers an immediate backup (useful for API-triggered backups).
func (s *Scheduler) RunNow() (*Manifest, string, error) {
	start := time.Now()
	key := fmt.Sprintf("backup-%s.tar.gz", start.UTC().Format("20060102-150405"))
	walSeq := s.walSeqFn()

	var buf bytes.Buffer
	manifest, err := s.mgr.CreateBackup(&buf, s.shards, walSeq)
	if err != nil {
		return nil, "", err
	}
	if err := s.store.Put(key, &buf); err != nil {
		return nil, "", err
	}
	return manifest, key, nil
}
