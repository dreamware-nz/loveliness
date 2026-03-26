package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WALEntry is a single write operation recorded in the intent log.
type WALEntry struct {
	Sequence  uint64    `json:"seq"`
	ShardID   int       `json:"shard_id"`
	Cypher    string    `json:"cypher"`
	Timestamp time.Time `json:"ts"`
}

// WAL (Write-Ahead Log) provides durable, ordered recording of all write
// operations per shard. Replicas use it to catch up after downtime instead
// of requiring a full rebuild.
//
// On-disk format: each entry is a 4-byte big-endian length prefix followed
// by the JSON-encoded WALEntry. This allows sequential reads without a
// framing protocol.
type WAL struct {
	mu       sync.Mutex
	dir      string
	files    map[int]*walFile // shardID → open file
	sequence uint64           // global monotonic sequence number
}

type walFile struct {
	f   *os.File
	seq uint64 // last sequence written to this file
}

// NewWAL creates or opens a WAL in the given directory.
// Each shard gets its own WAL file: wal-shard-0.log, wal-shard-1.log, etc.
func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	w := &WAL{
		dir:   dir,
		files: make(map[int]*walFile),
	}
	// Recover sequence number from existing files.
	if err := w.recover(); err != nil {
		return nil, fmt.Errorf("WAL recovery: %w", err)
	}
	return w, nil
}

// Append writes an entry to the WAL for the given shard. Returns the
// assigned sequence number. The entry is fsync'd before returning.
func (w *WAL) Append(shardID int, cypher string) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sequence++
	entry := WALEntry{
		Sequence:  w.sequence,
		ShardID:   shardID,
		Cypher:    cypher,
		Timestamp: time.Now(),
	}

	wf, err := w.getOrCreateFile(shardID)
	if err != nil {
		return 0, err
	}

	if err := writeEntry(wf.f, &entry); err != nil {
		return 0, fmt.Errorf("WAL write shard %d: %w", shardID, err)
	}
	if err := wf.f.Sync(); err != nil {
		return 0, fmt.Errorf("WAL sync shard %d: %w", shardID, err)
	}
	wf.seq = w.sequence
	return w.sequence, nil
}

// ReadFrom returns all WAL entries for a shard with sequence > afterSeq.
// Used by replicas to catch up from a known position.
func (w *WAL) ReadFrom(shardID int, afterSeq uint64) ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := w.shardPath(shardID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var entries []WALEntry
	for {
		entry, err := readEntry(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, fmt.Errorf("WAL read shard %d: %w", shardID, err)
		}
		if entry.Sequence > afterSeq {
			entries = append(entries, *entry)
		}
	}
	return entries, nil
}

// LastSequence returns the highest sequence number written to any shard.
func (w *WAL) LastSequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}

// ShardSequence returns the last sequence written for a specific shard.
func (w *WAL) ShardSequence(shardID int) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if wf, ok := w.files[shardID]; ok {
		return wf.seq
	}
	return 0
}

// Truncate removes all entries with sequence <= upToSeq from a shard's WAL.
// This is called after a backup or when entries are no longer needed for
// replica catch-up.
func (w *WAL) Truncate(shardID int, upToSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := w.shardPath(shardID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Read entries to keep.
	var keep []WALEntry
	for {
		entry, err := readEntry(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return err
		}
		if entry.Sequence > upToSeq {
			keep = append(keep, *entry)
		}
	}
	f.Close()

	// Close the existing file handle if open.
	if wf, ok := w.files[shardID]; ok {
		wf.f.Close()
		delete(w.files, shardID)
	}

	// Rewrite the file with only the kept entries.
	tmpPath := path + ".tmp"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	for i := range keep {
		if err := writeEntry(tmp, &keep[i]); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	tmp.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

// Close flushes and closes all WAL files.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var firstErr error
	for _, wf := range w.files {
		if err := wf.f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := wf.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.files = make(map[int]*walFile)
	return firstErr
}

func (w *WAL) shardPath(shardID int) string {
	return filepath.Join(w.dir, fmt.Sprintf("wal-shard-%d.log", shardID))
}

func (w *WAL) getOrCreateFile(shardID int) (*walFile, error) {
	if wf, ok := w.files[shardID]; ok {
		return wf, nil
	}
	path := w.shardPath(shardID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return nil, err
	}
	wf := &walFile{f: f}
	w.files[shardID] = wf
	return wf, nil
}

// recover scans all existing WAL files to find the highest sequence number.
func (w *WAL) recover() error {
	entries, err := filepath.Glob(filepath.Join(w.dir, "wal-shard-*.log"))
	if err != nil {
		return err
	}
	for _, path := range entries {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		for {
			entry, err := readEntry(f)
			if err != nil {
				break
			}
			if entry.Sequence > w.sequence {
				w.sequence = entry.Sequence
			}
		}
		f.Close()
	}
	return nil
}

// writeEntry writes a length-prefixed JSON entry to a file.
func writeEntry(f *os.File, entry *WALEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := f.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = f.Write(data)
	return err
}

// readEntry reads a length-prefixed JSON entry from a reader.
func readEntry(r io.Reader) (*WALEntry, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(lenBuf[:])
	if size > 10<<20 { // 10MB sanity limit
		return nil, fmt.Errorf("WAL entry too large: %d bytes", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	var entry WALEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}
