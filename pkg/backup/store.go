package backup

import (
	"io"
	"time"
)

// BackupMeta describes a stored backup.
type BackupMeta struct {
	Key       string    `json:"key"`
	SizeBytes int64     `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
}

// BackupStore is the interface for backup storage backends. Implementations
// handle the mechanics of storing and retrieving compressed backup archives.
type BackupStore interface {
	// Put uploads a backup archive. The key should be unique (e.g., timestamp-based).
	Put(key string, r io.Reader) error
	// Get downloads a backup archive by key.
	Get(key string) (io.ReadCloser, error)
	// List returns all stored backups ordered by creation time (newest first).
	List() ([]BackupMeta, error)
	// Delete removes a backup by key.
	Delete(key string) error
}
