package backup

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// Manifest describes the contents of a backup archive.
//
// Version 1: shard files + Loveliness WAL only.
// Version 2: also includes Kuzu per-shard WAL files (shard-N.wal) and
//
//	the Raft directory (raft/...) — without these, the FSM
//	state (database catalog, shard map, schema annotations)
//	and any unflushed Kuzu writes are lost on restore.
type Manifest struct {
	Version     int         `json:"version"`
	CreatedAt   time.Time   `json:"created_at"`
	NodeID      string      `json:"node_id"`
	ShardCount  int         `json:"shard_count"`
	WALSequence uint64      `json:"wal_sequence"`
	Shards      []ShardInfo `json:"shards"`
	// IncludesRaft is true when the archive carries the FSM state.
	// Restored archives without it will come up with an empty FSM —
	// useful only as a data-only restore.
	IncludesRaft bool `json:"includes_raft,omitempty"`
}

// Snapshotter is anything that can flush the Raft FSM to disk before
// the backup pipeline archives the raft/ directory. The Cluster type
// implements this; a nil Snapshotter is allowed (skips the explicit
// snapshot — Raft's own snapshot store is still archived as-is).
type Snapshotter interface {
	TakeSnapshot() error
}

// ShardInfo describes a single shard in a backup.
type ShardInfo struct {
	ID       int    `json:"id"`
	SizeBytes int64 `json:"size_bytes"`
}

// Manager handles online backup and restore of shard data.
type Manager struct {
	dataDir string
	nodeID  string
}

// NewManager creates a backup manager.
func NewManager(dataDir, nodeID string) *Manager {
	return &Manager{dataDir: dataDir, nodeID: nodeID}
}

// CreateBackup creates a compressed tar archive of the cluster's
// durable state: each shard's main database file and its Kuzu WAL,
// the Loveliness replication WAL, and the Raft directory (FSM
// snapshot + log). Pass a non-nil snap to force a fresh FSM snapshot
// before archiving — recommended for any cluster that mutates state
// faster than the Raft snapshot threshold. The archive is written to
// the given writer.
func (m *Manager) CreateBackup(w io.Writer, shards []*shard.Shard, walSeq uint64, snap Snapshotter) (*Manifest, error) {
	manifest := &Manifest{
		Version:     2,
		CreatedAt:   time.Now(),
		NodeID:      m.nodeID,
		ShardCount:  len(shards),
		WALSequence: walSeq,
	}

	// Force a Raft snapshot first so the snapshot store contains the
	// latest FSM state — without this, a quiet cluster archives only
	// the BoltDB log, and Restore has to replay every command.
	if snap != nil {
		if err := snap.TakeSnapshot(); err != nil {
			slog.Warn("backup: raft snapshot failed", "err", err)
			// Continue — the existing snapshot store + log still get
			// archived, which is enough for most cases.
		}
	}

	gw := gzip.NewWriter(w)
	defer func() { _ = gw.Close() }()
	tw := tar.NewWriter(gw)
	defer func() { _ = tw.Close() }()

	// Snapshot each shard database. Kuzu uses two files per shard:
	// `shard-N` (main DB) and `shard-N.wal` (its own WAL). Both are
	// required — restoring just the main file silently loses every
	// write made since the last Kuzu checkpoint, including schema DDL.
	for _, s := range shards {
		shardName := fmt.Sprintf("shard-%d", s.ID)
		shardPath := filepath.Join(m.dataDir, shardName)

		// Quiesce the shard with a no-op read so any in-flight writes
		// complete. Kuzu commits flush to the WAL synchronously, so
		// once this returns, both files on disk are consistent.
		if _, err := s.Query("RETURN 1"); err != nil {
			slog.Warn("backup: quiesce query failed", "shard", s.ID, "err", err)
		}

		info, err := addFileToTar(tw, shardPath, shardName)
		if err != nil {
			return nil, fmt.Errorf("backup shard %d: %w", s.ID, err)
		}
		manifest.Shards = append(manifest.Shards, ShardInfo{
			ID:        s.ID,
			SizeBytes: info,
		})

		// Kuzu WAL — best-effort, may be absent on a freshly checkpointed shard.
		walName := shardName + ".wal"
		walPath := filepath.Join(m.dataDir, walName)
		if _, err := os.Stat(walPath); err == nil {
			if _, err := addFileToTar(tw, walPath, walName); err != nil {
				return nil, fmt.Errorf("backup shard %d wal: %w", s.ID, err)
			}
		}
		slog.Info("backup: shard archived", "shard", s.ID, "size", info)
	}

	// Loveliness replication WAL.
	walDir := filepath.Join(m.dataDir, "wal")
	if entries, err := filepath.Glob(filepath.Join(walDir, "wal-shard-*.log")); err == nil {
		for _, walPath := range entries {
			name := filepath.Join("wal", filepath.Base(walPath))
			if _, err := addFileToTar(tw, walPath, name); err != nil {
				slog.Warn("backup: WAL file failed", "path", walPath, "err", err)
			}
		}
	}

	// Raft directory: snapshot store + bolt log. This carries the FSM
	// state — shard map, database catalog, schema annotations.
	raftDir := filepath.Join(m.dataDir, "raft")
	if _, err := os.Stat(raftDir); err == nil {
		if err := addDirToTar(tw, raftDir, "raft"); err != nil {
			return nil, fmt.Errorf("backup raft dir: %w", err)
		}
		manifest.IncludesRaft = true
	}

	// Write manifest as the last entry.
	manifestData, _ := json.MarshalIndent(manifest, "", "  ")
	if err := tw.WriteHeader(&tar.Header{
		Name:    "manifest.json",
		Size:    int64(len(manifestData)),
		Mode:    0640,
		ModTime: time.Now(),
	}); err != nil {
		return nil, fmt.Errorf("write manifest header: %w", err)
	}
	if _, err := tw.Write(manifestData); err != nil {
		return nil, fmt.Errorf("write manifest data: %w", err)
	}

	return manifest, nil
}

// RestoreBackup extracts a backup archive into the data directory.
// The server must be stopped before calling this. If the archive
// includes a raft/ directory (manifest version >= 2), the existing
// raft/ directory is wiped first to avoid stale FSM log entries
// mixing with the restored snapshot — Raft cannot reconcile state
// from two different lineages.
func (m *Manager) RestoreBackup(r io.Reader) (*Manifest, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gr.Close() }()
	tr := tar.NewReader(gr)

	cleanedRaft := false
	var manifest *Manifest
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar read: %w", err)
		}

		// Path-traversal guard: reject absolute paths and anything that
		// resolves outside the data dir.
		clean := filepath.Clean(filepath.FromSlash(header.Name))
		if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
			return nil, fmt.Errorf("refusing tar entry with unsafe path: %q", header.Name)
		}

		if clean == "manifest.json" {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("read manifest: %w", err)
			}
			manifest = &Manifest{}
			if err := json.Unmarshal(data, manifest); err != nil {
				return nil, fmt.Errorf("parse manifest: %w", err)
			}
			continue
		}

		// First time we see a raft/ entry, wipe the existing dir so the
		// restored snapshot/log doesn't get tangled with stale state.
		if !cleanedRaft && strings.HasPrefix(clean, "raft"+string(filepath.Separator)) {
			raftDir := filepath.Join(m.dataDir, "raft")
			if err := os.RemoveAll(raftDir); err != nil {
				return nil, fmt.Errorf("clean raft dir: %w", err)
			}
			cleanedRaft = true
		}

		// Extract file to data directory.
		destPath := filepath.Join(m.dataDir, clean)
		destDir := filepath.Dir(destPath)
		if err := os.MkdirAll(destDir, 0750); err != nil {
			return nil, fmt.Errorf("create dir for %s: %w", header.Name, err)
		}

		f, err := os.Create(destPath)
		if err != nil {
			return nil, fmt.Errorf("create %s: %w", header.Name, err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return nil, fmt.Errorf("write %s: %w", header.Name, err)
		}
		f.Close()
		slog.Info("restore: extracted", "file", clean, "size", header.Size)
	}

	if manifest == nil {
		return nil, fmt.Errorf("backup archive missing manifest.json")
	}
	return manifest, nil
}

// addFileToTar adds a file to a tar writer and returns the file size.
func addFileToTar(tw *tar.Writer, srcPath, archiveName string) (int64, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}

	if err := tw.WriteHeader(&tar.Header{
		Name:    archiveName,
		Size:    stat.Size(),
		Mode:    int64(stat.Mode()),
		ModTime: stat.ModTime(),
	}); err != nil {
		return 0, err
	}

	if _, err := io.Copy(tw, f); err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// addDirToTar walks srcDir recursively, adding every regular file
// found beneath it to the tar writer with paths rooted at archivePrefix.
// Symlinks and special files are skipped.
func addDirToTar(tw *tar.Writer, srcDir, archivePrefix string) error {
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		archiveName := filepath.ToSlash(filepath.Join(archivePrefix, rel))
		_, err = addFileToTar(tw, path, archiveName)
		return err
	})
}
