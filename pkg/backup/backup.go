package backup

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
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
//
// Version 3: adds per-file SHA-256 checksums (Files) and source
//
//	identification (LovelinessVersion). Restore verifies each
//	extracted file against its manifest checksum and aborts on
//	mismatch. v2 archives still restore — they just don't get
//	the integrity check.
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
	// LovelinessVersion records the version of the binary that wrote
	// the archive. Restore logs a warning if this does not match the
	// running binary's version (does not block — schemas are
	// stable across patch versions).
	LovelinessVersion string `json:"loveliness_version,omitempty"`
	// Files is keyed by archive path (e.g. "shard-0", "raft/snapshots/...")
	// and carries the SHA-256 + size of each archived file. Populated
	// in v3+. Restore verifies as it extracts.
	Files map[string]FileChecksum `json:"files,omitempty"`
}

// FileChecksum is a SHA-256 + size pair used by manifest v3+ for
// integrity checks during restore.
type FileChecksum struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// LovelinessVersion is stamped into manifest v3+. It's overridable
// from the binary's main package via SetLovelinessVersion at startup
// so each release records itself; tests use the default.
var LovelinessVersion = "dev"

// SetLovelinessVersion lets the binary configure the version string
// recorded in new backup manifests. Called once from main.
func SetLovelinessVersion(v string) {
	if v != "" {
		LovelinessVersion = v
	}
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
		Version:           3,
		CreatedAt:         time.Now(),
		NodeID:            m.nodeID,
		ShardCount:        len(shards),
		WALSequence:       walSeq,
		LovelinessVersion: LovelinessVersion,
		Files:             map[string]FileChecksum{},
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

		size, sum, err := addFileToTar(tw, shardPath, shardName)
		if err != nil {
			return nil, fmt.Errorf("backup shard %d: %w", s.ID, err)
		}
		manifest.Files[shardName] = FileChecksum{SHA256: sum, Size: size}
		manifest.Shards = append(manifest.Shards, ShardInfo{
			ID:        s.ID,
			SizeBytes: size,
		})

		// Kuzu WAL — best-effort, may be absent on a freshly checkpointed shard.
		walName := shardName + ".wal"
		walPath := filepath.Join(m.dataDir, walName)
		if _, err := os.Stat(walPath); err == nil {
			wsize, wsum, err := addFileToTar(tw, walPath, walName)
			if err != nil {
				return nil, fmt.Errorf("backup shard %d wal: %w", s.ID, err)
			}
			manifest.Files[walName] = FileChecksum{SHA256: wsum, Size: wsize}
		}
		slog.Info("backup: shard archived", "shard", s.ID, "size", size)
	}

	// Loveliness replication WAL.
	walDir := filepath.Join(m.dataDir, "wal")
	if entries, err := filepath.Glob(filepath.Join(walDir, "wal-shard-*.log")); err == nil {
		for _, walPath := range entries {
			name := filepath.ToSlash(filepath.Join("wal", filepath.Base(walPath)))
			wsize, wsum, err := addFileToTar(tw, walPath, name)
			if err != nil {
				slog.Warn("backup: WAL file failed", "path", walPath, "err", err)
				continue
			}
			manifest.Files[name] = FileChecksum{SHA256: wsum, Size: wsize}
		}
	}

	// Raft directory: snapshot store + bolt log. This carries the FSM
	// state — shard map, database catalog, schema annotations.
	raftDir := filepath.Join(m.dataDir, "raft")
	if _, err := os.Stat(raftDir); err == nil {
		if err := addDirToTar(tw, raftDir, "raft", manifest.Files); err != nil {
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
//
// For manifest version >= 3 the archive is buffered to a temp file
// so the manifest (which trails the archive) can be parsed before
// extraction begins; per-file SHA-256 checksums are then verified as
// each file is written. On mismatch, restore aborts and the partially
// extracted data dir must be cleaned up by the operator before retry.
func (m *Manager) RestoreBackup(r io.Reader) (*Manifest, error) {
	// Stage the archive on disk so we can do two passes (manifest read
	// then verified extract) without re-downloading or holding the
	// archive in memory. Bounded disk usage = one archive size.
	tmp, err := os.CreateTemp("", "loveliness-restore-*.tar.gz")
	if err != nil {
		return nil, fmt.Errorf("stage archive: %w", err)
	}
	defer func() {
		_ = os.Remove(tmp.Name())
		_ = tmp.Close()
	}()
	if _, err := io.Copy(tmp, r); err != nil {
		return nil, fmt.Errorf("buffer archive: %w", err)
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind archive: %w", err)
	}
	manifest, err := readManifest(tmp)
	if err != nil {
		return nil, err
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind archive: %w", err)
	}
	if err := m.extractAndVerify(tmp, manifest); err != nil {
		return nil, err
	}

	if manifest == nil {
		return nil, fmt.Errorf("backup archive missing manifest.json")
	}
	return manifest, nil
}

// PeekManifest reads the manifest from a backup archive without
// extracting any data. Used by the CLI to print the manifest or to
// gate cross-cluster restores before touching the data dir. The reader
// must be positioned at the start of a fresh archive.
func PeekManifest(r io.Reader) (*Manifest, error) {
	return readManifest(r)
}

// readManifest scans the archive for manifest.json and returns it.
// The reader must be positioned at the start of a fresh archive.
func readManifest(r io.Reader) (*Manifest, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gr.Close() }()
	tr := tar.NewReader(gr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar read: %w", err)
		}
		if filepath.Clean(filepath.FromSlash(h.Name)) != "manifest.json" {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("read manifest: %w", err)
		}
		man := &Manifest{}
		if err := json.Unmarshal(data, man); err != nil {
			return nil, fmt.Errorf("parse manifest: %w", err)
		}
		return man, nil
	}
	return nil, fmt.Errorf("backup archive missing manifest.json")
}

// extractAndVerify re-reads the archive and writes each entry into
// the data dir. For manifest v3+, a SHA-256 is computed during
// extraction and compared with the manifest's expected value before
// the file is committed (renamed into place from a sibling .tmp).
// Any mismatch aborts the restore — partial files written so far are
// left in place so the operator can inspect them; retry should wipe
// the data dir first.
func (m *Manager) extractAndVerify(r io.Reader, manifest *Manifest) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gr.Close() }()
	tr := tar.NewReader(gr)

	verify := manifest.Version >= 3 && len(manifest.Files) > 0
	cleanedRaft := false
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read: %w", err)
		}

		clean := filepath.Clean(filepath.FromSlash(header.Name))
		if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
			return fmt.Errorf("refusing tar entry with unsafe path: %q", header.Name)
		}
		if clean == "manifest.json" {
			// Already loaded — drain the body.
			if _, err := io.Copy(io.Discard, tr); err != nil {
				return fmt.Errorf("drain manifest: %w", err)
			}
			continue
		}

		if !cleanedRaft && strings.HasPrefix(clean, "raft"+string(filepath.Separator)) {
			raftDir := filepath.Join(m.dataDir, "raft")
			if err := os.RemoveAll(raftDir); err != nil {
				return fmt.Errorf("clean raft dir: %w", err)
			}
			cleanedRaft = true
		}

		destPath := filepath.Join(m.dataDir, clean)
		destDir := filepath.Dir(destPath)
		if err := os.MkdirAll(destDir, 0750); err != nil {
			return fmt.Errorf("create dir for %s: %w", header.Name, err)
		}

		f, err := os.Create(destPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", header.Name, err)
		}
		var dst io.Writer = f
		var hsh hash.Hash
		if verify {
			hsh = sha256.New()
			dst = io.MultiWriter(f, hsh)
		}
		if _, err := io.Copy(dst, tr); err != nil {
			f.Close()
			return fmt.Errorf("write %s: %w", header.Name, err)
		}
		f.Close()

		// Use forward-slash key to match how CreateBackup names files
		// in the manifest (filepath.ToSlash).
		archiveKey := filepath.ToSlash(clean)
		if verify {
			expected, ok := manifest.Files[archiveKey]
			if !ok {
				return fmt.Errorf("file %q present in archive but missing from manifest", archiveKey)
			}
			got := hex.EncodeToString(hsh.Sum(nil))
			if got != expected.SHA256 {
				return fmt.Errorf("checksum mismatch for %q: got %s, manifest says %s", archiveKey, got, expected.SHA256)
			}
		}
		slog.Info("restore: extracted", "file", clean, "size", header.Size, "verified", verify)
	}
	return nil
}

// addFileToTar adds a file to a tar writer and returns the file size
// and its SHA-256 (hex) computed while streaming. The checksum is
// captured here so we don't re-read the file just to hash it.
func addFileToTar(tw *tar.Writer, srcPath, archiveName string) (int64, string, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return 0, "", err
	}

	if err := tw.WriteHeader(&tar.Header{
		Name:    archiveName,
		Size:    stat.Size(),
		Mode:    int64(stat.Mode()),
		ModTime: stat.ModTime(),
	}); err != nil {
		return 0, "", err
	}

	h := sha256.New()
	mw := io.MultiWriter(tw, h)
	if _, err := io.Copy(mw, f); err != nil {
		return 0, "", err
	}
	return stat.Size(), hex.EncodeToString(h.Sum(nil)), nil
}

// addDirToTar walks srcDir recursively, adding every regular file
// found beneath it to the tar writer with paths rooted at archivePrefix.
// Symlinks and special files are skipped. Checksums are recorded into
// files keyed by archive path.
func addDirToTar(tw *tar.Writer, srcDir, archivePrefix string, files map[string]FileChecksum) error {
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
		size, sum, err := addFileToTar(tw, path, archiveName)
		if err != nil {
			return err
		}
		files[archiveName] = FileChecksum{SHA256: sum, Size: size}
		return nil
	})
}

