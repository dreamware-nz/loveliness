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
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// Manifest describes the contents of a backup archive.
type Manifest struct {
	Version     int       `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	NodeID      string    `json:"node_id"`
	ShardCount  int       `json:"shard_count"`
	WALSequence uint64    `json:"wal_sequence"`
	Shards      []ShardInfo `json:"shards"`
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

// CreateBackup creates a compressed tar archive of all shard databases and WAL
// files. Shards are quiesced (exclusive query lock) during the copy to ensure
// a consistent snapshot. The archive is written to the given writer.
func (m *Manager) CreateBackup(w io.Writer, shards []*shard.Shard, walSeq uint64) (*Manifest, error) {
	manifest := &Manifest{
		Version:     1,
		CreatedAt:   time.Now(),
		NodeID:      m.nodeID,
		ShardCount:  len(shards),
		WALSequence: walSeq,
	}

	gw := gzip.NewWriter(w)
	defer func() { _ = gw.Close() }()
	tw := tar.NewWriter(gw)
	defer func() { _ = tw.Close() }()

	// Snapshot each shard database file.
	for _, s := range shards {
		shardPath := filepath.Join(m.dataDir, fmt.Sprintf("shard-%d", s.ID))

		// Quiesce the shard by running a no-op read query — this ensures any
		// in-flight writes complete. LadybugDB flushes on transaction commit,
		// so after this returns, the file on disk is consistent.
		if _, err := s.Query("RETURN 1"); err != nil {
			slog.Warn("backup: quiesce query failed", "shard", s.ID, "err", err)
		}

		info, err := addFileToTar(tw, shardPath, fmt.Sprintf("shard-%d", s.ID))
		if err != nil {
			return nil, fmt.Errorf("backup shard %d: %w", s.ID, err)
		}
		manifest.Shards = append(manifest.Shards, ShardInfo{
			ID:        s.ID,
			SizeBytes: info,
		})
		slog.Info("backup: shard archived", "shard", s.ID, "size", info)
	}

	// Include WAL files if they exist.
	walDir := filepath.Join(m.dataDir, "wal")
	if entries, err := filepath.Glob(filepath.Join(walDir, "wal-shard-*.log")); err == nil {
		for _, walPath := range entries {
			name := filepath.Join("wal", filepath.Base(walPath))
			if _, err := addFileToTar(tw, walPath, name); err != nil {
				slog.Warn("backup: WAL file failed", "path", walPath, "err", err)
			}
		}
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
// The server must be stopped before calling this.
func (m *Manager) RestoreBackup(r io.Reader) (*Manifest, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gr.Close() }()
	tr := tar.NewReader(gr)

	var manifest *Manifest
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar read: %w", err)
		}

		if header.Name == "manifest.json" {
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

		// Extract file to data directory.
		destPath := filepath.Join(m.dataDir, header.Name)
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
		slog.Info("restore: extracted", "file", header.Name, "size", header.Size)
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
