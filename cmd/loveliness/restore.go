package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/johnjansen/loveliness/pkg/backup"
	"github.com/johnjansen/loveliness/pkg/config"
)

// runRestore is the entrypoint for `loveliness restore`. It applies a
// backup archive into a data dir on disk; the server must be stopped
// before this runs (Kuzu holds file handles on the shard files and
// will overwrite restored bytes on shutdown otherwise).
//
// Sources, in order:
//   - --file <path>: read archive from disk.
//   - --key <key>:   pull from the configured backup store (S3 if
//     LOVELINESS_S3_BUCKET is set, else LOVELINESS_BACKUP_DIR).
//
// If neither is given but a store is configured, the most recently
// uploaded archive is used.
//
// Cross-cluster guard: if the manifest's NodeID does not match the
// data dir's existing node identity (or LOVELINESS_NODE_ID), restore
// refuses unless --confirm-cross-cluster is passed. This is a foot-gun
// guard, not a security boundary — a determined operator can always
// pass the flag.
func runRestore(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	var (
		filePath          string
		key               string
		dataDir           string
		confirmCross      bool
		printManifestOnly bool
	)
	fs.StringVar(&filePath, "file", "", "Path to a local backup archive (.tar.gz). Overrides --key when set.")
	fs.StringVar(&key, "key", "", "Object key in the configured backup store (e.g. backup-YYYYMMDD-HHMMSS.tar.gz). If empty, the most recent backup is used.")
	fs.StringVar(&dataDir, "data-dir", "", "Target data dir. Defaults to LOVELINESS_DATA_DIR or ./data.")
	fs.BoolVar(&confirmCross, "confirm-cross-cluster", false, "Allow restoring an archive whose NodeID differs from this cluster's. Use with care.")
	fs.BoolVar(&printManifestOnly, "manifest-only", false, "Print the manifest of the source archive and exit (no extraction).")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	cfg := config.FromEnv()
	if dataDir == "" {
		dataDir = cfg.DataDir
	}
	if dataDir == "" {
		dataDir = "./data"
	}

	src, err := openArchive(filePath, key, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restore: %v\n", err)
		os.Exit(1)
	}
	defer src.Close()

	if printManifestOnly {
		// We don't have a public API to peek without extracting, so
		// stage the archive and parse manifest only.
		if err := printManifest(src); err != nil {
			fmt.Fprintf(os.Stderr, "restore: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := os.MkdirAll(dataDir, 0750); err != nil {
		fmt.Fprintf(os.Stderr, "restore: create data dir: %v\n", err)
		os.Exit(1)
	}
	mgr := backup.NewManager(dataDir, cfg.NodeID)

	// Stage to a temp file so we can read manifest first for the
	// cross-cluster check, then re-read for extraction. RestoreBackup
	// does the same staging internally; we duplicate it here only to
	// gate on NodeID before we touch the data dir.
	tmp, err := os.CreateTemp("", "loveliness-restore-cli-*.tar.gz")
	if err != nil {
		fmt.Fprintf(os.Stderr, "restore: stage archive: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = os.Remove(tmp.Name())
		_ = tmp.Close()
	}()
	if _, err := io.Copy(tmp, src); err != nil {
		fmt.Fprintf(os.Stderr, "restore: buffer archive: %v\n", err)
		os.Exit(1)
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		fmt.Fprintf(os.Stderr, "restore: rewind: %v\n", err)
		os.Exit(1)
	}

	manifest, err := backup.PeekManifest(tmp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restore: read manifest: %v\n", err)
		os.Exit(1)
	}
	if cfg.NodeID != "" && manifest.NodeID != "" && manifest.NodeID != cfg.NodeID && !confirmCross {
		fmt.Fprintf(os.Stderr,
			"restore: refusing cross-cluster restore: archive node_id=%q, this node=%q\n"+
				"  Pass --confirm-cross-cluster to override (e.g. when intentionally cloning a cluster).\n",
			manifest.NodeID, cfg.NodeID)
		os.Exit(2)
	}

	if _, err := tmp.Seek(0, 0); err != nil {
		fmt.Fprintf(os.Stderr, "restore: rewind: %v\n", err)
		os.Exit(1)
	}
	restored, err := mgr.RestoreBackup(tmp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restore: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("restored archive (version=%d, node=%s, shards=%d, wal_seq=%d, files=%d)\n",
		restored.Version, restored.NodeID, restored.ShardCount, restored.WALSequence, len(restored.Files))
	fmt.Println("Restart the cluster to load the restored data.")
}

// openArchive returns a ReadCloser for the chosen archive source.
func openArchive(filePath, key string, cfg config.Config) (io.ReadCloser, error) {
	if filePath != "" {
		f, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("open --file: %w", err)
		}
		return f, nil
	}

	store, err := openConfiguredStore(cfg)
	if err != nil {
		return nil, err
	}
	if key == "" {
		metas, err := store.List()
		if err != nil {
			return nil, fmt.Errorf("list store: %w", err)
		}
		if len(metas) == 0 {
			return nil, fmt.Errorf("no backups in store")
		}
		key = metas[0].Key
	}
	return store.Get(key)
}

// openConfiguredStore picks S3 if LOVELINESS_S3_BUCKET is set, else
// LOVELINESS_BACKUP_DIR. Mirrors what main.go does for serve.
func openConfiguredStore(cfg config.Config) (backup.BackupStore, error) {
	if cfg.S3Bucket != "" {
		return backup.NewS3Store(context.Background(), backup.S3Config{
			Bucket:   cfg.S3Bucket,
			Region:   cfg.S3Region,
			Prefix:   cfg.S3Prefix,
			Endpoint: cfg.S3Endpoint,
		})
	}
	if cfg.BackupDir != "" {
		return backup.NewLocalStore(cfg.BackupDir)
	}
	return nil, fmt.Errorf("no backup store configured (set --file, or LOVELINESS_S3_BUCKET, or LOVELINESS_BACKUP_DIR)")
}

func printManifest(r io.ReadCloser) error {
	tmp, err := os.CreateTemp("", "loveliness-manifest-*.tar.gz")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(tmp.Name())
		_ = tmp.Close()
	}()
	if _, err := io.Copy(tmp, r); err != nil {
		return err
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		return err
	}
	manifest, err := backup.PeekManifest(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(manifest)
}
