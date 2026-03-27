package backup

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"
)

func TestLocalStore(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Put a backup.
	data := []byte("fake backup data for testing")
	if err := store.Put("test-backup-1.tar.gz", bytes.NewReader(data)); err != nil {
		t.Fatal("Put:", err)
	}

	// List should show 1 backup.
	metas, err := store.List()
	if err != nil {
		t.Fatal("List:", err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 backup, got %d", len(metas))
	}
	if metas[0].Key != "test-backup-1.tar.gz" {
		t.Errorf("expected key test-backup-1.tar.gz, got %s", metas[0].Key)
	}
	if metas[0].SizeBytes != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), metas[0].SizeBytes)
	}

	// Get should return the data.
	rc, err := store.Get("test-backup-1.tar.gz")
	if err != nil {
		t.Fatal("Get:", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %d bytes, want %d", len(got), len(data))
	}

	// Put a second backup.
	time.Sleep(10 * time.Millisecond) // ensure different mtime
	if err := store.Put("test-backup-2.tar.gz", bytes.NewReader([]byte("second"))); err != nil {
		t.Fatal("Put 2:", err)
	}

	metas, _ = store.List()
	if len(metas) != 2 {
		t.Fatalf("expected 2 backups, got %d", len(metas))
	}
	// Newest first.
	if metas[0].Key != "test-backup-2.tar.gz" {
		t.Errorf("expected newest first, got %s", metas[0].Key)
	}

	// Delete.
	if err := store.Delete("test-backup-1.tar.gz"); err != nil {
		t.Fatal("Delete:", err)
	}
	metas, _ = store.List()
	if len(metas) != 1 {
		t.Fatalf("expected 1 backup after delete, got %d", len(metas))
	}

	// Get non-existent should error.
	_, err = store.Get("nonexistent.tar.gz")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
}

func TestLocalStoreCreatesDir(t *testing.T) {
	dir := t.TempDir()
	subdir := dir + "/nested/backups"

	store, err := NewLocalStore(subdir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Put("test.tar.gz", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}

	// Verify directory was created.
	if _, err := os.Stat(subdir); os.IsNotExist(err) {
		t.Error("directory was not created")
	}
}

func TestBackupStoreInterface(t *testing.T) {
	// Verify LocalStore satisfies BackupStore interface.
	var _ BackupStore = (*LocalStore)(nil)
	var _ BackupStore = (*S3Store)(nil)
}
