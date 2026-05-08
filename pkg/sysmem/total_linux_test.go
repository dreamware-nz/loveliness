package sysmem

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseMeminfoFixture(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meminfo")
	const fixture = `MemTotal:       49355436 kB
MemFree:        12345678 kB
MemAvailable:   45000000 kB
Buffers:           12345 kB
`
	if err := os.WriteFile(path, []byte(fixture), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := parseMeminfo(path)
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(49355436) * 1024
	if got != want {
		t.Fatalf("parseMeminfo = %d, want %d", got, want)
	}
}

func TestParseMeminfoMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meminfo")
	if err := os.WriteFile(path, []byte("MemFree: 1024 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMeminfo(path); err == nil {
		t.Fatal("expected error when MemTotal is absent")
	}
}
