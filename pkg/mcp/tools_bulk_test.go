package mcp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCSV(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		got, err := loadCSV("a,b\n1,2\n", "")
		if err != nil {
			t.Fatal(err)
		}
		if got != "a,b\n1,2\n" {
			t.Errorf("unexpected %q", got)
		}
	})
	t.Run("file", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "x.csv")
		if err := os.WriteFile(p, []byte("a\n1\n"), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := loadCSV("", p)
		if err != nil {
			t.Fatal(err)
		}
		if got != "a\n1\n" {
			t.Errorf("unexpected %q", got)
		}
	})
	t.Run("both rejected", func(t *testing.T) {
		_, err := loadCSV("x", "y")
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("neither rejected", func(t *testing.T) {
		_, err := loadCSV("", "")
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := loadCSV("", "/nonexistent/xyz.csv")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
