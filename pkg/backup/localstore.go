package backup

import (
	"io"
	"os"
	"path/filepath"
	"sort"
)

// LocalStore stores backups as files in a local directory.
type LocalStore struct {
	dir string
}

// NewLocalStore creates a local backup store. The directory is created if it
// does not exist.
func NewLocalStore(dir string) (*LocalStore, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	return &LocalStore{dir: dir}, nil
}

func (s *LocalStore) Put(key string, r io.Reader) error {
	path := filepath.Join(s.dir, key)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (s *LocalStore) Get(key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.dir, key))
}

func (s *LocalStore) List() ([]BackupMeta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var metas []BackupMeta
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		metas = append(metas, BackupMeta{
			Key:       e.Name(),
			SizeBytes: info.Size(),
			CreatedAt: info.ModTime(),
		})
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].CreatedAt.After(metas[j].CreatedAt)
	})
	return metas, nil
}

func (s *LocalStore) Delete(key string) error {
	return os.Remove(filepath.Join(s.dir, key))
}
