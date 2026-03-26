package cluster

import (
	"bytes"
	"io"
)

// mockSnapshotSink implements raft.SnapshotSink for tests.
type mockSnapshotSink struct {
	buf bytes.Buffer
}

func (s *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return s.buf.Write(p)
}

func (s *mockSnapshotSink) Close() error {
	return nil
}

func (s *mockSnapshotSink) ID() string {
	return "mock"
}

func (s *mockSnapshotSink) Cancel() error {
	return nil
}

func (s *mockSnapshotSink) Reader() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(s.buf.Bytes()))
}
