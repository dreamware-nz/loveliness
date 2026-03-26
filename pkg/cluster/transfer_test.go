package cluster

import (
	"context"
	"testing"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func TestShardTransferExportImport(t *testing.T) {
	st := NewShardTransfer()

	// Create a source shard with some data.
	src := shard.NewShard(0, &transferMockStore{
		data: []map[string]any{
			{"n.name": "Alice", "n.age": int64(30)},
			{"n.name": "Bob", "n.age": int64(25)},
		},
		columns: []string{"n.name", "n.age"},
	}, 1)

	// Export.
	stmts, err := st.ExportShard(context.Background(), src, []string{"Person"})
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}

	// Verify progress tracking.
	progress := st.GetProgress(0)
	if progress == nil {
		t.Fatal("expected progress tracking")
	}
	if progress.State != TransferComplete {
		t.Fatalf("expected COMPLETE state, got %s", progress.State)
	}

	// Import into a target shard.
	dst := shard.NewShard(1, &transferMockStore{}, 1)
	if err := st.ImportShard(context.Background(), dst, "node-1", stmts); err != nil {
		t.Fatal(err)
	}

	// Verify import progress.
	progress = st.GetProgress(1)
	if progress.RowsCopied != int64(len(stmts)) {
		t.Fatalf("expected %d rows copied, got %d", len(stmts), progress.RowsCopied)
	}

	// Clean up.
	st.Complete(0)
	st.Complete(1)
	if len(st.ActiveTransfers()) != 0 {
		t.Fatal("expected no active transfers after Complete")
	}
}

func TestBuildCreateStatement(t *testing.T) {
	row := map[string]any{
		"n.name": "Alice",
		"n.age":  int64(30),
	}
	cols := []string{"n.name", "n.age"}
	stmt := buildCreateStatement("Person", row, cols)

	if stmt == "" {
		t.Fatal("expected non-empty statement")
	}
	// Should contain CREATE and Person.
	if stmt[:7] != "CREATE " {
		t.Fatalf("unexpected statement: %s", stmt)
	}
}

type transferMockStore struct {
	data    []map[string]any
	columns []string
	written []string
}

func (m *transferMockStore) Query(cypher string) (*shard.QueryResponse, error) {
	if m.data != nil {
		return &shard.QueryResponse{
			Columns: m.columns,
			Rows:    m.data,
		}, nil
	}
	m.written = append(m.written, cypher)
	return &shard.QueryResponse{Rows: []map[string]any{}}, nil
}

func (m *transferMockStore) Close() error { return nil }
