package shard

import (
	"fmt"

	lbug "github.com/LadybugDB/go-ladybug"
)

// LbugStore is the production Store implementation backed by LadybugDB.
type LbugStore struct {
	db   *lbug.Database
	conn *lbug.Connection
}

// NewLbugStore opens a LadybugDB database at the given path.
func NewLbugStore(path string, maxThreads uint64) (*LbugStore, error) {
	cfg := lbug.DefaultSystemConfig()
	if maxThreads > 0 {
		cfg.MaxNumThreads = maxThreads
	}
	db, err := lbug.OpenDatabase(path, cfg)
	if err != nil {
		return nil, fmt.Errorf("open database at %s: %w", path, err)
	}
	conn, err := lbug.OpenConnection(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open connection: %w", err)
	}
	return &LbugStore{db: db, conn: conn}, nil
}

// Query executes a Cypher query against the LadybugDB instance.
func (s *LbugStore) Query(cypher string) (*QueryResponse, error) {
	result, err := s.conn.Query(cypher)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	columns := result.GetColumnNames()
	var rows []map[string]any
	for result.HasNext() {
		tuple, err := result.Next()
		if err != nil {
			return nil, fmt.Errorf("iterate results: %w", err)
		}
		row, err := tuple.GetAsMap()
		tuple.Close()
		if err != nil {
			return nil, fmt.Errorf("read tuple: %w", err)
		}
		rows = append(rows, row)
	}
	if rows == nil {
		rows = []map[string]any{}
	}

	return &QueryResponse{
		Columns: columns,
		Rows:    rows,
		Stats: QueryStats{
			CompileTimeMs: result.GetCompilingTime(),
			ExecTimeMs:    result.GetExecutionTime(),
		},
	}, nil
}

// Close releases LadybugDB resources.
func (s *LbugStore) Close() error {
	s.conn.Close()
	s.db.Close()
	return nil
}
