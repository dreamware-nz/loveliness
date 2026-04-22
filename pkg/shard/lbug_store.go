package shard

import (
	"fmt"

	lbug "github.com/LadybugDB/go-ladybug"
)

// LbugStore is the production Store implementation backed by LadybugDB.
// It maintains a pool of connections for concurrent query execution.
type LbugStore struct {
	db   *lbug.Database
	pool chan *lbug.Connection
}

// NewLbugStore opens a LadybugDB database at the given path with a connection pool.
// If bufferPoolBytes > 0, it overrides the default buffer pool size (which is 80%
// of system memory per database — dangerous with multiple shards).
func NewLbugStore(path string, maxThreads uint64, bufferPoolBytes ...uint64) (*LbugStore, error) {
	cfg := lbug.DefaultSystemConfig()
	if maxThreads > 0 {
		cfg.MaxNumThreads = maxThreads
	}
	if len(bufferPoolBytes) > 0 && bufferPoolBytes[0] > 0 {
		cfg.BufferPoolSize = bufferPoolBytes[0]
	}
	db, err := lbug.OpenDatabase(path, cfg)
	if err != nil {
		return nil, fmt.Errorf("open database at %s: %w", path, err)
	}

	poolSize := maxThreads
	if poolSize > 8 {
		poolSize = 8
	}
	if poolSize < 1 {
		poolSize = 1
	}

	pool := make(chan *lbug.Connection, poolSize)
	for i := uint64(0); i < poolSize; i++ {
		conn, err := lbug.OpenConnection(db)
		if err != nil {
			// Close already-opened connections.
			close(pool)
			for c := range pool {
				c.Close()
			}
			db.Close()
			return nil, fmt.Errorf("open connection %d: %w", i, err)
		}
		pool <- conn
	}

	return &LbugStore{db: db, pool: pool}, nil
}

// Query executes a Cypher query using a pooled connection.
func (s *LbugStore) Query(cypher string) (*QueryResponse, error) {
	conn := <-s.pool
	defer func() { s.pool <- conn }()

	result, err := conn.Query(cypher)
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

// Close drains the connection pool and releases all LadybugDB resources.
func (s *LbugStore) Close() error {
	close(s.pool)
	for conn := range s.pool {
		conn.Close()
	}
	s.db.Close()
	return nil
}
