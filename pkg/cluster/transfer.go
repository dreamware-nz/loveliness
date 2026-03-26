package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// TransferState tracks the progress of a shard data transfer.
type TransferState int

const (
	TransferIdle TransferState = iota
	TransferExporting
	TransferImporting
	TransferComplete
	TransferFailed
)

func (ts TransferState) String() string {
	switch ts {
	case TransferIdle:
		return "IDLE"
	case TransferExporting:
		return "EXPORTING"
	case TransferImporting:
		return "IMPORTING"
	case TransferComplete:
		return "COMPLETE"
	case TransferFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// TransferProgress tracks ongoing shard transfers.
type TransferProgress struct {
	ShardID    int           `json:"shard_id"`
	FromNode   string        `json:"from_node"`
	ToNode     string        `json:"to_node"`
	State      TransferState `json:"state"`
	TablesTotal int          `json:"tables_total"`
	TablesDone  int          `json:"tables_done"`
	RowsTotal   int64        `json:"rows_total"`
	RowsCopied  int64        `json:"rows_copied"`
	Error       string       `json:"error,omitempty"`
}

// ShardTransfer handles moving shard data between nodes during rebalancing.
// The protocol is:
//  1. Source node exports all tables as Cypher statements
//  2. Target node replays Cypher statements to build the shard
//  3. Source node confirms transfer complete
//  4. Raft FSM updates the shard assignment
//  5. Source node drops local shard data
type ShardTransfer struct {
	mu       sync.Mutex
	active   map[int]*TransferProgress // shardID → progress
}

// NewShardTransfer creates a transfer manager.
func NewShardTransfer() *ShardTransfer {
	return &ShardTransfer{
		active: make(map[int]*TransferProgress),
	}
}

// GetProgress returns the current transfer progress for a shard, or nil.
func (st *ShardTransfer) GetProgress(shardID int) *TransferProgress {
	st.mu.Lock()
	defer st.mu.Unlock()
	if p, ok := st.active[shardID]; ok {
		cp := *p
		return &cp
	}
	return nil
}

// ActiveTransfers returns all in-progress transfers.
func (st *ShardTransfer) ActiveTransfers() []TransferProgress {
	st.mu.Lock()
	defer st.mu.Unlock()
	var result []TransferProgress
	for _, p := range st.active {
		result = append(result, *p)
	}
	return result
}

// ExportShard extracts all data from a shard as a sequence of Cypher CREATE
// statements that can be replayed on the target node.
func (st *ShardTransfer) ExportShard(ctx context.Context, s *shard.Shard, tables []string) ([]string, error) {
	st.mu.Lock()
	progress := &TransferProgress{
		ShardID:     s.ID,
		State:       TransferExporting,
		TablesTotal: len(tables),
	}
	st.active[s.ID] = progress
	st.mu.Unlock()

	var statements []string

	for _, table := range tables {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Export nodes.
		cypher := fmt.Sprintf("MATCH (n:%s) RETURN n.*", table)
		resp, err := s.Query(cypher)
		if err != nil {
			st.setFailed(s.ID, err)
			return nil, fmt.Errorf("export table %s: %w", table, err)
		}

		for _, row := range resp.Rows {
			stmt := buildCreateStatement(table, row, resp.Columns)
			if stmt != "" {
				statements = append(statements, stmt)
			}
		}

		st.mu.Lock()
		progress.TablesDone++
		progress.RowsCopied += int64(len(resp.Rows))
		st.mu.Unlock()

		slog.Info("transfer: exported table",
			"shard", s.ID, "table", table, "rows", len(resp.Rows))
	}

	st.mu.Lock()
	progress.State = TransferComplete
	progress.RowsTotal = progress.RowsCopied
	st.mu.Unlock()

	return statements, nil
}

// ImportShard replays Cypher statements on a target shard.
func (st *ShardTransfer) ImportShard(ctx context.Context, s *shard.Shard, fromNode string, statements []string) error {
	st.mu.Lock()
	progress := &TransferProgress{
		ShardID:    s.ID,
		FromNode:   fromNode,
		State:      TransferImporting,
		RowsTotal:  int64(len(statements)),
	}
	st.active[s.ID] = progress
	st.mu.Unlock()

	for i, stmt := range statements {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if _, err := s.Query(stmt); err != nil {
			slog.Warn("transfer: import statement failed",
				"shard", s.ID, "idx", i, "err", err)
			// Continue — some statements may fail on duplicate keys
			// if partial transfer was already done.
		}

		st.mu.Lock()
		progress.RowsCopied = int64(i + 1)
		st.mu.Unlock()
	}

	st.mu.Lock()
	progress.State = TransferComplete
	st.mu.Unlock()

	slog.Info("transfer: import complete",
		"shard", s.ID, "statements", len(statements))
	return nil
}

// Complete removes a transfer from the active set.
func (st *ShardTransfer) Complete(shardID int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.active, shardID)
}

func (st *ShardTransfer) setFailed(shardID int, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if p, ok := st.active[shardID]; ok {
		p.State = TransferFailed
		p.Error = err.Error()
	}
}

// buildCreateStatement converts a result row into a CREATE Cypher statement.
func buildCreateStatement(table string, row map[string]any, columns []string) string {
	if len(row) == 0 {
		return ""
	}
	props := ""
	first := true
	for _, col := range columns {
		// Columns come back as "n.propName" — strip the prefix.
		propName := col
		if len(col) > 2 && col[1] == '.' {
			propName = col[2:]
		}
		val, ok := row[col]
		if !ok || val == nil {
			continue
		}
		if !first {
			props += ", "
		}
		first = false
		props += fmt.Sprintf("%s: %s", propName, cypherLiteral(val))
	}
	if props == "" {
		return ""
	}
	return fmt.Sprintf("CREATE (:%s {%s})", table, props)
}

// cypherLiteral converts a Go value to a Cypher literal string.
func cypherLiteral(v any) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", escapeCypherString(val))
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%f", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case int:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func escapeCypherString(s string) string {
	result := ""
	for _, c := range s {
		if c == '\'' {
			result += "\\'"
		} else if c == '\\' {
			result += "\\\\"
		} else {
			result += string(c)
		}
	}
	return result
}
