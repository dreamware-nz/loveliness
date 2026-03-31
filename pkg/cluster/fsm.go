package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/johnjansen/loveliness/pkg/catalog"
)

// ShardAssignment tracks which node owns the primary for a shard
// and which node has the replica.
type ShardAssignment struct {
	Primary string `json:"primary"`
	Replica string `json:"replica,omitempty"`
}

// ShardMap is the cluster-wide mapping of shard IDs to node assignments.
// This is the state managed by the Raft FSM.
type ShardMap struct {
	Assignments map[int]ShardAssignment `json:"assignments"`
	Nodes       map[string]NodeInfo     `json:"nodes"`
}

// NodeInfo tracks metadata about a cluster node.
type NodeInfo struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
	HTTPAddr string `json:"http_addr"`
	BoltAddr string `json:"bolt_addr"`
	Alive    bool   `json:"alive"`
}

// CommandType identifies the type of Raft log entry.
type CommandType uint8

const (
	CmdAssignShard CommandType = iota
	CmdJoinNode
	CmdRemoveNode
	CmdPromoteReplica
	CmdCreateDatabase
	CmdStopDatabase
	CmdStartDatabase
	CmdDeleteDatabase
)

// Command is a Raft log entry.
type Command struct {
	Type    CommandType `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// AssignShardPayload is the data for CmdAssignShard.
type AssignShardPayload struct {
	ShardID int    `json:"shard_id"`
	Primary string `json:"primary"`
	Replica string `json:"replica,omitempty"`
}

// JoinNodePayload is the data for CmdJoinNode.
type JoinNodePayload struct {
	NodeInfo NodeInfo `json:"node_info"`
}

// RemoveNodePayload is the data for CmdRemoveNode.
type RemoveNodePayload struct {
	NodeID string `json:"node_id"`
}

// PromoteReplicaPayload is the data for CmdPromoteReplica.
type PromoteReplicaPayload struct {
	ShardID    int    `json:"shard_id"`
	NewPrimary string `json:"new_primary"`
}

// CreateDatabasePayload is the data for CmdCreateDatabase.
type CreateDatabasePayload struct {
	Name       string `json:"name"`
	ShardCount int    `json:"shard_count"`
}

// DatabaseNamePayload is the data for CmdStopDatabase, CmdStartDatabase, CmdDeleteDatabase.
type DatabaseNamePayload struct {
	Name string `json:"name"`
}

// FSM implements the raft.FSM interface for managing the cluster shard map
// and database catalog.
type FSM struct {
	mu       sync.RWMutex
	shardMap ShardMap
	catalog  *catalog.Catalog
}

// NewFSM creates a new FSM with an empty shard map and catalog.
func NewFSM() *FSM {
	return &FSM{
		shardMap: ShardMap{
			Assignments: make(map[int]ShardAssignment),
			Nodes:       make(map[string]NodeInfo),
		},
		catalog: catalog.NewCatalog(),
	}
}

// GetCatalog returns the catalog for reading database metadata.
func (f *FSM) GetCatalog() *catalog.Catalog {
	return f.catalog
}

// GetShardMap returns a copy of the current shard map.
func (f *FSM) GetShardMap() ShardMap {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// Deep copy assignments.
	assignments := make(map[int]ShardAssignment, len(f.shardMap.Assignments))
	for k, v := range f.shardMap.Assignments {
		assignments[k] = v
	}
	nodes := make(map[string]NodeInfo, len(f.shardMap.Nodes))
	for k, v := range f.shardMap.Nodes {
		nodes[k] = v
	}
	return ShardMap{Assignments: assignments, Nodes: nodes}
}

// Apply applies a Raft log entry to the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdAssignShard:
		var p AssignShardPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal assign shard: %w", err)
		}
		f.shardMap.Assignments[p.ShardID] = ShardAssignment{
			Primary: p.Primary,
			Replica: p.Replica,
		}
		return nil

	case CmdJoinNode:
		var p JoinNodePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal join node: %w", err)
		}
		f.shardMap.Nodes[p.NodeInfo.ID] = p.NodeInfo
		return nil

	case CmdRemoveNode:
		var p RemoveNodePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal remove node: %w", err)
		}
		if info, ok := f.shardMap.Nodes[p.NodeID]; ok {
			info.Alive = false
			f.shardMap.Nodes[p.NodeID] = info
		}
		return nil

	case CmdPromoteReplica:
		var p PromoteReplicaPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal promote replica: %w", err)
		}
		if a, ok := f.shardMap.Assignments[p.ShardID]; ok {
			a.Primary = p.NewPrimary
			a.Replica = "" // replica slot now empty, needs reassignment
			f.shardMap.Assignments[p.ShardID] = a
		}
		return nil

	case CmdCreateDatabase:
		var p CreateDatabasePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal create database: %w", err)
		}
		db, err := f.catalog.CreateDatabase(p.Name, p.ShardCount)
		if err != nil {
			return err
		}
		return db

	case CmdStopDatabase:
		var p DatabaseNamePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal stop database: %w", err)
		}
		return f.catalog.StopDatabase(p.Name)

	case CmdStartDatabase:
		var p DatabaseNamePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal start database: %w", err)
		}
		return f.catalog.StartDatabase(p.Name)

	case CmdDeleteDatabase:
		var p DatabaseNamePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal delete database: %w", err)
		}
		return f.catalog.DeleteDatabase(p.Name)

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// fsmState is the combined state serialized in Raft snapshots.
type fsmState struct {
	ShardMap ShardMap                `json:"shard_map"`
	Catalog  catalog.CatalogSnapshot `json:"catalog"`
}

// Snapshot returns a snapshot of the FSM state for Raft snapshotting.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	state := fsmState{
		ShardMap: f.shardMap,
		Catalog:  f.catalog.Snapshot(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Try new format first (with catalog).
	raw, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var state fsmState
	if err := json.Unmarshal(raw, &state); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// If the snapshot has the new format (shard_map key), use it.
	// Otherwise it's a legacy snapshot with just the ShardMap at the top level.
	if state.ShardMap.Assignments != nil {
		f.shardMap = state.ShardMap
	} else {
		// Legacy format: the entire JSON is a ShardMap.
		var sm ShardMap
		if err := json.Unmarshal(raw, &sm); err != nil {
			return err
		}
		f.shardMap = sm
	}

	if state.Catalog.Databases != nil {
		f.catalog.Restore(state.Catalog)
	}

	return nil
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
