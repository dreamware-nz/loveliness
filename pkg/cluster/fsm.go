package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
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
	Alive    bool   `json:"alive"`
}

// CommandType identifies the type of Raft log entry.
type CommandType uint8

const (
	CmdAssignShard CommandType = iota
	CmdJoinNode
	CmdRemoveNode
	CmdPromoteReplica
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

// FSM implements the raft.FSM interface for managing the cluster shard map.
type FSM struct {
	mu       sync.RWMutex
	shardMap ShardMap
}

// NewFSM creates a new FSM with an empty shard map.
func NewFSM() *FSM {
	return &FSM{
		shardMap: ShardMap{
			Assignments: make(map[int]ShardAssignment),
			Nodes:       make(map[string]NodeInfo),
		},
	}
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

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// Snapshot returns a snapshot of the FSM state for Raft snapshotting.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data, err := json.Marshal(f.shardMap)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var sm ShardMap
	if err := json.NewDecoder(rc).Decode(&sm); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shardMap = sm
	return nil
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
