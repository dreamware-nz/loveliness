package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/johnjansen/loveliness/pkg/annotations"
	"github.com/johnjansen/loveliness/pkg/catalog"
)

// ShardAssignment tracks which nodes own a shard.
// Primary is the leader for the shard; Replicas are followers.
// Epoch is bumped on every placement change so out-of-order Raft
// applies are idempotent — readers can ignore an update whose epoch
// is not strictly greater than the one they last saw.
type ShardAssignment struct {
	Primary  string   `json:"primary"`
	Replicas []string `json:"replicas,omitempty"`
	Epoch    uint64   `json:"epoch,omitempty"`
}

// shardAssignmentLegacy is the on-disk shape from before issue #5.
// We keep it for snapshot back-compat: the old FSM wrote a single
// `replica` string, the new one writes a `replicas` slice. Both are
// accepted; only the new shape is written going forward.
type shardAssignmentLegacy struct {
	Primary  string   `json:"primary"`
	Replica  string   `json:"replica,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
	Epoch    uint64   `json:"epoch,omitempty"`
}

// UnmarshalJSON accepts both the legacy single-replica shape and the
// new multi-replica shape. If both `replica` and `replicas` are present
// the new field wins; the legacy field is folded in only when the new
// one is empty.
func (a *ShardAssignment) UnmarshalJSON(data []byte) error {
	var v shardAssignmentLegacy
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	a.Primary = v.Primary
	a.Epoch = v.Epoch
	if len(v.Replicas) > 0 {
		a.Replicas = v.Replicas
	} else if v.Replica != "" {
		a.Replicas = []string{v.Replica}
	} else {
		a.Replicas = nil
	}
	return nil
}

// ShardMap is the cluster-wide mapping of shard IDs to node assignments.
// This is the state managed by the Raft FSM.
type ShardMap struct {
	Assignments map[int]ShardAssignment `json:"assignments"`
	Nodes       map[string]NodeInfo     `json:"nodes"`
	SchemaKeys  map[string]string       `json:"schema_keys,omitempty"` // table name → shard key
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
	CmdRegisterTable
	CmdRemoveTable
	CmdCreateDatabase
	CmdStopDatabase
	CmdStartDatabase
	CmdDeleteDatabase
	CmdSetAnnotation
	CmdDeleteAnnotation
)

// Command is a Raft log entry.
type Command struct {
	Type    CommandType `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// AssignShardPayload is the data for CmdAssignShard.
// Replicas replaces the legacy single-replica field; UnmarshalJSON
// folds the old `replica` shape in for back-compat.
type AssignShardPayload struct {
	ShardID  int      `json:"shard_id"`
	Primary  string   `json:"primary"`
	Replicas []string `json:"replicas,omitempty"`
	Epoch    uint64   `json:"epoch,omitempty"`
}

type assignShardPayloadLegacy struct {
	ShardID  int      `json:"shard_id"`
	Primary  string   `json:"primary"`
	Replica  string   `json:"replica,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
	Epoch    uint64   `json:"epoch,omitempty"`
}

// UnmarshalJSON folds the legacy single-replica payload into the new
// Replicas slice when no new-shape replicas are provided.
func (p *AssignShardPayload) UnmarshalJSON(data []byte) error {
	var v assignShardPayloadLegacy
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	p.ShardID = v.ShardID
	p.Primary = v.Primary
	p.Epoch = v.Epoch
	if len(v.Replicas) > 0 {
		p.Replicas = v.Replicas
	} else if v.Replica != "" {
		p.Replicas = []string{v.Replica}
	} else {
		p.Replicas = nil
	}
	return nil
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

// RegisterTablePayload is the data for CmdRegisterTable.
type RegisterTablePayload struct {
	Name     string `json:"name"`
	ShardKey string `json:"shard_key"`
}

// RemoveTablePayload is the data for CmdRemoveTable.
type RemoveTablePayload struct {
	Name string `json:"name"`
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

// SetAnnotationPayload is the data for CmdSetAnnotation.
type SetAnnotationPayload struct {
	Annotation annotations.Annotation `json:"annotation"`
}

// DeleteAnnotationPayload is the data for CmdDeleteAnnotation.
type DeleteAnnotationPayload struct {
	Target string `json:"target"`
}

// ShardsForNode returns the shard IDs assigned to a node (as primary or replica).
func (sm ShardMap) ShardsForNode(nodeID string) []int {
	var ids []int
	for id, a := range sm.Assignments {
		if a.Primary == nodeID {
			ids = append(ids, id)
			continue
		}
		for _, r := range a.Replicas {
			if r == nodeID {
				ids = append(ids, id)
				break
			}
		}
	}
	return ids
}

// NodesForShard returns the node IDs that host a shard (primary first, then replicas).
func (sm ShardMap) NodesForShard(shardID int) []string {
	a, ok := sm.Assignments[shardID]
	if !ok {
		return nil
	}
	nodes := make([]string, 0, 1+len(a.Replicas))
	if a.Primary != "" {
		nodes = append(nodes, a.Primary)
	}
	for _, r := range a.Replicas {
		if r != "" {
			nodes = append(nodes, r)
		}
	}
	return nodes
}

// UnderReplicatedShards returns shard IDs whose total holder count
// (primary + replicas) is below rf. Used by observability and the
// rebalancer to decide which shards need additional placements.
//
// rf=1 means "every shard must have a primary"; rf=2 means "primary
// plus at least one replica"; etc. A shard with a missing primary
// also counts as under-replicated even if its replica list is full.
func (sm ShardMap) UnderReplicatedShards(rf int) []int {
	if rf < 1 {
		rf = 1
	}
	var ids []int
	for id, a := range sm.Assignments {
		holders := 0
		if a.Primary != "" {
			holders++
		}
		for _, r := range a.Replicas {
			if r != "" && r != a.Primary {
				holders++
			}
		}
		if holders < rf {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids
}

// PrimaryForShard returns the primary node for a shard.
func (sm ShardMap) PrimaryForShard(shardID int) string {
	if a, ok := sm.Assignments[shardID]; ok {
		return a.Primary
	}
	return ""
}

// SchemaCallback is called whenever the schema state changes in the FSM.
// The callback receives the full schema map (table name → shard key).
type SchemaCallback func(tables map[string]string)

// FSM implements the raft.FSM interface for managing the cluster shard map
// and database catalog.
type FSM struct {
	mu             sync.RWMutex
	shardMap       ShardMap
	catalog        *catalog.Catalog
	annotations    *annotations.Registry
	schemaCallback SchemaCallback
}

// NewFSM creates a new FSM with an empty shard map and catalog.
func NewFSM() *FSM {
	return &FSM{
		shardMap: ShardMap{
			Assignments: make(map[int]ShardAssignment),
			Nodes:       make(map[string]NodeInfo),
			SchemaKeys:  make(map[string]string),
		},
		catalog:     catalog.NewCatalog(),
		annotations: annotations.New(),
	}
}

// GetCatalog returns the catalog for reading database metadata.
func (f *FSM) GetCatalog() *catalog.Catalog {
	return f.catalog
}

// GetAnnotations returns the annotation registry for reading.
// Writes go through the FSM via Apply.
func (f *FSM) GetAnnotations() *annotations.Registry {
	return f.annotations
}

// SetSchemaCallback sets a callback that fires whenever schema state changes.
func (f *FSM) SetSchemaCallback(cb SchemaCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schemaCallback = cb
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
	schemaKeys := make(map[string]string, len(f.shardMap.SchemaKeys))
	for k, v := range f.shardMap.SchemaKeys {
		schemaKeys[k] = v
	}
	return ShardMap{Assignments: assignments, Nodes: nodes, SchemaKeys: schemaKeys}
}

// notifySchemaChange fires the schema callback with a copy of the current schema.
// Must be called with f.mu held.
func (f *FSM) notifySchemaChange() {
	if f.schemaCallback == nil {
		return
	}
	tables := make(map[string]string, len(f.shardMap.SchemaKeys))
	for k, v := range f.shardMap.SchemaKeys {
		tables[k] = v
	}
	f.schemaCallback(tables)
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
		// Idempotency: if a payload carries an epoch, drop applies that
		// don't strictly advance it. Epoch=0 (legacy / unset) always wins
		// over an existing epoch=0 entry — needed so the migration path
		// from old snapshots is a no-op rewrite, not a rejection.
		existing, hadExisting := f.shardMap.Assignments[p.ShardID]
		if hadExisting && p.Epoch != 0 && existing.Epoch != 0 && p.Epoch <= existing.Epoch {
			return nil
		}
		newEpoch := p.Epoch
		if newEpoch == 0 && hadExisting {
			newEpoch = existing.Epoch + 1
		}
		f.shardMap.Assignments[p.ShardID] = ShardAssignment{
			Primary:  p.Primary,
			Replicas: append([]string(nil), p.Replicas...),
			Epoch:    newEpoch,
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
			// Drop the promoted node from the replica list — it's now
			// the primary. Remaining replicas stay; the rebalancer
			// will fill any gap on the next pass.
			filtered := a.Replicas[:0]
			for _, r := range a.Replicas {
				if r != p.NewPrimary {
					filtered = append(filtered, r)
				}
			}
			a.Primary = p.NewPrimary
			a.Replicas = append([]string(nil), filtered...)
			a.Epoch++
			f.shardMap.Assignments[p.ShardID] = a
		}
		return nil

	case CmdRegisterTable:
		var p RegisterTablePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal register table: %w", err)
		}
		f.shardMap.SchemaKeys[p.Name] = p.ShardKey
		f.notifySchemaChange()
		return nil

	case CmdRemoveTable:
		var p RemoveTablePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal remove table: %w", err)
		}
		delete(f.shardMap.SchemaKeys, p.Name)
		f.notifySchemaChange()
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

	case CmdSetAnnotation:
		var p SetAnnotationPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal set annotation: %w", err)
		}
		return f.annotations.Set(p.Annotation)

	case CmdDeleteAnnotation:
		var p DeleteAnnotationPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal delete annotation: %w", err)
		}
		f.annotations.Delete(p.Target)
		return nil

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// fsmState is the combined state serialized in Raft snapshots.
type fsmState struct {
	ShardMap    ShardMap                          `json:"shard_map"`
	Catalog     catalog.CatalogSnapshot           `json:"catalog"`
	Annotations map[string]annotations.Annotation `json:"annotations,omitempty"`
}

// Snapshot returns a snapshot of the FSM state for Raft snapshotting.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	state := fsmState{
		ShardMap:    f.shardMap,
		Catalog:     f.catalog.Snapshot(),
		Annotations: f.annotations.Snapshot(),
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

	if f.shardMap.SchemaKeys == nil {
		f.shardMap.SchemaKeys = make(map[string]string)
	}

	if state.Catalog.Databases != nil {
		f.catalog.Restore(state.Catalog)
	}

	// Annotations are optional in older snapshots — Restore handles nil.
	f.annotations.Restore(state.Annotations)

	f.notifySchemaChange()
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
