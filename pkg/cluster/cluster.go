package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/johnjansen/loveliness/pkg/annotations"
	"github.com/johnjansen/loveliness/pkg/catalog"
)

// Cluster manages Raft consensus and the shard map for a Loveliness node.
type Cluster struct {
	raft     *raft.Raft
	fsm      *FSM
	nodeID   string
	raftAddr string
}

// New creates and starts a Raft node.
func New(nodeID, raftAddr, dataDir string, bootstrap bool) (*Cluster, error) {
	fsm := NewFSM()

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	// Tighten timeouts for faster failover detection.
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond
	config.LeaderLeaseTimeout = 500 * time.Millisecond

	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("create raft dir: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	r, err := raft.NewRaft(config, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: raft.ServerAddress(raftAddr),
				},
			},
		}
		r.BootstrapCluster(cfg)
	}

	return &Cluster{
		raft:     r,
		fsm:      fsm,
		nodeID:   nodeID,
		raftAddr: raftAddr,
	}, nil
}

// Join adds a new node to the Raft cluster. Must be called on the leader.
func (c *Cluster) Join(nodeID, raftAddr string) error {
	f := c.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 10*time.Second)
	return f.Error()
}

// IsLeader returns true if this node is the current Raft leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (c *Cluster) LeaderAddr() string {
	_, id := c.raft.LeaderWithID()
	return string(id)
}

// GetShardMap returns the current shard map from the FSM.
func (c *Cluster) GetShardMap() ShardMap {
	return c.fsm.GetShardMap()
}

// Apply submits a command to the Raft log. Must be called on the leader.
func (c *Cluster) Apply(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	f := c.raft.Apply(data, 10*time.Second)
	return f.Error()
}

// AssignShard records a shard assignment in the cluster state.
// Pass an empty replica list for primary-only placement (RF=1).
func (c *Cluster) AssignShard(shardID int, primary string, replicas []string) error {
	payload, _ := json.Marshal(AssignShardPayload{
		ShardID:  shardID,
		Primary:  primary,
		Replicas: replicas,
	})
	return c.Apply(Command{Type: CmdAssignShard, Payload: payload})
}

// RegisterNode records a node joining the cluster.
func (c *Cluster) RegisterNode(info NodeInfo) error {
	payload, _ := json.Marshal(JoinNodePayload{NodeInfo: info})
	return c.Apply(Command{Type: CmdJoinNode, Payload: payload})
}

// GetCatalog returns the database catalog from the FSM.
func (c *Cluster) GetCatalog() *catalog.Catalog {
	return c.fsm.GetCatalog()
}

// CreateDatabase proposes a new database to the Raft cluster.
func (c *Cluster) CreateDatabase(name string, shardCount int) error {
	payload, _ := json.Marshal(CreateDatabasePayload{
		Name:       name,
		ShardCount: shardCount,
	})
	return c.Apply(Command{Type: CmdCreateDatabase, Payload: payload})
}

// StopDatabase proposes stopping a database.
func (c *Cluster) StopDatabase(name string) error {
	payload, _ := json.Marshal(DatabaseNamePayload{Name: name})
	return c.Apply(Command{Type: CmdStopDatabase, Payload: payload})
}

// StartDatabase proposes starting a stopped database.
func (c *Cluster) StartDatabase(name string) error {
	payload, _ := json.Marshal(DatabaseNamePayload{Name: name})
	return c.Apply(Command{Type: CmdStartDatabase, Payload: payload})
}

// DeleteDatabase proposes deleting a database.
func (c *Cluster) DeleteDatabase(name string) error {
	payload, _ := json.Marshal(DatabaseNamePayload{Name: name})
	return c.Apply(Command{Type: CmdDeleteDatabase, Payload: payload})
}

// PromoteReplica promotes a replica to primary for the given shard.
func (c *Cluster) PromoteReplica(shardID int, newPrimary string) error {
	payload, _ := json.Marshal(PromoteReplicaPayload{
		ShardID:    shardID,
		NewPrimary: newPrimary,
	})
	return c.Apply(Command{Type: CmdPromoteReplica, Payload: payload})
}

// MarkNodeDown flips a node's Alive flag to false in the shard map. Used
// by the failure detector when a peer fails repeated liveness pings; the
// rebalancer then sees a dead node and plans replica promotion.
func (c *Cluster) MarkNodeDown(nodeID string) error {
	payload, _ := json.Marshal(RemoveNodePayload{NodeID: nodeID})
	return c.Apply(Command{Type: CmdRemoveNode, Payload: payload})
}

// RegisterTable replicates a schema registration (table name → shard key) via Raft.
func (c *Cluster) RegisterTable(name, shardKey string) error {
	payload, _ := json.Marshal(RegisterTablePayload{Name: name, ShardKey: shardKey})
	return c.Apply(Command{Type: CmdRegisterTable, Payload: payload})
}

// RemoveTable replicates a schema removal via Raft.
func (c *Cluster) RemoveTable(name string) error {
	payload, _ := json.Marshal(RemoveTablePayload{Name: name})
	return c.Apply(Command{Type: CmdRemoveTable, Payload: payload})
}

// GetSchema returns the current schema keys from the FSM.
func (c *Cluster) GetSchema() map[string]string {
	return c.fsm.GetShardMap().SchemaKeys
}

// GetAnnotations returns the annotation registry. Reads go directly;
// writes must go through SetAnnotation/DeleteAnnotation so they
// replicate via Raft.
func (c *Cluster) GetAnnotations() *annotations.Registry {
	return c.fsm.GetAnnotations()
}

// SetAnnotation replicates an annotation write via Raft. Must be
// called on the leader.
func (c *Cluster) SetAnnotation(a annotations.Annotation) error {
	if _, err := annotations.ValidateTarget(a.Target); err != nil {
		return err
	}
	payload, err := json.Marshal(SetAnnotationPayload{Annotation: a})
	if err != nil {
		return fmt.Errorf("marshal annotation: %w", err)
	}
	return c.Apply(Command{Type: CmdSetAnnotation, Payload: payload})
}

// DeleteAnnotation replicates an annotation delete via Raft.
func (c *Cluster) DeleteAnnotation(target string) error {
	payload, _ := json.Marshal(DeleteAnnotationPayload{Target: target})
	return c.Apply(Command{Type: CmdDeleteAnnotation, Payload: payload})
}

// SetSchemaCallback sets a callback that fires whenever schema state changes in the FSM.
func (c *Cluster) SetSchemaCallback(cb SchemaCallback) {
	c.fsm.SetSchemaCallback(cb)
}

// NodeID returns this node's ID.
func (c *Cluster) NodeID() string {
	return c.nodeID
}

// Bootstrap bootstraps this node as a single-node cluster.
// Used by DNS auto-discovery when this node is elected as the bootstrap node.
// Safe to call after New() was called with bootstrap=false.
func (c *Cluster) Bootstrap() error {
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(c.nodeID),
				Address: raft.ServerAddress(c.raftAddr),
			},
		},
	}
	f := c.raft.BootstrapCluster(cfg)
	return f.Error()
}

// BootstrapShards assigns shardCount shards across the given nodes
// using the supplied placement strategy. rf is the desired replication
// factor (1 = primary only, 2 = primary + 1 replica, etc.) — clamped
// up to len(nodeIDs) so a small cluster can still bootstrap, with the
// shortfall surfaced via UnderReplicatedShards. If strategy is nil,
// the round-robin default is used.
//
// Called once by the leader after cluster bootstrap. Idempotent at the
// FSM level via the epoch field, so re-runs after a partial failure
// don't corrupt the placement map.
func (c *Cluster) BootstrapShards(shardCount int, nodeIDs []string, rf int, strategy PlacementStrategy) error {
	if strategy == nil {
		strategy = RoundRobinStrategy{}
	}
	if len(nodeIDs) == 0 {
		return fmt.Errorf("no nodes to assign shards to")
	}
	if rf < 1 {
		rf = 1
	}
	for i := 0; i < shardCount; i++ {
		placement := strategy.Place(i, nodeIDs, rf, nil)
		if err := c.AssignShard(i, placement.Primary, placement.Replicas); err != nil {
			return fmt.Errorf("assign shard %d: %w", i, err)
		}
	}
	return nil
}

// Shutdown gracefully stops the Raft node.
func (c *Cluster) Shutdown() error {
	f := c.raft.Shutdown()
	return f.Error()
}

// TakeSnapshot forces Raft to write a fresh FSM snapshot to disk. The
// backup pipeline calls this before archiving so that the snapshot
// store under data/raft/ contains the latest FSM state — otherwise a
// long-running cluster that hasn't tripped the snapshot threshold
// would back up only the log, and Raft would have to replay it on
// restore. Returns nil if the snapshot succeeds, an error otherwise.
func (c *Cluster) TakeSnapshot() error {
	f := c.raft.Snapshot()
	return f.Error()
}
