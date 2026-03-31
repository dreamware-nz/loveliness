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
func (c *Cluster) AssignShard(shardID int, primary, replica string) error {
	payload, _ := json.Marshal(AssignShardPayload{
		ShardID: shardID,
		Primary: primary,
		Replica: replica,
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

// NodeID returns this node's ID.
func (c *Cluster) NodeID() string {
	return c.nodeID
}

// BootstrapShards assigns shardCount shards round-robin across the given nodes.
// Primary and replica are placed on different nodes.
// Called once by the leader after cluster bootstrap.
func (c *Cluster) BootstrapShards(shardCount int, nodeIDs []string) error {
	n := len(nodeIDs)
	if n == 0 {
		return fmt.Errorf("no nodes to assign shards to")
	}
	for i := 0; i < shardCount; i++ {
		primary := nodeIDs[i%n]
		replica := ""
		if n > 1 {
			replica = nodeIDs[(i+1)%n]
		}
		if err := c.AssignShard(i, primary, replica); err != nil {
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
