package catalog

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
)

// DatabaseState represents the lifecycle state of a database.
type DatabaseState int

const (
	StateCreating DatabaseState = iota
	StateOnline
	StateStopped
	StateDeleted
)

func (s DatabaseState) String() string {
	switch s {
	case StateCreating:
		return "CREATING"
	case StateOnline:
		return "ONLINE"
	case StateStopped:
		return "STOPPED"
	case StateDeleted:
		return "DELETED"
	default:
		return "UNKNOWN"
	}
}

// Database holds metadata for a single named database in the cluster.
type Database struct {
	Name       string        `json:"name"`
	ID         string        `json:"id"`
	State      DatabaseState `json:"state"`
	ShardCount int           `json:"shard_count"`
	CreatedAt  time.Time     `json:"created_at"`
	ShardIDs   []int         `json:"shard_ids"`
}

// DefaultMaxDatabases is the default maximum number of databases per cluster.
const DefaultMaxDatabases = 64

// DefaultMaxShards is the default maximum total shards per node.
const DefaultMaxShards = 256

var validName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Catalog tracks all databases in the cluster. Thread-safe.
// State is replicated via Raft FSM.
type Catalog struct {
	mu           sync.RWMutex
	databases    map[string]*Database // name -> database
	nextShardID  int                  // monotonically increasing global shard ID counter
	maxDatabases int
	maxShards    int
	totalShards  int // running count of allocated (non-deleted) shards
}

// NewCatalog creates an empty catalog with default limits.
func NewCatalog() *Catalog {
	return &Catalog{
		databases:    make(map[string]*Database),
		maxDatabases: DefaultMaxDatabases,
		maxShards:    DefaultMaxShards,
	}
}

// SetLimits configures resource limits. Zero means use defaults.
func (c *Catalog) SetLimits(maxDatabases, maxShards int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if maxDatabases > 0 {
		c.maxDatabases = maxDatabases
	}
	if maxShards > 0 {
		c.maxShards = maxShards
	}
}

// ValidateName checks that a database name conforms to naming rules.
func ValidateName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("database name cannot be empty")
	}
	if len(name) > 64 {
		return fmt.Errorf("database name too long (max 64 characters)")
	}
	if !validName.MatchString(name) {
		return fmt.Errorf("database name must match [a-z0-9][a-z0-9_-]*")
	}
	return nil
}

// CreateDatabase allocates shard IDs and adds a new database in ONLINE state.
// Returns the created Database or an error if validation fails.
func (c *Catalog) CreateDatabase(name string, shardCount int) (*Database, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	if shardCount < 1 {
		return nil, fmt.Errorf("shard_count must be >= 1")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if name is taken (including tombstones — name reuse requires
	// tombstone removal via RemoveTombstone first).
	if _, ok := c.databases[name]; ok {
		return nil, fmt.Errorf("database %q already exists", name)
	}

	// Check limits.
	nonDeleted := 0
	for _, db := range c.databases {
		if db.State != StateDeleted {
			nonDeleted++
		}
	}
	if nonDeleted >= c.maxDatabases {
		return nil, fmt.Errorf("max databases exceeded (%d)", c.maxDatabases)
	}
	if c.totalShards+shardCount > c.maxShards {
		return nil, fmt.Errorf("SHARD_LIMIT_EXCEEDED: allocating %d shards would exceed limit of %d (current: %d)", shardCount, c.maxShards, c.totalShards)
	}

	// Allocate shard IDs.
	shardIDs := make([]int, shardCount)
	for i := 0; i < shardCount; i++ {
		shardIDs[i] = c.nextShardID
		c.nextShardID++
	}

	db := &Database{
		Name:       name,
		ID:         uuid.New().String(),
		State:      StateOnline,
		ShardCount: shardCount,
		CreatedAt:  time.Now(),
		ShardIDs:   shardIDs,
	}
	c.databases[name] = db
	c.totalShards += shardCount
	return db, nil
}

// StopDatabase transitions a database from ONLINE to STOPPED.
func (c *Catalog) StopDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	db, ok := c.databases[name]
	if !ok {
		return fmt.Errorf("database %q not found", name)
	}
	if db.State != StateOnline {
		return fmt.Errorf("INVALID_STATE_TRANSITION: cannot stop database in state %s", db.State)
	}
	db.State = StateStopped
	return nil
}

// StartDatabase transitions a database from STOPPED to ONLINE.
func (c *Catalog) StartDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	db, ok := c.databases[name]
	if !ok {
		return fmt.Errorf("database %q not found", name)
	}
	if db.State != StateStopped {
		return fmt.Errorf("INVALID_STATE_TRANSITION: cannot start database in state %s", db.State)
	}
	db.State = StateOnline
	return nil
}

// DeleteDatabase transitions a database to DELETED (tombstone).
func (c *Catalog) DeleteDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	db, ok := c.databases[name]
	if !ok {
		return fmt.Errorf("database %q not found", name)
	}
	if db.State == StateDeleted {
		return fmt.Errorf("INVALID_STATE_TRANSITION: database %q is already deleted", name)
	}
	db.State = StateDeleted
	c.totalShards -= db.ShardCount
	return nil
}

// GetDatabase returns a copy of the database metadata, or nil if not found.
func (c *Catalog) GetDatabase(name string) *Database {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, ok := c.databases[name]
	if !ok {
		return nil
	}
	cp := *db
	cp.ShardIDs = make([]int, len(db.ShardIDs))
	copy(cp.ShardIDs, db.ShardIDs)
	return &cp
}

// ListDatabases returns metadata for all non-deleted databases.
func (c *Catalog) ListDatabases() []Database {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var out []Database
	for _, db := range c.databases {
		if db.State == StateDeleted {
			continue
		}
		cp := *db
		cp.ShardIDs = make([]int, len(db.ShardIDs))
		copy(cp.ShardIDs, db.ShardIDs)
		out = append(out, cp)
	}
	return out
}

// ShardOwner returns the database name that owns a given shard ID, or "" if unknown.
func (c *Catalog) ShardOwner(shardID int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, db := range c.databases {
		if db.State == StateDeleted {
			continue
		}
		for _, id := range db.ShardIDs {
			if id == shardID {
				return db.Name
			}
		}
	}
	return ""
}

// Snapshot returns a serializable copy of the catalog state.
type CatalogSnapshot struct {
	Databases   map[string]*Database `json:"databases"`
	NextShardID int                  `json:"next_shard_id"`
}

// Snapshot returns the current catalog state for Raft snapshotting.
func (c *Catalog) Snapshot() CatalogSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dbs := make(map[string]*Database, len(c.databases))
	for k, v := range c.databases {
		cp := *v
		cp.ShardIDs = make([]int, len(v.ShardIDs))
		copy(cp.ShardIDs, v.ShardIDs)
		dbs[k] = &cp
	}
	return CatalogSnapshot{
		Databases:   dbs,
		NextShardID: c.nextShardID,
	}
}

// Restore replaces the catalog state from a snapshot.
func (c *Catalog) Restore(snap CatalogSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.databases = snap.Databases
	c.nextShardID = snap.NextShardID

	// Recalculate totalShards.
	c.totalShards = 0
	for _, db := range c.databases {
		if db.State != StateDeleted {
			c.totalShards += db.ShardCount
		}
	}
}

// RemoveTombstone removes a deleted database entry from the catalog.
// Called during snapshot compaction after all nodes have cleaned up.
func (c *Catalog) RemoveTombstone(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if db, ok := c.databases[name]; ok && db.State == StateDeleted {
		delete(c.databases, name)
	}
}
