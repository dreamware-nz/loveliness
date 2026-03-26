package router

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
)

// Resolver handles cross-shard operations: multi-hop traversal resolution
// and cross-shard edge creation.
//
// For traversals: executes on source shard, then resolves target node
// properties from their owning shards (not from local reference copies).
//
// For edge creation: ensures reference nodes exist on the source shard
// before executing the CREATE, so LadybugDB has both endpoints locally.
type Resolver struct {
	shards     []*shard.Shard
	shardCount int
	schema     *schema.Registry
	router     *Router // back-reference for shard resolution and querying
}

// NewResolver creates a cross-shard resolver.
func NewResolver(shards []*shard.Shard, reg *schema.Registry, r *Router) *Resolver {
	return &Resolver{
		shards:     shards,
		shardCount: len(shards),
		schema:     reg,
		router:     r,
	}
}

// ResolveTraversal executes a traversal query with cross-shard resolution.
//
// Flow:
//  1. Execute traversal on source shard (has edges + reference nodes)
//  2. Extract target shard keys from results
//  3. For targets on other shards, query owning shards for fresh properties
//  4. Replace target columns in results with resolved data
func (res *Resolver) ResolveTraversal(ctx context.Context, parsed *ParsedQuery) (*Result, error) {
	trav := parsed.Traversal
	if trav == nil || len(parsed.ShardKeys) == 0 {
		return nil, fmt.Errorf("no traversal info or shard keys")
	}

	// Execute on source shard.
	sourceShardID := res.router.resolveShard(parsed.ShardKeys[0])
	stage1, err := res.router.queryShard(ctx, sourceShardID, parsed.Raw)
	if err != nil {
		return nil, err
	}
	if len(stage1.Rows) == 0 {
		return stage1, nil
	}

	// Determine target table's shard key.
	targetLabel := trav.TargetLabel
	if targetLabel == "" {
		// No label on target — can't resolve, return as-is.
		return stage1, nil
	}
	targetShardKeyProp := ""
	if res.schema != nil {
		targetShardKeyProp = res.schema.GetShardKey(targetLabel)
	}
	if targetShardKeyProp == "" {
		// Table not registered — return as-is.
		return stage1, nil
	}

	// Find the target shard key column in results.
	// It could be "b.name" or just "name" depending on RETURN clause.
	targetKeyCol := findTargetKeyColumn(stage1.Columns, trav.TargetVar, targetShardKeyProp)
	if targetKeyCol == "" {
		// Shard key not in RETURN — we can't resolve. Re-execute with it included.
		stage1, targetKeyCol, err = res.reExecuteWithShardKey(ctx, parsed, sourceShardID, trav, targetShardKeyProp)
		if err != nil || len(stage1.Rows) == 0 {
			return stage1, err
		}
	}

	// Extract unique target shard keys and group by shard.
	targetsByShard := make(map[int][]string) // shardID → list of shard key values
	for _, row := range stage1.Rows {
		keyVal, ok := row[targetKeyCol]
		if !ok {
			continue
		}
		keyStr := fmt.Sprintf("%v", keyVal)
		if keyStr == "" {
			continue
		}
		targetShard := res.router.resolveShard(keyStr)
		if targetShard == sourceShardID {
			continue // already have local data, no resolution needed
		}
		targetsByShard[targetShard] = append(targetsByShard[targetShard], keyStr)
	}

	if len(targetsByShard) == 0 {
		return stage1, nil // all targets are local
	}

	// Resolve targets from their owning shards.
	resolved := res.resolveFromShards(ctx, targetsByShard, targetLabel, targetShardKeyProp, trav.TargetVar, stage1.Columns)

	// Merge resolved data into stage1 results.
	for i, row := range stage1.Rows {
		keyVal, ok := row[targetKeyCol]
		if !ok {
			continue
		}
		keyStr := fmt.Sprintf("%v", keyVal)
		if props, found := resolved[keyStr]; found {
			for col, val := range props {
				stage1.Rows[i][col] = val
			}
		}
	}

	return stage1, nil
}

// reExecuteWithShardKey re-executes the query with the target's shard key
// property added to the RETURN clause so we can resolve cross-shard targets.
func (res *Resolver) reExecuteWithShardKey(ctx context.Context, parsed *ParsedQuery, sourceShardID int, trav *TraversalInfo, shardKeyProp string) (*Result, string, error) {
	addedCol := trav.TargetVar + "." + shardKeyProp
	modifiedCypher := addToReturn(parsed.Raw, addedCol)
	result, err := res.router.queryShard(ctx, sourceShardID, modifiedCypher)
	if err != nil {
		return nil, "", err
	}
	return result, addedCol, nil
}

// addToReturn appends a column to the RETURN clause.
// "MATCH ... RETURN b.age" + "b.name" → "MATCH ... RETURN b.age, b.name"
func addToReturn(cypher, col string) string {
	upper := strings.ToUpper(cypher)
	retIdx := strings.LastIndex(upper, "RETURN ")
	if retIdx == -1 {
		return cypher
	}

	afterReturn := cypher[retIdx+7:]
	// Find where RETURN clause ends (ORDER BY, LIMIT, or end of string).
	endIdx := len(afterReturn)
	for _, term := range []string{"ORDER BY", " LIMIT ", " SKIP "} {
		if idx := strings.Index(strings.ToUpper(afterReturn), term); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}

	returnBody := strings.TrimSpace(afterReturn[:endIdx])
	suffix := afterReturn[endIdx:]

	// Ensure there's a space before suffix (ORDER BY, LIMIT, etc.)
	if suffix != "" && !strings.HasPrefix(suffix, " ") {
		suffix = " " + suffix
	}

	return cypher[:retIdx+7] + returnBody + ", " + col + suffix
}

// resolveFromShards queries each shard for its target nodes' properties.
func (res *Resolver) resolveFromShards(ctx context.Context, targetsByShard map[int][]string, targetLabel, shardKeyProp, targetVar string, columns []string) map[string]map[string]any {
	// Determine which properties to fetch.
	var targetCols []string
	for _, col := range columns {
		if strings.HasPrefix(col, targetVar+".") {
			prop := col[len(targetVar)+1:]
			targetCols = append(targetCols, prop)
		}
	}

	type resolveResult struct {
		key  string
		data map[string]any
	}

	var mu sync.Mutex
	resolved := make(map[string]map[string]any)
	var wg sync.WaitGroup

	for shardID, keys := range targetsByShard {
		// Deduplicate keys.
		unique := dedupeStrings(keys)
		for _, key := range unique {
			wg.Add(1)
			go func(shardID int, key string) {
				defer wg.Done()

				// Build resolution query.
				cypher := buildResolveQuery(targetLabel, shardKeyProp, key, targetVar, targetCols)
				result, err := res.router.queryShard(ctx, shardID, cypher)
				if err != nil || len(result.Rows) == 0 {
					return
				}

				mu.Lock()
				resolved[key] = result.Rows[0]
				mu.Unlock()
			}(shardID, key)
		}
	}

	wg.Wait()
	return resolved
}

// buildResolveQuery builds a Cypher query to fetch specific properties of a node.
// e.g., buildResolveQuery("Person", "name", "Bob", "b", ["name", "age", "city"])
// → "MATCH (b:Person {name: 'Bob'}) RETURN b.name, b.age, b.city"
func buildResolveQuery(label, shardKeyProp, keyValue, variable string, props []string) string {
	// Escape single quotes in key value.
	escaped := strings.ReplaceAll(keyValue, "'", "\\'")

	if len(props) == 0 {
		return fmt.Sprintf("MATCH (%s:%s {%s: '%s'}) RETURN %s",
			variable, label, shardKeyProp, escaped, variable)
	}

	var returnParts []string
	for _, prop := range props {
		returnParts = append(returnParts, variable+"."+prop)
	}
	return fmt.Sprintf("MATCH (%s:%s {%s: '%s'}) RETURN %s",
		variable, label, shardKeyProp, escaped, strings.Join(returnParts, ", "))
}

// findTargetKeyColumn finds the column name containing the target's shard key.
func findTargetKeyColumn(columns []string, targetVar, shardKeyProp string) string {
	expected := targetVar + "." + shardKeyProp
	for _, col := range columns {
		if col == expected {
			return col
		}
	}
	return ""
}

// EnsureReferenceNode creates a PK-only reference node on the target shard
// so that LadybugDB can create edges to it. Returns nil if the reference
// already exists or was created successfully.
func (res *Resolver) EnsureReferenceNode(ctx context.Context, shardID int, label, shardKeyProp, keyValue string) error {
	escaped := strings.ReplaceAll(keyValue, "'", "\\'")

	// Check if it already exists.
	checkCypher := fmt.Sprintf("MATCH (n:%s {%s: '%s'}) RETURN n.%s",
		label, shardKeyProp, escaped, shardKeyProp)
	result, err := res.router.queryShard(ctx, shardID, checkCypher)
	if err == nil && len(result.Rows) > 0 {
		return nil // already exists
	}

	// Create reference node with PK only.
	createCypher := fmt.Sprintf("CREATE (n:%s {%s: '%s'})",
		label, shardKeyProp, escaped)
	_, err = res.router.queryShard(ctx, shardID, createCypher)
	return err
}

// HandleCrossShardEdgeCreate handles CREATE patterns where source and target
// nodes are on different shards.
//
// Flow:
//  1. Parse the query for source/target shard keys
//  2. Ensure target reference node exists on source shard
//  3. Execute the original CREATE on source shard
func (res *Resolver) HandleCrossShardEdgeCreate(ctx context.Context, parsed *ParsedQuery) (*Result, error) {
	if parsed.Traversal == nil || len(parsed.ShardKeys) < 2 {
		return nil, fmt.Errorf("cross-shard edge create requires traversal info and 2+ shard keys")
	}

	sourceKey := parsed.ShardKeys[0]
	sourceShardID := res.router.resolveShard(sourceKey)

	// Ensure all other shard keys' reference nodes exist on the source shard.
	for _, key := range parsed.ShardKeys[1:] {
		targetShardID := res.router.resolveShard(key)
		if targetShardID == sourceShardID {
			continue // same shard, no reference needed
		}

		// Determine the target label. For now, use the traversal target label.
		targetLabel := parsed.Traversal.TargetLabel
		if targetLabel == "" {
			return nil, &QueryError{
				Code:    "CROSS_SHARD_ERROR",
				Message: "cross-shard edge creation requires a labeled target node",
			}
		}

		targetShardKeyProp := ""
		if res.schema != nil {
			targetShardKeyProp = res.schema.GetShardKey(targetLabel)
		}
		if targetShardKeyProp == "" {
			return nil, &QueryError{
				Code:    "CROSS_SHARD_ERROR",
				Message: fmt.Sprintf("no shard key registered for table %s", targetLabel),
			}
		}

		if err := res.EnsureReferenceNode(ctx, sourceShardID, targetLabel, targetShardKeyProp, key); err != nil {
			return nil, &QueryError{
				Code:    "CROSS_SHARD_ERROR",
				Message: fmt.Sprintf("failed to create reference node for %s='%s': %v", targetShardKeyProp, key, err),
			}
		}
	}

	// Execute the original query on the source shard.
	return res.router.queryShard(ctx, sourceShardID, parsed.Raw)
}

func dedupeStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; !ok {
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	return out
}
