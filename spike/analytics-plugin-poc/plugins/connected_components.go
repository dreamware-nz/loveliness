package plugins

import (
	"context"
	"fmt"
	"sort"

	"github.com/johnjansen/loveliness/pkg/router"
)

// ConnectedComponents treats the result as an edge list and computes
// weakly-connected components via union-find. It's a stand-in for the
// real Leiden plugin during the spike — same contract, simpler maths.
//
// Params:
//
//	src (string, default "src") — column name holding the source node id
//	dst (string, default "dst") — column name holding the destination node id
type ConnectedComponents struct{}

func (ConnectedComponents) Name() string { return "connected_components" }

func (ConnectedComponents) Compute(_ context.Context, result *router.Result, params map[string]any) (any, error) {
	srcCol := stringOr(params, "src", "src")
	dstCol := stringOr(params, "dst", "dst")
	if !columnExists(result.Columns, srcCol) {
		return nil, fmt.Errorf("connected_components: src column %q not in result", srcCol)
	}
	if !columnExists(result.Columns, dstCol) {
		return nil, fmt.Errorf("connected_components: dst column %q not in result", dstCol)
	}

	uf := newUnionFind()
	for _, row := range result.Rows {
		s := keyOf(row[srcCol])
		d := keyOf(row[dstCol])
		uf.union(s, d)
	}

	groups := uf.groups()
	sizes := make([]int, 0, len(groups))
	for _, members := range groups {
		sizes = append(sizes, len(members))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sizes)))

	return map[string]any{
		"num_components": len(groups),
		"num_nodes":      uf.nodeCount(),
		"largest":        firstOr(sizes, 0),
		"size_histogram": sizes,
	}, nil
}

func stringOr(m map[string]any, k, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[k].(string); ok && v != "" {
		return v
	}
	return def
}

func firstOr(xs []int, def int) int {
	if len(xs) == 0 {
		return def
	}
	return xs[0]
}

func keyOf(v any) string { return fmt.Sprintf("%v", v) }

// minimal union-find on opaque string ids.
type unionFind struct {
	parent map[string]string
	rank   map[string]int
}

func newUnionFind() *unionFind {
	return &unionFind{parent: map[string]string{}, rank: map[string]int{}}
}

func (u *unionFind) find(x string) string {
	if _, seen := u.parent[x]; !seen {
		u.parent[x] = x
		u.rank[x] = 0
		return x
	}
	if u.parent[x] != x {
		u.parent[x] = u.find(u.parent[x])
	}
	return u.parent[x]
}

func (u *unionFind) union(a, b string) {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return
	}
	switch {
	case u.rank[ra] < u.rank[rb]:
		u.parent[ra] = rb
	case u.rank[ra] > u.rank[rb]:
		u.parent[rb] = ra
	default:
		u.parent[rb] = ra
		u.rank[ra]++
	}
}

func (u *unionFind) groups() map[string][]string {
	out := map[string][]string{}
	for node := range u.parent {
		root := u.find(node)
		out[root] = append(out[root], node)
	}
	return out
}

func (u *unionFind) nodeCount() int { return len(u.parent) }
