package router

import (
	"fmt"
	"testing"
)

func TestBloomFilter_BasicOperations(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	bf.Add("Alice")
	bf.Add("Bob")
	bf.Add("Charlie")

	if !bf.MayContain("Alice") {
		t.Error("expected Alice to be found")
	}
	if !bf.MayContain("Bob") {
		t.Error("expected Bob to be found")
	}
	if !bf.MayContain("Charlie") {
		t.Error("expected Charlie to be found")
	}
}

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(10000, 0.01)

	keys := make([]string, 5000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		bf.Add(keys[i])
	}

	for _, key := range keys {
		if !bf.MayContain(key) {
			t.Errorf("false negative for %s", key)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := 10000
	bf := NewBloomFilter(n, 0.01)

	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}

	// Test with keys that were NOT added.
	falsePositives := 0
	testCount := 10000
	for i := n; i < n+testCount; i++ {
		if bf.MayContain(fmt.Sprintf("key-%d", i)) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(testCount)
	// Allow 3x the target rate for statistical variance.
	if fpRate > 0.03 {
		t.Errorf("false positive rate too high: %.4f (expected < 0.03)", fpRate)
	}
}

func TestBloomFilter_Reset(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add("test")
	if !bf.MayContain("test") {
		t.Fatal("expected to find 'test'")
	}
	bf.Reset()
	// After reset, the key should (almost certainly) not be found.
	// There's a tiny chance of false positive but with a clean filter it's 0.
	if bf.MayContain("test") {
		t.Error("expected 'test' to be gone after reset")
	}
}

func TestBloomFilter_Count(t *testing.T) {
	bf := NewBloomFilter(10000, 0.01)
	for i := 0; i < 5000; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}
	count := bf.Count()
	// Count should be approximately 5000 (within 20%).
	if count < 4000 || count > 6000 {
		t.Errorf("expected count ~5000, got %d", count)
	}
}

func TestShardBloomIndex_LikelyShards(t *testing.T) {
	idx := NewShardBloomIndex()
	idx.InitShard(0, 1000, 0.01)
	idx.InitShard(1, 1000, 0.01)
	idx.InitShard(2, 1000, 0.01)

	idx.Add(0, "Alice")
	idx.Add(1, "Bob")
	idx.Add(2, "Charlie")

	shards := idx.LikelyShards("Alice")
	found := false
	for _, s := range shards {
		if s == 0 {
			found = true
		}
	}
	if !found {
		t.Error("expected shard 0 to be in likely shards for Alice")
	}
}

func TestShardBloomIndex_AddBatch(t *testing.T) {
	idx := NewShardBloomIndex()
	idx.InitShard(0, 1000, 0.01)

	keys := []string{"Alice", "Bob", "Charlie"}
	idx.AddBatch(0, keys)

	for _, key := range keys {
		shards := idx.LikelyShards(key)
		if len(shards) == 0 {
			t.Errorf("expected to find %s in shard 0", key)
		}
	}
}

func TestShardBloomIndex_Stats(t *testing.T) {
	idx := NewShardBloomIndex()
	idx.InitShard(0, 1000, 0.01)
	idx.InitShard(1, 1000, 0.01)

	for i := 0; i < 100; i++ {
		idx.Add(0, fmt.Sprintf("key-%d", i))
	}
	for i := 0; i < 50; i++ {
		idx.Add(1, fmt.Sprintf("key-%d", i))
	}

	stats := idx.Stats()
	if stats[0] < 80 || stats[0] > 120 {
		t.Errorf("shard 0: expected ~100, got %d", stats[0])
	}
	if stats[1] < 40 || stats[1] > 60 {
		t.Errorf("shard 1: expected ~50, got %d", stats[1])
	}
}

func TestShardBloomIndex_EmptyReturnsNil(t *testing.T) {
	idx := NewShardBloomIndex()
	shards := idx.LikelyShards("anything")
	if shards != nil {
		t.Errorf("expected nil for empty index, got %v", shards)
	}
}

func BenchmarkBloomFilter_Add(b *testing.B) {
	bf := NewBloomFilter(b.N, 0.01)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkBloomFilter_MayContain(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	for i := 0; i < 100000; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain(fmt.Sprintf("key-%d", i))
	}
}
