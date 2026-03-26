package router

import (
	"hash"
	"hash/fnv"
	"math"
	"sync"
)

// BloomFilter is a space-efficient probabilistic set membership test.
// Used for shard routing: each shard maintains a Bloom filter of its
// primary keys, allowing the router to skip shards that definitely
// don't contain a key — turning scatter-gather into targeted queries.
type BloomFilter struct {
	bits    []uint64
	numBits uint
	numHash int
	mu      sync.RWMutex
}

// NewBloomFilter creates a Bloom filter sized for the expected number of
// elements with the given false positive rate.
//
// For 1M elements at 1% FPR: ~1.2 MB memory, 7 hash functions.
// For 10M elements at 1% FPR: ~12 MB memory, 7 hash functions.
func NewBloomFilter(expectedElements int, fpRate float64) *BloomFilter {
	if expectedElements <= 0 {
		expectedElements = 1000
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}

	// Optimal number of bits: m = -n*ln(p) / (ln2)^2
	m := uint(math.Ceil(-float64(expectedElements) * math.Log(fpRate) / (math.Ln2 * math.Ln2)))
	// Round up to multiple of 64 for uint64 alignment.
	m = ((m + 63) / 64) * 64

	// Optimal number of hash functions: k = (m/n) * ln2
	k := int(math.Ceil(float64(m) / float64(expectedElements) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &BloomFilter{
		bits:    make([]uint64, m/64),
		numBits: m,
		numHash: k,
	}
}

// Add inserts a key into the Bloom filter.
func (bf *BloomFilter) Add(key string) {
	h1, h2 := bf.hashes(key)
	bf.mu.Lock()
	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(bf.numBits)
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
	bf.mu.Unlock()
}

// MayContain returns true if the key might be in the set.
// False means definitely not in the set. True means probably in the set
// (with false positive rate as configured).
func (bf *BloomFilter) MayContain(key string) bool {
	h1, h2 := bf.hashes(key)
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(bf.numBits)
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// Count returns the approximate number of elements added, using the
// formula: n ≈ -(m/k) * ln(1 - X/m) where X = number of set bits.
func (bf *BloomFilter) Count() int {
	bf.mu.RLock()
	setBits := 0
	for _, word := range bf.bits {
		setBits += popcount(word)
	}
	bf.mu.RUnlock()

	if setBits == 0 {
		return 0
	}
	m := float64(bf.numBits)
	k := float64(bf.numHash)
	return int(-m / k * math.Log(1-float64(setBits)/m))
}

// Reset clears the Bloom filter.
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.mu.Unlock()
}

// hashes computes two independent hash values using double hashing.
// All k hash positions are derived as h1 + i*h2.
func (bf *BloomFilter) hashes(key string) (uint64, uint64) {
	var h hash.Hash64 = fnv.New64a()
	h.Write([]byte(key))
	h1 := h.Sum64()

	// Second hash: FNV with a seed byte prefix.
	h.Reset()
	h.Write([]byte{0x9e}) // seed differentiation
	h.Write([]byte(key))
	h2 := h.Sum64()

	return h1, h2
}

func popcount(x uint64) int {
	// Brian Kernighan's algorithm.
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}

// ShardBloomIndex maintains per-shard Bloom filters for primary key routing.
// Instead of scatter-gathering to all shards, the router checks Bloom filters
// to identify which shard(s) likely contain the key.
type ShardBloomIndex struct {
	filters map[int]*BloomFilter // shardID → Bloom filter
	mu      sync.RWMutex
}

// NewShardBloomIndex creates a new per-shard Bloom index.
func NewShardBloomIndex() *ShardBloomIndex {
	return &ShardBloomIndex{
		filters: make(map[int]*BloomFilter),
	}
}

// InitShard creates a Bloom filter for a shard with the expected capacity.
func (idx *ShardBloomIndex) InitShard(shardID, expectedKeys int, fpRate float64) {
	idx.mu.Lock()
	idx.filters[shardID] = NewBloomFilter(expectedKeys, fpRate)
	idx.mu.Unlock()
}

// Add records a key on a specific shard.
func (idx *ShardBloomIndex) Add(shardID int, key string) {
	idx.mu.RLock()
	bf, ok := idx.filters[shardID]
	idx.mu.RUnlock()
	if ok {
		bf.Add(key)
	}
}

// AddBatch adds multiple keys for a shard efficiently.
func (idx *ShardBloomIndex) AddBatch(shardID int, keys []string) {
	idx.mu.RLock()
	bf, ok := idx.filters[shardID]
	idx.mu.RUnlock()
	if !ok {
		return
	}
	for _, key := range keys {
		bf.Add(key)
	}
}

// MayContainOnShard checks if a specific shard's Bloom filter may contain the key.
// Returns false if the shard has no Bloom filter or the key is definitely absent.
func (idx *ShardBloomIndex) MayContainOnShard(shardID int, key string) bool {
	idx.mu.RLock()
	bf, ok := idx.filters[shardID]
	idx.mu.RUnlock()
	if !ok {
		return false
	}
	return bf.MayContain(key)
}

// LikelyShards returns the shard IDs that may contain the given key.
// If no Bloom filters are initialized, returns nil (caller should fall back
// to hash-based routing or scatter-gather).
func (idx *ShardBloomIndex) LikelyShards(key string) []int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.filters) == 0 {
		return nil
	}

	var shards []int
	for shardID, bf := range idx.filters {
		if bf.MayContain(key) {
			shards = append(shards, shardID)
		}
	}
	return shards
}

// Stats returns per-shard approximate element counts.
func (idx *ShardBloomIndex) Stats() map[int]int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	stats := make(map[int]int, len(idx.filters))
	for id, bf := range idx.filters {
		stats[id] = bf.Count()
	}
	return stats
}
