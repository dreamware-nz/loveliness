package leiden

import "math"

// Plateau is a contiguous run of γ values that produce
// near-identical partitions, separated by region(s) where the
// partition shifts. Plateaus indicate natural community-structure
// scales: γ values inside one give the same answer, so picking any
// representative is safe.
type Plateau struct {
	// GammaMin/GammaMax bracket the plateau (inclusive on both ends).
	GammaMin float64
	GammaMax float64
	// Indices into the original gamma slice (inclusive). A plateau
	// of one entry has IndexLo == IndexHi.
	IndexLo int
	IndexHi int
	// NumCommunities is the partition size shared across the plateau.
	NumCommunities int
	// Representative is the index of the γ chosen as the plateau's
	// canonical sample (currently the midpoint).
	Representative int
}

// DetectPlateaus groups a list of γ-ordered partitions into plateaus.
// Two adjacent partitions belong to the same plateau when their
// normalized mutual information is ≥ threshold. The input must be
// sorted by ascending γ; partitions[i] is the partition produced at
// gammas[i] (every slice is over the same node set, with N == len of
// each entry).
//
// Threshold semantics: 1.0 means "must be identical"; 0.95 (the typical
// default) tolerates tiny boundary jitter; lower values widen plateaus
// at the cost of merging genuinely distinct scales.
//
// Returns plateaus in ascending γ order. An empty input returns nil.
func DetectPlateaus(gammas []float64, partitions [][]int, threshold float64) []Plateau {
	if len(gammas) == 0 {
		return nil
	}
	if len(gammas) != len(partitions) {
		return nil
	}

	plateaus := []Plateau{}
	startIdx := 0
	for i := 1; i < len(gammas); i++ {
		nmi := NMI(partitions[i-1], partitions[i])
		if nmi >= threshold {
			continue
		}
		plateaus = append(plateaus, makePlateau(gammas, partitions, startIdx, i-1))
		startIdx = i
	}
	plateaus = append(plateaus, makePlateau(gammas, partitions, startIdx, len(gammas)-1))
	return plateaus
}

func makePlateau(gammas []float64, partitions [][]int, lo, hi int) Plateau {
	rep := lo + (hi-lo)/2
	return Plateau{
		GammaMin:       gammas[lo],
		GammaMax:       gammas[hi],
		IndexLo:        lo,
		IndexHi:        hi,
		NumCommunities: countDistinct(partitions[rep]),
		Representative: rep,
	}
}

// NMI is the normalized mutual information between two partitions
// of the same node set, using the symmetric arithmetic-mean
// normalization 2·I(X;Y) / (H(X)+H(Y)).
//
// Range: [0, 1]. Returns 1 if both partitions are constant (single
// community on each side — H is zero on both, MI is zero, by
// convention NMI(trivial, trivial) is 1 because the partitions are
// trivially identical). Returns NaN for length-mismatched input.
func NMI(a, b []int) float64 {
	if len(a) != len(b) {
		return math.NaN()
	}
	n := len(a)
	if n == 0 {
		return 1
	}

	// Joint and marginal counts.
	joint := map[[2]int]int{}
	countA := map[int]int{}
	countB := map[int]int{}
	for i := 0; i < n; i++ {
		joint[[2]int{a[i], b[i]}]++
		countA[a[i]]++
		countB[b[i]]++
	}

	hA := entropy(countA, n)
	hB := entropy(countB, n)
	if hA == 0 && hB == 0 {
		// Both partitions are trivial; consider them identical.
		return 1
	}

	mi := 0.0
	for k, c := range joint {
		pXY := float64(c) / float64(n)
		pX := float64(countA[k[0]]) / float64(n)
		pY := float64(countB[k[1]]) / float64(n)
		mi += pXY * math.Log(pXY/(pX*pY))
	}

	denom := hA + hB
	if denom == 0 {
		return 0
	}
	nmi := 2 * mi / denom
	// Clamp to [0,1] to absorb floating-point drift; with finite-precision
	// arithmetic NMI can land at 1.0000000003 for an identical partition.
	if nmi > 1 {
		nmi = 1
	}
	if nmi < 0 {
		nmi = 0
	}
	return nmi
}

func entropy(counts map[int]int, n int) float64 {
	h := 0.0
	for _, c := range counts {
		if c == 0 {
			continue
		}
		p := float64(c) / float64(n)
		h -= p * math.Log(p)
	}
	return h
}
