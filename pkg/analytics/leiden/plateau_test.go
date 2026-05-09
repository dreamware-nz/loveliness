package leiden

import (
	"math"
	"reflect"
	"testing"
)

// TestNMI_Identical: a partition compared against itself yields 1.
func TestNMI_Identical(t *testing.T) {
	a := []int{0, 0, 1, 1, 2, 2}
	if v := NMI(a, a); math.Abs(v-1.0) > 1e-9 {
		t.Errorf("NMI(a,a) should be 1, got %v", v)
	}
}

// TestNMI_TrivialPartitions: two single-community partitions are
// considered identical by convention (both H=0).
func TestNMI_TrivialPartitions(t *testing.T) {
	a := []int{0, 0, 0, 0}
	b := []int{7, 7, 7, 7}
	if v := NMI(a, b); v != 1 {
		t.Errorf("NMI(trivial,trivial) should be 1, got %v", v)
	}
}

// TestNMI_RelabelInvariant: NMI must not change when one partition's
// labels are renamed (it cares about clusters, not labels).
func TestNMI_RelabelInvariant(t *testing.T) {
	a := []int{0, 0, 1, 1, 2, 2}
	b := []int{9, 9, 5, 5, 3, 3} // same clustering, different labels
	if v := NMI(a, b); math.Abs(v-1.0) > 1e-9 {
		t.Errorf("NMI should be 1 under relabeling, got %v", v)
	}
}

// TestNMI_OrthogonalPartitions: a uniform partition vs. its inverse
// has NMI between 0 and 1; we just lock that the result is finite,
// in [0,1], and not 1 (different clusterings).
func TestNMI_OrthogonalPartitions(t *testing.T) {
	a := []int{0, 1, 0, 1, 0, 1}
	b := []int{0, 0, 1, 1, 2, 2}
	v := NMI(a, b)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		t.Fatalf("NMI not finite: %v", v)
	}
	if v < 0 || v > 1 {
		t.Errorf("NMI out of [0,1]: %v", v)
	}
	if v == 1 {
		t.Errorf("expected NMI < 1 for distinct partitions, got 1")
	}
}

// TestNMI_LengthMismatch: returns NaN rather than panicking.
func TestNMI_LengthMismatch(t *testing.T) {
	if v := NMI([]int{0, 1}, []int{0, 0, 1}); !math.IsNaN(v) {
		t.Errorf("expected NaN for length mismatch, got %v", v)
	}
}

// TestDetectPlateaus_SingleStablePartition: same partition across all
// γ values → one plateau spanning the full range.
func TestDetectPlateaus_SingleStablePartition(t *testing.T) {
	gammas := []float64{0.5, 1.0, 1.5, 2.0}
	p := []int{0, 0, 1, 1, 2, 2}
	parts := [][]int{p, p, p, p}
	got := DetectPlateaus(gammas, parts, 0.95)
	if len(got) != 1 {
		t.Fatalf("expected 1 plateau, got %d (%+v)", len(got), got)
	}
	if got[0].GammaMin != 0.5 || got[0].GammaMax != 2.0 {
		t.Errorf("plateau range: [%v,%v], want [0.5,2.0]", got[0].GammaMin, got[0].GammaMax)
	}
	if got[0].NumCommunities != 3 {
		t.Errorf("plateau num_communities: %d", got[0].NumCommunities)
	}
}

// TestDetectPlateaus_TwoPlateaus: a clean break between two stable
// regions yields two plateaus split at the transition.
func TestDetectPlateaus_TwoPlateaus(t *testing.T) {
	gammas := []float64{0.5, 1.0, 2.0, 3.0}
	low := []int{0, 0, 0, 0, 0, 0}    // one community
	high := []int{0, 1, 2, 3, 4, 5}   // all singletons
	parts := [][]int{low, low, high, high}
	got := DetectPlateaus(gammas, parts, 0.95)
	if len(got) != 2 {
		t.Fatalf("expected 2 plateaus, got %d (%+v)", len(got), got)
	}
	if got[0].IndexLo != 0 || got[0].IndexHi != 1 {
		t.Errorf("plateau[0] indices: %+v", got[0])
	}
	if got[1].IndexLo != 2 || got[1].IndexHi != 3 {
		t.Errorf("plateau[1] indices: %+v", got[1])
	}
	if got[0].NumCommunities != 1 || got[1].NumCommunities != 6 {
		t.Errorf("plateau community counts: %+v", got)
	}
}

// TestDetectPlateaus_EmptyInput: zero γs → nil result.
func TestDetectPlateaus_EmptyInput(t *testing.T) {
	if got := DetectPlateaus(nil, nil, 0.95); got != nil {
		t.Errorf("expected nil for empty input, got %+v", got)
	}
}

// TestDetectPlateaus_SingleEntry: one γ → one plateau of length 1
// covering that single entry. Representative is the entry itself.
func TestDetectPlateaus_SingleEntry(t *testing.T) {
	got := DetectPlateaus([]float64{1.0}, [][]int{{0, 0, 1, 1}}, 0.95)
	if len(got) != 1 {
		t.Fatalf("expected 1 plateau, got %d", len(got))
	}
	want := Plateau{GammaMin: 1.0, GammaMax: 1.0, IndexLo: 0, IndexHi: 0, NumCommunities: 2, Representative: 0}
	if !reflect.DeepEqual(got[0], want) {
		t.Errorf("plateau: %+v, want %+v", got[0], want)
	}
}

// TestDetectPlateaus_LengthMismatch: defensive — returns nil on bad input.
func TestDetectPlateaus_LengthMismatch(t *testing.T) {
	got := DetectPlateaus([]float64{1.0, 2.0}, [][]int{{0, 0}}, 0.95)
	if got != nil {
		t.Errorf("expected nil for length mismatch, got %+v", got)
	}
}

// TestDetectPlateaus_RepresentativeIsMidpoint: a 5-entry plateau picks
// the middle index (2). For 4 entries, picks lo + (hi-lo)/2 = 0+3/2 = 1.
func TestDetectPlateaus_RepresentativeIsMidpoint(t *testing.T) {
	gammas := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
	p := []int{0, 0, 1, 1}
	parts := [][]int{p, p, p, p, p}
	got := DetectPlateaus(gammas, parts, 0.95)
	if len(got) != 1 {
		t.Fatalf("expected 1 plateau, got %d", len(got))
	}
	if got[0].Representative != 2 {
		t.Errorf("expected midpoint 2, got %d", got[0].Representative)
	}
}
