package sysmem

import "testing"

func TestTotalReturnsRealValue(t *testing.T) {
	n, err := Total()
	if err != nil {
		// On unsupported platforms, that's a clear error — don't fail
		// the suite just because it ran in CI without /proc.
		t.Skipf("sysmem.Total unsupported here: %v", err)
	}
	// The smallest CI runner we use has at least 2 GB. Anything below
	// that means we're reading something other than host RAM.
	if n < 1<<30 {
		t.Fatalf("Total() = %d bytes, want at least 1 GiB", n)
	}
}
