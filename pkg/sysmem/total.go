package sysmem

import (
	"fmt"
	"runtime"
)

// Total returns the host's total physical memory in bytes.
//
// It reads /proc/meminfo on Linux and `hw.memsize` on macOS. On other
// platforms it returns an error so the caller can pick a fallback and
// log a warning rather than silently using a misleading value.
func Total() (uint64, error) {
	n, err := total()
	if err != nil {
		return 0, fmt.Errorf("total memory on %s: %w", runtime.GOOS, err)
	}
	return n, nil
}
