//go:build !linux && !darwin

package sysmem

import (
	"fmt"
	"runtime"
)

func total() (uint64, error) {
	return 0, fmt.Errorf("no sysmem implementation for %s", runtime.GOOS)
}
