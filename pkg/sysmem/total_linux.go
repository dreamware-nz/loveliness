package sysmem

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func total() (uint64, error) {
	return parseMeminfo("/proc/meminfo")
}

// parseMeminfo extracts MemTotal from a /proc/meminfo file. Split out
// for testability — exported via an internal test that points it at a
// fixture.
func parseMeminfo(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		// "MemTotal:       49355436 kB"
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return 0, fmt.Errorf("meminfo MemTotal malformed: %q", line)
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("meminfo MemTotal value: %w", err)
		}
		// /proc/meminfo always reports in kB regardless of the unit
		// suffix, but check anyway in case that ever changes.
		if fields[2] != "kB" {
			return 0, fmt.Errorf("meminfo MemTotal unit %q not handled", fields[2])
		}
		return kb * 1024, nil
	}
	if err := s.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("MemTotal not found in %s", path)
}
