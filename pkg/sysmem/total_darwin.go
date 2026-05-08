package sysmem

import "golang.org/x/sys/unix"

func total() (uint64, error) {
	return unix.SysctlUint64("hw.memsize")
}
