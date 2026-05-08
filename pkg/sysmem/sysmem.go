// Package sysmem reports total host memory.
//
// We need this for sizing per-shard buffer pools. The previous
// implementation used runtime.MemStats.Sys, which reports Go's heap
// reservation rather than host RAM — at startup that's tens of MB,
// so a 48 GB host got the conservative fallback and clamped each
// shard to 256 MB. Reading the kernel directly fixes that.
package sysmem
