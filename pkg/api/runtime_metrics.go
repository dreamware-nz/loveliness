package api

import (
	"io"
	"runtime"
)

// writeRuntimeMetrics emits a small, fixed set of Go runtime gauges and
// counters. We deliberately don't pull in prometheus/client_golang's Go
// collector — its surface area is broad, and operators only need a
// handful of signals to spot memory or scheduler trouble. All series
// have zero label cardinality.
//
// Series exposed:
//
//	loveliness_go_goroutines                gauge
//	loveliness_go_memstats_alloc_bytes      gauge   (HeapAlloc)
//	loveliness_go_memstats_sys_bytes        gauge   (Sys — total OS memory obtained)
//	loveliness_go_memstats_heap_inuse_bytes gauge
//	loveliness_go_memstats_heap_objects     gauge
//	loveliness_go_gc_count_total            counter (NumGC)
//	loveliness_go_gc_pause_seconds_total    counter (PauseTotalNs / 1e9)
func writeRuntimeMetrics(w io.Writer) {
	emitHelp(w, "loveliness_go_goroutines", "Number of goroutines currently running.", "gauge")
	fprintf(w, "loveliness_go_goroutines %d\n", runtime.NumGoroutine())

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	emitHelp(w, "loveliness_go_memstats_alloc_bytes",
		"Bytes of heap objects currently allocated.", "gauge")
	fprintf(w, "loveliness_go_memstats_alloc_bytes %d\n", ms.HeapAlloc)

	emitHelp(w, "loveliness_go_memstats_sys_bytes",
		"Total bytes of memory obtained from the OS by the Go runtime.", "gauge")
	fprintf(w, "loveliness_go_memstats_sys_bytes %d\n", ms.Sys)

	emitHelp(w, "loveliness_go_memstats_heap_inuse_bytes",
		"Bytes in in-use heap spans.", "gauge")
	fprintf(w, "loveliness_go_memstats_heap_inuse_bytes %d\n", ms.HeapInuse)

	emitHelp(w, "loveliness_go_memstats_heap_objects",
		"Number of allocated heap objects.", "gauge")
	fprintf(w, "loveliness_go_memstats_heap_objects %d\n", ms.HeapObjects)

	emitHelp(w, "loveliness_go_gc_count_total",
		"Cumulative count of completed GC cycles.", "counter")
	fprintf(w, "loveliness_go_gc_count_total %d\n", ms.NumGC)

	emitHelp(w, "loveliness_go_gc_pause_seconds_total",
		"Cumulative time the program has been paused for GC, in seconds.", "counter")
	fprintf(w, "loveliness_go_gc_pause_seconds_total %s\n",
		formatGaugeFloat(float64(ms.PauseTotalNs)/1e9))
}
