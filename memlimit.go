package binfinity

import (
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// applyMemoryLimit keeps the addon (and, via the same cgroup, the binfinity CLI
// it spawns) within the container's memory budget by setting Go's soft memory
// limit from the detected cgroup cap. The GC then paces itself to stay under the
// cap instead of letting the heap overshoot and getting OOM-killed on a small
// edge node. It does NOT change how data is streamed or sent — only how hard the
// GC works. An explicit GOMEMLIMIT env wins (runtime honours it); BINFINITY_MEM_LIMIT
// (bytes) is an override when cgroup files aren't readable. Stdlib only (the SDK
// imports nothing from the monorepo); mirrors shared/resourcegov.
func applyMemoryLimit() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	limit := int64(0)
	if v := strings.TrimSpace(os.Getenv("BINFINITY_MEM_LIMIT")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			limit = n
		}
	}
	if limit <= 0 {
		limit = cgroupMemLimit()
	}
	if limit <= 0 {
		return
	}
	soft := max(limit-max(limit/5, 32<<20), 48<<20)
	debug.SetMemoryLimit(soft)
	log.Printf("[binfinity-addon] container memory cap ~%d MiB → GOMEMLIMIT %d MiB (GC paces to avoid OOM)", limit>>20, soft>>20)
}

func cgroupMemLimit() int64 {
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil { // cgroup v2
		s := strings.TrimSpace(string(b))
		if s != "" && s != "max" {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 {
				return n
			}
		}
	}
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil { // cgroup v1
		if n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil {
			if n > 0 && n < (int64(1)<<62) { // ignore the v1 "unlimited" sentinel
				return n
			}
		}
	}
	return 0
}
