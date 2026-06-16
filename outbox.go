package binfinity

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// outbox is a durable, at-least-once delivery buffer for command results (R3).
//
// A backup can take minutes; the report that tells CBS it succeeded is a single
// POST that can be lost to a network blip or a CBS restart at exactly the wrong
// moment. Without durability the result vanishes, the command sits dispatched
// until CBS's TTL reaper resets it, and the whole backup re-runs — wasteful even
// though it is safe (content-addressed + idempotent). The outbox closes that
// window: every result is written to disk *before* the first send attempt and is
// retried (across process restarts) until CBS acks it. CBS's Report is
// idempotent, so redelivering an already-applied result is a no-op.
//
// One file per command id keeps it crash-safe and trivially deduplicated: a
// second report for the same command overwrites the same file.
type outbox struct {
	dir string
	mu  sync.Mutex // serialises directory scans; file writes are atomic via rename
}

// pendingReport is the on-disk shape of an unacked result.
type pendingReport struct {
	CmdID  string `json:"cmd_id"`
	Result Result `json:"result"`
}

func newOutbox(dir string) *outbox {
	if dir == "" {
		return nil
	}
	_ = os.MkdirAll(dir, 0o700)
	return &outbox{dir: dir}
}

func (o *outbox) path(cmdID string) string {
	// cmdID is a server-minted UUID; still sanitise to keep it a single path element.
	return filepath.Join(o.dir, safeName(cmdID)+".json")
}

// enqueue durably records a result before any delivery attempt. An error here
// means the result is not safely on disk, so the caller should still try to send
// it best-effort (handled by the immediate send in report()).
func (o *outbox) enqueue(cmdID string, res Result) error {
	if o == nil {
		return nil
	}
	out, _ := json.Marshal(pendingReport{CmdID: cmdID, Result: res})
	tmp := o.path(cmdID) + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, o.path(cmdID)) // atomic publish
}

// ack removes a successfully delivered result.
func (o *outbox) ack(cmdID string) {
	if o == nil {
		return
	}
	_ = os.Remove(o.path(cmdID))
}

// pending lists the unacked results, oldest first (stable replay order).
func (o *outbox) pending() []pendingReport {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := make([]pendingReport, 0, len(names))
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(o.dir, n))
		if err != nil {
			continue
		}
		var p pendingReport
		if json.Unmarshal(b, &p) == nil && p.CmdID != "" {
			out = append(out, p)
		}
	}
	return out
}

// flush attempts to deliver every pending result once, removing those that ack.
// Returns the number still pending afterwards (used to back off the loop).
func (o *outbox) flush(ctx context.Context, send func(context.Context, pendingReport) bool) int {
	if o == nil {
		return 0
	}
	pend := o.pending()
	remaining := 0
	for _, p := range pend {
		if ctx.Err() != nil {
			return remaining + 1
		}
		if send(ctx, p) {
			o.ack(p.CmdID)
		} else {
			remaining++
		}
	}
	return remaining
}

// safeName keeps an id usable as a single filename element.
func safeName(s string) string {
	b := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b = append(b, r)
		default:
			b = append(b, '_')
		}
	}
	if len(b) == 0 {
		return "cmd"
	}
	return string(b)
}

// backoff returns a capped exponential delay with deterministic jitter derived
// from attempt + a per-runner seed, so a fleet of edges does not retry in
// lockstep (thundering herd) yet stays test-reproducible without rand.
func backoff(attempt int, base, max time.Duration, seed int64) time.Duration {
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	// jitter in [0, d/4) from a cheap LCG on (seed, attempt) — no global rand.
	span := int64(d / 4)
	if span <= 0 {
		return d
	}
	x := (seed*2654435761 + int64(attempt)*40503) & 0x7fffffff
	return d + time.Duration(x%span)
}
