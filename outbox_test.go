package binfinity

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestOutbox_PersistAndReplay covers the core R3 contract: a result is on disk
// before delivery, survives an unacked send, and is removed only after an ack.
func TestOutbox_PersistAndReplay(t *testing.T) {
	dir := t.TempDir()
	ob := newOutbox(filepath.Join(dir, "outbox"))

	if err := ob.enqueue("cmd-1", Result{Status: statusDone, BackupID: "bk-1", Bytes: 42}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// It is durably present until acked.
	if got := ob.pending(); len(got) != 1 || got[0].CmdID != "cmd-1" || got[0].Result.BackupID != "bk-1" {
		t.Fatalf("pending = %+v, want one cmd-1/bk-1", got)
	}

	// A fresh outbox over the same dir (simulating a process restart) still sees it.
	reopened := newOutbox(filepath.Join(dir, "outbox"))
	if got := reopened.pending(); len(got) != 1 {
		t.Fatalf("after reopen pending = %d, want 1 (durable across restart)", len(got))
	}

	// flush with a sender that fails once then succeeds: stays pending, then clears.
	var attempts int32
	send := func(_ context.Context, _ pendingReport) bool { return atomic.AddInt32(&attempts, 1) >= 2 }
	if rem := reopened.flush(context.Background(), send); rem != 1 {
		t.Fatalf("first flush remaining = %d, want 1 (send refused)", rem)
	}
	if rem := reopened.flush(context.Background(), send); rem != 0 {
		t.Fatalf("second flush remaining = %d, want 0 (send acked)", rem)
	}
	if got := reopened.pending(); len(got) != 0 {
		t.Fatalf("after ack pending = %d, want 0", len(got))
	}
}

// TestReport_RetriesUntilAcked drives the full runner: the report endpoint fails
// the first two attempts (500) then accepts, and the result must still land.
func TestReport_RetriesUntilAcked(t *testing.T) {
	var reportHits int32
	var mu sync.Mutex
	var got *Result

	mux := http.NewServeMux()
	mux.HandleFunc("POST /ams/api/v1/enroll", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(Credentials{ClientID: "system-abc", Name: "edge"})
	})
	mux.HandleFunc("POST /cbs/api/v1/agent/heartbeat", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	dispatched := int32(0)
	mux.HandleFunc("GET /cbs/api/v1/agent/commands", func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&dispatched, 1) == 1 {
			_ = json.NewEncoder(w).Encode(&Command{ID: "cmd-1", SystemID: "system-abc", Type: CmdBackup})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /cbs/api/v1/agent/commands/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&reportHits, 1) < 3 { // fail the first two attempts
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		var res Result
		_ = json.NewDecoder(r.Body).Decode(&res)
		mu.Lock()
		got = &res
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := baseConfig(srv.URL, &stubDataPlane{})
	cfg.CredPath = filepath.Join(t.TempDir(), "credentials.json") // enables the durable outbox
	cfg.ReportBase = 30 * time.Millisecond                        // fast retries for the test

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	go func() { _ = Run(ctx, echoConnector{payload: "data"}, cfg) }()

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		done := got != nil
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("result never acked after retries (report hits=%d)", atomic.LoadInt32(&reportHits))
		case <-time.After(20 * time.Millisecond):
		}
	}
	if n := atomic.LoadInt32(&reportHits); n < 3 {
		t.Fatalf("expected >=3 report attempts (2 failures + success), got %d", n)
	}
}

// incConnector implements IncrementalConnector to exercise the Layer-B contract.
type incConnector struct {
	gotCursor string
	restored  []string // "base" / "delta" markers in apply order
}

func (incConnector) DataType() string                                  { return "inc" }
func (incConnector) Backup(_ context.Context, w io.Writer) error       { _, e := w.Write([]byte("FULL")); return e }
func (incConnector) Restore(_ context.Context, _ io.Reader) error      { return nil }
func (c *incConnector) BackupIncremental(_ context.Context, w io.Writer, cursor string) (string, bool, error) {
	c.gotCursor = cursor
	_, err := w.Write([]byte("DELTA-since-" + cursor))
	return "cursor-" + cursor + "+1", true, err // advance the cursor
}
func (c *incConnector) RestoreIncremental(_ context.Context, _ io.Reader, isBase bool) error {
	if isBase {
		c.restored = append(c.restored, "base")
	} else {
		c.restored = append(c.restored, "delta")
	}
	return nil
}

// TestIncremental_BackupUsesCursorAndReturnsNext proves the SDK calls
// BackupIncremental with the command's cursor and rides the new cursor back.
func TestIncremental_BackupUsesCursorAndReturnsNext(t *testing.T) {
	conn := &incConnector{}
	dp := &stubDataPlane{}
	r, err := newRunner(conn, baseConfig("http://x", dp))
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	res := r.doBackup(context.Background(), Command{BackupID: "bk-2", Type: CmdBackup, Mode: "incremental", Cursor: "pos-100"})
	if res.Status != statusDone {
		t.Fatalf("status=%q err=%q", res.Status, res.Error)
	}
	if conn.gotCursor != "pos-100" {
		t.Fatalf("connector got cursor %q, want pos-100", conn.gotCursor)
	}
	if res.Cursor != "cursor-pos-100+1" {
		t.Fatalf("result cursor=%q, want advanced", res.Cursor)
	}
	if string(dp.produced) != "DELTA-since-pos-100" {
		t.Fatalf("data plane got %q, want the delta stream", dp.produced)
	}
}

// TestIncremental_RestoreWalksChainInOrder proves a chain restore applies the
// base first, then deltas, in order.
func TestIncremental_RestoreWalksChainInOrder(t *testing.T) {
	conn := &incConnector{}
	dp := &stubDataPlane{restoreData: []byte("x")}
	r, _ := newRunner(conn, baseConfig("http://x", dp))
	res := r.doRestore(context.Background(), Command{BackupID: "bk-3", Type: CmdRestore, Chain: []string{"full-1", "delta-2", "delta-3"}})
	if res.Status != statusDone {
		t.Fatalf("status=%q err=%q", res.Status, res.Error)
	}
	want := []string{"base", "delta", "delta"}
	if len(conn.restored) != 3 || conn.restored[0] != "base" || conn.restored[1] != "delta" || conn.restored[2] != "delta" {
		t.Fatalf("chain applied as %v, want %v", conn.restored, want)
	}
}

func TestBackoff_GrowsAndStaysCapped(t *testing.T) {
	base, max := time.Second, 30*time.Second
	prev := time.Duration(0)
	for attempt := 0; attempt < 10; attempt++ {
		d := backoff(attempt, base, max, 12345)
		if d < base || d > max+max/4 {
			t.Fatalf("attempt %d: %v out of [%v, %v]", attempt, d, base, max+max/4)
		}
		if attempt > 0 && attempt < 6 && d < prev {
			t.Fatalf("attempt %d: %v should be >= prev %v (monotonic in the growth region)", attempt, d, prev)
		}
		prev = d
	}
}
