package binfinity

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeConsole is a minimal in-memory stand-in for AMS+CBS: it enrolls, hands out
// one queued command, records heartbeats and the reported result.
type fakeConsole struct {
	mu         sync.Mutex
	srv        *httptest.Server
	cmd        *Command // the single command to dispatch, then nil
	dispatched bool
	heartbeats int
	lastHB     Heartbeat
	result     *Result
	resultCmd  string
}

func newFakeConsole(cmd *Command) *fakeConsole {
	f := &fakeConsole{cmd: cmd}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ams/api/v1/enroll", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SetupKey string `json:"setup_key"`
			Name     string `json:"name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.SetupKey == "" {
			http.Error(w, `{"error":"setup_key is required"}`, http.StatusBadRequest)
			return
		}
		if req.SetupKey != "bsk_good" {
			http.Error(w, `{"error":"invalid"}`, http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(Credentials{
			ClientID: "system-abc", Name: req.Name, TenantID: "t1", Roles: []string{"system"},
		})
	})
	mux.HandleFunc("POST /cbs/api/v1/agent/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var hb Heartbeat
		_ = json.NewDecoder(r.Body).Decode(&hb)
		f.mu.Lock()
		f.heartbeats++
		f.lastHB = hb
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /cbs/api/v1/agent/commands", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.cmd == nil || f.dispatched {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		f.dispatched = true
		_ = json.NewEncoder(w).Encode(f.cmd)
	})
	mux.HandleFunc("POST /cbs/api/v1/agent/commands/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		var res Result
		_ = json.NewDecoder(r.Body).Decode(&res)
		f.mu.Lock()
		f.result = &res
		f.resultCmd = r.PathValue("id")
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *fakeConsole) close() { f.srv.Close() }

func (f *fakeConsole) gotResult() (Result, string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.result == nil {
		return Result{}, "", false
	}
	return *f.result, f.resultCmd, true
}

// stubDataPlane captures what the runtime asked of the data plane and drives the
// connector's produce/consume without any CLI.
type stubDataPlane struct {
	mu          sync.Mutex
	backupID    string
	sourceType  string
	produced    []byte
	restoreData []byte
	consumed    []byte
}

func (s *stubDataPlane) Backup(_ context.Context, id, srcType string, produce func(io.Writer) error) (int64, error) {
	var buf strings.Builder
	if err := produce(stringWriter{&buf}); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backupID, s.sourceType, s.produced = id, srcType, []byte(buf.String())
	return int64(len(s.produced)), nil
}

func (s *stubDataPlane) Restore(_ context.Context, _ string, consume func(io.Reader) error) error {
	return consume(strings.NewReader(string(s.restoreData)))
}

type stringWriter struct{ b *strings.Builder }

func (w stringWriter) Write(p []byte) (int, error) { return w.b.Write(p) }

// echoConnector backs up a fixed payload and records what it restores.
type echoConnector struct {
	payload  string
	restored chan string
}

func (echoConnector) DataType() string { return "echo" }
func (c echoConnector) Backup(_ context.Context, w io.Writer) error {
	_, err := io.WriteString(w, c.payload)
	return err
}
func (c echoConnector) Restore(_ context.Context, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	c.restored <- string(b)
	return nil
}

func baseConfig(endpoint string, dp DataPlane) Config {
	return Config{
		Endpoint:  endpoint,
		SetupKey:  "bsk_good",
		Store:     "stub",
		CredPath:  "", // not persisted in tests (write fails silently)
		Heartbeat: 20 * time.Millisecond,
		Poll:      10 * time.Millisecond,
		DataPlane: dp,
		Logf:      func(string, ...any) {},
	}
}

func TestRun_BackupCommand_EndToEnd(t *testing.T) {
	fc := newFakeConsole(&Command{ID: "cmd-1", SystemID: "system-abc", Type: CmdBackup})
	defer fc.close()
	dp := &stubDataPlane{}
	conn := echoConnector{payload: "SELECT 1; -- dump bytes"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, conn, baseConfig(fc.srv.URL, dp)) }()

	res := waitForResult(t, fc)
	cancel()
	<-done

	if res.Status != statusDone {
		t.Fatalf("status = %q, want done (err=%q)", res.Status, res.Error)
	}
	if res.BackupID == "" {
		t.Fatal("expected the edge to assign a backup id for an empty-id backup command")
	}
	if !strings.HasPrefix(res.BackupID, "echo-") {
		t.Errorf("assigned id %q should be prefixed with the data type", res.BackupID)
	}
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if string(dp.produced) != conn.payload {
		t.Errorf("data plane produced %q, want %q", dp.produced, conn.payload)
	}
	if dp.sourceType != "echo" {
		t.Errorf("source-type = %q, want echo", dp.sourceType)
	}
	if res.Bytes != int64(len(conn.payload)) {
		t.Errorf("bytes = %d, want %d", res.Bytes, len(conn.payload))
	}
}

func TestRun_RestoreCommand_FeedsConnector(t *testing.T) {
	fc := newFakeConsole(&Command{ID: "cmd-2", SystemID: "system-abc", Type: CmdRestore, BackupID: "echo-123"})
	defer fc.close()
	dp := &stubDataPlane{restoreData: []byte("restored payload")}
	conn := echoConnector{restored: make(chan string, 1)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = Run(ctx, conn, baseConfig(fc.srv.URL, dp)) }()

	select {
	case got := <-conn.restored:
		if got != "restored payload" {
			t.Errorf("connector restored %q, want %q", got, "restored payload")
		}
	case <-ctx.Done():
		t.Fatal("connector.Restore was never called")
	}
	res := waitForResult(t, fc)
	if res.Status != statusDone || res.BackupID != "echo-123" {
		t.Errorf("restore result = %+v, want done/echo-123", res)
	}
}

func TestRun_UnknownCommandType_ReportsFailed(t *testing.T) {
	fc := newFakeConsole(&Command{ID: "cmd-3", SystemID: "system-abc", Type: "frobnicate"})
	defer fc.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = Run(ctx, echoConnector{}, baseConfig(fc.srv.URL, &stubDataPlane{})) }()

	res := waitForResult(t, fc)
	if res.Status != statusFailed || !strings.Contains(res.Error, "unknown command type") {
		t.Errorf("result = %+v, want failed/unknown command type", res)
	}
}

func TestRun_Heartbeat_ReportsDataTypeAndActivity(t *testing.T) {
	fc := newFakeConsole(nil) // no commands; just heartbeats
	defer fc.close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	go func() { _ = Run(ctx, echoConnector{}, baseConfig(fc.srv.URL, &stubDataPlane{})) }()

	deadline := time.After(500 * time.Millisecond)
	for {
		fc.mu.Lock()
		n, hb := fc.heartbeats, fc.lastHB
		fc.mu.Unlock()
		if n > 0 {
			if hb.DataType != "echo" || hb.SystemID != "system-abc" || hb.Activity != ActivityIdle {
				t.Errorf("heartbeat = %+v, want echo/system-abc/idle", hb)
			}
			cancel()
			return
		}
		select {
		case <-deadline:
			t.Fatal("no heartbeat received")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRun_RejectsMissingEndpoint(t *testing.T) {
	err := Run(context.Background(), echoConnector{}, Config{SetupKey: "bsk_good"})
	if err == nil || !strings.Contains(err.Error(), "Endpoint") {
		t.Fatalf("err = %v, want a missing-Endpoint error", err)
	}
}

func TestRun_RejectsBadSetupKeyWithNoCreds(t *testing.T) {
	fc := newFakeConsole(nil)
	defer fc.close()
	cfg := baseConfig(fc.srv.URL, &stubDataPlane{})
	cfg.SetupKey = "bsk_wrong"
	err := Run(context.Background(), echoConnector{}, cfg)
	if err == nil || !strings.Contains(err.Error(), "enrollment failed") {
		t.Fatalf("err = %v, want enrollment failure", err)
	}
}

func waitForResult(t *testing.T, fc *fakeConsole) Result {
	t.Helper()
	deadline := time.After(1500 * time.Millisecond)
	for {
		if res, _, ok := fc.gotResult(); ok {
			return res
		}
		select {
		case <-deadline:
			t.Fatal("no result reported within deadline")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
