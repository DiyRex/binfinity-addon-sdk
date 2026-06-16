// Package binfinity is the Binfinity Addon SDK. It implements the universal edge
// contract once so that building a backup connector for a new data source means
// writing only the source-specific convert step — produce a byte stream to back
// up, consume one to restore. Everything else (enroll, heartbeat, poll, execute,
// report, BSP/crypto/transport via the CLI) is handled here.
//
// Minimal addon:
//
//	type files struct{ root string }
//	func (files) DataType() string { return "files" }
//	func (f files) Backup(ctx context.Context, w io.Writer) error {
//		c := exec.CommandContext(ctx, "tar", "-c", "-C", f.root, "."); c.Stdout = w; return c.Run()
//	}
//	func (f files) Restore(ctx context.Context, r io.Reader) error {
//		c := exec.CommandContext(ctx, "tar", "-x", "-C", f.root); c.Stdin = r; return c.Run()
//	}
//	func main() { binfinity.Main(files{root: "/data"}) }
//
// The bytes always leave as the single canonical BSP stream, so nothing on the
// server is source-specific — schedules, retention, restore, multi-cloud and the
// live Console map all work with zero server changes. See DEVELOPMENT.md.
package binfinity

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Connector is the ONLY thing an addon author implements. It captures the three
// source-specific facts the SDK cannot know: what the data type is called, how
// to turn the source into a byte stream, and how to put a byte stream back.
//
// Backup writes the source's bytes to w (e.g. set a command's Stdout to w, or
// io.Copy into it). Restore reads the recovered bytes from r and applies them to
// the source. Both receive a context that is cancelled on shutdown; honour it.
// The streams are opaque to Binfinity — any format works, you just have to
// reproduce the same bytes on restore.
type Connector interface {
	DataType() string
	Backup(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
}

// Config holds everything the runtime needs. Use ConfigFromEnv for the standard
// environment-driven setup, then override fields as needed.
type Config struct {
	Endpoint   string        // Console base URL (BF_ENDPOINT), e.g. https://binfinity.example.com
	SetupKey   string        // enrollment key minted in the Console (BF_SETUP_KEY)
	Store      string        // SMS data-plane spec (STORE_SPEC), e.g. grpc://host:8090
	Passphrase string        // tenant passphrase (BINFINITY_PASSPHRASE) — never leaves the edge
	Name       string        // system name shown in the Console (default <data_type>-<hostname>)
	CredPath   string        // where durable credentials are persisted (CRED_PATH)
	Heartbeat  time.Duration // heartbeat interval (HEARTBEAT_INTERVAL, default 10s)
	Poll       time.Duration // command poll interval (POLL_INTERVAL, default 5s)
	Insecure   bool          // skip TLS verification (BF_TLS_INSECURE=true) — dev only

	HTTPClient *http.Client // optional; a sane default is built if nil
	DataPlane  DataPlane    // optional; defaults to CLIDataPlane (shells out to `binfinity`)
	Logf       func(string, ...any)
}

// ConfigFromEnv reads the standard addon environment. data_type seeds the
// default system name. Unset durations fall back to 10s/5s.
func ConfigFromEnv(dataType string) Config {
	return Config{
		Endpoint:   os.Getenv("BF_ENDPOINT"),
		SetupKey:   os.Getenv("BF_SETUP_KEY"),
		Store:      envOr("STORE_SPEC", "grpc://localhost:8090"),
		Passphrase: os.Getenv("BINFINITY_PASSPHRASE"),
		Name:       envOr("BF_NAME", dataType+"-"+hostname()),
		CredPath:   envOr("CRED_PATH", "/data/credentials.json"),
		Heartbeat:  durationOr("HEARTBEAT_INTERVAL", 10*time.Second),
		Poll:       durationOr("POLL_INTERVAL", 5*time.Second),
		Insecure:   os.Getenv("BF_TLS_INSECURE") == "true",
	}
}

// Main is the one-line entrypoint: it builds Config from the environment, wires
// SIGINT/SIGTERM to graceful shutdown, runs the agent, and exits non-zero on a
// fatal startup error. Most addons need nothing more than `func main() {
// binfinity.Main(myConnector{}) }`.
func Main(c Connector) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, c, ConfigFromEnv(c.DataType())); err != nil {
		log.Fatalf("[binfinity-addon] fatal: %v", err)
	}
}

// Run enrolls (or reuses saved credentials), then runs the heartbeat + poll
// loops until ctx is cancelled. It returns an error only on a fatal startup
// problem (no endpoint, or enrollment failed with no saved credentials);
// transient runtime errors are logged and retried on the next tick.
func Run(ctx context.Context, c Connector, cfg Config) error {
	r, err := newRunner(c, cfg)
	if err != nil {
		return err
	}
	creds, err := r.ensureEnrolled(ctx)
	if err != nil {
		return fmt.Errorf("enrollment failed (need a valid BF_SETUP_KEY or saved credentials): %w", err)
	}
	r.systemID, r.name, r.secret = creds.ClientID, creds.Name, creds.Secret
	r.logf("enrolled as system %s (%s); data_type=%s store=%s", r.systemID, r.name, c.DataType(), cfg.Store)

	go r.heartbeatLoop(ctx)
	r.pollLoop(ctx) // blocks until ctx cancelled
	r.logf("stopped")
	return nil
}

// runner is the live agent state. Unexported: the public surface is Connector +
// Config + Run/Main.
type runner struct {
	conn     Connector
	cfg      Config
	hc       *http.Client
	dp       DataPlane
	systemID string
	secret   string
	name     string

	// pmu guards activity + the live-progress fields below (the heartbeat
	// goroutine reads them while the poll goroutine drives a backup/restore).
	pmu        sync.Mutex
	activity   string
	startedAt  time.Time
	bytesDone  int64
	bytesTotal int64

	tmu    sync.Mutex
	tok    string
	tokExp time.Time
}

// SizeEstimator is an OPTIONAL Connector capability. If a connector implements
// it, the SDK calls EstimateBytes once before a backup to seed the progress
// total, so the Console can show an approximate % and ETA. It is best-effort:
// return 0 when the size isn't known cheaply (a pure stream need not implement
// it). The value is an ESTIMATE of source bytes, not the stored (deduped,
// compressed, encrypted) size.
type SizeEstimator interface {
	EstimateBytes(ctx context.Context) int64
}

// countingWriter tallies bytes as they flow to the underlying writer, so the
// heartbeat can report exact backup progress without buffering or staging.
type countingWriter struct {
	w   io.Writer
	add func(int64)
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.add(int64(n))
	}
	return n, err
}

// countingReader tallies bytes as the connector consumes the restore stream.
type countingReader struct {
	r   io.Reader
	add func(int64)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.add(int64(n))
	}
	return n, err
}

func (r *runner) beginBackup(total int64) {
	r.pmu.Lock()
	r.activity, r.startedAt, r.bytesDone, r.bytesTotal = ActivityBackingUp, time.Now().UTC(), 0, total
	r.pmu.Unlock()
}

func (r *runner) beginRestore() {
	r.pmu.Lock()
	r.activity, r.startedAt, r.bytesDone, r.bytesTotal = ActivityRestoring, time.Now().UTC(), 0, 0
	r.pmu.Unlock()
}

func (r *runner) addBytes(n int64) {
	r.pmu.Lock()
	r.bytesDone += n
	r.pmu.Unlock()
}

func (r *runner) endActivity() {
	r.pmu.Lock()
	r.activity, r.startedAt, r.bytesDone, r.bytesTotal = ActivityIdle, time.Time{}, 0, 0
	r.pmu.Unlock()
}

// progress snapshots the live state for a heartbeat.
func (r *runner) progress() (activity string, done, total int64, started time.Time) {
	r.pmu.Lock()
	defer r.pmu.Unlock()
	return r.activity, r.bytesDone, r.bytesTotal, r.startedAt
}

// token returns a cached AMS access token, refreshing via the client-credentials
// grant (enrolled client_id + secret) when expired. Best-effort: "" on failure
// so requests still go out (CBS enforces only when agent auth is required).
func (r *runner) token(ctx context.Context) string {
	r.tmu.Lock()
	defer r.tmu.Unlock()
	if r.tok != "" && time.Now().Before(r.tokExp) {
		return r.tok
	}
	if r.systemID == "" || r.secret == "" {
		return ""
	}
	body, _ := json.Marshal(map[string]string{"client_id": r.systemID, "client_secret": r.secret})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.Endpoint+"/ams/auth/token", bytes.NewReader(body))
	if err != nil {
		return ""
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.hc.Do(req) // raw — no auth header, avoids recursion
	if err != nil || resp == nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var t struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if json.NewDecoder(resp.Body).Decode(&t) != nil || t.AccessToken == "" {
		return ""
	}
	ttl := t.ExpiresIn
	if ttl <= 0 {
		ttl = 300
	}
	r.tok, r.tokExp = t.AccessToken, time.Now().Add(time.Duration(ttl-30)*time.Second)
	return r.tok
}

func newRunner(c Connector, cfg Config) (*runner, error) {
	if c == nil {
		return nil, errors.New("nil Connector")
	}
	if cfg.Endpoint == "" {
		return nil, errors.New("Config.Endpoint (BF_ENDPOINT) is required")
	}
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 10 * time.Second
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 5 * time.Second
	}
	hc := cfg.HTTPClient
	if hc == nil {
		tr := &http.Transport{}
		if cfg.Insecure {
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // dev opt-in via BF_TLS_INSECURE
		}
		hc = &http.Client{Timeout: 30 * time.Second, Transport: tr}
	}
	dp := cfg.DataPlane
	if dp == nil {
		dp = CLIDataPlane{Store: cfg.Store, Passphrase: cfg.Passphrase}
	}
	return &runner{conn: c, cfg: cfg, hc: hc, dp: dp, activity: ActivityIdle}, nil
}

func (r *runner) logf(format string, args ...any) {
	if r.cfg.Logf != nil {
		r.cfg.Logf(format, args...)
		return
	}
	log.Printf("[binfinity-addon] "+format, args...)
}

// ---- enrollment (redeem a setup key once; reuse durable credentials after) ----

func (r *runner) ensureEnrolled(ctx context.Context) (Credentials, error) {
	if b, err := os.ReadFile(r.cfg.CredPath); err == nil {
		var c Credentials
		if json.Unmarshal(b, &c) == nil && c.ClientID != "" {
			r.logf("reusing saved credentials (%s)", r.cfg.CredPath)
			return c, nil
		}
	}
	if r.cfg.SetupKey == "" {
		return Credentials{}, errors.New("no saved credentials and no BF_SETUP_KEY")
	}
	body, _ := json.Marshal(map[string]string{"setup_key": r.cfg.SetupKey, "name": r.cfg.Name})
	resp, err := r.post(ctx, r.cfg.Endpoint+"/ams/api/v1/enroll", body)
	if err != nil {
		return Credentials{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return Credentials{}, fmt.Errorf("enroll rejected (%d): %s", resp.StatusCode, bytes.TrimSpace(data))
	}
	var c Credentials
	if err := json.Unmarshal(data, &c); err != nil {
		return Credentials{}, fmt.Errorf("decode enroll response: %w", err)
	}
	c.EnrolledAt = time.Now().UTC().Format(time.RFC3339)
	if err := r.persist(c); err != nil {
		r.logf("warning: could not persist credentials: %v", err)
	}
	return c, nil
}

func (r *runner) persist(c Credentials) error {
	if dir := filepath.Dir(r.cfg.CredPath); dir != "" {
		_ = os.MkdirAll(dir, 0o700)
	}
	out, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(r.cfg.CredPath, out, 0o600)
}

// ---- control loops ----

func (r *runner) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.Heartbeat)
	defer t.Stop()
	r.sendHeartbeat(ctx) // immediately, so the Console sees us at once
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sendHeartbeat(ctx)
		}
	}
}

func (r *runner) sendHeartbeat(ctx context.Context) {
	act, done, total, started := r.progress()
	hb := Heartbeat{SystemID: r.systemID, Name: r.name, DataType: r.conn.DataType(), Activity: act}
	if act != ActivityIdle {
		hb.BytesDone, hb.BytesTotal = done, total
		if !started.IsZero() {
			hb.StartedAt = started.Format(time.RFC3339)
		}
	}
	body, _ := json.Marshal(hb)
	resp, err := r.post(ctx, r.cfg.Endpoint+"/cbs/api/v1/agent/heartbeat", body)
	if err != nil {
		r.logf("heartbeat: %v", err)
		return
	}
	resp.Body.Close()
}

func (r *runner) pollLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if cmd := r.poll(ctx); cmd != nil {
				r.execute(ctx, *cmd)
			}
		}
	}
}

func (r *runner) poll(ctx context.Context) *Command {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		r.cfg.Endpoint+"/cbs/api/v1/agent/commands?system="+r.systemID, nil)
	if t := r.token(ctx); t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
	}
	resp, err := r.hc.Do(req)
	if err != nil {
		r.logf("poll: %v", err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { // 204 = nothing to do; anything else = skip this tick
		return nil
	}
	var cmd Command
	if json.NewDecoder(resp.Body).Decode(&cmd) != nil {
		return nil
	}
	return &cmd
}

// execute runs one command end-to-end and reports the outcome. It sets activity
// for the live map and always reports, even on failure.
func (r *runner) execute(ctx context.Context, cmd Command) {
	r.logf("command %s: %s %s", cmd.ID, cmd.Type, cmd.BackupID)
	var res Result
	switch cmd.Type {
	case CmdBackup:
		res = r.doBackup(ctx, cmd.BackupID)
	case CmdRestore:
		res = r.doRestore(ctx, cmd.BackupID)
	default:
		res = Result{Status: statusFailed, BackupID: cmd.BackupID, Error: "unknown command type " + cmd.Type}
	}
	r.endActivity()
	r.report(ctx, cmd.ID, res)
}

func (r *runner) doBackup(ctx context.Context, id string) Result {
	if id == "" { // the edge assigns the id when the Console leaves it blank
		id = r.conn.DataType() + "-" + time.Now().UTC().Format("20060102T150405Z")
	}
	var total int64
	if est, ok := r.conn.(SizeEstimator); ok {
		if n := est.EstimateBytes(ctx); n > 0 {
			total = n
		}
	}
	r.beginBackup(total)
	stored, err := r.dp.Backup(ctx, id, r.conn.DataType(), func(w io.Writer) error {
		return r.conn.Backup(ctx, &countingWriter{w: w, add: r.addBytes})
	})
	if err != nil {
		return Result{Status: statusFailed, BackupID: id, Error: err.Error()}
	}
	r.logf("backup %s done (%d bytes stored)", id, stored)
	return Result{Status: statusDone, BackupID: id, Bytes: stored}
}

func (r *runner) doRestore(ctx context.Context, id string) Result {
	if id == "" {
		return Result{Status: statusFailed, Error: "restore requires a backup_id"}
	}
	r.beginRestore()
	err := r.dp.Restore(ctx, id, func(rd io.Reader) error {
		return r.conn.Restore(ctx, &countingReader{r: rd, add: r.addBytes})
	})
	if err != nil {
		return Result{Status: statusFailed, BackupID: id, Error: err.Error()}
	}
	r.logf("restore %s done", id)
	return Result{Status: statusDone, BackupID: id}
}

func (r *runner) report(ctx context.Context, cmdID string, res Result) {
	body, _ := json.Marshal(res)
	url := r.cfg.Endpoint + "/cbs/api/v1/agent/commands/" + cmdID + "/result?system=" + r.systemID
	if resp, err := r.post(ctx, url, body); err == nil {
		resp.Body.Close()
	} else {
		r.logf("report: %v", err)
	}
}

// ---- helpers ----

func (r *runner) post(ctx context.Context, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if t := r.token(ctx); t != "" { // authenticate CBS control-plane calls
		req.Header.Set("Authorization", "Bearer "+t)
	}
	return r.hc.Do(req)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func durationOr(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func hostname() string {
	if h, _ := os.Hostname(); h != "" {
		return h
	}
	return "edge"
}
