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
// live Console map all work with zero server changes. See ../DEVELOPMENT.md.
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

// IncrementalConnector is an OPTIONAL capability (ADR-0009 Layer B). A connector
// that implements it can emit only the data changed since the last backup —
// MySQL binlog from a position, Postgres WAL from an LSN, files newer than an
// mtime — instead of re-reading the whole source. Connectors that do NOT
// implement it transparently fall back to full Backup + content-addressed dedup
// (Layer A), so every data type still works.
//
// The "cursor" is an opaque, connector-defined string (binlog file+pos, LSN,
// snapshot id, timestamp). Binfinity never interprets it: it persists the cursor
// a backup returns and hands it back on the next incremental backup.
type IncrementalConnector interface {
	Connector
	// BackupIncremental writes only data changed since cursor (empty cursor = the
	// connector should produce a full snapshot) and returns the NEW cursor to
	// persist. isDelta=false means "I produced a full stream; record this as full".
	BackupIncremental(ctx context.Context, w io.Writer, cursor string) (next string, isDelta bool, err error)
	// RestoreIncremental applies ONE stream of a restore chain, in order: isBase
	// for the base full, then each delta. E.g. MySQL: base = SQL import; delta =
	// binlog replay. The SDK feeds the chain elements in order.
	RestoreIncremental(ctx context.Context, r io.Reader, isBase bool) error
}

// Config holds everything the runtime needs. Use ConfigFromEnv for the standard
// environment-driven setup, then override fields as needed.
type Config struct {
	Endpoint   string        // Console base URL (BF_ENDPOINT), e.g. https://binfinity.example.com
	SetupKey   string        // enrollment key minted in the Console (BF_SETUP_KEY)
	Store      string        // SMS data-plane spec (STORE_SPEC); empty → use what enrollment delivers
	SmsToken   string        // SMS data-plane bearer (SMS_AUTH_TOKEN); empty → use what enrollment delivers
	Passphrase string        // tenant passphrase (BINFINITY_PASSPHRASE) — never leaves the edge
	Name       string        // system name shown in the Console (default <data_type>-<hostname>)
	CredPath   string        // where durable credentials are persisted (CRED_PATH)
	Heartbeat  time.Duration // heartbeat interval (HEARTBEAT_INTERVAL, default 10s)
	Poll       time.Duration // command poll interval (POLL_INTERVAL, default 5s)
	ReportBase time.Duration // base delay for durable report retries (REPORT_RETRY_BASE, default 2s)
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
		Store:      os.Getenv("STORE_SPEC"),     // empty → enrollment-delivered store wins
		SmsToken:   os.Getenv("SMS_AUTH_TOKEN"), // empty → enrollment-delivered token wins
		Passphrase: os.Getenv("BINFINITY_PASSPHRASE"),
		Name:       envOr("BF_NAME", dataType+"-"+hostname()),
		CredPath:   envOr("CRED_PATH", defaultCredPath()),
		Heartbeat:  durationOr("HEARTBEAT_INTERVAL", 10*time.Second),
		Poll:       durationOr("POLL_INTERVAL", 5*time.Second),
		ReportBase: durationOr("REPORT_RETRY_BASE", 2*time.Second),
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
	r.seed = seedFrom(r.systemID) // per-system jitter seed (no global rand)

	// Resolve the data-plane config: an explicit env (STORE_SPEC / SMS_AUTH_TOKEN)
	// always wins; otherwise use what the platform delivered at enrollment, so an
	// addon needs only a setup key + passphrase. Only rebuild the DEFAULT CLI data
	// plane (a caller-injected DataPlane is left untouched).
	// Data-plane config: explicit env wins; else what enrollment delivered. Token
	// grants refresh it later (also reaching addons whose saved creds predate delivery).
	store, smsToken := cfg.Store, cfg.SmsToken
	if store == "" {
		store = creds.StoreSpec
	}
	if smsToken == "" {
		smsToken = creds.SmsToken
	}
	r.applyDP(store, smsToken)
	if store == "" {
		store = "grpc://localhost:8090"
	}
	r.logf("enrolled as system %s (%s); data_type=%s store=%s", r.systemID, r.name, c.DataType(), store)

	go r.heartbeatLoop(ctx)
	go r.reportLoop(ctx) // R3: drain any results unacked from a prior run, then retry
	r.pollLoop(ctx)      // blocks until ctx cancelled
	r.logf("stopped")
	return nil
}

// runner is the live agent state. Unexported: the public surface is Connector +
// Config + Run/Main.
type runner struct {
	conn     Connector
	cfg      Config
	hc       *http.Client
	systemID string
	secret   string
	name     string
	seed     int64   // per-system jitter seed for backoff (no global rand)
	out      *outbox // R3: durable, at-least-once result delivery

	// dmu guards the data plane + its resolved config. The token goroutine may adopt
	// platform-delivered store/token (see fetchToken) while a backup reads the plane.
	dmu      sync.Mutex
	dp       DataPlane
	dpStore  string // resolved STORE_SPEC (env > enrollment/token-delivered > default)
	dpToken  string // resolved SMS_AUTH_TOKEN
	customDP bool   // caller injected cfg.DataPlane → never rebuilt

	amu        sync.Mutex // guards activity + live progress (heartbeat reads; execute writes)
	activity   string
	startedAt  time.Time
	bytesDone  int64
	bytesTotal int64

	// jmu guards the in-flight job's cancel hook. execute installs jobCancel when a
	// backup/restore starts; the heartbeat goroutine calls requestCancel() when CBS
	// signals cancel:true, aborting the job's context (which kills the data-plane
	// stream). cancelAsked distinguishes an operator cancel from a process shutdown.
	jmu         sync.Mutex
	jobCancel   context.CancelFunc
	cancelAsked bool

	tmu    sync.Mutex
	tok    string
	tokExp time.Time
}

// startJob derives a cancellable context for one command and registers its cancel
// hook so the heartbeat goroutine can abort it on an operator cancel request.
func (r *runner) startJob(parent context.Context) context.Context {
	jctx, cancel := context.WithCancel(parent)
	r.jmu.Lock()
	r.jobCancel, r.cancelAsked = cancel, false
	r.jmu.Unlock()
	return jctx
}

func (r *runner) endJob() {
	r.jmu.Lock()
	if r.jobCancel != nil {
		r.jobCancel()
	}
	r.jobCancel = nil
	r.jmu.Unlock()
}

// requestCancel aborts the in-flight job (if any). Safe to call repeatedly.
func (r *runner) requestCancel() {
	r.jmu.Lock()
	cancel := r.jobCancel
	if cancel != nil {
		r.cancelAsked = true
	}
	r.jmu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *runner) wasCancelled() bool {
	r.jmu.Lock()
	defer r.jmu.Unlock()
	return r.cancelAsked
}

// currentDP returns the effective data plane (guarded; it may be swapped when the
// platform delivers updated data-plane config).
func (r *runner) currentDP() DataPlane {
	r.dmu.Lock()
	defer r.dmu.Unlock()
	return r.dp
}

// applyDP adopts non-empty store/token and rebuilds the default CLI data plane, so
// an addon needs only a setup key + passphrase — the SMS endpoint + bearer are
// delivered by the platform (at enrollment and on every token grant). A
// caller-injected DataPlane is never replaced.
func (r *runner) applyDP(store, token string) {
	r.dmu.Lock()
	defer r.dmu.Unlock()
	if r.customDP {
		return
	}
	if store != "" {
		r.dpStore = store
	}
	if token != "" {
		r.dpToken = token
	}
	useStore := r.dpStore
	if useStore == "" {
		useStore = "grpc://localhost:8090"
	}
	r.dp = CLIDataPlane{Store: useStore, Passphrase: r.cfg.Passphrase, SmsToken: r.dpToken}
}

// SizeEstimator is an OPTIONAL Connector capability: if implemented, the SDK calls
// EstimateBytes once before a backup to seed the progress total so the Console can
// show an approximate % and ETA. Best-effort — return 0 when unknown (a pure
// stream need not implement it); the value estimates source bytes, not stored size.
type SizeEstimator interface {
	EstimateBytes(ctx context.Context) int64
}

// countingWriter / countingReader tally bytes as they flow, so the heartbeat can
// report exact backup/restore progress without buffering or staging.
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

// token returns a cached AMS access token, refreshing via the client-credentials
// grant (enrolled client_id + secret) when expired. Best-effort: "" on failure
// so requests still go out (CBS enforces only when agent auth is required).
func (r *runner) token(ctx context.Context) string {
	r.tmu.Lock()
	if r.tok != "" && time.Now().Before(r.tokExp) {
		t := r.tok
		r.tmu.Unlock()
		return t
	}
	sysID, secret := r.systemID, r.secret
	r.tmu.Unlock()
	if sysID == "" || secret == "" {
		return ""
	}
	// R4: a transient AMS blip must not silently demote us to unauthenticated.
	// Retry the grant a few times with short jittered backoff before giving up;
	// the network call runs without the lock so callers never serialise behind it.
	const base, max = 250 * time.Millisecond, 2 * time.Second
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(backoff(attempt-1, base, max, r.seed))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ""
			case <-timer.C:
			}
		}
		tok, ttl, ok := r.fetchToken(ctx, sysID, secret)
		if ok {
			r.tmu.Lock()
			r.tok, r.tokExp = tok, time.Now().Add(time.Duration(ttl-30)*time.Second)
			r.tmu.Unlock()
			return tok
		}
	}
	r.logf("token: AMS grant unavailable after retries; proceeding unauthenticated this tick")
	return ""
}

// fetchToken performs one client-credentials grant. ok is false on any failure
// (network, non-200, empty token) so the caller can retry.
func (r *runner) fetchToken(ctx context.Context, sysID, secret string) (tok string, ttl int, ok bool) {
	body, _ := json.Marshal(map[string]string{"client_id": sysID, "client_secret": secret})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.Endpoint+"/ams/auth/token", bytes.NewReader(body))
	if err != nil {
		return "", 0, false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.hc.Do(req) // raw — no auth header, avoids recursion
	if err != nil || resp == nil {
		return "", 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, false
	}
	var t struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		StoreSpec   string `json:"store_spec"`
		SmsToken    string `json:"sms_token"`
	}
	if json.NewDecoder(resp.Body).Decode(&t) != nil || t.AccessToken == "" {
		return "", 0, false
	}
	if t.ExpiresIn <= 0 {
		t.ExpiresIn = 300
	}
	// Adopt platform-delivered data-plane config (env always wins). This reaches
	// addons enrolled before delivery existed and keeps store/token current.
	adoptStore, adoptToken := "", ""
	if r.cfg.Store == "" {
		adoptStore = t.StoreSpec
	}
	if r.cfg.SmsToken == "" {
		adoptToken = t.SmsToken
	}
	if adoptStore != "" || adoptToken != "" {
		r.applyDP(adoptStore, adoptToken)
	}
	return t.AccessToken, t.ExpiresIn, true
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
	if cfg.ReportBase <= 0 {
		cfg.ReportBase = 2 * time.Second
	}
	hc := cfg.HTTPClient
	if hc == nil {
		tr := &http.Transport{}
		if cfg.Insecure {
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // dev opt-in via BF_TLS_INSECURE
		}
		hc = &http.Client{Timeout: 30 * time.Second, Transport: tr}
	}
	customDP := cfg.DataPlane != nil
	var dp DataPlane
	if customDP {
		dp = cfg.DataPlane
	} else {
		// Seeded from env; Run() + token grants adopt platform-delivered config.
		st := cfg.Store
		if st == "" {
			st = "grpc://localhost:8090"
		}
		dp = CLIDataPlane{Store: st, Passphrase: cfg.Passphrase, SmsToken: cfg.SmsToken}
	}
	// Durable result outbox lives beside the credentials so it survives restarts.
	ob := newOutbox(filepath.Join(filepath.Dir(cfg.CredPath), "outbox"))
	return &runner{conn: c, cfg: cfg, hc: hc, dp: dp, customDP: customDP, dpStore: cfg.Store, dpToken: cfg.SmsToken, activity: ActivityIdle, out: ob}, nil
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
	act, done, total, started := r.snapshot()
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
	defer resp.Body.Close()
	// The heartbeat is the live channel while a backup runs (the poll loop is busy
	// executing it), so CBS rides the cancel signal back on the heartbeat response.
	var hr struct {
		Cancel bool `json:"cancel"`
	}
	if json.NewDecoder(resp.Body).Decode(&hr) == nil && hr.Cancel {
		r.logf("cancel requested by Console; aborting in-flight job")
		r.requestCancel()
	}
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
	// Run under a cancellable job context so an operator cancel (delivered via the
	// heartbeat response) can abort the data-plane stream mid-flight.
	jctx := r.startJob(ctx)
	defer r.endJob()
	var res Result
	switch cmd.Type {
	case CmdBackup:
		r.setActivity(ActivityBackingUp)
		res = r.doBackup(jctx, cmd)
	case CmdRestore:
		r.setActivity(ActivityRestoring)
		res = r.doRestore(jctx, cmd)
	default:
		res = Result{Status: statusFailed, BackupID: cmd.BackupID, Error: "unknown command type " + cmd.Type}
	}
	r.endActivity()
	// An operator cancel is a terminal, NON-error outcome: the data-plane aborted
	// before committing a manifest, so report "cancelled" (not "failed"). CBS then
	// releases the lock and records no catalog entry — existing backups are intact.
	if r.wasCancelled() {
		res = Result{Status: statusCancelled, BackupID: res.BackupID}
		r.logf("command %s cancelled by operator", cmd.ID)
	}
	// Report on the parent ctx, not the cancelled job ctx, so the outcome still lands.
	r.report(ctx, cmd.ID, res)
}

// setActivity / getActivity guard the live-map activity string, which the
// heartbeat loop reads concurrently with execute's writes.
func (r *runner) setActivity(a string) {
	r.amu.Lock()
	r.activity = a
	r.amu.Unlock()
}

func (r *runner) getActivity() string {
	r.amu.Lock()
	defer r.amu.Unlock()
	return r.activity
}

// beginProgress seeds the live progress for a new job (total may be 0 = unknown).
func (r *runner) beginProgress(total int64) {
	r.amu.Lock()
	r.startedAt, r.bytesDone, r.bytesTotal = time.Now().UTC(), 0, total
	r.amu.Unlock()
}

func (r *runner) addBytes(n int64) {
	r.amu.Lock()
	r.bytesDone += n
	r.amu.Unlock()
}

// endActivity returns to idle and clears progress, so a finished job reports no
// stale bytes/percentage.
func (r *runner) endActivity() {
	r.amu.Lock()
	r.activity, r.startedAt, r.bytesDone, r.bytesTotal = ActivityIdle, time.Time{}, 0, 0
	r.amu.Unlock()
}

func (r *runner) snapshot() (activity string, done, total int64, started time.Time) {
	r.amu.Lock()
	defer r.amu.Unlock()
	return r.activity, r.bytesDone, r.bytesTotal, r.startedAt
}

func (r *runner) doBackup(ctx context.Context, cmd Command) Result {
	id := cmd.BackupID
	if id == "" { // the edge assigns the id when the Console leaves it blank
		id = r.conn.DataType() + "-" + time.Now().UTC().Format("20060102T150405Z")
	}
	// Source-incremental (Layer B): only when the Console asked for it AND the
	// connector opted in. Otherwise a full content-addressed backup (Layer A),
	// which every connector supports.
	var total int64
	if est, ok := r.conn.(SizeEstimator); ok {
		if n := est.EstimateBytes(ctx); n > 0 {
			total = n
		}
	}
	r.beginProgress(total)
	inc, canInc := r.conn.(IncrementalConnector)
	var nextCursor string
	var producedDelta bool
	produce := func(w io.Writer) error {
		cw := &countingWriter{w: w, add: r.addBytes}
		if cmd.Mode == "incremental" && canInc {
			n, d, err := inc.BackupIncremental(ctx, cw, cmd.Cursor)
			nextCursor, producedDelta = n, d
			return err
		}
		return r.conn.Backup(ctx, cw)
	}
	stored, err := r.currentDP().Backup(ctx, id, r.conn.DataType(), produce)
	if err != nil {
		return Result{Status: statusFailed, BackupID: id, Error: err.Error()}
	}
	r.logf("backup %s done (%d bytes stored, delta=%v)", id, stored, producedDelta)
	return Result{Status: statusDone, BackupID: id, Bytes: stored, Cursor: nextCursor}
}

func (r *runner) doRestore(ctx context.Context, cmd Command) Result {
	id := cmd.BackupID
	if id == "" {
		return Result{Status: statusFailed, Error: "restore requires a backup_id"}
	}
	r.beginProgress(0) // restore size isn't known up front; report bytes + elapsed
	// Chain restore (Layer B): when the Console supplies an ordered chain and the
	// connector knows how to apply deltas, fetch + apply each in order (base, then
	// deltas). Otherwise a single self-contained restore (Layer A).
	if inc, ok := r.conn.(IncrementalConnector); ok && len(cmd.Chain) > 0 {
		for i, bid := range cmd.Chain {
			isBase := i == 0
			err := r.currentDP().Restore(ctx, bid, func(rd io.Reader) error {
				return inc.RestoreIncremental(ctx, &countingReader{r: rd, add: r.addBytes}, isBase)
			})
			if err != nil {
				return Result{Status: statusFailed, BackupID: id, Error: "chain element " + bid + ": " + err.Error()}
			}
		}
		r.logf("restore %s done (chain of %d)", id, len(cmd.Chain))
		return Result{Status: statusDone, BackupID: id}
	}
	err := r.currentDP().Restore(ctx, id, func(rd io.Reader) error {
		return r.conn.Restore(ctx, &countingReader{r: rd, add: r.addBytes})
	})
	if err != nil {
		return Result{Status: statusFailed, BackupID: id, Error: err.Error()}
	}
	r.logf("restore %s done", id)
	return Result{Status: statusDone, BackupID: id}
}

// report durably records the result, then attempts immediate delivery. If the
// send fails (or partially), reportLoop keeps retrying until CBS acks (R3). The
// result is on disk before the first attempt, so a crash mid-send loses nothing.
func (r *runner) report(ctx context.Context, cmdID string, res Result) {
	if err := r.out.enqueue(cmdID, res); err != nil {
		r.logf("report: could not persist result for %s: %v (will send best-effort)", cmdID, err)
	}
	if r.sendReport(ctx, pendingReport{CmdID: cmdID, Result: res}) {
		r.out.ack(cmdID)
		return
	}
	r.logf("report %s not yet acked; queued for retry", cmdID)
}

// sendReport POSTs one result and returns true only on a 2xx ack. Anything else
// (network error, 5xx, auth failure) leaves it queued so it is retried — the
// edge never silently drops a completed backup's outcome.
func (r *runner) sendReport(ctx context.Context, p pendingReport) bool {
	body, _ := json.Marshal(p.Result)
	url := r.cfg.Endpoint + "/cbs/api/v1/agent/commands/" + p.CmdID + "/result?system=" + r.systemID
	resp, err := r.post(ctx, url, body)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// reportLoop drains the durable outbox: on boot it replays results unacked from
// a previous run, then retries any that fail with capped exponential backoff +
// jitter so a fleet does not retry in lockstep.
func (r *runner) reportLoop(ctx context.Context) {
	base, max := r.cfg.ReportBase, 2*time.Minute
	if base > max {
		max = base
	}
	attempt := 0
	for {
		remaining := r.out.flush(ctx, r.sendReport)
		if remaining == 0 {
			attempt = 0 // idle cadence: cheap periodic re-scan picks up new entries
		} else {
			attempt++
			r.logf("report retry: %d result(s) still pending", remaining)
		}
		wait := backoff(attempt, base, max, r.seed)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
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

// defaultCredPath chooses where to persist enrollment credentials. It uses $HOME
// when set so the file lives in the addon's STATE dir, never inside a backup root
// — critical for a connector like files-client whose BACKUP_ROOT is /data: a
// /data/credentials.json would both show up in the workload and get backed up
// (leaking the addon secret). Each addon image sets HOME to its state dir
// (mysql: /data — which it does not back up; files/postgres: /state). Falls back
// to the working directory when HOME is unset. Override with CRED_PATH.
func defaultCredPath() string {
	if h := os.Getenv("HOME"); h != "" {
		return filepath.Join(h, "credentials.json")
	}
	return "credentials.json"
}

// seedFrom derives a stable per-system jitter seed from the system id (FNV-1a),
// so each edge backs off on its own schedule without a global RNG.
func seedFrom(s string) int64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return int64(h & 0x7fffffffffffffff)
}
