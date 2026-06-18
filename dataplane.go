package binfinity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
)

// DataPlane moves bytes between a source and Binfinity storage in the canonical
// BSP format — chunking, dedup, client-side encryption, integrity and
// content-addressing all happen here, never in addon code. The default
// implementation (CLIDataPlane) shells out to the `binfinity` CLI, which is the
// supported path for every language. Advanced authors who cannot ship the
// binary may implement this interface against the SMS gRPC Storage proto + BSP
// directly (see DEVELOPMENT.md §"Going native"); tests substitute a stub.
//
// Backup calls produce to obtain the source bytes (written to the supplied
// writer) and returns the ciphertext size stored. Restore calls consume with a
// reader of the recovered plaintext bytes.
type DataPlane interface {
	Backup(ctx context.Context, backupID, sourceType string, produce func(io.Writer) error) (storedBytes int64, err error)
	Restore(ctx context.Context, backupID string, consume func(io.Reader) error) error
}

// CLIDataPlane is the default DataPlane: it pipes bytes through the `binfinity`
// CLI. It keeps the streaming guarantee of the contract — on backup the source
// is piped straight into `binfinity backup --in -` so a dump larger than local
// free disk never needs staging.
type CLIDataPlane struct {
	Binary     string // path to the binfinity binary (default "binfinity")
	Store      string // STORE_SPEC, e.g. grpc://host:8090 | s3://bucket | local:/path
	Passphrase string // exported as BINFINITY_PASSPHRASE for the CLI (zero-knowledge)
	SmsToken   string // exported as SMS_AUTH_TOKEN for the CLI's gRPC data-plane auth
}

// bytesStoredRe extracts the ciphertext size from `binfinity backup` output
// (line "bytes stored:   N"). Best-effort; a miss just yields 0.
var bytesStoredRe = regexp.MustCompile(`bytes stored:\s+(\d+)`)

func (d CLIDataPlane) binary() string {
	if d.Binary != "" {
		return d.Binary
	}
	return "binfinity"
}

// env returns the CLI's environment with the passphrase injected, so encryption
// works even if the caller's connector cleared the process environment.
func (d CLIDataPlane) env() []string {
	e := os.Environ()
	if d.Passphrase != "" {
		e = append(e, "BINFINITY_PASSPHRASE="+d.Passphrase)
	}
	if d.SmsToken != "" {
		e = append(e, "SMS_AUTH_TOKEN="+d.SmsToken)
	}
	return e
}

// Backup streams produce's bytes into `binfinity backup --in -`. produce and the
// CLI run concurrently across an io.Pipe (constant memory, any size).
func (d CLIDataPlane) Backup(ctx context.Context, backupID, sourceType string, produce func(io.Writer) error) (int64, error) {
	cmd := exec.CommandContext(ctx, d.binary(), "backup",
		"--in", "-", "--store", d.Store, "--id", backupID, "--source-type", sourceType)
	cmd.Env = d.env()

	pr, pw := io.Pipe()
	cmd.Stdin = pr
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut

	if err := cmd.Start(); err != nil {
		_ = pr.CloseWithError(err)
		return 0, fmt.Errorf("start binfinity backup: %w", err)
	}

	// Run the source-specific producer; its bytes flow into the CLI's stdin.
	produceErr := produce(pw)
	// Closing the writer signals EOF to the CLI. Propagating a producer error
	// makes the CLI fail fast rather than store a truncated stream.
	if produceErr != nil {
		_ = pw.CloseWithError(produceErr)
	} else {
		_ = pw.Close()
	}

	waitErr := cmd.Wait()
	if produceErr != nil {
		return 0, fmt.Errorf("produce source: %w", produceErr)
	}
	if waitErr != nil {
		return 0, fmt.Errorf("binfinity backup: %w: %s", waitErr, bytes.TrimSpace(errOut.Bytes()))
	}

	var stored int64
	if m := bytesStoredRe.FindSubmatch(out.Bytes()); m != nil {
		stored, _ = strconv.ParseInt(string(m[1]), 10, 64)
	}
	return stored, nil
}

// Restore STREAMS the recovered plaintext from `binfinity restore --out -`
// straight into consume across an io.Pipe — constant memory, no staging the whole
// backup to local disk (so a multi-GB restore works on a small node). The CLI
// verifies integrity (incl. the Merkle root) as it streams; if a chunk is missing
// or corrupt it closes the pipe with that error, so the consumer sees a torn
// stream as an error, never a silent truncation. The summary line goes to the
// CLI's stderr, so stdout carries ONLY the recovered bytes.
func (d CLIDataPlane) Restore(ctx context.Context, backupID string, consume func(io.Reader) error) error {
	cmd := exec.CommandContext(ctx, d.binary(), "restore",
		"--id", backupID, "--store", d.Store, "--out", "-")
	cmd.Env = d.env()

	pr, pw := io.Pipe()
	cmd.Stdout = pw
	var errOut bytes.Buffer
	cmd.Stderr = &errOut

	if err := cmd.Start(); err != nil {
		_ = pr.CloseWithError(err)
		return fmt.Errorf("start binfinity restore: %w", err)
	}

	// Close the pipe writer when the CLI exits, propagating any CLI error so the
	// consumer sees it as a read error (not a clean EOF).
	waitCh := make(chan error, 1)
	go func() {
		werr := cmd.Wait()
		_ = pw.CloseWithError(werr)
		waitCh <- werr
	}()

	// Consume the recovered bytes as they arrive (the connector imports the DB +
	// extracts files from this stream). An empty stream surfaces to the connector
	// as an immediate EOF — the WordPress connector's "no DB dump" guard turns that
	// into a loud failure rather than a silent no-op restore.
	consumeErr := consume(pr)
	if consumeErr != nil {
		_ = pr.CloseWithError(consumeErr) // unblock the CLI if we stopped early
	}
	waitErr := <-waitCh

	if waitErr != nil {
		return fmt.Errorf("binfinity restore: %w: %s", waitErr, bytes.TrimSpace(errOut.Bytes()))
	}
	if consumeErr != nil {
		return fmt.Errorf("consume restored stream: %w", consumeErr)
	}
	return nil
}
