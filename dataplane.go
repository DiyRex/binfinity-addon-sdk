package binfinity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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

// Restore recovers a backup to a temp file (integrity-verified by the CLI), then
// hands consume a reader over it. A temp file is used because restore is
// recover-then-import; the file is removed when done.
func (d CLIDataPlane) Restore(ctx context.Context, backupID string, consume func(io.Reader) error) error {
	tmp := filepath.Join(os.TempDir(), "binfinity-restore-"+sanitize(backupID)+".bin")
	defer os.Remove(tmp)

	cmd := exec.CommandContext(ctx, d.binary(), "restore",
		"--id", backupID, "--store", d.Store, "--out", tmp)
	cmd.Env = d.env()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("binfinity restore: %w: %s", err, bytes.TrimSpace(out))
	}

	f, err := os.Open(tmp)
	if err != nil {
		return fmt.Errorf("open restored stream: %w", err)
	}
	defer f.Close()
	// A zero-byte recovery means the CLI exited 0 but wrote nothing (e.g. an
	// unsupported --out target). Consuming it would let the connector see an empty
	// stream and report a no-op restore as success — fail loudly instead.
	if fi, err := f.Stat(); err == nil && fi.Size() == 0 {
		return fmt.Errorf("restored stream is empty (recovered 0 bytes for backup %s)", backupID)
	}
	if err := consume(f); err != nil {
		return fmt.Errorf("consume restored stream: %w", err)
	}
	return nil
}

// sanitize keeps a backup id safe to embed in a temp filename.
func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}
