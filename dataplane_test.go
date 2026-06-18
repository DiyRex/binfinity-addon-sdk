package binfinity

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// fakeBinfinity writes a tiny shell script that stands in for the `binfinity`
// CLI, so the exec/pipe wiring in CLIDataPlane is exercised without a real build.
func fakeBinfinity(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "binfinity")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestCLIRestoreStreamsCleanly: the recovered bytes stream to the consumer with
// constant memory, and the CLI's stderr summary must NOT leak into the stream.
func TestCLIRestoreStreamsCleanly(t *testing.T) {
	bin := fakeBinfinity(t, `printf 'RECOVERED-TAR-BYTES'; printf 'restored x (merkle abc)\n' 1>&2; exit 0`)
	dp := CLIDataPlane{Binary: bin, Store: "grpc://x:8090"}
	var got bytes.Buffer
	err := dp.Restore(context.Background(), "bk-1", func(r io.Reader) error {
		_, e := io.Copy(&got, r)
		return e
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got.String() != "RECOVERED-TAR-BYTES" {
		t.Fatalf("consumed %q — stderr summary leaked into the stream or data lost", got.String())
	}
}

// TestCLIRestoreErrorPropagates: a CLI failure (e.g. a failed Merkle verify) must
// surface as an error, never a clean EOF the consumer mistakes for success.
func TestCLIRestoreErrorPropagates(t *testing.T) {
	bin := fakeBinfinity(t, `printf 'partial'; echo 'merkle root mismatch' 1>&2; exit 1`)
	dp := CLIDataPlane{Binary: bin, Store: "grpc://x:8090"}
	err := dp.Restore(context.Background(), "bk-1", func(r io.Reader) error {
		_, _ = io.Copy(io.Discard, r)
		return nil
	})
	if err == nil {
		t.Fatal("Restore must error when the CLI exits non-zero (torn stream)")
	}
}
