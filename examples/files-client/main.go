// Command files-client is a Binfinity files/folder edge addon built on the Addon
// SDK. Backup streams a tar of a directory; Restore extracts it. Any file type
// works — Binfinity treats the stream as opaque bytes. See ../../DEVELOPMENT.md.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	binfinity "github.com/DiyRex/binfinity-addon-sdk"
)

type filesConnector struct {
	root string
}

func (filesConnector) DataType() string { return "files" }

// Backup tars the directory contents with relative paths (so a restore lands
// back under root). tar is not atomic — for live application data, snapshot or
// quiesce the source first (the consistency responsibility is the addon author's).
func (f filesConnector) Backup(ctx context.Context, w io.Writer) error {
	c := exec.CommandContext(ctx, "tar", "-c", "-C", f.root, ".")
	c.Stdout = w
	var errb bytes.Buffer
	c.Stderr = &errb
	if err := c.Run(); err != nil {
		return fmt.Errorf("tar create: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	return nil
}

// Restore extracts the tar stream back under root.
func (f filesConnector) Restore(ctx context.Context, r io.Reader) error {
	if err := os.MkdirAll(f.root, 0o755); err != nil {
		return fmt.Errorf("mkdir restore root: %w", err)
	}
	c := exec.CommandContext(ctx, "tar", "-x", "-C", f.root)
	c.Stdin = r
	var errb bytes.Buffer
	c.Stderr = &errb
	if err := c.Run(); err != nil {
		return fmt.Errorf("tar extract: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	return nil
}

func main() {
	binfinity.Main(filesConnector{root: env("FILES_ROOT", "/data")})
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
