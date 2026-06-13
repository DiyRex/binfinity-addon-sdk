// Command postgres-client is a Binfinity PostgreSQL edge addon built on the
// Addon SDK. Only the convert step is Postgres-specific: Backup streams a
// consistent logical dump (pg_dump), Restore replays it (psql). Everything else
// — enroll, auth, heartbeat/poll, reporting, BSP data plane — is the SDK. See
// ../../DEVELOPMENT.md.
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

type pgConnector struct {
	host, port, user, pass, db string
}

func (pgConnector) DataType() string { return "postgres" }

// Backup streams a logical dump. pg_dump runs in a single transaction, so the
// dump is a consistent point-in-time snapshot. Plain SQL streams without seeking;
// --clean --if-exists makes the restore idempotent against an existing database.
func (p pgConnector) Backup(ctx context.Context, w io.Writer) error {
	dump := exec.CommandContext(ctx, "pg_dump",
		"-h", p.host, "-p", p.port, "-U", p.user,
		"--clean", "--if-exists", "--no-owner", "--no-privileges", p.db)
	dump.Env = append(os.Environ(), "PGPASSWORD="+p.pass)
	dump.Stdout = w
	var errb bytes.Buffer
	dump.Stderr = &errb
	if err := dump.Run(); err != nil {
		return fmt.Errorf("pg_dump: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	return nil
}

// Restore replays the SQL stream into the target database.
func (p pgConnector) Restore(ctx context.Context, r io.Reader) error {
	imp := exec.CommandContext(ctx, "psql",
		"-h", p.host, "-p", p.port, "-U", p.user, "-d", p.db,
		"--set", "ON_ERROR_STOP=on")
	imp.Env = append(os.Environ(), "PGPASSWORD="+p.pass)
	imp.Stdin = r
	var errb bytes.Buffer
	imp.Stderr = &errb
	if err := imp.Run(); err != nil {
		return fmt.Errorf("psql restore: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	return nil
}

func main() {
	binfinity.Main(pgConnector{
		host: env("PG_HOST", "postgres"),
		port: env("PG_PORT", "5432"),
		user: env("PG_USER", "postgres"),
		pass: os.Getenv("PG_PASSWORD"),
		db:   env("PG_DB", "postgres"),
	})
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
