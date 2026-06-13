// Command mysql-client is the Binfinity MySQL edge addon — and the reference for
// how to build ANY addon on top of the Binfinity Addon SDK.
//
// The SDK (github.com/DiyRex/binfinity-addon-sdk) implements the whole
// universal edge contract — enroll → heartbeat → poll → execute → report, plus
// the BSP data plane via the `binfinity` CLI. So this file contains ONLY the
// MySQL-specific convert step: a Connector whose Backup streams `mysqldump` and
// whose Restore pipes into `mysql`. To build another addon (postgres, files,
// WordPress…), copy this shape and swap those two methods. See ../../DEVELOPMENT.md.
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

// mysqlConnector implements binfinity.Connector for MySQL.
type mysqlConnector struct {
	host, user, pass, db string
}

func (mysqlConnector) DataType() string { return "mysql" }

// Backup streams a consistent logical dump into the BSP pipe (constant memory,
// so a dump larger than free disk never needs staging).
func (m mysqlConnector) Backup(ctx context.Context, w io.Writer) error {
	dump := exec.CommandContext(ctx, "mysqldump",
		"-h", m.host, "-u", m.user, "--databases", m.db,
		"--add-drop-table", "--single-transaction", "--skip-comments")
	dump.Env = append(os.Environ(), "MYSQL_PWD="+m.pass)
	dump.Stdout = w
	var errb bytes.Buffer
	dump.Stderr = &errb
	if err := dump.Run(); err != nil {
		return fmt.Errorf("mysqldump: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	return nil
}

// Restore feeds the recovered SQL stream back into the database.
func (m mysqlConnector) Restore(ctx context.Context, r io.Reader) error {
	imp := exec.CommandContext(ctx, "mysql", "-h", m.host, "-u", m.user)
	imp.Env = append(os.Environ(), "MYSQL_PWD="+m.pass)
	imp.Stdin = r
	var errb bytes.Buffer
	imp.Stderr = &errb
	if err := imp.Run(); err != nil {
		return fmt.Errorf("mysql import: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	return nil
}

func main() {
	binfinity.Main(mysqlConnector{
		host: env("MYSQL_HOST", "mysql"),
		user: env("MYSQL_USER", "root"),
		pass: os.Getenv("MYSQL_PASSWORD"),
		db:   env("MYSQL_DB", "demo"),
	})
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
