// Command wordpress-client is a Binfinity WordPress edge addon built on the Addon
// SDK. WordPress is a COMPOSITE source — a MySQL database PLUS the site files
// (wp-content: themes, plugins, uploads). The addon bundles both into ONE stream
// on backup and splits them again on restore. This is the reference pattern for
// any "database + files" source (Drupal, a Rails app + uploads, …). The SDK does
// everything else. See ../../DEVELOPMENT.md.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	binfinity "github.com/DiyRex/binfinity-addon-sdk"
)

type wpConnector struct {
	path   string // site root (contains wp-content)
	dbHost string
	dbUser string
	dbPass string
	dbName string
}

func (wpConnector) DataType() string { return "wordpress" }

// Backup quiesces the site, dumps the DB, and tars {db.sql + wp-content} as a
// single stream — so the database and files are captured at the same instant.
func (s wpConnector) Backup(ctx context.Context, w io.Writer) error {
	// Maintenance mode for a coherent snapshot; always cleared, even on failure.
	maint := filepath.Join(s.path, ".maintenance")
	if err := os.WriteFile(maint, []byte("<?php $upgrading = time(); ?>"), 0o644); err == nil {
		defer os.Remove(maint)
	}

	stage, err := os.MkdirTemp("", "wp-backup-")
	if err != nil {
		return fmt.Errorf("stage dir: %w", err)
	}
	defer os.RemoveAll(stage)

	// 1. DB → stage/db.sql (consistent logical dump).
	dbFile := filepath.Join(stage, "db.sql")
	f, err := os.Create(dbFile)
	if err != nil {
		return fmt.Errorf("create db dump: %w", err)
	}
	dump := exec.CommandContext(ctx, "mysqldump",
		"-h", s.dbHost, "-u", s.dbUser, "--databases", s.dbName,
		"--add-drop-table", "--single-transaction", "--skip-comments")
	dump.Env = append(os.Environ(), "MYSQL_PWD="+s.dbPass)
	dump.Stdout = f
	var derr bytes.Buffer
	dump.Stderr = &derr
	if err := dump.Run(); err != nil {
		f.Close()
		return fmt.Errorf("mysqldump: %w: %s", err, bytes.TrimSpace(derr.Bytes()))
	}
	f.Close()

	// 2. {db.sql, wp-content} → one tar stream (GNU tar honours multiple -C).
	tar := exec.CommandContext(ctx, "tar", "-c",
		"-C", stage, "db.sql",
		"-C", s.path, "wp-content")
	tar.Stdout = w
	var terr bytes.Buffer
	tar.Stderr = &terr
	if err := tar.Run(); err != nil {
		return fmt.Errorf("tar create: %w: %s", err, bytes.TrimSpace(terr.Bytes()))
	}
	return nil
}

// Restore unbundles the stream, imports the DB, then syncs wp-content into place.
func (s wpConnector) Restore(ctx context.Context, r io.Reader) error {
	stage, err := os.MkdirTemp("", "wp-restore-")
	if err != nil {
		return fmt.Errorf("stage dir: %w", err)
	}
	defer os.RemoveAll(stage)

	// 1. unbundle
	untar := exec.CommandContext(ctx, "tar", "-x", "-C", stage)
	untar.Stdin = r
	var uerr bytes.Buffer
	untar.Stderr = &uerr
	if err := untar.Run(); err != nil {
		return fmt.Errorf("tar extract: %w: %s", err, bytes.TrimSpace(uerr.Bytes()))
	}

	// 2. DB import
	f, err := os.Open(filepath.Join(stage, "db.sql"))
	if err != nil {
		return fmt.Errorf("open db dump: %w", err)
	}
	defer f.Close()
	imp := exec.CommandContext(ctx, "mysql", "-h", s.dbHost, "-u", s.dbUser)
	imp.Env = append(os.Environ(), "MYSQL_PWD="+s.dbPass)
	imp.Stdin = f
	var ierr bytes.Buffer
	imp.Stderr = &ierr
	if err := imp.Run(); err != nil {
		return fmt.Errorf("mysql import: %w: %s", err, bytes.TrimSpace(ierr.Bytes()))
	}

	// 3. files into place (preserve perms; merge over the existing site).
	dst := filepath.Join(s.path, "wp-content")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("mkdir wp-content: %w", err)
	}
	cp := exec.CommandContext(ctx, "cp", "-a", filepath.Join(stage, "wp-content")+"/.", dst+"/")
	var cerr bytes.Buffer
	cp.Stderr = &cerr
	if err := cp.Run(); err != nil {
		return fmt.Errorf("copy wp-content: %w: %s", err, bytes.TrimSpace(cerr.Bytes()))
	}
	return nil
}

func main() {
	binfinity.Main(wpConnector{
		path:   env("WP_PATH", "/var/www/html"),
		dbHost: env("WORDPRESS_DB_HOST", "mysql"),
		dbUser: env("WORDPRESS_DB_USER", "wordpress"),
		dbPass: os.Getenv("WORDPRESS_DB_PASSWORD"),
		dbName: env("WORDPRESS_DB_NAME", "wordpress"),
	})
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
