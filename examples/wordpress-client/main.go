// Command wordpress-client is a Binfinity WordPress edge addon built on the Addon
// SDK. WordPress is a COMPOSITE source — a MySQL database PLUS the site files
// (wp-content: themes, plugins, uploads). The addon bundles both into ONE stream
// on backup and splits them again on restore. This is the reference pattern for
// any "database + files" source (Drupal, a Rails app + uploads, …). The SDK does
// everything else. See ../../DEVELOPMENT.md.
//
// Zero-config by default: it auto-discovers wp-config.php across the common
// WordPress layouts (official Docker/vanilla, Bitnami on Lightsail/VMs, the
// Debian package, cPanel docroots…) — with a bounded filesystem search as a last
// resort — and reads the DB credentials straight out of it. Override any field
// with WP_PATH / WP_CONFIG / WORDPRESS_DB_* (or WP_DB_*) when you need to pin it.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

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
		"-h", s.dbHost, "-u", s.dbUser,
		// --column-statistics=0: the MySQL 8 client queries information_schema.
		// COLUMN_STATISTICS, which MariaDB does not have — without this the dump
		// fails against MariaDB ("Unknown table 'COLUMN_STATISTICS'"). Harmless on
		// real MySQL 8 (just disables the histogram stats query).
		"--column-statistics=0",
		"--add-drop-table", "--single-transaction", "--skip-comments",
		"--databases", s.dbName)
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
	binfinity.Main(newConnector())
}

// newConnector resolves where WordPress lives and how to reach its database with
// ZERO required configuration: it locates wp-config.php (across vanilla/Docker,
// Bitnami, Debian, cPanel… layouts, with a filesystem search as a last resort) and
// reads the DB_* constants straight out of it. Explicit env vars always win, so an
// operator can still pin any field — but on a stock install nothing needs to be set.
func newConnector() wpConnector {
	cfgPath := findWPConfig()
	cfg := parseWPConfig(cfgPath) // best-effort; empty map if not found/unreadable
	// Site root: an explicit WP_PATH wins; otherwise the directory holding wp-config.php
	// (WordPress keeps wp-content beside wp-config.php on every standard layout).
	root := firstNonEmpty(os.Getenv("WP_PATH"), filepath.Dir(cfgPath))
	return wpConnector{
		path:   root,
		dbHost: firstNonEmpty(os.Getenv("WORDPRESS_DB_HOST"), os.Getenv("WP_DB_HOST"), stripPort(cfg["DB_HOST"]), "localhost"),
		dbUser: firstNonEmpty(os.Getenv("WORDPRESS_DB_USER"), os.Getenv("WP_DB_USER"), cfg["DB_USER"], "wordpress"),
		dbPass: firstNonEmpty(os.Getenv("WORDPRESS_DB_PASSWORD"), os.Getenv("WP_DB_PASSWORD"), cfg["DB_PASSWORD"]),
		dbName: firstNonEmpty(os.Getenv("WORDPRESS_DB_NAME"), os.Getenv("WP_DB_NAME"), cfg["DB_NAME"], "wordpress"),
	}
}

// findWPConfig locates wp-config.php across WordPress distributions. Resolution order:
//  1. WP_CONFIG — an explicit full path to the file (highest priority).
//  2. <WP_PATH>/wp-config.php, and one directory above it (WordPress permits the
//     config to live one level up from the install for security).
//  3. A list of well-known install roots (official Docker/vanilla, Bitnami on
//     Lightsail/VMs, the Debian/Ubuntu package, cPanel-style docroots…).
//  4. Last resort: a bounded filesystem search beneath the common web roots.
//
// It returns the first existing match; if nothing is found it returns the
// conventional default path so downstream errors point somewhere sensible (DB creds
// can still come entirely from env in that case).
func findWPConfig() string {
	if p := os.Getenv("WP_CONFIG"); p != "" {
		return p
	}
	var candidates []string
	if wp := os.Getenv("WP_PATH"); wp != "" {
		candidates = append(candidates,
			filepath.Join(wp, "wp-config.php"),
			filepath.Join(filepath.Dir(filepath.Clean(wp)), "wp-config.php"))
	}
	candidates = append(candidates,
		"/var/www/html/wp-config.php",          // official Docker image + most vanilla installs
		"/var/www/wp-config.php",               // config one level above the docroot
		"/opt/bitnami/wordpress/wp-config.php", // Bitnami (AWS Lightsail / VM images)
		"/bitnami/wordpress/wp-config.php",     // Bitnami persisted volume
		"/app/wp-config.php",                   // some PaaS / container layouts
		"/usr/share/wordpress/wp-config.php",   // Debian/Ubuntu wordpress package
		"/srv/www/wordpress/wp-config.php",
		"/var/www/wordpress/wp-config.php",
	)
	for _, c := range candidates {
		if isFile(c) {
			return c
		}
	}
	for _, base := range []string{"/var/www", "/opt/bitnami", "/srv", "/app", "/home"} {
		if p := searchWPConfig(base, 6); p != "" {
			return p
		}
	}
	return firstNonEmpty(os.Getenv("WP_PATH"), "/var/www/html") + "/wp-config.php"
}

// searchWPConfig walks base (bounded to maxDepth levels) and returns the first
// wp-config.php it finds, skipping noisy/irrelevant trees. Unreadable directories
// are skipped rather than aborting the search.
func searchWPConfig(base string, maxDepth int) string {
	baseDepth := strings.Count(filepath.Clean(base), string(filepath.Separator))
	var found string
	_ = filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable entry — skip, keep searching
		}
		if d.IsDir() {
			if strings.Count(filepath.Clean(p), string(filepath.Separator))-baseDepth > maxDepth {
				return filepath.SkipDir
			}
			switch d.Name() {
			case "node_modules", ".git", "vendor", "cache", "uploads", "proc", "sys", "dev":
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() == "wp-config.php" {
			found = p
			return filepath.SkipAll
		}
		return nil
	})
	return found
}

// parseWPConfig extracts the DB_* constants from wp-config.php WITHOUT running PHP,
// matching `define('DB_NAME', 'value')` with flexible quoting/spacing. Best-effort:
// returns an empty map if the file is absent or unreadable.
func parseWPConfig(path string) map[string]string {
	out := map[string]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	re := regexp.MustCompile(`(?i)define\(\s*['"](DB_NAME|DB_USER|DB_PASSWORD|DB_HOST)['"]\s*,\s*['"]([^'"]*)['"]`)
	for _, m := range re.FindAllStringSubmatch(string(b), -1) {
		out[strings.ToUpper(m[1])] = m[2]
	}
	return out
}

func isFile(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir()
}

// stripPort drops a ":port" suffix from a DB host (wp-config.php often stores
// "host:3306"); the dump/import shell out with -h only, so the bare host is wanted.
func stripPort(h string) string {
	h = strings.TrimSpace(h)
	if i := strings.LastIndex(h, ":"); i >= 0 && !strings.Contains(h[i+1:], "/") {
		return h[:i]
	}
	return h
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
