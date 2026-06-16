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
	"strconv"
	"strings"
	"time"

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

// Compile-time proof that the connector implements both the base Connector and the
// optional Layer-B IncrementalConnector — a signature typo here would otherwise make
// the SDK silently fall back to full backups.
var _ binfinity.IncrementalConnector = wpConnector{}

// EstimateBytes implements binfinity.SizeEstimator: a best-effort estimate of the
// backup's source size (wp-content tree + database data/index length) so the
// Console can show an approximate % and ETA. Errors are swallowed — a partial or
// zero estimate just means the Console shows bytes/throughput without a %.
func (s wpConnector) EstimateBytes(ctx context.Context) int64 {
	var total int64
	_ = filepath.WalkDir(filepath.Join(s.path, "wp-content"), func(_ string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			if fi, e := d.Info(); e == nil {
				total += fi.Size()
			}
		}
		return nil
	})
	q := exec.CommandContext(ctx, pickBinary("mysql", "mariadb"),
		"-h", s.dbHost, "-u", s.dbUser, "-N", "-B", "-e",
		"SELECT IFNULL(SUM(data_length+index_length),0) FROM information_schema.tables WHERE table_schema=DATABASE()", s.dbName)
	q.Env = append(os.Environ(), "MYSQL_PWD="+s.dbPass)
	if out, err := q.Output(); err == nil {
		if n, e := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); e == nil {
			total += n
		}
	}
	return total
}

// Backup writes a full, self-contained backup (Layer A): a full DB dump + all of
// wp-content, as one tar stream — database and files captured at the same instant.
func (s wpConnector) Backup(ctx context.Context, w io.Writer) error {
	return s.writeArchive(ctx, w, time.Time{})
}

// BackupIncremental implements binfinity.IncrementalConnector (ADR-0009 Layer B).
// The DB is ALWAYS dumped full — a logical SQL dump can't be mtime-incremental —
// but wp-content is scoped to files modified since the cursor, so a delta ships only
// changed themes/plugins/uploads. The new cursor is this backup's start time;
// isDelta is false on the first run (empty cursor) so the Console records it as the
// chain's base full. (Like UpdraftPlus incremental: full DB + changed files.)
func (s wpConnector) BackupIncremental(ctx context.Context, w io.Writer, cursor string) (string, bool, error) {
	since, isDelta := time.Time{}, false
	if cursor != "" {
		if t, err := time.Parse(time.RFC3339, cursor); err == nil {
			since, isDelta = t, true
		}
	}
	next := time.Now().UTC().Format(time.RFC3339)
	if err := s.writeArchive(ctx, w, since); err != nil {
		return "", false, err
	}
	return next, isDelta, nil
}

// writeArchive dumps the DB (always full) and tars {db.sql + wp-content} to w. When
// since is non-zero, only wp-content files modified after it are included (the delta)
// — the freshly written db.sql is always newer than since, so the DB is always present.
func (s wpConnector) writeArchive(ctx context.Context, w io.Writer, since time.Time) error {
	// NOTE: we deliberately do NOT put the site into maintenance mode. A
	// `.maintenance` flag written here would be orphaned if the backup is cancelled
	// or the process is killed mid-run (its cleanup defer never runs), leaving the
	// live site stuck at HTTP 503 — exactly the "never harm the site" failure we must
	// avoid. The DB is dumped with --single-transaction (an InnoDB MVCC-consistent
	// snapshot needs no lock), and wp-content is read live, matching UpdraftPlus.
	// Best-effort: clear any stale flag a previous (killed) run may have left behind.
	_ = os.Remove(filepath.Join(s.path, ".maintenance"))

	stage, err := os.MkdirTemp("", "wp-backup-")
	if err != nil {
		return fmt.Errorf("stage dir: %w", err)
	}
	defer os.RemoveAll(stage)

	if err := s.dumpDB(ctx, filepath.Join(stage, "db.sql")); err != nil {
		return err
	}

	// {db.sql, wp-content} → one tar stream (GNU tar honours multiple -C). For a
	// delta, --newer-mtime keeps only wp-content files changed since the cursor.
	tarArgs := []string{"-c"}
	if !since.IsZero() {
		tarArgs = append(tarArgs, "--newer-mtime=@"+strconv.FormatInt(since.Unix(), 10))
	}
	tarArgs = append(tarArgs, "-C", stage, "db.sql", "-C", s.path, "wp-content")
	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	tarCmd.Stdout = w
	var terr bytes.Buffer
	tarCmd.Stderr = &terr
	if err := tarCmd.Run(); err != nil {
		return fmt.Errorf("tar create: %w: %s", err, bytes.TrimSpace(terr.Bytes()))
	}
	return nil
}

// dumpDB writes a consistent full logical dump to dbFile. Works against any
// MySQL/MariaDB client+server (resolves the dumper binary and only passes
// --column-statistics=0 when supported).
func (s wpConnector) dumpDB(ctx context.Context, dbFile string) error {
	f, err := os.Create(dbFile)
	if err != nil {
		return fmt.Errorf("create db dump: %w", err)
	}
	defer f.Close()
	dumper := pickBinary("mysqldump", "mariadb-dump")
	args := []string{"-h", s.dbHost, "-u", s.dbUser}
	if dumperSupports(ctx, dumper, "column-statistics") {
		args = append(args, "--column-statistics=0")
	}
	args = append(args, "--add-drop-table", "--single-transaction", "--skip-comments", "--databases", s.dbName)
	dump := exec.CommandContext(ctx, dumper, args...)
	dump.Env = append(os.Environ(), "MYSQL_PWD="+s.dbPass)
	dump.Stdout = f
	var derr bytes.Buffer
	dump.Stderr = &derr
	if err := dump.Run(); err != nil {
		return fmt.Errorf("mysqldump: %w: %s", err, bytes.TrimSpace(derr.Bytes()))
	}
	return nil
}

// Restore reconstructs a full (Layer A) backup: import the DB + replace wp-content.
func (s wpConnector) Restore(ctx context.Context, r io.Reader) error {
	return s.apply(ctx, r, true)
}

// RestoreIncremental applies ONE element of a restore chain in order (ADR-0009
// Layer B). The base (isBase) clears wp-content first for a clean point-in-time
// replace; every element imports its full DB dump (the LAST one applied wins, since
// each backup carries a full DB) and overlays its wp-content files. Net result:
// the target's DB + the union of base∪deltas files (newest mtime wins).
func (s wpConnector) RestoreIncremental(ctx context.Context, r io.Reader, isBase bool) error {
	return s.apply(ctx, r, isBase)
}

// apply unbundles one stream, imports its DB (if present) and overlays its
// wp-content. When isBase, wp-content is cleared first so the restore is a
// point-in-time replace rather than an overlay onto whatever is there now.
func (s wpConnector) apply(ctx context.Context, r io.Reader, isBase bool) error {
	stage, err := os.MkdirTemp("", "wp-restore-")
	if err != nil {
		return fmt.Errorf("stage dir: %w", err)
	}
	defer os.RemoveAll(stage)

	untar := exec.CommandContext(ctx, "tar", "-x", "-C", stage)
	untar.Stdin = r
	var uerr bytes.Buffer
	untar.Stderr = &uerr
	if err := untar.Run(); err != nil {
		return fmt.Errorf("tar extract: %w: %s", err, bytes.TrimSpace(uerr.Bytes()))
	}

	// DB import — each chain element carries a full dump; the last applied wins.
	if dbFile := filepath.Join(stage, "db.sql"); isFile(dbFile) {
		f, err := os.Open(dbFile)
		if err != nil {
			return fmt.Errorf("open db dump: %w", err)
		}
		imp := exec.CommandContext(ctx, pickBinary("mysql", "mariadb"), "-h", s.dbHost, "-u", s.dbUser)
		imp.Env = append(os.Environ(), "MYSQL_PWD="+s.dbPass)
		imp.Stdin = f
		var ierr bytes.Buffer
		imp.Stderr = &ierr
		runErr := imp.Run()
		f.Close()
		if runErr != nil {
			return fmt.Errorf("mysql import: %w: %s", runErr, bytes.TrimSpace(ierr.Bytes()))
		}
	}

	// wp-content into place (preserve perms). The base clears it first; deltas
	// overlay (cp -a overwrites changed files, newest mtime wins).
	dst := filepath.Join(s.path, "wp-content")
	if isBase {
		if err := clearDir(dst); err != nil {
			return fmt.Errorf("clear wp-content: %w", err)
		}
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("mkdir wp-content: %w", err)
	}
	if src := filepath.Join(stage, "wp-content"); dirExists(src) {
		cp := exec.CommandContext(ctx, "cp", "-a", src+"/.", dst+"/")
		var cerr bytes.Buffer
		cp.Stderr = &cerr
		if err := cp.Run(); err != nil {
			return fmt.Errorf("copy wp-content: %w: %s", err, bytes.TrimSpace(cerr.Bytes()))
		}
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

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// clearDir removes everything inside dir (not dir itself) so a base restore
// replaces wp-content rather than overlaying onto whatever is currently there.
// A missing dir is fine (nothing to clear).
func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// pickBinary returns primary if it's on PATH, else fallback if it is, else
// primary (so the run fails with a clear "not found"). Lets the addon use the
// MySQL tools (mysqldump/mysql) or MariaDB's renamed ones (mariadb-dump/mariadb),
// whichever the host provides.
func pickBinary(primary, fallback string) string {
	if _, err := exec.LookPath(primary); err == nil {
		return primary
	}
	if _, err := exec.LookPath(fallback); err == nil {
		return fallback
	}
	return primary
}

// dumperSupports reports whether the dump binary advertises the given long
// option in its --help (e.g. "column-statistics"), so version-specific flags are
// only passed to clients that understand them.
func dumperSupports(ctx context.Context, bin, opt string) bool {
	out, _ := exec.CommandContext(ctx, bin, "--help").CombinedOutput()
	return bytes.Contains(out, []byte(opt))
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
