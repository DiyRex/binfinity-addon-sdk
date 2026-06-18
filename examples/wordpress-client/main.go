// Command wordpress-client is the Binfinity WordPress edge addon — "Presserve".
//
// WordPress is a COMPOSITE source: a MySQL/MariaDB database + the wp-content
// filesystem (themes, plugins, uploads). Presserve backs both up as ONE BSP
// stream — a tar bundling `db/dump.sql` + `files/…` (the wp-content tree) — and
// restores them together as a point-in-time REPLACE. It needs NO PHP: it talks to
// MySQL over the wire (mysqldump/mysql) and reads wp-content from disk, operating
// *below* WordPress. The same binary runs as a Kubernetes/Compose sidecar or as a
// systemd/cron agent on a single VM (AWS Lightsail, Bitnami, EC2). See docs/20.
//
// Built on the Binfinity Addon SDK: it implements Layer A (full each time, deduped)
// and Layer B (full DB + mtime-scoped file deltas + chain restore).
//
//	WP_PATH        WordPress root (holds wp-config.php) — default /var/www/html
//	WP_CONTENT     wp-content dir to back up — default $WP_PATH/wp-content
//	WP_EXCLUDE     comma dir names to skip under wp-content — default "cache,upgrade"
//	WP_DB_{HOST,PORT,USER,PASSWORD,NAME}  override the values parsed from wp-config.php
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	binfinity "github.com/DiyRex/binfinity-addon-sdk"
)

const (
	dbEntry  = "db/dump.sql" // canonical (always-full) DB dump member written by this version
	filesPfx = "files/"      // canonical wp-content prefix written by this version
)

// isDBDump reports whether a tar member is the database dump, tolerant of the
// name used by ANY addon version so a backup written by one build always restores
// with another: the canonical "db/dump.sql", the legacy "db.sql", or any *.sql at
// the archive root or directly under db/. Without this, a member-name skew makes
// restore silently skip the DB (reporting success while importing nothing).
func isDBDump(name string) bool {
	switch name {
	case dbEntry, "db.sql":
		return true
	}
	if !strings.HasSuffix(name, ".sql") {
		return false
	}
	if strings.Contains(name, "/") {
		return strings.HasPrefix(name, "db/") && !strings.Contains(name[3:], "/")
	}
	return true
}

// contentRel strips the wp-content prefix used by any addon version ("files/" or
// the legacy "wp-content/") and returns the path relative to wp-content plus
// whether the member belongs to the content tree.
func contentRel(name string) (string, bool) {
	for _, p := range []string{filesPfx, "wp-content/"} {
		if rel, ok := strings.CutPrefix(name, p); ok {
			return rel, true
		}
	}
	return "", false
}

type dbConf struct{ host, port, user, pass, name string }

type wpConnector struct {
	content string   // wp-content path (backed up / restored)
	exclude []string // dir names skipped under wp-content
	db      dbConf
	redis   redisConf // persistent object cache (Object Cache Pro / Valkey) to flush after restore
}

// redisConf points at the WordPress persistent object cache. After a restore the
// DB is replaced, but WordPress would keep serving STALE cached values (notably
// alloptions → active_plugins, and cached post objects) until the object cache is
// flushed — so a restore that doesn't flush LOOKS like it didn't apply. Empty
// Host = no object cache (flush is a no-op).
type redisConf struct{ host, port, pass string }

func (wpConnector) DataType() string { return "wordpress" }

// EstimateBytes implements binfinity.SizeEstimator: a best-effort estimate of the
// source size (wp-content tree + DB data/index length) so the Console can show an
// approximate % and ETA. Errors are swallowed — a zero/partial estimate just means
// the Console shows bytes/throughput without a %.
func (c wpConnector) EstimateBytes(ctx context.Context) int64 {
	var total int64
	_ = filepath.Walk(c.content, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	q := exec.CommandContext(ctx, pickBinary("mysql", "mariadb"),
		"-h", c.db.host, "-P", c.db.port, "-u", c.db.user, "-N", "-B", "-e",
		"SELECT IFNULL(SUM(data_length+index_length),0) FROM information_schema.tables WHERE table_schema=DATABASE()", c.db.name)
	q.Env = append(os.Environ(), "MYSQL_PWD="+c.db.pass)
	if out, err := q.Output(); err == nil {
		if n, e := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); e == nil {
			total += n
		}
	}
	return total
}

// ─────────────────────────────── Backup ─────────────────────────────────────

// Backup writes a full composite (Layer A): full DB dump + the whole wp-content.
func (c wpConnector) Backup(ctx context.Context, w io.Writer) error {
	return c.write(ctx, w, time.Time{})
}

// BackupIncremental writes a delta (Layer B): the DB is ALWAYS dumped full (a
// logical dump isn't mtime-incremental); wp-content is scoped to files modified
// after the cursor. Returns a new cursor = this backup's start time.
func (c wpConnector) BackupIncremental(ctx context.Context, w io.Writer, cursor string) (string, bool, error) {
	since := time.Time{}
	isDelta := false
	if cursor != "" {
		if t, err := time.Parse(time.RFC3339Nano, cursor); err == nil {
			since, isDelta = t, true
		}
	}
	next := time.Now().UTC().Format(time.RFC3339Nano)
	if err := c.write(ctx, w, since); err != nil {
		return "", false, err
	}
	return next, isDelta, nil
}

func (c wpConnector) write(ctx context.Context, w io.Writer, since time.Time) error {
	tw := tar.NewWriter(w)
	defer tw.Close()
	if err := c.dumpDB(ctx, tw); err != nil {
		return err
	}
	return c.addContent(ctx, tw, since)
}

// dumpDB streams a consistent logical dump to a temp file (so the tar header can
// carry its size without buffering the whole dump in RAM), then writes it as the
// db/dump.sql entry. --add-drop-database makes restore a point-in-time replace.
func (c wpConnector) dumpDB(ctx context.Context, tw *tar.Writer) error {
	// Spool to BF_SPOOL_DIR if set (point it at a roomy, DISK-backed volume — NOT
	// a tmpfs/memory mount — on limited-storage/RAM nodes), else the OS temp dir
	// ($TMPDIR or /tmp). The dump streams to this file so the tar header can carry
	// its size without buffering the whole dump in RAM.
	tmp, err := os.CreateTemp(os.Getenv("BF_SPOOL_DIR"), "presserve-db-*.sql")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	dumper := pickBinary("mysqldump", "mariadb-dump")
	args := []string{"-h", c.db.host, "-P", c.db.port, "-u", c.db.user}
	// --column-statistics=0 is a MySQL-8-client-only flag; the MySQL 8 dumper
	// queries information_schema.COLUMN_STATISTICS, which MariaDB/MySQL 5.7 servers
	// lack. Add it ONLY when this dumper supports it, so the addon works with any
	// MySQL/MariaDB client + server combination.
	if dumperSupports(ctx, dumper, "column-statistics") {
		args = append(args, "--column-statistics=0")
	}
	args = append(args, "--single-transaction", "--quick", "--routines", "--triggers", "--events",
		"--add-drop-database", "--add-drop-table", "--skip-comments", "--databases", c.db.name)
	dump := exec.CommandContext(ctx, dumper, args...)
	dump.Env = append(os.Environ(), "MYSQL_PWD="+c.db.pass)
	dump.Stdout = tmp
	var errb bytes.Buffer
	dump.Stderr = &errb
	if err := dump.Run(); err != nil {
		return fmt.Errorf("mysqldump: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: dbEntry, Mode: 0o600, Size: size, Typeflag: tar.TypeReg, ModTime: time.Now()}); err != nil {
		return err
	}
	_, err = io.Copy(tw, tmp)
	return err
}

// addContent tars the wp-content tree under files/. When since is non-zero only
// regular files modified after it are included (the delta); excluded dirs are skipped.
func (c wpConnector) addContent(ctx context.Context, tw *tar.Writer, since time.Time) error {
	return filepath.Walk(c.content, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rel, err := filepath.Rel(c.content, p)
		if err != nil || rel == "." {
			return nil
		}
		if info.IsDir() {
			if c.isExcluded(rel) {
				return filepath.SkipDir
			}
			if since.IsZero() { // dirs only needed in a full (delta overlays an existing tree)
				hdr, _ := tar.FileInfoHeader(info, "")
				hdr.Name = filesPfx + filepath.ToSlash(rel) + "/"
				return tw.WriteHeader(hdr)
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if !since.IsZero() && !info.ModTime().After(since) {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filesPfx + filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		fh, err := os.Open(p)
		if err != nil {
			return err
		}
		defer fh.Close()
		_, err = io.Copy(tw, fh)
		return err
	})
}

// isExcluded reports whether a wp-content-relative dir should be skipped. Excludes
// are anchored at the wp-content ROOT (e.g. "cache" → wp-content/cache, "upgrade" →
// wp-content/upgrade) — NOT matched by basename anywhere in the tree. Matching by
// basename was a data-loss bug: it also skipped nested dirs of the same name such
// as plugins/elementor/core/upgrade and plugins/*/cache, so backups silently
// omitted them and restores produced broken plugins.
func (c wpConnector) isExcluded(rel string) bool {
	rel = filepath.ToSlash(rel)
	for _, e := range c.exclude {
		e = strings.Trim(strings.TrimSpace(e), "/")
		if e == "" {
			continue
		}
		if rel == e || strings.HasPrefix(rel, e+"/") {
			return true
		}
	}
	return false
}

// ─────────────────────────────── Restore ────────────────────────────────────

// Restore reconstructs a self-contained backup: load the DB + replace wp-content.
func (c wpConnector) Restore(ctx context.Context, r io.Reader) error {
	return c.apply(ctx, r, true)
}

// RestoreIncremental applies ONE chain element. On the base it clears wp-content
// (point-in-time). Every element loads its DB dump (the LAST/target element wins —
// the DB is full per backup), and overlays its files. Net: target DB + merged files.
func (c wpConnector) RestoreIncremental(ctx context.Context, r io.Reader, isBase bool) error {
	return c.apply(ctx, r, isBase)
}

func (c wpConnector) apply(ctx context.Context, r io.Reader, isBase bool) error {
	dbApplied, files, err := c.consume(ctx, r, isBase)
	if err != nil {
		return err
	}

	// Every WordPress backup carries a full DB dump. Reaching EOF without importing
	// one means the restore did NOT actually restore the site — fail LOUDLY instead
	// of reporting success. This is the guard against the silent-skip data-loss bug
	// (member-name skew, or an empty/truncated recovered stream): a restore that
	// imports no DB must surface as FAILED, never as "done".
	if !dbApplied {
		return fmt.Errorf("restore stream contained no database dump (applied %d file entries); refusing to report success", files)
	}

	// DB + files are in place; flush the WordPress object cache so WP serves the
	// restored state, not stale cached active_plugins/options/posts (best-effort —
	// the DB restore is already correct regardless).
	if ferr := c.flushObjectCache(ctx); ferr != nil {
		fmt.Fprintf(os.Stderr, "[presserve] object-cache flush failed (DB/files still restored): %v\n", ferr)
	}
	return nil
}

// consume reads the composite tar, importing the DB dump and extracting the
// wp-content tree (tolerant of any addon version's member names). It reports
// whether a DB dump was imported and how many content entries were written, so
// apply can enforce the "a restore must import a DB" invariant.
func (c wpConnector) consume(ctx context.Context, r io.Reader, isBase bool) (dbApplied bool, files int, err error) {
	// On a base restore wp-content is replaced point-in-time. Clear it LAZILY, only
	// once we have a real content entry to restore — so an empty/garbage stream
	// fails WITHOUT first wiping the live wp-content to nothing.
	cleared := false
	clearOnce := func() error {
		if isBase && !cleared {
			cleared = true
			return clearDir(c.content)
		}
		return nil
	}

	tr := tar.NewReader(r)
	for {
		hdr, e := tr.Next()
		if e == io.EOF {
			return dbApplied, files, nil
		}
		if e != nil {
			return dbApplied, files, fmt.Errorf("read restore stream: %w", e)
		}
		switch {
		case isDBDump(hdr.Name):
			if e := c.loadDB(ctx, tr); e != nil {
				return dbApplied, files, e
			}
			dbApplied = true
		default:
			rel, ok := contentRel(hdr.Name)
			if !ok {
				continue // unknown member: ignore (forward-compatible)
			}
			if e := clearOnce(); e != nil {
				return dbApplied, files, e
			}
			if e := extractInto(c.content, rel, hdr, tr); e != nil {
				return dbApplied, files, e
			}
			files++
		}
	}
}

// loadDB feeds the recovered dump into mysql; the dump's DROP DATABASE/CREATE make
// it a point-in-time replace (tables created after the backup are removed).
func (c wpConnector) loadDB(ctx context.Context, r io.Reader) error {
	imp := exec.CommandContext(ctx, pickBinary("mysql", "mariadb"), "-h", c.db.host, "-P", c.db.port, "-u", c.db.user)
	imp.Env = append(os.Environ(), "MYSQL_PWD="+c.db.pass)
	imp.Stdin = r
	var errb bytes.Buffer
	imp.Stderr = &errb
	if err := imp.Run(); err != nil {
		return fmt.Errorf("mysql import: %w: %s", err, bytes.TrimSpace(errb.Bytes()))
	}
	return nil
}

// flushObjectCache clears the WordPress persistent object cache (Object Cache
// Pro / Valkey/Redis) so a restore is immediately visible — WP reads
// active_plugins, options and cached posts from here, not the DB. No-op when no
// cache is configured. Talks raw RESP (no client dependency); best-effort.
func (c wpConnector) flushObjectCache(ctx context.Context) error {
	if c.redis.host == "" {
		return nil // no persistent object cache
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(c.redis.host, c.redis.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if c.redis.pass != "" {
		if _, err := conn.Write(respCmd("AUTH", c.redis.pass)); err != nil {
			return err
		}
		if err := respExpectOK(conn); err != nil {
			return fmt.Errorf("redis auth: %w", err)
		}
	}
	if _, err := conn.Write(respCmd("FLUSHALL")); err != nil {
		return err
	}
	return respExpectOK(conn)
}

// respCmd encodes a RESP array command (e.g. *1\r\n$8\r\nFLUSHALL\r\n).
func respCmd(args ...string) []byte {
	b := fmt.Appendf(nil, "*%d\r\n", len(args))
	for _, a := range args {
		b = fmt.Appendf(b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b
}

// respExpectOK reads a single RESP reply, treating +OK as success and -ERR as failure.
func respExpectOK(conn net.Conn) error {
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	if n > 0 && buf[0] == '-' {
		return fmt.Errorf("redis: %s", bytes.TrimSpace(buf[1:n]))
	}
	return nil
}

func main() {
	binfinity.Main(newConnector())
}

// ─────────────────────────────── config ─────────────────────────────────────

func newConnector() wpConnector {
	cfgPath := findWPConfig()     // discover across distros
	cfg := parseWPConfig(cfgPath) // best-effort
	root := firstNonEmpty(os.Getenv("WP_PATH"), filepath.Dir(cfgPath))
	content := firstNonEmpty(os.Getenv("WP_CONTENT"), filepath.Join(root, "wp-content"))
	// DB creds, in priority order: the addon's own WP_DB_* env; the OFFICIAL
	// WordPress image's WORDPRESS_DB_* env (the common case — that image's wp-config
	// uses getenv_docker(), so parsing it yields nothing); then wp-config literals;
	// then defaults. Without the WORDPRESS_DB_* fallback the host resolves to
	// "localhost" and mysql tries the local socket — which fails inside the addon
	// container (no local mysqld), breaking every backup AND restore.
	host, port := splitHostPort(firstNonEmpty(os.Getenv("WP_DB_HOST"), os.Getenv("WORDPRESS_DB_HOST"), cfg["DB_HOST"], "localhost"))
	if p := firstNonEmpty(os.Getenv("WP_DB_PORT"), os.Getenv("WORDPRESS_DB_PORT")); p != "" {
		port = p
	}
	return wpConnector{
		content: content,
		// Default excludes follow UpdraftPlus's model: back up the real content
		// (plugins, themes, uploads, and everything else under wp-content) and skip
		// only regenerable/irrelevant ROOT-level dirs — caches, WordPress's upgrade
		// temp + update leftovers, and other backup tools' own backup dirs (so we
		// don't back up backups). All anchored at the wp-content root (see
		// isExcluded), so nested dirs of the same name (e.g. plugins/*/core/upgrade)
		// are ALWAYS captured.
		exclude: strings.Split(envOr("WP_EXCLUDE", "cache,upgrade,upgrade-temp-backup,updraft,ai1wm-backups,backup,backups"), ","),
		db: dbConf{
			host: host, port: port,
			user: firstNonEmpty(os.Getenv("WP_DB_USER"), os.Getenv("WORDPRESS_DB_USER"), cfg["DB_USER"], "root"),
			pass: firstNonEmpty(os.Getenv("WP_DB_PASSWORD"), os.Getenv("WORDPRESS_DB_PASSWORD"), cfg["DB_PASSWORD"]),
			name: firstNonEmpty(os.Getenv("WP_DB_NAME"), os.Getenv("WORDPRESS_DB_NAME"), cfg["DB_NAME"], "wordpress"),
		},
		redis: discoverObjectCache(cfg, content),
	}
}

// discoverObjectCache locates the WordPress persistent object cache so restore can
// flush it. It is OPTIONAL — many sites have none — so an empty Host (the common
// case) simply disables the flush. Resolution: WP_REDIS_HOST/PORT/PASSWORD env
// (explicit, what the chart sets) → values parsed from wp-config. If a cache
// drop-in (wp-content/object-cache.php) exists but no host is known, we log a hint
// rather than guess, so we never FLUSH the wrong server.
func discoverObjectCache(cfg map[string]string, content string) redisConf {
	host := firstNonEmpty(os.Getenv("WP_REDIS_HOST"), cfg["WP_REDIS_HOST"])
	rc := redisConf{
		host: host,
		port: firstNonEmpty(os.Getenv("WP_REDIS_PORT"), cfg["WP_REDIS_PORT"], "6379"),
		pass: firstNonEmpty(os.Getenv("WP_REDIS_PASSWORD"), cfg["WP_REDIS_PASSWORD"]),
	}
	if host == "" {
		if _, err := os.Stat(filepath.Join(content, "object-cache.php")); err == nil {
			fmt.Fprintln(os.Stderr, "[presserve] a persistent object cache (object-cache.php) is present but WP_REDIS_HOST is unset — "+
				"set it so restore can flush the cache (else restored data may be masked by stale cache)")
		}
	}
	return rc
}

// findWPConfig locates wp-config.php across WordPress distributions. Resolution:
//  1. WP_CONFIG — an explicit full path to the file (highest priority).
//  2. <WP_PATH>/wp-config.php, and one directory above (WordPress permits the
//     config one level up from the install).
//  3. Well-known install roots: official Docker/vanilla, Bitnami (Lightsail/VMs),
//     the Debian package, cPanel-style docroots.
//  4. Last resort: a bounded filesystem search beneath the common web roots.
//
// Returns the first existing match, else the conventional default so downstream
// errors point somewhere sensible (DB creds can still come entirely from env).
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
// wp-config.php found, skipping noisy trees; unreadable dirs are skipped, not fatal.
func searchWPConfig(base string, maxDepth int) string {
	baseDepth := strings.Count(filepath.Clean(base), string(filepath.Separator))
	var found string
	_ = filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
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

func isFile(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir()
}

// pickBinary returns primary if it's on PATH, else fallback if it is, else
// primary (so the run fails with a clear "not found"). Lets the addon use the
// MySQL tools (mysqldump/mysql) or MariaDB's renamed ones (mariadb-dump/mariadb).
func pickBinary(primary, fallback string) string {
	if _, err := exec.LookPath(primary); err == nil {
		return primary
	}
	if _, err := exec.LookPath(fallback); err == nil {
		return fallback
	}
	return primary
}

// dumperSupports reports whether the dump binary advertises the given long option
// in its --help, so version-specific flags are only passed to clients that grok them.
func dumperSupports(ctx context.Context, bin, opt string) bool {
	out, _ := exec.CommandContext(ctx, bin, "--help").CombinedOutput()
	return bytes.Contains(out, []byte(opt))
}

// parseWPConfig extracts DB_* constants from wp-config.php without running PHP.
func parseWPConfig(path string) map[string]string {
	out := map[string]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	// define( 'DB_NAME', 'value' );  — single or double quotes, flexible spacing.
	re := regexp.MustCompile(`(?i)define\(\s*['"](DB_NAME|DB_USER|DB_PASSWORD|DB_HOST)['"]\s*,\s*['"]([^'"]*)['"]`)
	for _, m := range re.FindAllStringSubmatch(string(b), -1) {
		out[strings.ToUpper(m[1])] = m[2]
	}
	return out
}

func splitHostPort(h string) (string, string) {
	h = strings.TrimSpace(h)
	if i := strings.LastIndex(h, ":"); i >= 0 && !strings.Contains(h[i+1:], "/") {
		return h[:i], h[i+1:]
	}
	return h, "3306"
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ─────────────────────────────── tar fs helpers ─────────────────────────────

// clearDir removes everything inside root (not root itself) so a restore replaces
// the tree rather than overlaying onto current data.
func clearDir(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(root, 0o755)
		}
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(root, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// extractInto writes one tar entry (rel path) under root, rejecting traversal.
func extractInto(root, rel string, hdr *tar.Header, tr io.Reader) error {
	clean := filepath.Clean(filepath.FromSlash(rel))
	if clean == ".." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil
	}
	dst := filepath.Join(root, clean)
	if hdr.Typeflag == tar.TypeDir {
		return os.MkdirAll(dst, 0o755)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(hdr.Mode))
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, tr)
	return err
}
