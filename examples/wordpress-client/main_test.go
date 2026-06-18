package main

import (
	"archive/tar"
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, root, rel, body string, mtime time.Time) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}
func read(t *testing.T, root, rel string) (string, bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		return "", false
	}
	return string(b), true
}

// tarContent runs addContent into a buffer (no DB, so no mysql needed).
func tarContent(t *testing.T, c wpConnector, since time.Time) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := c.addContent(context.Background(), tw, since); err != nil {
		t.Fatalf("addContent: %v", err)
	}
	tw.Close()
	return &buf
}

// TestContentRoundTrip: wp-content backs up + restores byte-identical.
func TestContentRoundTrip(t *testing.T) {
	src := t.TempDir()
	now := time.Now()
	writeFile(t, src, "themes/x/style.css", "body{}", now)
	writeFile(t, src, "uploads/2026/pic.bin", string([]byte{0, 1, 2, 255}), now)

	buf := tarContent(t, wpConnector{content: src}, time.Time{})

	dst := t.TempDir()
	rc := wpConnector{content: dst}
	if _, _, err := rc.consume(context.Background(), buf, true); err != nil {
		t.Fatalf("consume: %v", err)
	}
	for _, rel := range []string{"themes/x/style.css", "uploads/2026/pic.bin"} {
		want, _ := read(t, src, rel)
		if got, ok := read(t, dst, rel); !ok || got != want {
			t.Errorf("%s: got %q ok=%v want %q", rel, got, ok, want)
		}
	}
}

// TestPointInTimeReplace: restoring removes files created after the backup.
func TestPointInTimeReplace(t *testing.T) {
	src := t.TempDir()
	writeFile(t, src, "themes/keep.txt", "v1", time.Now())
	buf := tarContent(t, wpConnector{content: src}, time.Time{})

	dst := t.TempDir()
	writeFile(t, dst, "plugins/evil.php", "added later", time.Now())
	rc := wpConnector{content: dst}
	if _, _, err := rc.consume(context.Background(), buf, true); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if got, ok := read(t, dst, "themes/keep.txt"); !ok || got != "v1" {
		t.Errorf("backed-up file missing: %q ok=%v", got, ok)
	}
	if _, ok := read(t, dst, "plugins/evil.php"); ok {
		t.Error("file created after the backup must be removed on restore (point-in-time replace)")
	}
}

// TestExcludeAndDelta: excluded dirs are skipped; a delta only carries changed files.
func TestExcludeAndDelta(t *testing.T) {
	src := t.TempDir()
	t0 := time.Now().Add(-time.Hour)
	writeFile(t, src, "uploads/a.jpg", "a", t0)
	writeFile(t, src, "cache/junk.tmp", "junk", t0) // excluded dir

	c := wpConnector{content: src, exclude: []string{"cache"}}
	full := tarNames(t, tarContent(t, c, time.Time{}))
	if !full["files/uploads/a.jpg"] {
		t.Error("full should include uploads/a.jpg")
	}
	if full["files/cache/junk.tmp"] {
		t.Error("excluded 'cache' dir must be skipped")
	}

	// Modify one file; a delta since now-30m should carry only it.
	t1 := time.Now()
	writeFile(t, src, "uploads/b.jpg", "b", t1)
	delta := tarNames(t, tarContent(t, c, time.Now().Add(-30*time.Minute)))
	if !delta["files/uploads/b.jpg"] {
		t.Error("delta should include the new file")
	}
	if delta["files/uploads/a.jpg"] {
		t.Error("delta must not include the unchanged file")
	}
}

func TestTraversalRejected(t *testing.T) {
	dst := t.TempDir()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, body := range map[string]string{"files/../escape.txt": "evil", "files/ok.txt": "good"} {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg})
		tw.Write([]byte(body))
	}
	tw.Close()
	if _, _, err := (wpConnector{content: dst}).consume(context.Background(), &buf, true); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, ok := read(t, filepath.Dir(dst), "escape.txt"); ok {
		t.Error("path traversal not blocked")
	}
	if got, _ := read(t, dst, "ok.txt"); got != "good" {
		t.Error("legit entry should restore")
	}
}

func TestParseWPConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := `<?php
define( 'DB_NAME', 'wp_db' );
define('DB_USER',"wp_user");
define( 'DB_PASSWORD', 'p@ss:word' );
define( 'DB_HOST', '127.0.0.1:3307' );
`
	if err := os.WriteFile(filepath.Join(dir, "wp-config.php"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	m := parseWPConfig(filepath.Join(dir, "wp-config.php"))
	if m["DB_NAME"] != "wp_db" || m["DB_USER"] != "wp_user" || m["DB_PASSWORD"] != "p@ss:word" || m["DB_HOST"] != "127.0.0.1:3307" {
		t.Fatalf("parsed wrong: %#v", m)
	}
	h, p := splitHostPort(m["DB_HOST"])
	if h != "127.0.0.1" || p != "3307" {
		t.Fatalf("splitHostPort: %s %s", h, p)
	}
	h2, p2 := splitHostPort("localhost")
	if h2 != "localhost" || p2 != "3306" {
		t.Fatalf("default port: %s %s", h2, p2)
	}
}

func TestFindWPConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "wp-config.php")
	if err := os.WriteFile(cfg, []byte("<?php\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// WP_CONFIG wins outright.
	t.Setenv("WP_CONFIG", cfg)
	if got := findWPConfig(); got != cfg {
		t.Fatalf("WP_CONFIG: got %q want %q", got, cfg)
	}
	// WP_PATH/wp-config.php is found when WP_CONFIG is unset.
	t.Setenv("WP_CONFIG", "")
	t.Setenv("WP_PATH", dir)
	if got := findWPConfig(); got != cfg {
		t.Fatalf("WP_PATH: got %q want %q", got, cfg)
	}
}

func TestSearchWPConfig(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "sites", "blog")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(nested, "wp-config.php")
	if err := os.WriteFile(cfg, []byte("<?php\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := searchWPConfig(base, 6); got != cfg {
		t.Fatalf("searchWPConfig: got %q want %q", got, cfg)
	}
	if got := searchWPConfig(base, 1); got != "" {
		t.Fatalf("searchWPConfig(depth=1): got %q want empty", got)
	}
}

// TestMemberNameTolerance: the restore recognizes the DB dump and content tree
// under the names used by ANY addon version, so a backup written by one build
// always restores with another (no silent member-name skew).
func TestMemberNameTolerance(t *testing.T) {
	for _, n := range []string{"db/dump.sql", "db.sql", "backup.sql", "db/wordpress.sql"} {
		if !isDBDump(n) {
			t.Errorf("isDBDump(%q) = false, want true", n)
		}
	}
	for _, n := range []string{"files/x.php", "wp-content/x.php", "db/dump.sql.gz", "themes/x.sql/y.txt", "a/b/c.sql"} {
		if isDBDump(n) {
			t.Errorf("isDBDump(%q) = true, want false", n)
		}
	}
	cases := map[string]string{"files/themes/a.css": "themes/a.css", "wp-content/plugins/p.php": "plugins/p.php"}
	for in, want := range cases {
		if got, ok := contentRel(in); !ok || got != want {
			t.Errorf("contentRel(%q) = (%q,%v), want (%q,true)", in, got, ok, want)
		}
	}
	if _, ok := contentRel("db/dump.sql"); ok {
		t.Error("contentRel must not treat the DB dump as content")
	}
}

// TestRestoreWithoutDBFailsLoudly: a stream that carries no DB dump must surface as
// an error, never as a silent success — the core bug being fixed (a restore that
// reports "done" while importing no database).
func TestRestoreWithoutDBFailsLoudly(t *testing.T) {
	src := t.TempDir()
	writeFile(t, src, "themes/x/style.css", "body{}", time.Now())
	buf := tarContent(t, wpConnector{content: src}, time.Time{}) // content only, no db
	err := (wpConnector{content: t.TempDir()}).apply(context.Background(), buf, true)
	if err == nil {
		t.Fatal("apply must FAIL when the stream contains no database dump (silent-skip guard)")
	}
}

// TestLegacyContentNamePtReplace: a base restore whose content tree uses the legacy
// "wp-content/" prefix still clears + replaces wp-content point-in-time.
func TestLegacyContentNamePtReplace(t *testing.T) {
	dst := t.TempDir()
	writeFile(t, dst, "plugins/stale.php", "old", time.Now())
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "wp-content/themes/new.css", Mode: 0o644, Size: 2, Typeflag: tar.TypeReg})
	tw.Write([]byte("ok"))
	tw.Close()
	dbApplied, files, err := (wpConnector{content: dst}).consume(context.Background(), &buf, true)
	if err != nil || dbApplied || files != 1 {
		t.Fatalf("consume legacy content: dbApplied=%v files=%d err=%v", dbApplied, files, err)
	}
	if _, ok := read(t, dst, "themes/new.css"); !ok {
		t.Error("legacy wp-content/ entry should restore")
	}
	if _, ok := read(t, dst, "plugins/stale.php"); ok {
		t.Error("base restore must clear pre-existing files (point-in-time)")
	}
}

// TestExcludeAnchoredAtRoot: excludes apply ONLY at the wp-content root, never to
// nested dirs of the same name — otherwise plugin internals like
// plugins/elementor/core/upgrade get silently dropped from the backup (which broke
// Elementor on restore).
func TestExcludeAnchoredAtRoot(t *testing.T) {
	c := wpConnector{exclude: []string{"cache", "upgrade"}}
	excluded := []string{"cache", "upgrade", "cache/x", "upgrade/sub/y"}
	kept := []string{
		"plugins/elementor/core/upgrade",
		"plugins/elementor/core/upgrade/manager.php",
		"plugins/wp-rocket/cache",
		"themes/x/cache-helper",
		"uploads/2026",
	}
	for _, p := range excluded {
		if !c.isExcluded(p) {
			t.Errorf("isExcluded(%q) = false, want true (root-level exclude)", p)
		}
	}
	for _, p := range kept {
		if c.isExcluded(p) {
			t.Errorf("isExcluded(%q) = true, want false (nested same-name dir must be kept)", p)
		}
	}
}

func tarNames(t *testing.T, buf *bytes.Buffer) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		names[hdr.Name] = true
	}
	return names
}

func TestFlushObjectCacheNoCacheIsNoop(t *testing.T) {
	// No object cache configured (the common case) → flush is a clean no-op.
	c := wpConnector{}
	if err := c.flushObjectCache(context.Background()); err != nil {
		t.Fatalf("flush with no cache should be a no-op, got %v", err)
	}
}

func TestRespCmdEncoding(t *testing.T) {
	got := string(respCmd("FLUSHALL"))
	if got != "*1\r\n$8\r\nFLUSHALL\r\n" {
		t.Fatalf("respCmd FLUSHALL = %q", got)
	}
	got = string(respCmd("AUTH", "pw"))
	if got != "*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n" {
		t.Fatalf("respCmd AUTH = %q", got)
	}
}

func TestFlushObjectCacheFlushesWhenConfigured(t *testing.T) {
	// Stand up a fake RESP server that accepts FLUSHALL and replies +OK.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	gotFlush := make(chan bool, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 256)
		n, _ := conn.Read(buf)
		gotFlush <- bytes.Contains(buf[:n], []byte("FLUSHALL"))
		conn.Write([]byte("+OK\r\n"))
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	c := wpConnector{redis: redisConf{host: host, port: port}}
	if err := c.flushObjectCache(context.Background()); err != nil {
		t.Fatalf("flush against fake redis: %v", err)
	}
	if !<-gotFlush {
		t.Fatal("expected FLUSHALL to be sent")
	}
}
