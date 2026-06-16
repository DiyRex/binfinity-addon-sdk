package main

import (
	"os"
	"path/filepath"
	"testing"
)

const sampleWPConfig = `<?php
define( 'DB_NAME', 'wp_db' );
define('DB_USER',"wp_user");
define( 'DB_PASSWORD', 's3cr3t!' );
define( 'DB_HOST', 'mariadb:3306' );
$table_prefix = 'wp_';
`

func TestParseWPConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "wp-config.php")
	if err := os.WriteFile(cfg, []byte(sampleWPConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	got := parseWPConfig(cfg)
	for k, want := range map[string]string{
		"DB_NAME": "wp_db", "DB_USER": "wp_user", "DB_PASSWORD": "s3cr3t!", "DB_HOST": "mariadb:3306",
	} {
		if got[k] != want {
			t.Errorf("parseWPConfig[%s] = %q, want %q", k, got[k], want)
		}
	}
	// Missing/unreadable file → empty map, never an error.
	if len(parseWPConfig(filepath.Join(dir, "nope.php"))) != 0 {
		t.Error("parseWPConfig of a missing file should be empty")
	}
}

func TestFindWPConfig_WPConfigEnvWins(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "custom-wp-config.php")
	if err := os.WriteFile(cfg, []byte(sampleWPConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WP_CONFIG", cfg)
	if got := findWPConfig(); got != cfg {
		t.Errorf("findWPConfig() = %q, want WP_CONFIG %q", got, cfg)
	}
}

func TestFindWPConfig_WPPath(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "wp-config.php")
	if err := os.WriteFile(cfg, []byte(sampleWPConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WP_CONFIG", "")
	t.Setenv("WP_PATH", dir)
	if got := findWPConfig(); got != cfg {
		t.Errorf("findWPConfig() = %q, want %q", got, cfg)
	}
}

func TestSearchWPConfig_Nested(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "sites", "blog")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(nested, "wp-config.php")
	if err := os.WriteFile(cfg, []byte(sampleWPConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := searchWPConfig(base, 6); got != cfg {
		t.Errorf("searchWPConfig() = %q, want %q", got, cfg)
	}
	// Beyond the depth bound it should not be found.
	if got := searchWPConfig(base, 1); got != "" {
		t.Errorf("searchWPConfig(depth=1) = %q, want empty (too deep)", got)
	}
}

func TestNewConnector_DiscoversFromConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wp-config.php"), []byte(sampleWPConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	// Clear any inherited overrides so discovery is what's exercised.
	for _, k := range []string{"WP_CONFIG", "WORDPRESS_DB_HOST", "WP_DB_HOST", "WORDPRESS_DB_USER", "WP_DB_USER", "WORDPRESS_DB_PASSWORD", "WP_DB_PASSWORD", "WORDPRESS_DB_NAME", "WP_DB_NAME"} {
		t.Setenv(k, "")
	}
	t.Setenv("WP_PATH", dir)

	c := newConnector()
	if c.path != dir {
		t.Errorf("path = %q, want %q", c.path, dir)
	}
	if c.dbHost != "mariadb" { // port stripped
		t.Errorf("dbHost = %q, want %q", c.dbHost, "mariadb")
	}
	if c.dbUser != "wp_user" || c.dbName != "wp_db" || c.dbPass != "s3cr3t!" {
		t.Errorf("db creds not read from wp-config.php: %+v", c)
	}

	// An explicit env var must override the discovered value.
	t.Setenv("WORDPRESS_DB_HOST", "override-host")
	if c := newConnector(); c.dbHost != "override-host" {
		t.Errorf("env override ignored: dbHost = %q", c.dbHost)
	}
}
