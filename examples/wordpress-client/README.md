# wordpress-client — Binfinity WordPress edge addon (example)

WordPress is a **composite source**: a MySQL database **plus** the site files
(`wp-content` — themes, plugins, uploads). This addon shows the reference pattern
for any "database + files" source — **bundle both into one stream**, split them
on restore. Built on the [Addon SDK](../../README.md); the composite logic lives
in [`main.go`](main.go).

```
backup:   maintenance-mode on
          mysqldump > db.sql
          tar {db.sql, wp-content}  →  binfinity backup --in -
          maintenance-mode off
restore:  binfinity restore  →  tar -x
          mysql < db.sql
          cp -a wp-content → site
```

The DB dump and files are captured at the same point in time (maintenance mode +
`--single-transaction`), and the whole site is restored coherently. See the
[Addon Development Guide](../../DEVELOPMENT.md).

## Configuration (env)

| Env | Purpose |
|-----|---------|
| `BF_ENDPOINT` / `BF_SETUP_KEY` / `STORE_SPEC` / `BINFINITY_PASSPHRASE` | Binfinity contract |
| `WP_PATH` | site root containing `wp-content` (default `/var/www/html`) |
| `WORDPRESS_DB_HOST` / `WORDPRESS_DB_USER` / `WORDPRESS_DB_PASSWORD` / `WORDPRESS_DB_NAME` | database |

> `WORDPRESS_DB_HOST` must be a host (not `host:port`); split a port into a
> separate flag if your setup uses one. The addon needs read access to
> `wp-content` and write access to drop the temporary `.maintenance` flag.

## Build

```bash
cd examples/wordpress-client && GOWORK=off go build -o wordpress-edge .
# or (from the repo root):
docker build -f examples/wordpress-client/Dockerfile -t binfinity-wordpress-edge .
```

## Run as a sidecar

Co-locate with the WordPress container, sharing the site volume and the DB
network. Backups/restores are triggered from the Binfinity Console, per system —
never from the app.
