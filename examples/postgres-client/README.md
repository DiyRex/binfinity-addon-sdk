# postgres-client — Binfinity PostgreSQL edge addon (example)

A complete PostgreSQL connector built on the [Addon SDK](../../README.md). The
only Postgres-specific code is the convert step in [`main.go`](main.go):

```
backup:   pg_dump --clean --if-exists <db>  →  binfinity backup --in -
restore:  binfinity restore  →  psql -d <db>
```

`pg_dump` runs in a single transaction, so each backup is a consistent
point-in-time snapshot. See the [Addon Development Guide](../../DEVELOPMENT.md).

## Configuration (env)

| Env | Purpose |
|-----|---------|
| `BF_ENDPOINT` / `BF_SETUP_KEY` / `STORE_SPEC` / `BINFINITY_PASSPHRASE` | Binfinity contract (see SDK README) |
| `PG_HOST` / `PG_PORT` / `PG_USER` / `PG_PASSWORD` / `PG_DB` | source connection |

## Build

```bash
cd examples/postgres-client && GOWORK=off go build -o postgres-edge .
# or, as a container (from the repo root):
docker build -f examples/postgres-client/Dockerfile -t binfinity-postgres-edge .
```

> For very large databases, `pg_dump`/`psql` (logical) is portable but re-exports
> the whole DB each run; Binfinity dedups at the chunk level so only changed data
> is stored. For physical/PITR backups, wrap your tool of choice the same way.
