# mysql-client — Binfinity MySQL edge addon

The reference addon, and the worked example for the [Binfinity Addon
SDK](../../README.md). It runs next to a MySQL database (as a sidecar on the
customer's host), enrolls with a setup key, and is then driven entirely from the
Binfinity Console — backups, restores, schedules and retention.

## How it's built

It is built on the SDK, so [`main.go`](main.go) is ~70 lines: a `Connector` whose
two convert methods are the only MySQL-specific code.

```go
func (m mysqlConnector) Backup(ctx, w)  // mysqldump --single-transaction → w  (streamed)
func (m mysqlConnector) Restore(ctx, r) // r → mysql  (the recovered SQL stream)
func main() { binfinity.Main(mysqlConnector{...}) }
```

Everything else — enroll, token auth, heartbeat, poll, report, and the BSP data
plane (`binfinity backup`/`restore`) — is the SDK's job. To build an addon for a
different source, copy this shape and swap the two methods. See the
**[Addon Development Guide](../../DEVELOPMENT.md)**.

## Configuration (env)

| Env | Purpose |
|-----|---------|
| `BF_ENDPOINT` | Console base URL (`https://…`) |
| `BF_SETUP_KEY` | enrollment key minted in the Console |
| `STORE_SPEC` | `grpc://<host>:8090` (SMS data plane) |
| `BINFINITY_PASSPHRASE` | tenant passphrase (never leaves the edge) |
| `MYSQL_HOST` / `MYSQL_USER` / `MYSQL_PASSWORD` / `MYSQL_DB` | source connection |

On first start it enrolls and persists credentials (`CRED_PATH`, default
`/data/credentials.json`), then waits for Console commands. Used by `example-app`
as a sidecar. Backups/restores/schedules are triggered from the **Console**, per
system — never from the app.

## Build

```bash
# from the repo root (the Dockerfile bundles the binfinity CLI + this agent):
docker build -f examples/mysql-client/Dockerfile -t binfinity-mysql-edge .
# or locally (addons are outside go.work):
cd examples/mysql-client && GOWORK=off go build -o mysql-edge .
```

Convert steps:

```
backup:   mysqldump --databases <db> | binfinity backup --in - --store grpc://…:8090
restore:  binfinity restore --id <id> --out dump.sql && mysql < dump.sql
```
