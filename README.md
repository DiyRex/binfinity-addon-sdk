# Binfinity Addon SDK (Go)

`github.com/DiyRex/binfinity-addon-sdk`

Build a Binfinity backup connector for **any** data source by writing **one small
type**. The SDK implements the entire universal edge contract — enroll →
heartbeat → poll → execute → report, plus the BSP data plane (chunking, dedup,
client-side encryption, integrity, multi-cloud) via the `binfinity` CLI. You
implement only the source-specific *convert step*.

It is a **standalone module with no third-party dependencies** (stdlib only) and
imports nothing from the Binfinity monorepo, so addons stay decoupled from server
internals. For the full contract, validations and non-Go path, see
[`DEVELOPMENT.md`](DEVELOPMENT.md).

## Install

```bash
go get github.com/DiyRex/binfinity-addon-sdk
```

## The whole API

```go
// Implement this. Three methods.
type Connector interface {
	DataType() string                                  // "mysql", "postgres", "files", …
	Backup(ctx context.Context, w io.Writer) error     // write the source's bytes to w
	Restore(ctx context.Context, r io.Reader) error    // apply the recovered bytes from r
}

// Run it. One line.
func Main(c Connector)                                  // env-driven, signal-aware
func Run(ctx context.Context, c Connector, cfg Config) error // explicit control
func ConfigFromEnv(dataType string) Config
```

## Complete example — a directory backup addon

```go
package main

import (
	"context"
	"io"
	"os/exec"

	binfinity "github.com/DiyRex/binfinity-addon-sdk"
)

type files struct{ root string }

func (files) DataType() string { return "files" }

func (f files) Backup(ctx context.Context, w io.Writer) error {
	c := exec.CommandContext(ctx, "tar", "-c", "-C", f.root, ".")
	c.Stdout = w // SDK pipes this straight into `binfinity backup --in -`
	return c.Run()
}

func (f files) Restore(ctx context.Context, r io.Reader) error {
	c := exec.CommandContext(ctx, "tar", "-x", "-C", f.root)
	c.Stdin = r // SDK feeds the recovered stream here
	return c.Run()
}

func main() { binfinity.Main(files{root: "/data"}) }
```

That's a complete, production-shaped addon: it enrolls, shows up connected in the
Console, and responds to Console-driven backup/restore/schedule/retention.

## Configuration (environment)

`ConfigFromEnv` / `Main` read these:

| Env | Meaning | Default |
|-----|---------|---------|
| `BF_ENDPOINT` | Console base URL (`https://…`) | — (**required**) |
| `BF_SETUP_KEY` | enrollment key minted in the Console | — (required on first run) |
| `STORE_SPEC` | SMS data plane: `grpc://host:8090`, `s3://bucket`, `local:/path` | `grpc://localhost:8090` |
| `BINFINITY_PASSPHRASE` | tenant passphrase — **never leaves the edge** | — (required) |
| `BF_NAME` | system name shown in the Console | `<data_type>-<hostname>` |
| `CRED_PATH` | where durable credentials are persisted | `/data/credentials.json` |
| `HEARTBEAT_INTERVAL` / `POLL_INTERVAL` | loop cadence (Go durations) | `10s` / `5s` |
| `BF_TLS_INSECURE` | skip TLS verification (**dev only**) | `false` |

## What the SDK does for you

- **Enroll once**, persist credentials to `CRED_PATH`, reuse them forever (re-enroll
  is skipped if credentials exist). Obtains and refreshes an AMS access token
  (client-credentials grant) and attaches it to control-plane calls.
- **Heartbeat** liveness + `activity` (`idle`/`backing-up`/`restoring`) so the
  Console shows the system connected and animates its live map.
- **Poll** for commands outbound over HTTPS (NAT-friendly — nothing dials in).
- **Execute**: on `backup` it streams your `Backup(w)` bytes through
  `binfinity backup` (constant memory, any size); on `restore` it runs
  `binfinity restore` and feeds the verified stream to your `Restore(r)`.
- **Report** a result for every command (releases the per-system state lock).

## Customising

`Run(ctx, c, cfg)` takes a `Config` you can build by hand or from
`ConfigFromEnv`. Notable seams:

- `Config.DataPlane` — swap the default `CLIDataPlane` for your own
  `DataPlane` (e.g. a native BSP/gRPC implementation, or a test stub). The CLI
  path is the supported default.
- `Config.HTTPClient`, `Config.Logf` — inject a client or logger.

## Test

```bash
GOWORK=off go test ./...
```

The suite drives the full enroll→poll→execute→report loop against an in-memory
fake Console with a stub `DataPlane` — no binary or network required.

## Repository layout

| Path | What |
|------|------|
| `*.go` | the SDK package (`package binfinity`) |
| [`DEVELOPMENT.md`](DEVELOPMENT.md) | full developer guide — connect, data flow, every request/response + validation, non-Go path, and how it compares to Velero, Bacula/Bareos, restic/Kopia and the Postgres backup tools |
| [`BLUEPRINT.md`](BLUEPRINT.md) | the one-page mental model |
| [`examples/`](examples/) | complete, SDK-based reference connectors: [mysql](examples/mysql-client/), [postgres](examples/postgres-client/), [files](examples/files-client/), and [wordpress](examples/wordpress-client/) (the composite *database + files* pattern) |

## License

[Apache-2.0](LICENSE).
