# files-client — Binfinity files/folder edge addon (example)

Backs up an arbitrary directory tree. Built on the [Addon SDK](../../README.md);
the only source-specific code in [`main.go`](main.go) is:

```
backup:   tar -c -C <FILES_ROOT> .  →  binfinity backup --in -
restore:  binfinity restore  →  tar -x -C <FILES_ROOT>
```

Any file type works — text, binary, media, already-encrypted — because Binfinity
treats the stream as opaque bytes. See the
[Addon Development Guide](../../DEVELOPMENT.md).

## Configuration (env)

| Env | Purpose |
|-----|---------|
| `BF_ENDPOINT` / `BF_SETUP_KEY` / `STORE_SPEC` / `BINFINITY_PASSPHRASE` | Binfinity contract |
| `FILES_ROOT` | directory to back up (default `/data`) |

## Build

```bash
cd examples/files-client && GOWORK=off go build -o files-edge .
# or (from the repo root):
docker build -f examples/files-client/Dockerfile -t binfinity-files-edge .
```

> `tar` is not atomic. For live application data, take a filesystem/volume
> snapshot (or quiesce the writer) before the backup runs so the captured tree is
> internally consistent.
