# Binfinity Addon Blueprint

How to build a Binfinity **edge addon** — in **any language**, for **any data
source or file type**. An addon backs a source up to Binfinity and restores it,
driven entirely from the Binfinity Console.

You implement only one thing that's specific to your source: **convert it to a
byte stream and back**. Everything else — chunking, dedup, encryption, integrity,
content-addressing, multi-cloud — is done for you, and the wire protocol is the
same for every addon. That's the point: **the server is source-agnostic; the edge
is the only thing that knows your data.**

> **Two ways to build:**
> - **Go (fast path):** use the [Binfinity Addon SDK](README.md) — implement
>   three methods (`DataType`/`Backup`/`Restore`) and the SDK does enrollment,
>   auth, the heartbeat/poll loops, reporting and the BSP data plane for you.
> - **Any language / full contract:** this blueprint is the mental model; the
>   exact wire spec (every endpoint, field, status code and validation rule) is in
>   the **[Addon Development Guide](DEVELOPMENT.md)**.

---

## 1. The big picture

```
   your data source                  EDGE ADDON (you build this)            BINFINITY
 ┌──────────────────┐   convert    ┌───────────────────────────┐   BSP    ┌────────────┐
 │ MySQL / Postgres │ ───────────► │ 1. enroll (setup key)     │ ───────► │ SMS (data) │
 │ files / S3 / a   │  to a byte   │ 2. heartbeat              │  stream  │ object     │
 │ WordPress site / │  stream      │ 3. poll for commands      │ ◄─────── │ storage    │
 │ anything         │ ◄─────────── │ 4. backup / restore       │  control │ CBS (ctrl) │
 └──────────────────┘   restore    └───────────────────────────┘          └────────────┘
```

Two planes, two simple jobs for you:

- **Control plane (HTTPS + JSON):** enroll once, then heartbeat + poll + report. Any language with an HTTP client can do this. The edge **always connects outbound** (it may be behind NAT); Binfinity never dials in.
- **Data plane (BSP):** push/pull the actual bytes. **Easiest path: shell out to the `binfinity` CLI** — a single static binary that does chunking, encryption (client-side, zero-knowledge), integrity and storage for you. You never reimplement crypto.

---

## 2. What you implement: the convert step

Your addon turns its source into a **byte stream** for backup, and consumes a
byte stream for restore. That's the *only* data-type-specific code.

| Source | backup = produce bytes | restore = consume bytes |
|--------|------------------------|--------------------------|
| MySQL | `mysqldump --databases db` | `mysql < dump.sql` |
| PostgreSQL | `pg_dump db` | `psql < dump.sql` |
| MongoDB | `mongodump --archive` | `mongorestore --archive` |
| Files / a directory | `tar -c /path` | `tar -x -C /path` |
| A single file / any filetype | `cat file` (stream raw bytes) | write bytes to `file` |
| WordPress | `wp db export` + `tar` of uploads | reverse |
| S3 bucket → another | stream objects | re-put objects |

Binfinity treats the stream as **opaque bytes** — it chunks and content-addresses
whatever you give it, so **any file type works** (text, binary, compressed,
encrypted-at-source, media). You decide the format; you just have to reproduce
the same bytes on restore.

---

## 3. The universal control contract (HTTP + JSON)

Base URL is the Console domain (e.g. `https://binfinity.example.com`). Control
goes through the edge under `/cbs` and identity under `/ams` (nginx routes them).

### 3.1 Enroll (once, on first start)

The operator mints a **setup key** in the Console (Systems → Setup keys). You bake
it into the addon's config. On first run:

```
POST /ams/api/v1/enroll
Content-Type: application/json
{ "setup_key": "bsk_…", "name": "edge-mysql-eu-1" }

201 → { "client_id": "system-…", "secret": "…", "tenant_id": "…", "name": "…", "roles": [...] }
```

Persist that response (e.g. `credentials.json`). `client_id` is your **`system_id`**
for every later call. Re-enrollment is skipped if credentials already exist.

### 3.2 Heartbeat (every ~10 s)

```
POST /cbs/api/v1/agent/heartbeat
{ "system_id": "system-…", "name": "edge-mysql-eu-1", "data_type": "mysql", "activity": "idle" }
```

`activity` ∈ `idle | backing-up | restoring`. This is what makes the system show
as **connected** in the Console and animates its live data-flow map.

### 3.3 Poll for a command (every ~5 s)

```
GET /cbs/api/v1/agent/commands?system=system-…

204 No Content                      → nothing to do
200 → { "id":"cmd-…", "type":"backup"|"restore", "backup_id":"…" }
```

For `backup`, `backup_id` may be empty — you assign one (e.g. `mysql-<unixtime>`).
For `restore`, `backup_id` is the backup to restore.

### 3.4 Execute, then report

Run the convert step (§4), then:

```
POST /cbs/api/v1/agent/commands/{id}/result?system=system-…
{ "status": "done"|"failed", "backup_id": "…", "bytes": 12345, "error": "" }
```

A successful backup is recorded in that system's backup list automatically.

> **Keys never travel.** Commands carry no passphrase/keys. Encryption is
> client-side (zero-knowledge): the edge holds its own passphrase/KEK. The
> Console can trigger a restore but can't read your data.

---

## 4. The data plane: `binfinity` CLI (recommended for any language)

Bundle the `binfinity` binary in your addon image. It speaks the BSP data plane
to SMS; you just pipe bytes.

```bash
# BACKUP — stream your source straight in (constant memory, any size):
<your-export> | binfinity backup --in - \
    --store grpc://<host>:8090 --id <backup_id> --source-type <data_type>
# (set BINFINITY_PASSPHRASE in the environment)

# RESTORE — pull the backup out (integrity-verified), then import:
binfinity restore --id <backup_id> --store grpc://<host>:8090 --out dump.bin
<your-import> < dump.bin
```

`--store` also accepts `s3://bucket` or `local:/path`. The CLI prints
`bytes stored: N` (parse it for your Result). For DR, `binfinity restore --offline`
works with **no Binfinity services running** — just the store + passphrase.

> Advanced: instead of the CLI you may implement the SMS gRPC `Storage` proto and
> the BSP format (`proto/binfinity/v1/storage.proto`, ADR-0003) natively. Only do
> this if you can't ship the binary; the CLI is the supported path.

---

## 5. The agent loop (language-agnostic pseudocode)

```
config  = read_env(BF_ENDPOINT, BF_SETUP_KEY, STORE_SPEC, source creds, BINFINITY_PASSPHRASE)
creds   = load("credentials.json") or POST /ams/api/v1/enroll {setup_key, name}
system  = creds.client_id

spawn forever every 10s: POST /cbs/api/v1/agent/heartbeat {system, name, data_type, activity}

loop every 5s:
    cmd = GET /cbs/api/v1/agent/commands?system=system
    if cmd is 204: continue
    activity = cmd.type == "backup" ? "backing-up" : "restoring"
    try:
        if cmd.type == "backup":
            id = cmd.backup_id or generate()
            run: <export> | binfinity backup --in - --store STORE --id id
            result = {status:"done", backup_id:id, bytes: parsed}
        else:
            run: binfinity restore --id cmd.backup_id --store STORE --out tmp
            run: <import> < tmp
            result = {status:"done", backup_id: cmd.backup_id}
    catch e:
        result = {status:"failed", backup_id: ..., error: str(e)}
    activity = "idle"
    POST /cbs/api/v1/agent/commands/{cmd.id}/result?system=system  result
```

That's the entire addon. In Go you don't write this loop at all — the
[SDK](README.md) does, and you implement only the convert step. The
reference [`examples/mysql-client/main.go`](examples/mysql-client/main.go) is ~70 lines,
almost all of it `mysqldump`/`mysql`.

---

## 6. The manifest — `addon.yaml`

Declare what your addon handles:

```yaml
name: postgres-client
data_type: postgres            # free-form tag shown in the Console + BSP source_type
version: 0.1.0
capabilities: [backup, restore]
requires_env:
  - BF_ENDPOINT
  - BF_SETUP_KEY
  - STORE_SPEC
  - BINFINITY_PASSPHRASE
  - PG_HOST
  - PG_USER
  - PG_PASSWORD
  - PG_DB
```

---

## 7. Packaging

- Containerize so the addon runs as a sidecar next to the source. Bundle the
  `binfinity` binary + your runtime + the source's client tools (`pg_dump`, `tar`, …).
- Configure entirely by env (`requires_env`). No inbound ports needed — the edge
  only makes outbound HTTPS/gRPC calls, so it works behind NAT/firewalls.
- See [`examples/mysql-client/Dockerfile`](examples/mysql-client/Dockerfile) for a template
  (multi-stage: build the CLI + your agent, run on a base that has your tools).

---

## 8. Checklist for a new addon

- [ ] `addon.yaml` with your `data_type` + `requires_env`.
- [ ] Enroll on first start; persist credentials; reuse `client_id` as `system_id`.
- [ ] Heartbeat loop (report `activity`).
- [ ] Poll loop: backup → `<export> | binfinity backup`; restore → `binfinity restore` → `<import>`.
- [ ] Report a Result for every command.
- [ ] `BINFINITY_PASSPHRASE` set; verify a backup → mutate the source → restore → state matches.
- [ ] Dockerfile bundling `binfinity` + your tools; configured by env only.

Build those seven things in any language and your source is a first-class
Binfinity citizen — schedules, retention, restore, multi-cloud and the live
Console map all work with zero server changes.
