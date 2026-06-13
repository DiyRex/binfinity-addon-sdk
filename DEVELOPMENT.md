# Binfinity Addon Development Guide

How to build a Binfinity **addon** (edge backup connector) for any data source —
how it connects to the platform, how data flows in and out, exactly what each
message accepts and returns, and how every value is validated.

- New here? Read [`BLUEPRINT.md`](BLUEPRINT.md) first for the one-page mental model.
- Building in **Go**? The [Addon SDK](README.md) collapses everything below
  into three methods — start there and use this guide as the contract reference.
- Building in **another language**? §3–§7 are the complete wire spec.

---

## 1. The model: source-agnostic core, source-specific edge

Binfinity backs up *anything* by making the **edge** (your addon) the only thing
that understands the data, and keeping the **server** completely source-agnostic.
Everything moves as one canonical format — **BSP** (Binfinity Stream Protocol):
content-defined chunked, BLAKE3 content-addressed, client-side encrypted,
Merkle-verified. The server only ever sees opaque, deduplicated ciphertext.

```
  your source            EDGE ADDON (you build)                  BINFINITY (you don't touch)
┌────────────┐  convert ┌──────────────────────────┐  control  ┌──────────────────────────┐
│ MySQL / PG │ ───────► │ enroll → heartbeat → poll │ ◄═══════► │ AMS (identity/enroll)    │
│ files / S3 │   bytes  │ → execute → report        │  HTTPS/   │ CBS (control brain)      │
│ anything   │ ◄─────── │  (the SDK does all this)  │   JSON    │                          │
└────────────┘  restore └─────────────┬────────────┘           └──────────────────────────┘
                                       │  BSP stream (bytes)     ┌──────────────────────────┐
                                       └───────────────────────► │ SMS (data plane, storage)│
                                          gRPC / S3 / local       └──────────────────────────┘
```

**Two planes, two responsibilities:**

| Plane | Transport | What you do | Who initiates |
|-------|-----------|-------------|----------------|
| **Control** | HTTPS + JSON to CBS/AMS | enroll, heartbeat, poll, report | **edge → server** (outbound only; NAT-friendly) |
| **Data** | BSP via the `binfinity` CLI (gRPC/S3/local) | pipe source bytes in/out | edge → SMS |

The edge **always connects outbound** — the server never dials in. This is what
lets an addon run behind NAT/firewalls on a customer's own host.

You implement exactly one source-specific thing: **convert the source to a byte
stream and back.** Chunking, dedup, encryption, integrity, content-addressing and
multi-cloud are done for you, identically for every addon.

---

## 2. The fast path (Go): implement three methods

```go
type Connector interface {
	DataType() string                              // source tag, e.g. "postgres"
	Backup(ctx context.Context, w io.Writer) error // write source bytes to w
	Restore(ctx context.Context, r io.Reader) error // apply recovered bytes from r
}
func main() { binfinity.Main(myConnector{}) }
```

The [SDK](README.md) owns enrollment, token auth, the heartbeat/poll loops,
result reporting, and the `binfinity` CLI plumbing. The reference addon
[`examples/mysql-client/main.go`](examples/mysql-client/main.go) is ~70 lines, almost all of it the
`mysqldump`/`mysql` convert step. The rest of this document is the contract that
the SDK implements — read it to build a non-Go addon, or to understand exactly
what crosses the wire.

---

## 3. Connecting to the platform

### 3.1 Enroll (once, on first start)

An operator mints a **setup key** in the Console (Systems → Setup keys). You bake
it into the addon's config (`BF_SETUP_KEY`). On first run you redeem it for
durable client credentials.

```
POST {BF_ENDPOINT}/ams/api/v1/enroll
Content-Type: application/json

{ "setup_key": "bsk_…", "name": "edge-postgres-eu-1" }
```

**Accepts**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `setup_key` | string | **yes** | the `bsk_…` value from the Console |
| `name` | string | no | system display name; defaults to the key's name if omitted |

**Returns** `201 Created`

```json
{
  "client_id": "system-…",   // ← this is your system_id for every later call
  "secret": "…",             // ← shown ONCE; persist it
  "tenant_id": "…",
  "name": "edge-postgres-eu-1",
  "roles": ["system"]
}
```

**Validations / status codes**

| Condition | Status | Body |
|-----------|--------|------|
| body not valid JSON | `400` | `{"error":"invalid body"}` |
| `setup_key` empty | `400` | `{"error":"setup_key is required"}` |
| key not found / expired / exhausted / revoked | `401` | `{"error":"invalid or exhausted setup key"}` |
| success | `201` | credentials (above) |

> A setup key may be one-off or reusable, with an optional expiry and a use cap —
> all enforced server-side. Not-found and invalid are deliberately indistinguishable.

**Persist the whole response** (e.g. `credentials.json`, mode `0600`). On
subsequent starts, reuse it and skip enrollment. `client_id` **is** your
`system_id` everywhere below.

### 3.2 Get an access token (then refresh as needed)

Authenticate control-plane calls with a short-lived bearer token from the
client-credentials grant.

```
POST {BF_ENDPOINT}/ams/auth/token
Content-Type: application/json

{ "client_id": "system-…", "client_secret": "…" }
```

**Returns** `200 OK` → `{ "access_token": "…", "token_type": "Bearer", "expires_in": 300 }`
(invalid body → `400`; bad credentials → `401`).

Attach it as `Authorization: Bearer <access_token>` on the CBS calls in §4.
Refresh before `expires_in` elapses. (The Go SDK caches and refreshes
automatically.)

> **Keys never travel.** The access token authenticates the *control* channel. It
> is **not** an encryption key. Your tenant `BINFINITY_PASSPHRASE` / KEK never
> leaves the edge — the platform stores only ciphertext it cannot decrypt
> (zero-knowledge).

---

## 4. The control contract (three signals)

Once enrolled, the edge speaks exactly three messages to CBS, the same for every
data type. All are outbound from the edge.

### 4.1 Heartbeat — "I'm alive and here's what I'm doing"  (edge → server)

```
POST {BF_ENDPOINT}/cbs/api/v1/agent/heartbeat
Authorization: Bearer <token>

{ "system_id": "system-…", "name": "edge-postgres-eu-1",
  "data_type": "postgres", "activity": "idle" }
```

| Field | Type | Required | Values |
|-------|------|----------|--------|
| `system_id` | string | **yes** | your `client_id` |
| `name` | string | no | display name |
| `data_type` | string | no | free-form source tag (drives the Console icon) |
| `activity` | string | no | `idle` \| `backing-up` \| `restoring` |

Send every ~10s. **Returns** `200 {"status":"ok"}`; missing `system_id` (or bad
body) → `400 {"error":"system_id required"}`. A system counts as **connected** for
**30s** after its last heartbeat — this is what lights it up on the live map.

### 4.2 Poll for a command — "anything for me?"  (edge → server)

```
GET {BF_ENDPOINT}/cbs/api/v1/agent/commands?system=system-…
Authorization: Bearer <token>
```

| Outcome | Status | Body |
|---------|--------|------|
| nothing to do | `204 No Content` | — |
| a command is queued | `200 OK` | Command (below) |
| `system` query param missing | `400` | `{"error":"system query param required"}` |

```jsonc
// 200 OK — Command
{
  "id": "cmd-…",            // echo this back in the result
  "system_id": "system-…",
  "type": "backup",         // "backup" | "restore"
  "backup_id": "",          // backup: may be empty (you assign); restore: the id to restore
  "status": "dispatched",
  "created_at": "2026-06-13T17:00:00Z"
}
```

Poll every ~5s. Polling marks the command **dispatched** (it won't be handed out
again). A `Command` **never carries a passphrase or key**.

### 4.3 Report a result — "here's what happened"  (edge → server)

After executing (§5), report the outcome — **always**, success or failure.

```
POST {BF_ENDPOINT}/cbs/api/v1/agent/commands/{id}/result?system=system-…
Authorization: Bearer <token>

{ "status": "done", "backup_id": "postgres-20260613T170000Z", "bytes": 1048576, "error": "" }
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `status` | string | **yes** | `done` \| `failed` |
| `backup_id` | string | yes for a successful backup | the id you stored under (assign one if the command's was empty) |
| `bytes` | int | no | ciphertext bytes stored (informational) |
| `error` | string | on failure | human-readable reason |

**Returns** `200 {"status":"recorded"}`; missing `system` param or bad body →
`400`. A successful **backup** result becomes a durable **catalog entry** — it
appears in the system's backup list and is eligible for restore, retention and
transfer. Reporting also **releases the per-system state lock** (§6).

---

## 5. The data plane: how bytes go in and out

The control channel never carries your data — only the *decision* to move it. The
bytes flow on the data plane, which you drive with the **`binfinity` CLI** (a
single static binary you bundle in your image). The CLI does CDC chunking, BLAKE3
hashing, client-side encryption under your passphrase, Merkle integrity and
content-addressed dedup, then writes ciphertext to the store.

### 5.1 Backup — produce bytes, stream straight in

```bash
<your-export> | binfinity backup \
    --in - --store "$STORE_SPEC" --id "$BACKUP_ID" --source-type "$DATA_TYPE"
# BINFINITY_PASSPHRASE must be in the environment
```

`--in -` reads stdin, so the source streams through with **constant memory** — a
1 GB dump backs up on a host with 100 MB free; nothing is staged to disk. The CLI
prints a summary to stdout including `bytes stored:   N` (parse it for your
result):

```
backup id:      postgres-20260613T170000Z
chunks total:   812
chunks stored:  37 (new)        ← only new chunks hit storage (dedup)
chunks deduped: 775
bytes in:       1048576
bytes stored:   65536
merkle root:    9f2c…           ← backup is "complete" only when this verifies
```

**Accepts:** `--in` (file or `-` for stdin, required), `--store`, `--id`
(default: generated UUIDv7), `--source-type` (BSP tag), and
`BINFINITY_PASSPHRASE` (required). **Returns:** exit 0 + the summary above on
success; non-zero + stderr on failure.

### 5.2 Restore — pull bytes out, consume them

```bash
binfinity restore --id "$BACKUP_ID" --store "$STORE_SPEC" --out dump.bin
<your-import> < dump.bin
```

Restore verifies integrity (re-checks the Merkle root) before handing you bytes.
**Accepts:** `--out` (required), `--id` *or* `--manifest`, `--store`,
`BINFINITY_PASSPHRASE`, `--offline` (default true). **Returns:** the recovered
plaintext written to `--out`; exit non-zero on integrity failure or wrong
passphrase.

> **Break-glass DR:** `binfinity restore --offline` works with **no Binfinity
> services running** — just the store + your passphrase. Your data is never
> hostage to the platform being up.

### 5.3 Store specs (`--store` / `STORE_SPEC`)

| Spec | Backend |
|------|---------|
| `grpc://host:8090` | SMS data plane (the normal remote path) |
| `s3://bucket` | S3-compatible object storage directly |
| `local:/path` | local filesystem (dev / co-located) |

### 5.4 What "the stream" is

Binfinity treats your stream as **opaque bytes** — any file type works (text,
binary, compressed, media, already-encrypted-at-source). You choose the format;
your only contract is: **the bytes you emit on backup must reproduce the source
when fed back on restore.** Snapshot consistency (e.g. `--single-transaction`,
filesystem freeze, engine snapshot) is *your* responsibility and the main thing a
connector author must get right.

---

## 6. Validation, ordering & guarantees (read this)

Beyond per-field validation, the platform enforces behavioural rules your addon
must respect:

- **One operation per system at a time (state lock).** While a backup or restore
  is pending or dispatched for a system, the Console refuses to enqueue another —
  a manual trigger returns `409 Conflict`. The lock releases when you **report**
  the result. ⇒ *Always report promptly, even on failure, or the system stays
  stuck "busy".*
- **Idempotent results.** Reporting the same command twice, or reporting a command
  already terminal, is ignored. Safe to retry a failed report.
- **At-least-once, verified delivery (data plane).** Every chunk is acked and
  hash-verified; a backup is "complete" only when the Merkle root verifies.
  Interrupted backups resume (content-addressing means already-stored chunks are
  skipped). No silent truncation.
- **Backup id rules.** For a `backup` command, `backup_id` may be empty — **you**
  assign one (a stable, sortable id like `<data_type>-<RFC3339-ish>` is ideal).
  For a `restore`, `backup_id` is required and names an existing backup.
- **Connected window = 30s.** Miss heartbeats for 30s and the Console shows the
  system disconnected (commands still queue and are delivered when you return).
- **Zero-knowledge.** Commands and tokens never carry encryption keys. If you lose
  the passphrase, no one — including the platform operator — can restore. Manage
  it like the secret it is.

### 6.1 Who triggers what

You don't initiate backups; the **Console** does (manually, or via a per-system
schedule, or retention maintenance). Your addon's job is to be connected and to
execute+report. For reference, the operator-facing triggers are
`POST /cbs/api/v1/systems/{id}/backup` and `…/restore` (both `409` if the system
is busy) — these enqueue the commands you then poll for.

---

## 7. End-to-end lifecycle

```
addon boot
  └─ load credentials.json  ──(absent)──►  POST /ams/api/v1/enroll {setup_key}  → persist creds
  └─ POST /ams/auth/token {client_id, client_secret}  → bearer token (refresh on expiry)
  ├─ every 10s:  POST /cbs/.../heartbeat {system_id, data_type, activity}
  └─ every 5s:   GET  /cbs/.../commands?system=ID
                   ├─ 204 → loop
                   └─ 200 {type, backup_id} →
                        activity = backing-up | restoring
                        BACKUP:  <export> | binfinity backup --in - --id <id|assigned>
                        RESTORE: binfinity restore --id <id> --out tmp ; <import> < tmp
                        activity = idle
                        POST /cbs/.../commands/{id}/result {status, backup_id, bytes, error}
```

---

## 8. Building in another language

Any language with an HTTP client and the ability to exec a binary can implement
this. Follow §3–§5 verbatim:

1. Enroll (§3.1), persist credentials, reuse `client_id` as `system_id`.
2. Get + refresh a token (§3.2); send it as `Authorization: Bearer`.
3. Heartbeat loop (§4.1) reporting `activity`.
4. Poll loop (§4.2): on `backup`, run `<export> | binfinity backup …`; on
   `restore`, run `binfinity restore …` then `<import>`.
5. Report a result for every command (§4.3).
6. Bundle the `binfinity` binary + your source's client tools.

> **Going native (advanced).** Instead of the CLI you may implement the SMS gRPC
> `Storage` proto and the BSP format directly (`proto/binfinity/v1/storage.proto`,
> ADR-0003 (the BSP canonical stream format)). Only do this if you
> can't ship the binary — the CLI is the supported, conformance-tested path. In Go,
> implement the SDK's `DataPlane` interface instead of replacing the CLI wholesale.

---

## 9. Manifest & packaging

Declare what your addon handles in [`addon.yaml`](examples/mysql-client/addon.yaml):

```yaml
name: postgres-client
data_type: postgres            # Console tag + BSP source_type
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

- **Containerize** as a sidecar next to the source. Bundle `binfinity` + your
  runtime + the source's client tools (`pg_dump`, `tar`, …). See
  [`examples/mysql-client/Dockerfile`](examples/mysql-client/Dockerfile) (multi-stage: build the CLI
  from the monorepo + your agent, run on a base that has your tools).
- **Configure entirely by env.** No inbound ports — the edge only makes outbound
  HTTPS/gRPC, so it works behind NAT.

---

## 10. Conformance checklist

- [ ] `addon.yaml` with your `data_type` + `requires_env`.
- [ ] Enroll on first start; persist credentials `0600`; reuse `client_id` as `system_id`.
- [ ] Acquire + refresh an access token; send `Authorization: Bearer` on CBS calls.
- [ ] Heartbeat loop reports `activity` (`idle`/`backing-up`/`restoring`).
- [ ] Poll loop: `backup` → `<export> | binfinity backup`; `restore` → `binfinity restore` → `<import>`.
- [ ] **Report a result for every command** (releases the state lock).
- [ ] Source consistency on backup (transaction/snapshot/freeze).
- [ ] `BINFINITY_PASSPHRASE` set; **prove the round-trip**: back up → mutate the
      source → restore → state matches byte-for-byte.
- [ ] Dockerfile bundling `binfinity` + your tools; env-only config; no inbound ports.

---

## 11. Prior art — how established backup tools structure this, and what Binfinity borrows

Binfinity's design is a deliberate synthesis of patterns proven by mature backup
systems. Understanding them clarifies *why* the contract looks the way it does.

### Velero (Kubernetes) — pluggable, source-agnostic core
Velero keeps its core generic and pushes all provider/source specifics into
**plugins** (`ObjectStore`, `VolumeSnapshotter`, `BackupItemAction`/`RestoreItemAction`)
that run as **separate processes** over a gRPC plugin contract (HashiCorp
`go-plugin`), so a plugin crash can't take down the core and anyone can extend
backup/restore without forking the engine. **Binfinity borrows the core idea —**
a source-agnostic server plus a narrow, well-typed extension contract — but moves
the plugin *out of the server entirely* to the customer's edge (your addon), and
makes the contract HTTP/JSON + a CLI rather than an in-cluster gRPC plugin, so a
connector can be written in any language and run anywhere behind NAT.

### Bacula / Bareos — daemon split + outbound File Daemon
Bacula/Bareos separate the **Director** (orchestration/scheduling), **Storage
Daemon** (writes to media), and **File Daemon** (runs next to the data and reads
it), with a **File Daemon plugin API** for source-specific capture. Binfinity's
split mirrors this — **CBS** ≈ Director (schedules, drives jobs), **SMS** ≈ Storage
Daemon (the data plane), and **your addon** ≈ File Daemon (lives with the data).
The key modernization: Binfinity's edge **polls outbound over HTTPS** instead of
accepting an inbound connection, which is what makes it NAT/firewall-friendly for
SaaS, and encryption is **client-side** so the storage tier is zero-knowledge.

### restic / Kopia / Borg — content-addressed dedup & CDC
These popularized the storage model Binfinity uses on the data plane:
**content-defined chunking** (rolling-hash chunk boundaries) so shifted/modified
data still dedups, **content-addressed** chunks (a chunk's hash is its name) for
global dedup, client-side encryption, and **pluggable backends** (local, S3, …)
behind one repository abstraction. BSP is Binfinity's equivalent canonical format
(FastCDC + BLAKE3 + Merkle trailer + deterministic AEAD), and `--store`
(`grpc://`/`s3://`/`local:`) is the backend abstraction. The difference: restic et
al. are *single-tool* CLIs; Binfinity exposes this engine *through* the addon
contract so many heterogeneous sources share one deduplicated, multi-cloud store.

### Databasus / WAL-G / Barman / pgBackRest — central control vs. local agent
The PostgreSQL ecosystem shows the spectrum Binfinity sits on. **Databasus** is a
web-centralized, container-deployed tool that connects to the DB and takes
**logical** dumps (`pg_dump`, SELECT-only) on a schedule to S3-compatible
targets — close in spirit to a Binfinity MySQL/Postgres addon (logical export,
Console-driven schedule, object storage). **Barman** is a *central server* that
pulls/receives physical backups from many instances over SSH; **pgBackRest** needs
direct filesystem access to the data directory for block-level incrementals;
**WAL-G** is a cloud-first CLI driven by cron. Binfinity generalizes the
**Databasus-style logical-export + central-console** model beyond Postgres to *any*
source, while adding what those single-DB tools lack: one canonical format across
sources, cross-source dedup, zero-knowledge encryption, and a uniform edge contract.

**The synthesis:** Velero's source-agnostic extension contract + Bacula's
daemon/agent topology (made outbound and zero-knowledge) + restic's
content-addressed CDC storage + Databasus's console-driven logical-export
ergonomics — unified so that *one* addon contract makes *any* source a
first-class, deduplicated, multi-cloud, restorable citizen with **zero server
changes**.

> Sources: [Velero plugins](https://velero.io/docs/main/custom-plugins/) ·
> [Velero plugin Go pkg](https://pkg.go.dev/github.com/vmware-tanzu/velero/pkg/plugin/velero) ·
> [Bareos plugins](https://docs.bareos.org/bareos-20/TasksAndConcepts/Plugins.html) ·
> [Bacula FD Plugin API](https://www.bacula.org/15.0.x-manuals/en/developers/Bacula_FD_Plugin_API.html) ·
> [restic CDC foundation](https://restic.net/blog/2015-09-12/restic-foundation1-cdc/) ·
> [restic architecture (DeepWiki)](https://deepwiki.com/restic/restic) ·
> [Postgres tools compared (Databasus/WAL-G/pgBackRest/Barman)](https://dev.to/piteradyson/postgresql-backup-tools-comparison-databasus-wal-g-pgbackrest-and-barman-2kg).
