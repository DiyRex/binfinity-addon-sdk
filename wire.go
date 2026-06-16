package binfinity

// This file is the on-the-wire vocabulary of the universal edge contract. These
// types mirror the server side exactly (services/cbs/internal/agents and
// services/ams/internal/enroll). They are the *entire* data model an addon
// exchanges with Binfinity — three control signals plus the enrollment reply.
// Keep the JSON tags byte-identical to the server; the contract is the API.

// Credentials are what an addon receives from AMS when it redeems a setup key
// and persists for the rest of its life. ClientID doubles as the system_id used
// on every later control call. Secret is returned exactly once at enrollment.
//
// Wire source: AMS POST /ams/api/v1/enroll → iam.Client.
type Credentials struct {
	InternalID string   `json:"internal_id"`
	ClientID   string   `json:"client_id"`
	Secret     string   `json:"secret"`
	Name       string   `json:"name"`
	TenantID   string   `json:"tenant_id"`
	Roles      []string `json:"roles"`
	EnrolledAt string   `json:"enrolled_at,omitempty"` // stamped by the SDK on persist
}

// Heartbeat is the periodic liveness + state signal the edge POSTs to CBS. It is
// what makes a system show as "connected" in the Console and animates its live
// data-flow map. Activity is one of "idle" | "backing-up" | "restoring".
//
// Wire sink: CBS POST /cbs/api/v1/agent/heartbeat (200 → {"status":"ok"};
// 400 if system_id is empty).
type Heartbeat struct {
	SystemID string `json:"system_id"`
	Name     string `json:"name"`
	DataType string `json:"data_type"`
	Activity string `json:"activity"`

	// Live progress, sent only while Activity != "idle" (all optional). BytesDone
	// is exact source bytes processed; BytesTotal is the connector's ESTIMATE of
	// total source size (0 = unknown); StartedAt (RFC3339) is when the job began.
	// The Console derives elapsed / throughput / ~% / ~ETA from these.
	StartedAt  string `json:"started_at,omitempty"`
	BytesDone  int64  `json:"bytes_done,omitempty"`
	BytesTotal int64  `json:"bytes_total,omitempty"`
}

// Command is the single inbound control signal: the Console telling this system
// to back up or restore. The edge obtains it by POLLING (outbound, NAT-friendly)
// — Binfinity never dials in. A Command NEVER carries a passphrase or key:
// encryption is client-side (zero-knowledge); the edge holds its own key
// material. For a backup, BackupID may be empty (the edge assigns one); for a
// restore, BackupID names the backup to restore.
//
// Wire source: CBS GET /cbs/api/v1/agent/commands?system=<id>
// (200 → Command; 204 → nothing to do; 400 if the system param is missing).
type Command struct {
	ID       string `json:"id"`
	SystemID string `json:"system_id"`
	Type     string `json:"type"` // "backup" | "restore"
	BackupID string `json:"backup_id"`
	Status   string `json:"status"`
	Created  string `json:"created_at"`

	// Source-incremental (ADR-0009 Layer B; all optional, backward-compatible).
	// For a backup: Mode "incremental" + a prior Cursor asks an IncrementalConnector
	// to emit only the delta since Cursor (relative to BaseBackupID). For a restore:
	// Chain is the ordered backup ids to apply (base first, then deltas) so the
	// connector reconstructs point-in-time; empty Chain = single, self-contained
	// restore (Layer A).
	Mode         string   `json:"mode,omitempty"`           // "" | "full" | "incremental"
	BaseBackupID string   `json:"base_backup_id,omitempty"` // base full this delta builds on
	Cursor       string   `json:"cursor,omitempty"`         // opaque since-point from the prior backup
	Chain        []string `json:"chain,omitempty"`          // restore: ordered ids (base→…→target)
}

// Command type constants — the only two values Command.Type ever takes.
const (
	CmdBackup  = "backup"
	CmdRestore = "restore"
)

// Result is the single outbound signal the edge POSTs after running a Command.
// A successful backup Result becomes a durable catalog entry (the system's
// backup list). Status is "done" | "failed"; Bytes is the ciphertext size the
// data plane reported (informational); Error is set only on failure.
//
// Wire sink: CBS POST /cbs/api/v1/agent/commands/{id}/result?system=<id>
// (200 → {"status":"recorded"}).
type Result struct {
	Status   string `json:"status"`
	BackupID string `json:"backup_id"`
	Bytes    int64  `json:"bytes"`
	Error    string `json:"error,omitempty"`
	// Cursor is the new opaque since-point an IncrementalConnector produced for
	// this backup (binlog pos / WAL LSN / snapshot id). CBS persists it per system
	// and hands it back as Command.Cursor on the next incremental backup (ADR-0009).
	Cursor string `json:"cursor,omitempty"`
}

// Activity values reported in Heartbeat.Activity.
const (
	ActivityIdle      = "idle"
	ActivityBackingUp = "backing-up"
	ActivityRestoring = "restoring"
)

// statusDone / statusFailed are the two Result.Status values.
const (
	statusDone   = "done"
	statusFailed = "failed"
)
