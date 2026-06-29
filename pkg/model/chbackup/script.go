// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chbackup

import (
	"fmt"
	"strings"
)

// scriptPreamble defines the common shell helpers shared by backup and restore jobs.
// It builds optional auth flags from the CLICKHOUSE_USER/CLICKHOUSE_PASSWORD env vars
// (provided via an optional Secret) and a run_action helper that submits a command to
// the clickhouse-backup integration table (system.backup_actions) of the sidecar
// running inside the target host's pod and polls until it succeeds or fails.
const scriptPreamble = `set -euo pipefail
CH_PORT="${CLICKHOUSE_PORT:-9000}"
CH_AUTH=""
if [ -n "${CLICKHOUSE_USER:-}" ]; then CH_AUTH="--user=${CLICKHOUSE_USER}"; fi
if [ -n "${CLICKHOUSE_PASSWORD:-}" ]; then CH_AUTH="${CH_AUTH} --password=${CLICKHOUSE_PASSWORD}"; fi
MAX_POLLS="${MAX_POLLS:-1440}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"

run_action() {
  svc="$1"; cmd="$2"
  echo ">> ${svc}: ${cmd}"
  clickhouse-client --host="${svc}" --port="${CH_PORT}" ${CH_AUTH} \
    --query="INSERT INTO system.backup_actions(command) VALUES('${cmd}')"
  n=0
  while true; do
    n=$((n+1))
    status="$(clickhouse-client --host="${svc}" --port="${CH_PORT}" ${CH_AUTH} \
      --query="SELECT status FROM system.backup_actions WHERE command='${cmd}' ORDER BY start DESC LIMIT 1")"
    case "${status}" in
      success) echo "   done"; return 0 ;;
      error)
        err="$(clickhouse-client --host="${svc}" --port="${CH_PORT}" ${CH_AUTH} \
          --query="SELECT error FROM system.backup_actions WHERE command='${cmd}' ORDER BY start DESC LIMIT 1")"
        echo "   FAILED on ${svc}: ${err}" >&2; return 1 ;;
    esac
    if [ "${n}" -ge "${MAX_POLLS}" ]; then echo "   TIMEOUT waiting for '${cmd}' on ${svc}" >&2; return 1; fi
    sleep "${POLL_INTERVAL}"
  done
}
`

func shellList(services []string) string {
	return strings.Join(services, " ")
}

// BackupOpts collects the optional knobs for a create_remote backup.
type BackupOpts struct {
	SchemaOnly     bool
	Tables         string   // clickhouse-backup --tables pattern
	Partitions     []string // clickhouse-backup --partitions ids
	DiffFromRemote string   // base backup name for an incremental backup
	KeepLastRemote *int32   // retention: keep only the N most recent remote backups
}

func (o BackupOpts) createRemoteFlags() string {
	flags := ""
	if o.SchemaOnly {
		flags += "--schema "
	}
	if o.DiffFromRemote != "" {
		flags += "--diff-from-remote=" + o.DiffFromRemote + " "
	}
	if o.Tables != "" {
		flags += "--tables=" + o.Tables + " "
	}
	if len(o.Partitions) > 0 {
		flags += "--partitions=" + strings.Join(o.Partitions, ",") + " "
	}
	return flags
}

// BackupScript renders the shell script that triggers create_remote on each target host.
//
// backupNameExpr is the shell expression assigned to BACKUP_NAME. For one-off backups it
// is a quoted literal (e.g. "my-backup"); for scheduled backups it is a runtime expression
// (e.g. "${PREFIX}$(date -u +%Y%m%d-%H%M%S)") so each run gets a unique name.
//
// When KeepLastRemote is set, a best-effort retention step prunes remote backups beyond the
// N most recent (via system.backup_list + a `delete remote` action); retention failures never
// fail the backup itself.
func BackupScript(services []string, backupNameExpr string, opts BackupOpts) string {
	flags := opts.createRemoteFlags()
	var b strings.Builder
	b.WriteString(scriptPreamble)
	b.WriteString(fmt.Sprintf("\nBACKUP_NAME=%s\n", backupNameExpr))
	b.WriteString(fmt.Sprintf("SERVICES=%q\n", shellList(services)))
	b.WriteString(fmt.Sprintf("for svc in ${SERVICES}; do run_action \"${svc}\" \"create_remote %s${BACKUP_NAME}\"; done\n", flags))
	b.WriteString("echo \"Backup ${BACKUP_NAME} completed on all shards.\"\n")

	if opts.KeepLastRemote != nil {
		// Remote storage is shared per shard; prune from the first targeted host. Best-effort.
		b.WriteString(fmt.Sprintf("\necho \"Retention: keeping last %d remote backups\"\n", *opts.KeepLastRemote))
		b.WriteString("RETAIN_SVC=\"$(echo ${SERVICES} | awk '{print $1}')\"\n")
		b.WriteString(fmt.Sprintf("OLD_BACKUPS=\"$(clickhouse-client --host=\"${RETAIN_SVC}\" --port=\"${CH_PORT}\" ${CH_AUTH} --query=\"SELECT name FROM system.backup_list WHERE location='remote' ORDER BY created DESC LIMIT 1000000 OFFSET %d\" 2>/dev/null || true)\"\n", *opts.KeepLastRemote))
		// Serialize the deletes via run_action: the sidecar runs one action at a time, so
		// firing them all at once yields "another operation is currently running". Tolerant:
		// a failed prune (e.g. an object store that rejects the delete) never fails the backup.
		b.WriteString("for old in ${OLD_BACKUPS}; do echo \"  pruning ${old}\"; run_action \"${RETAIN_SVC}\" \"delete remote ${old}\" || true; done\n")
	}
	return b.String()
}

// VerifyScript renders a best-effort verification: download the remote backup to each
// shard-first host, confirm it materializes locally (system.backup_list), then drop the
// local copy. It touches no cluster data; it only proves the remote backup is pullable.
func VerifyScript(services []string, backupName string) string {
	var b strings.Builder
	b.WriteString(scriptPreamble)
	b.WriteString(fmt.Sprintf("\nBACKUP_NAME=%q\n", backupName))
	b.WriteString(fmt.Sprintf("SERVICES=%q\n", shellList(services)))
	b.WriteString("for svc in ${SERVICES}; do\n")
	b.WriteString("  echo \">> verifying ${BACKUP_NAME} on ${svc}\"\n")
	// create_remote leaves a local copy; drop it so the download genuinely pulls from remote.
	b.WriteString("  clickhouse-client --host=\"${svc}\" --port=\"${CH_PORT}\" ${CH_AUTH} --query=\"INSERT INTO system.backup_actions(command) VALUES('delete local ${BACKUP_NAME}')\" || true\n")
	b.WriteString("  for i in $(seq 1 30); do lc=\"$(clickhouse-client --host=\"${svc}\" --port=\"${CH_PORT}\" ${CH_AUTH} --query=\"SELECT count() FROM system.backup_list WHERE name='${BACKUP_NAME}' AND location='local'\")\"; [ \"${lc}\" = \"0\" ] && break; sleep 2; done\n")
	b.WriteString("  run_action \"${svc}\" \"download ${BACKUP_NAME}\"\n")
	b.WriteString("  cnt=\"$(clickhouse-client --host=\"${svc}\" --port=\"${CH_PORT}\" ${CH_AUTH} --query=\"SELECT count() FROM system.backup_list WHERE name='${BACKUP_NAME}' AND location='local'\")\"\n")
	b.WriteString("  if [ \"${cnt}\" -lt 1 ]; then echo \"verify failed: ${BACKUP_NAME} not present locally on ${svc} after download\" >&2; exit 1; fi\n")
	b.WriteString("  clickhouse-client --host=\"${svc}\" --port=\"${CH_PORT}\" ${CH_AUTH} --query=\"INSERT INTO system.backup_actions(command) VALUES('delete local ${BACKUP_NAME}')\" || true\n")
	b.WriteString("done\n")
	b.WriteString("echo \"Backup ${BACKUP_NAME} verified.\"\n")
	return b.String()
}

// RestoreScript renders the shell script that restores a remote backup.
//
// Before touching any data it runs preflight safety checks:
//   - topology check (when validateTopology is true): every schema host must be reachable,
//     ensuring the full target cluster is up before a ReplicatedMergeTree restore;
//   - overwrite guard (when overwrite is false): refuses if any target data host already
//     holds user tables, preventing accidental data loss.
//
// Schema and data are then restored on the first replica of each shard. For Replicated*
// tables the sidecar must set restore_schema_on_cluster so the schema CREATE is issued
// ON CLUSTER from that node and reaches every replica with an identical Keeper path;
// native replication then clones the data to the remaining replicas. When overwrite is
// true, existing tables are dropped first via clickhouse-backup's --rm.
func RestoreScript(schemaServices, dataServices []string, backupName string, schemaOnly, overwrite, validateTopology bool) string {
	rm := ""
	if overwrite {
		rm = "--rm "
	}
	var b strings.Builder
	b.WriteString(scriptPreamble)
	b.WriteString(fmt.Sprintf("\nBACKUP_NAME=%q\n", backupName))
	b.WriteString(fmt.Sprintf("SCHEMA_SERVICES=%q\n", shellList(schemaServices)))
	b.WriteString(fmt.Sprintf("DATA_SERVICES=%q\n", shellList(dataServices)))

	if validateTopology {
		b.WriteString("echo \"Preflight: validating target topology is reachable...\"\n")
		b.WriteString("for svc in ${SCHEMA_SERVICES}; do\n")
		b.WriteString("  clickhouse-client --host=\"${svc}\" --port=\"${CH_PORT}\" ${CH_AUTH} --query=\"SELECT 1\" >/dev/null \\\n")
		b.WriteString("    || { echo \"topology check failed: host ${svc} is not reachable\" >&2; exit 1; }\n")
		b.WriteString("done\n")
	}
	if !overwrite {
		b.WriteString("echo \"Preflight: overwrite guard (refuse non-empty target)...\"\n")
		b.WriteString("for svc in ${DATA_SERVICES}; do\n")
		b.WriteString("  cnt=\"$(clickhouse-client --host=\"${svc}\" --port=\"${CH_PORT}\" ${CH_AUTH} --query=\"SELECT count() FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') AND is_temporary=0\")\"\n")
		b.WriteString("  if [ \"${cnt}\" -gt 0 ]; then echo \"refusing restore: target ${svc} already has ${cnt} user table(s); set spec.overwrite=true to proceed\" >&2; exit 1; fi\n")
		b.WriteString("done\n")
	}

	b.WriteString("echo \"Restoring schema...\"\n")
	b.WriteString(fmt.Sprintf("for svc in ${SCHEMA_SERVICES}; do run_action \"${svc}\" \"restore_remote --schema %s${BACKUP_NAME}\"; done\n", rm))
	if !schemaOnly {
		b.WriteString("echo \"Restoring data...\"\n")
		b.WriteString("for svc in ${DATA_SERVICES}; do run_action \"${svc}\" \"restore_remote --data ${BACKUP_NAME}\"; done\n")
	}
	b.WriteString("echo \"Restore ${BACKUP_NAME} completed.\"\n")
	return b.String()
}
