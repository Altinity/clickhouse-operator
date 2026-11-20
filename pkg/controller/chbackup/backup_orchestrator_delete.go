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
	"context"
	"fmt"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// The deletion of a backup that did not complete: a run of its own, on every cluster the backup ran on, that kills the
// commands of the backup still running and deletes what the backup left, local and remote.
const (
	// operationDelete is the operation of the run that deletes what a backup that did not complete left
	operationDelete = "delete"
	// commandKill kills the command of clickhouse-backup it names, which it takes quoted as a single argument
	commandKill = "kill"
	stepKill    = "kill"
	stepDelete  = "delete"
	// planGiveUp tells a deletion to try every host once, never waiting for one that does not answer: the installation
	// is going away, or a host did not answer for the unreachable timeout already
	planGiveUp = "giveUp"
)

// deleteUnfinished kills the commands of the backup still running on the hosts of the run and deletes the local copy
// of the backup on each, and its remote copy once per shard. Every step is bounded: a host that does not answer is
// retried for the unreachable timeout, after which every host left is tried once, a kill or a delete is tried once and
// its row read errorResponsesMax times at most, never waiting for a sidecar that runs another command. What it cannot
// delete is told in a Warning, with how to delete it by hand, and fails the run, whose message tells it.
func (o *orchestrator) deleteUnfinished(ctx context.Context) error {
	o.mu.Lock()
	hosts := slices.Clone(o.status.Hosts)
	o.mu.Unlock()
	o.bestEffort = o.plan(planGiveUp) != ""
	defer func() { o.bestEffort = false }()
	deletedRemote := map[string]bool{}
	var left []string
	for _, host := range hosts {
		if host.Backup == "" {
			continue
		}
		unreachable, err := o.deleteFrom(ctx, host.Host, host.Backup, deletedRemote)
		if err == nil {
			continue
		}
		left = append(left, fmt.Sprintf("%s=%s (%v)", host.Host, host.Backup, err))
		o.eventf(core.EventTypeWarning, "OrchestratorDeletionLeft", "%s: what the unfinished backup %s left there is left: %v; %s",
			host.Host, host.Backup, err, deleteBackupByHand(host.Backup))
		if unreachable && !o.bestEffort {
			o.logf("%s does not answer: every host left is tried once", host.Host)
			o.bestEffort = true
		}
	}
	if len(left) > 0 {
		return failf("left on %s", strings.Join(left, "; "))
	}
	return nil
}

// deleteFrom kills the commands of the backup running on the host and deletes its local copy there, and its remote
// copy unless another replica of the shard deleted it. It returns why something is left, and whether that is as the
// host does not answer.
func (o *orchestrator) deleteFrom(ctx context.Context, host, name string, deletedRemote map[string]bool) (bool, error) {
	var rows []actionRow
	if err := o.retry(ctx, host, "read the commands in progress", func() (err error) {
		rows, err = o.sc.Running(ctx, host)
		return err
	}); err != nil {
		return true, err
	}
	for _, row := range rows {
		// A command of another backup, or of anything else, is never killed: the delete that follows is refused then
		if !row.running() || !namesBackup(row.Command, name) {
			continue
		}
		if err := o.kill(ctx, host, row.Command); err != nil {
			return false, err
		}
	}

	local, err := o.listed(ctx, host, "local", name)
	if err != nil {
		return true, err
	}
	if local {
		if err := o.cleanupCommand(ctx, stepDelete, host, commandDeleteLocal+" "+name); err != nil {
			return false, err
		}
		o.eventf(core.EventTypeNormal, "OrchestratorDeletionDeleted", "%s: the local copy of the unfinished backup %s deleted", host, name)
	}

	shard := o.shardOf(host) + "|" + name
	if deletedRemote[shard] {
		return false, nil
	}
	remote, err := o.listed(ctx, host, "remote", name)
	if err != nil {
		return true, err
	}
	if remote {
		if err := o.cleanupCommand(ctx, stepDelete, host, commandDeleteRemote+" "+name); err != nil {
			return false, err
		}
		o.eventf(core.EventTypeNormal, "OrchestratorDeletionDeleted", "%s: the remote copy of the unfinished backup %s deleted", host, name)
	}
	deletedRemote[shard] = true
	return false, nil
}

// kill kills a command of the backup the sidecar of the host runs, and waits, errorResponsesMax polls at most, for it to
// stop.
func (o *orchestrator) kill(ctx context.Context, host, command string) error {
	kill := killCommand(command)
	timeFrom, err := o.sc.Now(ctx, host)
	if err != nil {
		return fmt.Errorf("cannot read the time of ClickHouse: %s", describeQueryError(err))
	}
	o.record(api.BackupRunAction{Step: stepKill, Host: host, Command: kill, TimeFrom: timeFrom})
	o.logf("%s: %s", host, kill)
	o.eventf(core.EventTypeNormal, "OrchestratorCommand", "%s: %s", host, kill)
	pushErr := o.sc.Push(ctx, host, kill, o.httpReceiveTimeout())
	polls := max(o.errorResponsesMax(), 1)
	for attempt := 0; attempt < polls; attempt++ {
		if (attempt > 0) && !o.sleep(ctx) {
			return ctx.Err()
		}
		rows, err := o.sc.Running(ctx, host)
		if err != nil {
			continue
		}
		if !slices.ContainsFunc(rows, func(row actionRow) bool { return row.running() && (row.Command == command) }) {
			o.eventf(core.EventTypeNormal, "OrchestratorDeletionKilled", "%s: %s killed", host, command)
			return nil
		}
	}
	if pushErr != nil {
		return fmt.Errorf("%s: %s", kill, describeQueryError(pushErr))
	}
	return fmt.Errorf("%s still runs %d polls after it was killed", command, polls)
}

// killCommand returns the command that kills the command of clickhouse-backup, which the sidecar splits as a shell
// does: the command to kill goes quoted, as one argument.
func killCommand(command string) string {
	return commandKill + ` "` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(command) + `"`
}

// namesBackup reports whether the command, of system.backup_actions, works on the backup: one of its arguments is the
// name of the backup.
func namesBackup(command, name string) bool {
	fields := strings.Fields(command)
	return (len(fields) > 1) && slices.Contains(fields[1:], name)
}

// deleteBackupByHand tells how to delete a backup, local and remote, by hand on a host.
func deleteBackupByHand(name string) string {
	return fmt.Sprintf("delete it by hand with %s and INSERT INTO system.backup_actions(command) VALUES ('%s %s')",
		deleteByHand(name), commandDeleteRemote, name)
}
