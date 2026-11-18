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
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	// componentName names the operator binary as the source on Kubernetes Events, matching the
	// CHI and CHK controllers so `kubectl get events` shows one component for all kinds.
	componentName = "clickhouse-operator"
	// eventGenerateName prefixes the names of the Events the backup controllers emit.
	eventGenerateName = "chop-backup-"
)

const (
	// pendingRequeueInterval paces an operation waiting for its installation to become ready or
	// for another operation on its cluster to finish.
	pendingRequeueInterval = 15 * time.Second
	// targetRequeueInterval paces an operation whose installation cannot run it as configured:
	// only a change to the installation can unblock it.
	targetRequeueInterval = time.Minute
	// runningRequeueInterval is a safety net under the notifications of the runs, which report progress
	// themselves.
	runningRequeueInterval = 30 * time.Second
	// topologyRecheckInterval is a safety net under the installation watch.
	topologyRecheckInterval = 10 * time.Minute
	// dueRecheckDelay places the recheck of a backup waiting for the time it is due, or for its starting
	// deadline, just past that time, so that the recheck finds it passed.
	dueRecheckDelay = time.Second
	// dueRecheckCap bounds the wait for the time a backup is due between two rechecks: a timer measures
	// the time that passes, and the wall clock may jump meanwhile.
	dueRecheckCap = time.Hour
)

// messageValidated is the message of a passed Validated condition.
const messageValidated = "spec and target are valid"

// staleOwnerMessage explains the wait for an object of the expected name that is going away: its
// controller, typically a deleted predecessor of the same name, is deleted, or the object itself is.
func staleOwnerMessage(kind, name string) string {
	return fmt.Sprintf("%s %s belongs to a deleted owner, or is deleted itself, and awaits garbage collection", kind, name)
}

// nameConflictMessage explains why a recovery cannot run on a cluster: the object of the name it needs there belongs
// to the named owner.
func nameConflictMessage(kind, name, owner string) string {
	return fmt.Sprintf("the %s name %s is taken by %s: the two names joined with their clusters coincide; rename one of them", kind, name, owner)
}

// operationInProgressMessage explains the wait for another operation on a cluster of the installation.
func operationInProgressMessage(chiName, cluster string) string {
	return fmt.Sprintf("another backup or restore of cluster %s of ClickHouseInstallation %s is in progress", cluster, chiName)
}

// accessRestoreInProgressMessage explains the wait of a backup of a cluster for a restore of the access entities of
// that cluster to finish.
func accessRestoreInProgressMessage(chiName, cluster, restore string) string {
	return fmt.Sprintf("backup of cluster %s of ClickHouseInstallation %s waits for ClickHouseRestore %s, which restores "+
		"the access entities of cluster %s, which no backup may capture half restored", cluster, chiName, restore, cluster)
}

// operationLockClusterSeparator separates the installation from the cluster in the key of an
// operation lock: no namespace, installation or cluster name holds it.
const operationLockClusterSeparator = "/"

// Reasons of the backup custom resources the API does not define: the status and the events of the controllers
// tell them.
const (
	// reasonScheduledBackupSucceeded and reasonScheduledBackupFailed tell how a scheduled run went, on the
	// schedule.
	reasonScheduledBackupSucceeded = "ScheduledBackupSucceeded"
	reasonScheduledBackupFailed    = "ScheduledBackupFailed"
	// reasonScheduledBackupStarted tells that a run of a schedule started on a cluster, and reasonScheduledRunSkipped
	// that a slot was skipped.
	reasonScheduledBackupStarted = "ScheduledBackupStarted"
	reasonScheduledRunSkipped    = "ScheduledRunSkipped"
	// reasonScheduleActive, reasonScheduleSuspended tell the transitions of the phase of a schedule.
	reasonScheduleActive    = "ScheduleActive"
	reasonScheduleSuspended = "ScheduleSuspended"
	reasonScheduleAborted   = "ScheduleAborted"
	// reasonDeletionRemoteBackupKept tells that the deletion of a backup that completed keeps its remote backup.
	reasonDeletionRemoteBackupKept = "DeletionRemoteBackupKept"
	// reasonDeletionStoppingRuns tells that the runs of a backup deleted unfinished stop, reasonDeletionCleanup that the
	// commands of it are killed and what it left is deleted on a cluster, reasonDeletionCleanedUp that this is done, and
	// reasonDeletionLeftover, a warning, what is left, with how to delete it by hand.
	reasonDeletionStoppingRuns = "DeletionStoppingRuns"
	reasonDeletionCleanup      = "DeletionCleanup"
	reasonDeletionCleanedUp    = "DeletionCleanedUp"
	reasonDeletionLeftover     = "DeletionLeftover"
	// reasonDeletionWaitingForRun tells that a restore or a schedule deleted while a run of it is in progress waits for
	// the run to finish before it goes, and reasonDeletionRunFinished that the run finished and the resource goes.
	reasonDeletionWaitingForRun = "DeletionWaitingForRun"
	reasonDeletionRunFinished   = "DeletionRunFinished"
	// reasonTaskIDRerun tells that a new spec.taskID reran a backup or a restore that finished, reasonTaskIDReset that it
	// reset a schedule that was aborted, and reasonTaskIDIgnored, a warning, that it reran nothing: the backup or the
	// restore has not finished, the restore completed, or the backup it would make again bears the name of one it made.
	reasonTaskIDRerun   = "TaskIDRerun"
	reasonTaskIDReset   = "TaskIDReset"
	reasonTaskIDIgnored = "TaskIDIgnored"
)

// abortReason reports whether a failure with the given reason is the operator stopping an operation, or never
// starting it, rather than the operation failing: the phase is then Aborted, not Failed.
func abortReason(reason string) bool {
	switch reason {
	case api.BackupReasonFailed, api.BackupReasonFailedUnchanged, api.BackupReasonPartiallyCompleted:
		return false
	}
	return true
}

// failurePhase returns the phase of an operation, or of a cluster of it, that failed for the given reason.
func failurePhase(reason string) api.OperationPhase {
	if abortReason(reason) {
		return api.OperationPhaseAborted
	}
	return api.OperationPhaseFailed
}

// failedPhase reports whether a phase is that of an operation that did not succeed: Failed or Aborted.
func failedPhase(phase api.OperationPhase) bool {
	return (phase == api.OperationPhaseFailed) || (phase == api.OperationPhaseAborted)
}
