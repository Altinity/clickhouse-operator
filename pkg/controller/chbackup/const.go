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
	pendingRequeueInterval = 15 * time.Second
	// targetRequeueInterval paces an operation whose installation cannot run it as configured:
	// only a change to the installation can unblock it.
	targetRequeueInterval   = time.Minute
	runningRequeueInterval  = 30 * time.Second
	topologyRecheckInterval = 10 * time.Minute
	// dueRecheckDelay places the recheck of a backup waiting for its scheduled time, or for its starting
	// deadline, just past that time, so that the recheck finds it passed.
	dueRecheckDelay = time.Second
	// dueRecheckCap bounds the wait for the scheduled time of a backup between two rechecks: a timer measures
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

// operationInProgressMessage explains the wait for another operation on a cluster of the installation.
func operationInProgressMessage(chiName, cluster string) string {
	return fmt.Sprintf("another backup or restore of cluster %s of ClickHouseInstallation %s is in progress", cluster, chiName)
}

func accessRestoreInProgressMessage(chiName, cluster, restore string) string {
	return fmt.Sprintf("backup of cluster %s of ClickHouseInstallation %s waits for ClickHouseRestore %s, which restores "+
		"the access entities of cluster %s, which no backup may capture half restored", cluster, chiName, restore, cluster)
}

// operationLockClusterSeparator separates the installation from the cluster in the key of an
// operation lock: no namespace, installation or cluster name holds it.
const operationLockClusterSeparator = "/"

const (
	reasonScheduledBackupSucceeded = "ScheduledBackupSucceeded"
	reasonScheduledBackupFailed    = "ScheduledBackupFailed"
	reasonScheduledBackupStarted   = "ScheduledBackupStarted"
	reasonScheduledRunSkipped      = "ScheduledRunSkipped"
	reasonScheduleActive           = "ScheduleActive"
	reasonScheduleSuspended        = "ScheduleSuspended"
	reasonScheduleAborted          = "ScheduleAborted"
	reasonDeletionRemoteBackupKept = "DeletionRemoteBackupKept"
	reasonDeletionStoppingRuns     = "DeletionStoppingRuns"
	reasonDeletionCleanup          = "DeletionCleanup"
	reasonDeletionCleanedUp        = "DeletionCleanedUp"
	reasonDeletionLeftover         = "DeletionLeftover"
	reasonDeletionWaitingForRun    = "DeletionWaitingForRun"
	reasonDeletionRunFinished      = "DeletionRunFinished"
	reasonTaskIDRerun              = "TaskIDRerun"
	reasonTaskIDReset              = "TaskIDReset"
	reasonTaskIDIgnored            = "TaskIDIgnored"
)

func abortReason(reason string) bool {
	switch reason {
	case api.BackupReasonFailed, api.BackupReasonFailedUnchanged, api.BackupReasonPartiallyCompleted:
		return false
	}
	return true
}

func failurePhase(reason string) api.OperationPhase {
	if abortReason(reason) {
		return api.OperationPhaseAborted
	}
	return api.OperationPhaseFailed
}

func failedPhase(phase api.OperationPhase) bool {
	return (phase == api.OperationPhaseFailed) || (phase == api.OperationPhaseAborted)
}
