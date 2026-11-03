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
	// for another operation on its cluster to finish. A restore also waits that long for a CronJob it
	// holds to be suspended, so that the CronJob controller, which acts on its own cache, sees it.
	pendingRequeueInterval = 15 * time.Second
	// targetRequeueInterval paces an operation whose installation cannot run it as configured:
	// only a change to the installation can unblock it.
	targetRequeueInterval = time.Minute
	// runningRequeueInterval is a safety net under the Job watch, which reports progress itself.
	runningRequeueInterval = time.Minute
	// topologyRecheckInterval is a safety net under the installation watch: a schedule refreshes
	// the hosts of its CronJob this often, as a relisted watch can hide the completion of a
	// reconcile that changed them.
	topologyRecheckInterval = 10 * time.Minute
	// jobStartTimeout bounds how long a Job may go without a pod that started: a pod admission
	// refuses, or whose image cannot be pulled, would otherwise hold its operation, and every other
	// operation on its cluster, until the Job's active deadline.
	jobStartTimeout = 10 * time.Minute
	// jobNotStartedDeadlineSeconds is the active deadline a Job whose pod never started is stopped
	// with, the shortest one the API accepts.
	jobNotStartedDeadlineSeconds int64 = 1
	// startTimeoutRecheckDelay places the recheck of a run waiting for its pod past the run's start
	// timeout, so that the recheck finds the timeout expired rather than just short of it.
	startTimeoutRecheckDelay = time.Second
	// dueRecheckDelay places the recheck of a backup waiting for the time it is due, or for its starting
	// deadline, just past that time, so that the recheck finds it passed.
	dueRecheckDelay = time.Second
	// dueRecheckCap bounds the wait for the time a backup is due between two rechecks: a timer measures
	// the time that passes, and the wall clock may jump meanwhile.
	dueRecheckCap = time.Hour
)

const (
	// terminationMessageMaxLength bounds the part of a Job's termination message a status and an
	// event take over. kubelet keeps up to 4096 bytes, which may be the tail of the Job's log.
	terminationMessageMaxLength = 1024
	// terminationMessageHeadLength is how much of terminationMessageMaxLength a termination message
	// cut short keeps from its start; the rest comes from its end.
	terminationMessageHeadLength = 256
	// truncationMarker marks where a termination message was cut short.
	truncationMarker = "..."
)

// Kinds the status messages name.
const (
	kindJob     = "Job"
	kindCronJob = "CronJob"
)

// messageValidated is the message of a passed Validated condition.
const messageValidated = "spec and target are valid"

// staleOwnerMessage explains the wait for an object of the expected name that is going away: its
// controller, typically a deleted predecessor of the same name, is deleted, or the object itself is.
func staleOwnerMessage(kind, name string) string {
	return fmt.Sprintf("%s %s belongs to a deleted owner, or is deleted itself, and awaits garbage collection", kind, name)
}

// unknownOwner names, in a message, the owner of an object that tells no controller.
const unknownOwner = "another resource"

// nameConflictMessage explains why an operation cannot run on a cluster: the object of the name it needs
// there belongs to the named owner. The joined names of two resources and their clusters may coincide,
// as "a-b" with cluster "c" and "a" with cluster "b-c" do, and only renaming one of them parts them.
func nameConflictMessage(kind, name, owner string) string {
	return fmt.Sprintf("the %s name %s is taken by %s: the two names joined with their clusters coincide; rename one of them", kind, name, owner)
}

// orphanMessage explains why an operation cannot run on a cluster: no resource controls the object of the
// name it needs there, so no garbage collection ever removes it, and only deleting it makes way.
func orphanMessage(kind, name string) string {
	return fmt.Sprintf("the %s name %s is taken by a %s no resource controls, which no garbage collection removes: delete it",
		kind, name, kind)
}

// operationInProgressMessage explains the wait for another operation on a cluster of the installation.
func operationInProgressMessage(chiName, cluster string) string {
	return fmt.Sprintf("another backup or restore of cluster %s of ClickHouseInstallation %s is in progress", cluster, chiName)
}

// accessRestoreInProgressMessage explains the wait of a backup for a restore of the access entities of the
// installation, which they belong to as a whole, to finish.
func accessRestoreInProgressMessage(chiName, restore string) string {
	return fmt.Sprintf("ClickHouseRestore %s restores the access entities of ClickHouseInstallation %s, which no backup "+
		"may capture half restored", restore, chiName)
}

// operationLockClusterSeparator separates the installation from the cluster in the key of an
// operation lock: no namespace, installation or cluster name holds it.
const operationLockClusterSeparator = "/"
