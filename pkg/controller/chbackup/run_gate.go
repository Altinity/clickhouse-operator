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
	"errors"
	"slices"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// startGate holds the start of an operation on a cluster while check tells why it waits. A gate that reads the state
// of other clusters of the installation names them, so that it runs under their locks as well as under the one of the
// cluster the operation runs on.
type startGate struct {
	// clusters are the clusters of the installation the gate reads, whose locks it runs under too
	clusters []string
	// sources are the clusters of another installation the gate reads, named by operationLockKey, whose locks it
	// runs under too
	sources []string
	check   func(context.Context) (*pending, error)
}

func (g *startGate) lockedClusters() []string {
	if g == nil {
		return nil
	}
	return g.clusters
}

func (g *startGate) lockedSources() []string {
	if g == nil {
		return nil
	}
	return g.sources
}

// runKeyOf returns the key of the run of the operation of the resource on the cluster.
func runKeyOf(owner client.Object, kind, cluster, operation string) runKey {
	return runKey{kind: kind, namespace: owner.GetNamespace(), name: owner.GetName(), uid: owner.GetUID(), cluster: cluster,
		operation: operation}
}

// startRun starts the run of an operation on a cluster of owner's installation unless gate tells why the operation
// waits, or another operation on that cluster is in progress, and returns it, nil when it waits, with why.
//
// Two operations on one cluster must never overlap: a restore drops what a backup reads, and both drive the same
// sidecars. The gate, the check and the start run under the cluster's lock, and the locks of the clusters the gate
// reads, so no concurrent reconcile in this operator can slip in between.
func (r *reconciler) startRun(
	ctx context.Context,
	owner client.Object,
	chiName string,
	key runKey,
	gate *startGate,
	spec func() (*model.RunSpec, error),
) (*run, *pending, error) {
	unlock := lockKeys(append(operationLockKeys(owner.GetNamespace(), chiName, append([]string{key.cluster}, gate.lockedClusters()...)...),
		gate.lockedSources()...)...)
	defer unlock()

	if gate != nil {
		if blocked, err := gate.check(ctx); (err != nil) || (blocked != nil) {
			return nil, blocked, err
		}
	}
	busy, err := r.operationInProgress(ctx, owner.GetNamespace(), chiName, key.cluster, key)
	if err != nil {
		return nil, nil, err
	}
	if busy {
		return nil, &pending{
			reason:  api.BackupReasonOperationInProgress,
			message: operationInProgressMessage(chiName, key.cluster),
			requeue: pendingRequeueInterval,
		}, nil
	}
	runSpec, err := spec()
	if err != nil {
		return nil, nil, err
	}
	started, err := runs.start(key, chiName, owner, runSpec, nil, r.clock.Now(), 0, r.postRunEvent)
	if errors.Is(err, errNoClickHouseAccess) {
		return nil, noClickHouseAccess(), nil
	}
	return started, nil, err
}

// noClickHouseAccess explains why no operation starts: the operator has no ClickHouse credentials to drive the
// sidecars with.
func noClickHouseAccess() *pending {
	return &pending{
		reason: api.BackupReasonNoClickHouseAccess,
		message: "the operator drives clickhouse-backup through SQL with the ClickHouse credentials of its configuration, " +
			"clickhouse.access, which sets none: set clickhouse.access.username and password, or clickhouse.access.secret",
		requeue: targetRequeueInterval,
	}
}

// resumeRun returns the run of an operation the status of a cluster records running, resumed from that status after
// a restart of the operator; nil when its spec cannot be built any more, with why.
func (r *reconciler) resumeRun(
	owner client.Object,
	chiName string,
	key runKey,
	cluster *api.OperationClusterStatus,
	spec func() (*model.RunSpec, error),
) (*run, error) {
	if existing := runs.get(key); existing != nil {
		return existing, nil
	}
	runSpec, err := spec()
	if err != nil {
		return nil, err
	}
	startedAt := r.clock.Now()
	if cluster.StartTime != nil {
		startedAt = cluster.StartTime.Time
	}
	log.V(1).M(r.meta(owner)).F().Info("resuming %s of cluster %s at step %q", key.operation, key.cluster, cluster.Run.Step)
	return runs.start(key, chiName, owner, runSpec, cluster.Run, startedAt, r.clock.Since(startedAt), r.postRunEvent)
}

// postRunEvent posts an Event of a run on the resource it works for.
func (r *reconciler) postRunEvent(obj client.Object, eventType, reason, message string) {
	if eventType == core.EventTypeWarning {
		r.events.EventWarning(obj, a.EventActionReconcile, reason, message)
		return
	}
	r.events.EventInfo(obj, a.EventActionReconcile, reason, message)
}

// operationInProgress reports whether any operation other than the one of except runs on a cluster of the
// installation: a run of this operator, or one the status of a resource records running, which the operator resumes
// after a restart.
func (r *reconciler) operationInProgress(ctx context.Context, namespace, chiName, cluster string, except runKey) (bool, error) {
	if runs.busy(namespace, chiName, cluster, except) {
		return true, nil
	}
	owned := func(obj client.Object, kind string) bool {
		return (except.kind == kind) && (except.uid == obj.GetUID())
	}
	running := func(statuses []api.OperationClusterStatus) bool {
		return slices.ContainsFunc(statuses, func(status api.OperationClusterStatus) bool {
			return (status.Name == cluster) && (status.Run != nil) && (status.Phase == api.OperationPhaseRunning)
		})
	}
	backups := &api.ClickHouseBackupList{}
	if err := r.apiReader.List(ctx, backups, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range backups.Items {
		chb := &backups.Items[i]
		if (chb.TargetInstallation() == chiName) && !owned(chb, api.ClickHouseBackupCRDResourceKind) && running(chb.Status.Clusters) {
			return true, nil
		}
	}
	restores := &api.ClickHouseRestoreList{}
	if err := r.apiReader.List(ctx, restores, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range restores.Items {
		chr := &restores.Items[i]
		if (chr.Spec.ClickHouseInstallation == chiName) && !owned(chr, api.ClickHouseRestoreCRDResourceKind) && running(chr.Status.Clusters) {
			return true, nil
		}
	}
	schedules := &api.ClickHouseBackupScheduleList{}
	if err := r.apiReader.List(ctx, schedules, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range schedules.Items {
		chbs := &schedules.Items[i]
		if (chbs.Spec.ClickHouseInstallation != chiName) || owned(chbs, api.ClickHouseBackupScheduleCRDResourceKind) {
			continue
		}
		for _, active := range chbs.Status.Active {
			if (active.Name == cluster) && (active.Run != nil) && !active.Phase.IsTerminal() {
				return true, nil
			}
		}
	}
	return false, nil
}

// scheduledRunActive reports whether a run of a schedule of the installation works on the cluster.
func (r *reconciler) scheduledRunActive(ctx context.Context, namespace, chiName, cluster string) (bool, error) {
	for _, key := range runs.keys() {
		if (key.kind == api.ClickHouseBackupScheduleCRDResourceKind) && (key.namespace == namespace) && (key.cluster == cluster) {
			if run := runs.get(key); (run != nil) && (run.chi == chiName) {
				if _, done := run.finished(); !done {
					return true, nil
				}
			}
		}
	}
	schedules := &api.ClickHouseBackupScheduleList{}
	if err := r.apiReader.List(ctx, schedules, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range schedules.Items {
		if schedules.Items[i].Spec.ClickHouseInstallation != chiName {
			continue
		}
		for _, active := range schedules.Items[i].Status.Active {
			if (active.Name == cluster) && !active.Phase.IsTerminal() {
				return true, nil
			}
		}
	}
	return false, nil
}

// keys returns the keys of the runs.
func (r *runRegistry) keys() []runKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]runKey, 0, len(r.runs))
	for key := range r.runs {
		keys = append(keys, key)
	}
	return keys
}

// lockOperations locks the start of operations on the given clusters of an installation, and returns the unlock.
func lockOperations(namespace, chiName string, clusters ...string) func() {
	return lockKeys(operationLockKeys(namespace, chiName, clusters...)...)
}

// operationLockKeys returns the keys operationLocks locks the given clusters of an installation by.
func operationLockKeys(namespace, chiName string, clusters ...string) []string {
	keys := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		keys = append(keys, operationLockKey(namespace, chiName, cluster))
	}
	return keys
}

// lockKeys locks the given keys of operationLocks and returns the unlock. Keys are locked in sorted order, each once,
// so that two callers locking overlapping sets never wait for each other in a circle.
func lockKeys(keys ...string) func() {
	keys = slices.Clone(keys)
	slices.Sort(keys)
	keys = slices.Compact(keys)
	unlocks := make([]func(), 0, len(keys))
	for _, key := range keys {
		unlocks = append(unlocks, operationLocks.lock(key))
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}

// operationLockKey is the key operationLocks locks a cluster of an installation by.
func operationLockKey(namespace, chiName, cluster string) string {
	return kubeTypes.NamespacedName{Namespace: namespace, Name: chiName}.String() + operationLockClusterSeparator + cluster
}

// operationLocks serializes the start of operations per cluster of an installation across every backup controller of
// the process.
var operationLocks = newKeyedLocks()

// keyedLocks keeps a lock for every key some goroutine holds or waits for, and drops it with the last of them.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	sync.Mutex
	users int
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{locks: make(map[string]*keyedLock)}
}

func (k *keyedLocks) lock(key string) func() {
	k.mu.Lock()
	l, ok := k.locks[key]
	if !ok {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.users++
	k.mu.Unlock()

	l.Lock()
	return func() {
		l.Unlock()
		k.mu.Lock()
		defer k.mu.Unlock()
		l.users--
		if l.users == 0 {
			delete(k.locks, key)
		}
	}
}

// startOn records the run of an operation started on a cluster.
func startOn(cluster *api.OperationClusterStatus, run *run, hosts []string, configSecret string, now time.Time) {
	cluster.Phase = api.OperationPhaseRunning
	cluster.Hosts = hosts
	cluster.ConfigSecret = configSecret
	if cluster.StartTime == nil {
		cluster.StartTime = &meta.Time{Time: now}
	}
	cluster.Reason = api.BackupReasonStarted
	cluster.Message = "started"
	cluster.Run = run.snapshot()
}

// observeOn records the state of a run of a cluster, and returns how it went once it finished.
func observeOn(cluster *api.OperationClusterStatus, run *run) (runOutcome, bool) {
	cluster.Run = run.snapshot()
	if hosts := cluster.Run.Hosts; len(hosts) > 0 {
		cluster.Hosts = nil
		for _, host := range hosts {
			cluster.Hosts = append(cluster.Hosts, host.Host)
		}
	}
	return run.finished()
}

// outcomeReason returns the reason a failed run of the operation fails a cluster with.
func outcomeReason(outcome runOutcome, operation string) string {
	switch {
	case outcome.deadline:
		return api.BackupReasonDeadlineExceeded
	case outcome.unchanged && (operation == api.BackupRunOperationRestore):
		return api.BackupReasonFailedUnchanged
	}
	return api.BackupReasonFailed
}

// ignoreConflict turns a conflict into a requeue.
func ignoreConflict(err error) (ctrl.Result, error) {
	if apiErrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}
