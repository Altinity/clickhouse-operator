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

// A gate that reads other clusters names them, so the start also runs under their locks.
type startGate struct {
	clusters []string
	sources  []string
	check    func(context.Context) (*pending, error)
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

func runKeyOf(owner client.Object, kind, cluster string, operation api.BackupRunOperation) runKey {
	return runKey{kind: kind, namespace: owner.GetNamespace(), name: owner.GetName(), uid: owner.GetUID(), cluster: cluster,
		operation: operation}
}

// Operations on one cluster must never overlap: a restore drops what a backup reads, and both use the same sidecars.
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

func noClickHouseAccess() *pending {
	return &pending{
		reason: api.BackupReasonNoClickHouseAccess,
		message: "the operator drives clickhouse-backup through SQL with the ClickHouse credentials of its configuration, " +
			"clickhouse.access, which sets none: set clickhouse.access.username and password, or clickhouse.access.secret",
		requeue: targetRequeueInterval,
	}
}

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

func (r *reconciler) postRunEvent(obj client.Object, eventType, reason, message string) {
	if eventType == core.EventTypeWarning {
		r.events.EventWarning(obj, a.EventActionReconcile, reason, message)
		return
	}
	r.events.EventInfo(obj, a.EventActionReconcile, reason, message)
}

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

func (r *reconciler) scheduledRunActive(ctx context.Context, namespace, chiName, cluster string) (bool, error) {
	if runs.unfinished(api.ClickHouseBackupScheduleCRDResourceKind, namespace, chiName, cluster) {
		return true, nil
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

// unfinished reports whether a run of the kind on the cluster is still going. An interrupted run counts as unfinished.
func (r *runRegistry) unfinished(kind, namespace, chiName, cluster string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, run := range r.runs {
		if (key.kind != kind) || (key.namespace != namespace) || (key.cluster != cluster) || (run.chi != chiName) {
			continue
		}
		if _, done := run.finished(); !done {
			return true
		}
	}
	return false
}

func operationLockKeys(namespace, chiName string, clusters ...string) []string {
	keys := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		keys = append(keys, operationLockKey(namespace, chiName, cluster))
	}
	return keys
}

// lockKeys locks each key once in sorted order, so callers with overlapping keys cannot deadlock.
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

func operationLockKey(namespace, chiName, cluster string) string {
	return kubeTypes.NamespacedName{Namespace: namespace, Name: chiName}.String() + operationLockClusterSeparator + cluster
}

var operationLocks = newKeyedLocks()

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

func outcomeReason(outcome runOutcome, operation api.BackupRunOperation) string {
	switch {
	case outcome.deadline:
		return api.BackupReasonDeadlineExceeded
	case outcome.unchanged && (operation == api.BackupRunOperationRestore):
		return api.BackupReasonFailedUnchanged
	}
	return api.BackupReasonFailed
}

func ignoreConflict(err error) (ctrl.Result, error) {
	if apiErrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}
