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
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// The controllers are leader-elected, so only the leader adds and removes finalizers.
const (
	backupFinalizer   = "finalizer.clickhousebackup.altinity.com"
	restoreFinalizer  = "finalizer.clickhouserestore.altinity.com"
	scheduleFinalizer = "finalizer.clickhousebackupschedule.altinity.com"
)

const stoppingRequeueInterval = time.Second

// The patch carries the read resourceVersion, so it never overwrites a concurrent finalizer change.
func (r *reconciler) addFinalizer(ctx context.Context, obj client.Object, finalizer string) error {
	if controllerutil.ContainsFinalizer(obj, finalizer) {
		return nil
	}
	before := obj.DeepCopyObject().(client.Object)
	controllerutil.AddFinalizer(obj, finalizer)
	return r.Patch(ctx, obj, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *reconciler) removeFinalizer(ctx context.Context, obj client.Object, finalizer string) error {
	if !controllerutil.ContainsFinalizer(obj, finalizer) {
		return nil
	}
	before := obj.DeepCopyObject().(client.Object)
	controllerutil.RemoveFinalizer(obj, finalizer)
	if err := r.Patch(ctx, obj, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return client.IgnoreNotFound(err)
	}
	deletions.forget(obj)
	runs.forgetOwner(r.kind, obj.GetUID())
	return nil
}

func deleting(obj client.Object) bool {
	return obj.GetDeletionTimestamp() != nil
}

type deletionNotices struct {
	announced sync.Map
}

var deletions = &deletionNotices{}

func (d *deletionNotices) once(obj client.Object, step string) bool {
	_, loaded := d.announced.LoadOrStore(string(obj.GetUID())+"/"+step, true)
	return !loaded
}

func (d *deletionNotices) made(obj client.Object, step string) bool {
	_, found := d.announced.Load(string(obj.GetUID()) + "/" + step)
	return found
}

func (d *deletionNotices) forget(obj client.Object) {
	prefix := string(obj.GetUID()) + "/"
	d.announced.Range(func(key, _ any) bool {
		if strings.HasPrefix(key.(string), prefix) {
			d.announced.Delete(key)
		}
		return true
	})
}

// The wait is marked announced only when the event is emitted, so a status conflict cannot lose it.
// If the status did not change, commitStatus emits nothing, so the announcement is emitted here.
func (r *reconciler) commitWaiting(
	ctx context.Context,
	obj client.Object,
	changed bool,
	result ctrl.Result,
	out announcements,
	waiting func(),
) (ctrl.Result, error) {
	if !deletions.made(obj, "waiting") {
		announce := func() {
			if deletions.once(obj, "waiting") {
				waiting()
			}
		}
		if !changed {
			announce()
		} else {
			out = append(out, announce)
		}
	}
	return r.commitStatus(ctx, obj, changed, result, out)
}

type deletionLeft struct {
	cluster string
	hosts   []api.BackupRunHost
	why     string
}

// A completed backup keeps its remote backup for retention or a manual delete.
func (c *BackupController) finalize(ctx context.Context, chb *api.ClickHouseBackup) (ctrl.Result, error) {
	kind, uid := api.ClickHouseBackupCRDResourceKind, chb.GetUID()
	if !controllerutil.ContainsFinalizer(chb, backupFinalizer) {
		// The finalizer was removed by hand, so only stop the runs.
		runs.stopOwner(kind, uid)
		runs.forgetOwner(kind, uid)
		return ctrl.Result{}, nil
	}
	name := chb.Status.BackupName
	if chb.Status.Phase == api.OperationPhaseCompleted {
		c.info(chb, a.EventActionDelete, reasonDeletionRemoteBackupKept, "backup %s completed: its deletion keeps the remote "+
			"backup, which the retention of its schedule or a delete by hand removes", name)()
		return ctrl.Result{}, c.removeFinalizer(ctx, chb, backupFinalizer)
	}
	if runs.stopOwner(kind, uid) {
		if deletions.once(chb, "stopping") {
			c.info(chb, a.EventActionDelete, reasonDeletionStoppingRuns, "backup %s is deleted unfinished: its runs stop, then its "+
				"commands are killed and what it left is deleted", name)()
		}
		return ctrl.Result{RequeueAfter: stoppingRequeueInterval}, nil
	}

	var left []deletionLeft
	var cleaned []string
	running := false
	targets := c.newTargetResolver(ctx, chb.GetNamespace(), chb.TargetInstallation(), false)
	for i := range chb.Status.Clusters {
		cluster := &chb.Status.Clusters[i]
		if cluster.Phase == api.OperationPhaseCompleted {
			continue
		}
		hosts := unfinishedHosts(chb, cluster)
		if len(hosts) == 0 {
			continue
		}
		key := runKeyOf(chb, kind, cluster.Name, api.BackupRunOperationDelete)
		run := runs.get(key)
		if run == nil {
			var err error
			run, err = c.startDeletion(ctx, chb, cluster.Name, key, hosts, targets)
			var given *deletionGivenUp
			switch {
			case errors.As(err, &given):
				left = append(left, deletionLeft{cluster: cluster.Name, hosts: hosts, why: given.why})
				continue
			case err != nil:
				return ctrl.Result{}, err
			}
		}
		outcome, finished := run.finished()
		switch {
		case !finished:
			running = true
		case outcome.failed:
			left = append(left, deletionLeft{cluster: cluster.Name, why: outcome.message})
		default:
			cleaned = append(cleaned, cluster.Name)
		}
	}
	if running {
		return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
	}
	switch {
	case len(left) > 0:
		var told []string
		for _, l := range left {
			if len(l.hosts) == 0 {
				told = append(told, fmt.Sprintf("cluster %s: %s", l.cluster, l.why))
				continue
			}
			var names []string
			for _, host := range l.hosts {
				names = append(names, host.Host+"="+host.Backup)
			}
			told = append(told, fmt.Sprintf("cluster %s, left on %s: %s", l.cluster, strings.Join(names, " "), l.why))
		}
		c.warning(chb, a.EventActionDelete, reasonDeletionLeftover, "backup %s is deleted unfinished, leaving what it made: %s; "+
			"%s on each host", name, strings.Join(told, "; "), deleteBackupByHand(name))()
	case len(cleaned) > 0:
		c.info(chb, a.EventActionDelete, reasonDeletionCleanedUp, "backup %s is deleted unfinished: its commands were killed and "+
			"what it left deleted on cluster(s) %s", name, strings.Join(cleaned, ", "))()
	default:
		c.info(chb, a.EventActionDelete, reasonDeletionCleanedUp, "backup %s is deleted unfinished: it sent no command to any sidecar",
			name)()
	}
	return ctrl.Result{}, c.removeFinalizer(ctx, chb, backupFinalizer)
}

// unfinishedHosts returns none when the run sent no command: a backup of that name belongs to another backup.
func unfinishedHosts(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus) []api.BackupRunHost {
	status := cluster.Run
	if run := runs.get(runKeyOf(chb, api.ClickHouseBackupCRDResourceKind, cluster.Name, api.BackupRunOperationBackup)); run != nil {
		status = run.snapshot()
	}
	var hosts []api.BackupRunHost
	switch {
	case status == nil:
	case len(status.Hosts) > 0:
		hosts = slices.Clone(status.Hosts)
	default:
		hosts = hostsOfInsertedCommands(chb, cluster, status)
	}
	if previous := api.FindPreviousRun(chb.Status.PreviousRuns, cluster.Name); previous != nil {
		for _, host := range previous.Hosts {
			if !slices.Contains(hosts, host) {
				hosts = append(hosts, host)
			}
		}
	}
	return hosts
}

// hostsOfInsertedCommands returns every host when a changing command is marked sent but its record is lost.
func hostsOfInsertedCommands(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, status *api.BackupRunStatus) []api.BackupRunHost {
	var hosts []api.BackupRunHost
	for _, action := range status.Actions {
		if action.TimeFrom == "" {
			continue
		}
		name := chb.Status.BackupName
		if fields := strings.Fields(action.Command); len(fields) > 1 {
			name = fields[len(fields)-1]
		}
		host := api.BackupRunHost{Host: action.Host, Backup: name}
		if !slices.Contains(hosts, host) {
			hosts = append(hosts, host)
		}
	}
	if (len(hosts) == 0) && status.Mutated && (chb.Status.BackupName != "") {
		for _, host := range cluster.Hosts {
			hosts = append(hosts, api.BackupRunHost{Host: host, Backup: chb.Status.BackupName})
		}
	}
	return hosts
}

type deletionGivenUp struct {
	why string
}

func (d *deletionGivenUp) Error() string {
	return d.why
}

func (c *BackupController) startDeletion(
	ctx context.Context,
	chb *api.ClickHouseBackup,
	cluster string,
	key runKey,
	hosts []api.BackupRunHost,
	targets *targetResolver,
) (*run, error) {
	target, wait, err := targets.forCluster(cluster)
	switch {
	case err != nil:
		return nil, err
	case wait != nil:
		return nil, &deletionGivenUp{why: wait.message}
	}
	spec := model.BackupRunSpec(chb, target.topology)
	spec.Operation = api.BackupRunOperationDelete
	spec.Deadline = 0
	status := &api.BackupRunStatus{Operation: api.BackupRunOperationDelete, Hosts: hosts}
	if deleting(target.chi) {
		status.Plan = &api.BackupRunPlan{GiveUp: true}
	}
	run, err := runs.start(key, chb.TargetInstallation(), chb, spec, status, c.clock.Now(), 0, c.postRunEvent)
	switch {
	case errors.Is(err, errNoClickHouseAccess):
		return nil, &deletionGivenUp{why: err.Error()}
	case err != nil:
		return nil, err
	}
	if deletions.once(chb, "cleanup/"+cluster) {
		var names []string
		for _, host := range hosts {
			names = append(names, host.Host+"="+host.Backup)
		}
		c.info(chb, a.EventActionDelete, reasonDeletionCleanup, "backup %s is deleted unfinished: killing its commands and "+
			"deleting what it left on cluster %s, %s", chb.Status.BackupName, cluster, strings.Join(names, " "))()
	}
	return run, nil
}

// A running restore run is never killed. The restore goes when no run runs.
func (c *RestoreController) finalize(ctx context.Context, cur *api.ClickHouseRestore) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cur, restoreFinalizer) {
		return ctrl.Result{}, nil
	}
	next := cur.DeepCopy()
	var out announcements
	targets := c.newTargetResolver(ctx, next.GetNamespace(), next.Spec.ClickHouseInstallation, false)
	var result ctrl.Result
	var running []string
	for i := range next.Status.Clusters {
		cluster := &next.Status.Clusters[i]
		if cluster.Phase.IsTerminal() || (cluster.Run == nil) {
			continue
		}
		clusterResult, err := c.reconcileCluster(ctx, next, cluster, false, targets, &out)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !cluster.Phase.IsTerminal() {
			running = append(running, cluster.Name)
			result = shorterResult(result, clusterResult)
		}
	}
	changed := !apiEquality.Semantic.DeepEqual(cur.Status, next.Status)
	if len(running) > 0 {
		return c.commitWaiting(ctx, next, changed, result, out, c.info(next, a.EventActionDelete, reasonDeletionWaitingForRun,
			"restore of backup %s is deleted while it runs on cluster(s) %s: waiting for the restore run to finish before "+
				"deletion; a restore is never killed, and what it downloaded stays", restoredBackupName(next),
			strings.Join(running, ", ")))
	}
	if deletions.made(next, "waiting") {
		out.add(c.info(next, a.EventActionDelete, reasonDeletionRunFinished, "the restore run finished, the deleted restore of "+
			"backup %s goes", restoredBackupName(next)))
	}
	if result, err := c.commitStatus(ctx, next, changed, ctrl.Result{}, out); (err != nil) || result.Requeue {
		return result, err
	}
	return ignoreConflict(c.removeFinalizer(ctx, next, restoreFinalizer))
}

func (c *ScheduleController) finalize(ctx context.Context, cur *api.ClickHouseBackupSchedule) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cur, scheduleFinalizer) {
		return ctrl.Result{}, nil
	}
	next := cur.DeepCopy()
	var out announcements
	recheck, err := c.observeActive(ctx, next, &out)
	if err != nil {
		return ctrl.Result{}, err
	}
	changed := !apiEquality.Semantic.DeepEqual(cur.Status, next.Status)
	if len(next.Status.Active) > 0 {
		var clusters []string
		for _, scheduled := range next.Status.Active {
			clusters = append(clusters, scheduled.Name)
		}
		return c.commitWaiting(ctx, next, changed, ctrl.Result{RequeueAfter: recheck}, out, c.info(next, a.EventActionDelete,
			reasonDeletionWaitingForRun, "schedule is deleted while it runs on cluster(s) %s: it starts no run more, and goes "+
				"once the runs in progress, retention included, finish", strings.Join(clusters, ", ")))
	}
	if deletions.made(next, "waiting") {
		out.add(c.info(next, a.EventActionDelete, reasonDeletionRunFinished, "the runs of the deleted schedule finished, it goes"))
	}
	if result, err := c.commitStatus(ctx, next, changed, ctrl.Result{}, out); (err != nil) || result.Requeue {
		return result, err
	}
	return ignoreConflict(c.removeFinalizer(ctx, next, scheduleFinalizer))
}
