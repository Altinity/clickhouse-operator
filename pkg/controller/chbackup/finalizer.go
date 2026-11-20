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

// The finalizers of the backup custom resources: the operator settles what a resource deleted while it works leaves,
// before the resource goes. The controllers are leader-elected, so only the leader adds and removes them.
const (
	// backupFinalizer: a backup that did not complete is killed and deleted before it goes
	backupFinalizer = "finalizer.clickhousebackup.altinity.com"
	// restoreFinalizer: a restore goes once its runs finished
	restoreFinalizer = "finalizer.clickhouserestore.altinity.com"
	// scheduleFinalizer: a schedule goes once its runs, retention included, finished
	scheduleFinalizer = "finalizer.clickhousebackupschedule.altinity.com"
)

// stoppingRequeueInterval paces the deletion of a backup whose runs are stopping: a run tells when it stopped, which
// this only backs up.
const stoppingRequeueInterval = time.Second

// addFinalizer adds the finalizer to the resource, unless it has it: the patch carries the resourceVersion the resource
// was read with, so a concurrent change of its finalizers is never overwritten. The resource is updated in place.
func (r *reconciler) addFinalizer(ctx context.Context, obj client.Object, finalizer string) error {
	if controllerutil.ContainsFinalizer(obj, finalizer) {
		return nil
	}
	before := obj.DeepCopyObject().(client.Object)
	controllerutil.AddFinalizer(obj, finalizer)
	return r.Patch(ctx, obj, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

// removeFinalizer removes the finalizer of the resource, which lets it go, and forgets what was announced of its
// deletion.
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

// deleting reports whether the resource is being deleted.
func deleting(obj client.Object) bool {
	return obj.GetDeletionTimestamp() != nil
}

// deletionNotices remembers which deletion announcements were made, so that a deletion that waits announces what it
// waits for once, not at every reconcile. A restart of the operator announces it once more.
type deletionNotices struct {
	announced sync.Map
}

var deletions = &deletionNotices{}

// once reports whether the announcement of the step of the deletion of the resource is to be made: the first time it
// is asked.
func (d *deletionNotices) once(obj client.Object, step string) bool {
	_, loaded := d.announced.LoadOrStore(string(obj.GetUID())+"/"+step, true)
	return !loaded
}

// made reports whether the announcement of the step of the deletion of the resource was made.
func (d *deletionNotices) made(obj client.Object, step string) bool {
	_, found := d.announced.Load(string(obj.GetUID()) + "/" + step)
	return found
}

// forget drops what was announced of the deletion of the resource.
func (d *deletionNotices) forget(obj client.Object) {
	prefix := string(obj.GetUID()) + "/"
	d.announced.Range(func(key, _ any) bool {
		if strings.HasPrefix(key.(string), prefix) {
			d.announced.Delete(key)
		}
		return true
	})
}

// deletionLeft is what the deletion of a backup that did not complete leaves on a cluster, and why: on the hosts, where
// the deletion could not run, else as why tells.
type deletionLeft struct {
	cluster string
	hosts   []api.BackupRunHost
	why     string
}

// finalize settles the deletion of a backup. A backup that completed keeps its remote backup, which retention or a
// delete by hand removes: its finalizer goes at once. One that did not complete has its runs stopped, then, on every
// cluster it ran on, the commands of it the sidecars still run killed and what it left deleted, local and remote; what
// cannot be deleted in the bounds of its timeouts is told in a Warning, with how to delete it by hand, and the backup
// goes all the same.
func (c *BackupController) finalize(ctx context.Context, chb *api.ClickHouseBackup) (ctrl.Result, error) {
	kind, uid := api.ClickHouseBackupCRDResourceKind, chb.GetUID()
	if !controllerutil.ContainsFinalizer(chb, backupFinalizer) {
		// A backup whose finalizer was removed by hand goes as it is: its runs stop at least
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
		key := runKeyOf(chb, kind, cluster.Name, operationDelete)
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

// unfinishedHosts returns the hosts a backup that did not complete may have sent a command to on a cluster, with the
// name of the backup on each: those its run made the backup on, as the run tells them, else those it inserted a command
// into before it recorded its hosts, and those its runs there made it on before spec.taskID reran it. None when its run
// sent no command: one the preflight refused, as for a backup of that name the storage holds already, which is another
// backup's, never the run's own to delete.
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

// hostsOfInsertedCommands returns the hosts a run that recorded no hosts yet may have sent a command to, with the name
// of the backup on each, which the command tells: a run that failed in its preflight inserted none, while one that
// replaced the copies an earlier run of the backup left inserted a delete before it recorded its hosts. A run that
// marks a command that changes something sent, but whose record of the command was lost, may have sent it to any host
// of the cluster, under the name of the backup.
func hostsOfInsertedCommands(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, status *api.BackupRunStatus) []api.BackupRunHost {
	var hosts []api.BackupRunHost
	for _, action := range status.Actions {
		if action.TimeFrom == "" {
			// Planned, never inserted
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

// deletionGivenUp tells why the deletion of what a backup left on a cluster cannot run at all.
type deletionGivenUp struct {
	why string
}

func (d *deletionGivenUp) Error() string {
	return d.why
}

// startDeletion starts the run that kills the commands of the backup on the hosts of a cluster and deletes what it
// left. It gives up, as deletionGivenUp tells, where the installation, the cluster or the access to its ClickHouse is
// gone; an installation that is being deleted, its namespace say, is tried once on every host, never waited for.
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
	spec.Operation = operationDelete
	spec.Deadline = 0
	status := &api.BackupRunStatus{Operation: operationDelete, Hosts: hosts}
	if deleting(target.chi) {
		status.Plan = map[string]string{planGiveUp: "true"}
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

// finalize settles the deletion of a restore: a cluster it has not started on never starts, a run in progress is waited
// for, never killed, and its outcome recorded, and the restore goes once none runs. What the restore downloaded stays,
// as that of a failed restore does.
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
		if deletions.once(next, "waiting") {
			out.add(c.info(next, a.EventActionDelete, reasonDeletionWaitingForRun, "restore of backup %s is deleted while it runs on "+
				"cluster(s) %s: waiting for the restore run to finish before deletion; a restore is never killed, and what it "+
				"downloaded stays", restoredBackupName(next), strings.Join(running, ", ")))
		}
		return c.commitStatus(ctx, next, changed, result, out)
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

// finalize settles the deletion of a schedule: no slot starts from then on, and the schedule goes once the runs in
// progress, retention included, finished, whose outcomes are recorded.
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
		if deletions.once(next, "waiting") {
			var clusters []string
			for _, scheduled := range next.Status.Active {
				clusters = append(clusters, scheduled.Name)
			}
			out.add(c.info(next, a.EventActionDelete, reasonDeletionWaitingForRun, "schedule is deleted while it runs on cluster(s) "+
				"%s: it starts no run more, and goes once the runs in progress, retention included, finish",
				strings.Join(clusters, ", ")))
		}
		return c.commitStatus(ctx, next, changed, ctrl.Result{RequeueAfter: recheck}, out)
	}
	if deletions.made(next, "waiting") {
		out.add(c.info(next, a.EventActionDelete, reasonDeletionRunFinished, "the runs of the deleted schedule finished, it goes"))
	}
	if result, err := c.commitStatus(ctx, next, changed, ctrl.Result{}, out); (err != nil) || result.Requeue {
		return result, err
	}
	return ignoreConflict(c.removeFinalizer(ctx, next, scheduleFinalizer))
}
