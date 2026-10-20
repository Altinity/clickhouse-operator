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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	batch "k8s.io/api/batch/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chbackup/metrics"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ScheduleController reconciles a ClickHouseBackupSchedule into a CronJob per cluster that runs its
// backups there. The CronJobs follow the installation: their host lists are rebuilt whenever the
// installation completes a reconcile, the CronJob of a cluster that leaves the schedule is deleted,
// and the CronJob of a cluster is suspended while a restore of the cluster, or of the access entities of
// the installation, is about to start, runs or has failed, and while the cluster waits to be recovered.
type ScheduleController struct {
	*reconciler
}

// NewScheduleController creates a ScheduleController with its request-independent collaborators
// initialized once, as controller-runtime shares it across all reconcile goroutines.
func NewScheduleController(
	c client.Client,
	apiReader client.Reader,
	scheme *apiMachinery.Scheme,
	cr interfaces.IKubeCR,
) *ScheduleController {
	return &ScheduleController{
		reconciler: newReconciler(c, apiReader, scheme, cr, api.ClickHouseBackupScheduleCRDResourceKind),
	}
}

// Reconcile keeps the CronJobs of a ClickHouseBackupSchedule in line with the schedule and its
// installation, and reports the scheduled runs.
func (c *ScheduleController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if c.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}

	cur := &api.ClickHouseBackupSchedule{}
	if err := c.Get(ctx, req.NamespacedName, cur); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	next := cur.DeepCopy()
	var out announcements
	result, err := c.reconcile(ctx, next, &out)
	if err != nil {
		return ctrl.Result{}, err
	}
	return c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), result, out)
}

// clusterCronJob is the CronJob of a schedule on a cluster.
type clusterCronJob struct {
	cluster  string
	topology *model.Topology
	existing *batch.CronJob
	// ranOn are the hosts the runs of the existing CronJob prune on, before the reconcile updates it
	ranOn   []string
	suspend bool
	applied *batch.CronJob
}

func (c *ScheduleController) reconcile(ctx context.Context, chbs *api.ClickHouseBackupSchedule, out *announcements) (ctrl.Result, error) {
	owned, err := c.ownedCronJobs(ctx, chbs)
	if err != nil {
		return ctrl.Result{}, err
	}
	runs, err := c.scheduledRuns(ctx, chbs)
	if err != nil {
		return ctrl.Result{}, err
	}
	outcomes := runOutcomes{}

	// The CronJobs run the last valid spec, which the edit meant to replace, so they are held until the
	// spec is fixed. Only a change to the spec can fix it: nothing is requeued.
	if err := chbs.Spec.Validate(); err != nil {
		reportScheduleOutcome(chbs)
		// The installation tells the clusters without a storage, which the hold does not count whatever
		// holds the schedule. It is read as far as it can be: an installation the spec misnames, or that cannot
		// be read now, only has them counted until the next reconcile.
		var chi *api.ClickHouseInstallation
		if chbs.Spec.ClickHouseInstallation != "" {
			chi, _, _ = c.readInstallation(ctx, chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, false)
		}
		return c.hold(ctx, chbs, nil, owned, runs, outcomes, &pending{reason: api.BackupReasonInvalidSpec, message: err.Error()},
			heldWithoutStorage(chbs, chi), out)
	}

	chi, wait, err := c.readInstallation(ctx, chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, true)
	if err != nil {
		return ctrl.Result{}, err
	}
	var target *target
	if wait == nil {
		if target, wait, err = newTarget(chi, chbs.Spec.Cluster); err != nil {
			return ctrl.Result{}, err
		}
	}
	// The series of an installation found gone went with it. Those of the clusters it completed its reconcile
	// without, or without backup on, go, and the status the schedule keeps for when they back up again brings
	// none back. A cluster that turns backup on without a storage keeps its series, which the status
	// brings back: their age tells it backs up no more.
	switch {
	case wait == nil:
		reportScheduleOutcome(chbs)
		retainRequestedClusters(chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, target.chi)
	case wait.reason == api.BackupReasonNoBackupStorage:
		reportScheduleOutcome(chbs)
		retainRequestedClusters(chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, chi)
	case wait.refusesOneOff():
		retainRequestedClusters(chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, chi)
	case wait.reason != api.BackupReasonCHINotFound:
		reportScheduleOutcome(chbs)
	}
	if wait != nil {
		return c.hold(ctx, chbs, chi, owned, runs, outcomes, wait, heldWithoutStorage(chbs, chi), out)
	}

	clusters := target.topology.ClusterNames()
	guard := sharesSourcePrefix(chbs, target.chi)
	holds, err := c.restoreHolds(ctx, c.Client, chbs, target.chi, clusters)
	if err != nil {
		return ctrl.Result{}, err
	}

	var planned []*clusterCronJob
	var resuming []string
	// A cluster whose CronJob name another resource holds runs no schedule; the others run theirs
	blocked := map[string]*pending{}
	for _, cluster := range clusters {
		existing, err := c.getCronJob(ctx, chbs, cluster)
		if err != nil {
			return ctrl.Result{}, err
		}
		if (existing != nil) && !meta.IsControlledBy(existing, chbs) {
			if blocked[cluster], err = c.foreignCronJob(ctx, chbs, cluster, existing); err != nil {
				return ctrl.Result{}, err
			}
			scheduleCluster(chbs, cluster)
			continue
		}
		topology, err := target.topology.ForCluster(cluster)
		if err != nil {
			return ctrl.Result{}, err
		}
		plan := &clusterCronJob{
			cluster:  cluster,
			topology: topology,
			existing: existing,
			ranOn:    model.CronJobRetentionHosts(existing),
			suspend:  chbs.Spec.IsSuspended() || (guard != nil) || (holds[cluster] != nil),
		}
		if !plan.suspend && ((existing == nil) || cronJobSuspended(existing)) {
			resuming = append(resuming, cluster)
		}
		planned = append(planned, plan)
	}

	if result, done, err := c.applyCronJobs(ctx, chbs, target.chi, planned, resuming, holds, outcomes, out); done {
		return result, err
	}
	// Only once the API server took the CronJobs: one it refuses holds the schedule as invalid, which a validation
	// recorded before would undo on every reconcile
	c.validated(chbs, target, out)

	recheck := time.Duration(0)
	for _, plan := range planned {
		cluster := scheduleCluster(chbs, plan.cluster)
		if err := c.recordRetentionHosts(chbs, cluster, runs[plan.applied.GetUID()], plan.ranOn, model.RetentionHosts(chbs, plan.topology), out); err != nil {
			return ctrl.Result{}, err
		}
		delay, err := c.observeScheduledJobs(ctx, chbs, cluster, runs[plan.applied.GetUID()], outcomes)
		if err != nil {
			return ctrl.Result{}, err
		}
		recheck = shorterDelay(recheck, delay)
		c.syncStatus(chbs, cluster, plan.applied)
	}
	if err := c.dropLeavingClusters(ctx, chbs, owned, runs, clusters, heldWithoutStorage(chbs, target.chi), outcomes, out); err != nil {
		return ctrl.Result{}, err
	}
	orderClusters(chbs, clusters)
	c.aggregate(chbs, clusters, outcomes)

	var cronJobs []string
	for _, plan := range planned {
		cronJobs = append(cronJobs, plan.applied.GetName())
	}
	if wait := combineHolds(clusters, blocked); wait != nil {
		c.setCondition(chbs, api.BackupConditionJobCreated, meta.ConditionFalse, wait.reason, wait.message)
	} else {
		c.setCondition(chbs, api.BackupConditionJobCreated, meta.ConditionTrue, api.BackupReasonJobCreated, cronJobsCreatedMessage(cronJobs))
	}
	for cluster, wait := range blocked {
		holds[cluster] = wait
	}
	hold := guard
	if hold == nil {
		hold = combineHolds(clusters, holds)
	}
	c.refreshReady(chbs, cronJobs, hold, out)
	if hold != nil {
		return ctrl.Result{RequeueAfter: shorterDelay(hold.requeue, recheck)}, nil
	}
	return ctrl.Result{RequeueAfter: shorterDelay(recheck, topologyRecheckInterval)}, nil
}

// applyCronJobs creates or updates the CronJob of every cluster of the schedule. A CronJob that a
// restore of its cluster may hold is created or resumed under the lock a restore takes to start, after
// the restores were checked again past the cache: a restore validated since the cache was read either
// sees the CronJob it waits for, or is seen holding it. done tells that the reconcile ends here.
func (c *ScheduleController) applyCronJobs(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	chi *api.ClickHouseInstallation,
	planned []*clusterCronJob,
	resuming []string,
	holds map[string]*pending,
	outcomes runOutcomes,
	out *announcements,
) (ctrl.Result, bool, error) {
	apply := func(plan *clusterCronJob) error {
		desired := model.BuildBackupCronJob(chbs, chi, plan.topology, plan.suspend)
		applied, err := c.applyCronJob(ctx, chbs, plan.existing, desired)
		plan.applied = applied
		return err
	}
	applyAll := func(resume bool) error {
		for _, plan := range planned {
			if slices.Contains(resuming, plan.cluster) != resume {
				continue
			}
			if err := apply(plan); err != nil {
				return err
			}
		}
		return nil
	}

	err := applyAll(false)
	if (err == nil) && (len(resuming) > 0) {
		err = func() error {
			unlock := lockOperations(chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, resuming...)
			defer unlock()
			recheck, err := c.restoreHolds(ctx, c.apiReader, chbs, chi, resuming)
			if err != nil {
				return err
			}
			for _, plan := range planned {
				if hold := recheck[plan.cluster]; hold != nil {
					holds[plan.cluster] = hold
					plan.suspend = true
				}
			}
			return applyAll(true)
		}()
	}
	switch {
	case apiErrors.IsAlreadyExists(err) || apiErrors.IsConflict(err):
		// The cache has not caught up with a write of a previous reconcile yet.
		return ctrl.Result{Requeue: true}, true, nil
	case apiErrors.IsInvalid(err):
		// The API server validates what the operator does not, the cron expression above all: the
		// spec is invalid like any other, and the CronJobs of the last valid one are held.
		owned, listErr := c.ownedCronJobs(ctx, chbs)
		if listErr != nil {
			return ctrl.Result{}, true, listErr
		}
		runs, listErr := c.scheduledRuns(ctx, chbs)
		if listErr != nil {
			return ctrl.Result{}, true, listErr
		}
		result, err := c.hold(ctx, chbs, nil, owned, runs, outcomes, &pending{reason: api.BackupReasonInvalidSpec, message: err.Error()},
			heldWithoutStorage(chbs, chi), out)
		return result, true, err
	case err != nil:
		return ctrl.Result{}, true, err
	}
	return ctrl.Result{}, false, nil
}

// heldWithoutStorage returns the clusters a schedule of every cluster holds as they turn backup on without a
// storage, as the last completed reconcile of the installation left them: their CronJobs stay, suspended,
// as the CronJob of a schedule of one of them does, so that they back up again once they have one and their last
// runs stay reported meanwhile. A schedule of one cluster holds that cluster whole instead, and gets none.
func heldWithoutStorage(chbs *api.ClickHouseBackupSchedule, chi *api.ClickHouseInstallation) []string {
	if (chbs.Spec.Cluster != "") || (chi == nil) {
		return nil
	}
	return model.BackupClustersWithoutStorage(chi.GetAncestorT())
}

// dropLeavingClusters deletes the CronJobs of clusters the schedule no longer backs up, once their
// runs are reported. A cluster that leaves while retention is on leaves its backups unpruned,
// which its status entry keeps telling until its hosts are cleared by hand or rejoin. The CronJobs of
// the clusters without a storage are suspended instead, and their status entries kept with them.
func (c *ScheduleController) dropLeavingClusters(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	owned map[string]*batch.CronJob,
	runs map[kubeTypes.UID][]*batch.Job,
	clusters []string,
	withoutStorage []string,
	outcomes runOutcomes,
	out *announcements,
) error {
	for _, name := range sortedKeys(owned) {
		if slices.Contains(clusters, name) {
			continue
		}
		cronJob := owned[name]
		cluster := scheduleCluster(chbs, name)
		if slices.Contains(withoutStorage, name) {
			if err := c.suspendCronJob(ctx, cronJob); err != nil {
				return err
			}
			if _, err := c.observeScheduledJobs(ctx, chbs, cluster, runs[cronJob.GetUID()], outcomes); err != nil {
				return err
			}
			c.syncStatus(chbs, cluster, cronJob)
			continue
		}
		if err := c.recordRetentionHosts(chbs, cluster, runs[cronJob.GetUID()], model.CronJobRetentionHosts(cronJob), nil, out); err != nil {
			return err
		}
		if _, err := c.observeScheduledJobs(ctx, chbs, cluster, runs[cronJob.GetUID()], outcomes); err != nil {
			return err
		}
		if err := c.Delete(ctx, cronJob, client.PropagationPolicy(meta.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return err
		}
		log.V(1).M(c.meta(chbs)).F().Info("deleted CronJob %s of cluster %s, which the schedule no longer backs up", cronJob.GetName(), name)
		cluster.CronJobName = ""
	}
	var kept []api.ScheduleClusterStatus
	for i := range chbs.Status.Clusters {
		cluster := &chbs.Status.Clusters[i]
		_, running := owned[cluster.Name]
		if running && slices.Contains(withoutStorage, cluster.Name) {
			kept = append(kept, *cluster)
			continue
		}
		if !slices.Contains(clusters, cluster.Name) {
			if !running {
				if err := c.recordRetentionHosts(chbs, cluster, nil, nil, nil, out); err != nil {
					return err
				}
			}
			if len(cluster.UnprunedHosts) == 0 {
				continue
			}
			cluster.CronJobName = ""
		}
		kept = append(kept, *cluster)
	}
	chbs.Status.Clusters = kept
	return nil
}

// foreignCronJob tells why the schedule runs nothing on a cluster whose CronJob name a CronJob it does not
// control holds: one awaiting garbage collection, of a deleted predecessor of the same name above all,
// until it is gone; one of another schedule, until one of the two is renamed; one of no resource, until
// it is deleted.
func (c *ScheduleController) foreignCronJob(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	cluster string,
	cronJob *batch.CronJob,
) (*pending, error) {
	stale, err := c.awaitsGarbageCollection(ctx, chbs, cronJob)
	switch {
	case err != nil:
		return nil, err
	case stale:
		return &pending{
			reason:  api.BackupReasonWaitingForGarbageCollection,
			message: fmt.Sprintf("scheduled backups of cluster %s wait: %s", cluster, staleOwnerMessage(kindCronJob, cronJob.GetName())),
			requeue: pendingRequeueInterval,
		}, nil
	}
	return &pending{
		reason:  api.BackupReasonNameConflict,
		message: fmt.Sprintf("scheduled backups of cluster %s cannot run: %s", cluster, takenNameMessage(kindCronJob, cronJob)),
		requeue: targetRequeueInterval,
	}, nil
}

// validated records that the schedule and its installation are valid. Two things are warned about once the
// schedule is found valid with them, and again only after it was found otherwise meanwhile: an installation
// restored from a backup typically shares the storage path of the installation the backup came from,
// where retention tells the backups of the two apart by their name prefix alone, and a schedule of every cluster
// leaves out the clusters that turn backup on without a storage.
func (c *ScheduleController) validated(chbs *api.ClickHouseBackupSchedule, target *target, out *announcements) {
	reason, message := api.BackupReasonValidated, messageValidated
	if recoveredFrom := target.chi.GetAnnotations()[api.AnnotationRecoveredFrom]; recoveredFrom != "" {
		reason = api.BackupReasonCHIRecovered
		message = fmt.Sprintf("ClickHouseInstallation %s was restored from backup %s and may share the storage path "+
			"of the installation that made it: retention prunes the backups named %s%s there, so no schedule of that "+
			"installation may use this backup name prefix", target.chi.GetName(), recoveredFrom, model.BackupNamePrefix(chbs),
			api.BackupNameScheduleSuffix)
	}
	if leftOut := leftOutWithoutStorage(target, chbs.Spec.Cluster); leftOut != "" {
		if reason == api.BackupReasonValidated {
			reason, message = api.BackupReasonNoBackupStorage, "the schedule "+leftOut
		} else {
			message += "; the schedule " + leftOut
		}
	}
	previous := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionValidated)
	warned := (previous != nil) && (previous.Reason == reason) && (previous.Message == message)
	c.setCondition(chbs, api.BackupConditionValidated, meta.ConditionTrue, reason, message)
	if (reason != api.BackupReasonValidated) && !warned {
		out.add(c.warning(chbs, a.EventActionReconcile, reason, "%s", message))
	}
}

// sharesSourcePrefix tells why a schedule is held for the backup name prefix it defaults to, nil when
// it is not: the installation is restored, or about to be, from a backup that carries the prefix the
// schedules of the installation default to. The installation that made the backup typically has the
// same namespace and name, in another Kubernetes cluster, and the same storage paths: a schedule
// of the same name there would overwrite the backups this one makes in the same slot, and prune them,
// and a schedule of another name may still share the storage path of a cluster with this one. Every
// cluster may share it, those the recovery leaves out included, so every schedule that defaults its
// prefix is held, whatever cluster it backs up, from the moment the recovery is asked for. An explicit
// prefix, even the same one, is an informed choice and releases the hold.
func sharesSourcePrefix(chbs *api.ClickHouseBackupSchedule, chi *api.ClickHouseInstallation) *pending {
	if chbs.Spec.BackupNamePrefix != "" {
		return nil
	}
	sources := model.RecoveredBackupNames(chi.GetAnnotations()[api.AnnotationRecoveredFrom])
	// A recovery held for a cluster without a storage is asked for all the same
	recoveries := append(model.Recoveries(chi.GetAncestorT()), model.RecoveriesWithoutStorage(chi.GetAncestorT())...)
	for _, recovery := range recoveries {
		sources = append(sources, recovery.BackupName)
	}
	for _, source := range sources {
		if model.HasDefaultBackupNamePrefix(chi.GetNamespace(), chi.GetName(), source) {
			return &pending{
				reason: api.BackupReasonSharesSourcePrefix,
				message: fmt.Sprintf("ClickHouseInstallation %s is, or is to be, recovered from backup %s, which carries the backup "+
					"name prefix the schedules of the installation default to: the installation that made the backup may run "+
					"schedules on the same storage paths, which would overwrite and prune the backups this schedule makes "+
					"with its default prefix %s, and the other way round. Set spec.backupNamePrefix, which can be set once "+
					"and is immutable after, to this prefix or another one, to run the schedule",
					chi.GetName(), source, model.BackupNamePrefix(chbs)),
				requeue: targetRequeueInterval,
			}
		}
	}
	return nil
}

// recordRetentionHosts records the hosts retention prunes on, of one cluster: those of the hosts the
// schedule backs up there that a run of its CronJob has backed up, as the Job of the run lists them. A
// host no run reached - of a suspended schedule, or added and removed again between two runs - holds
// no backup of the schedule and is never recorded. A recorded host that leaves the hosts the schedule
// backs up while retention is on leaves its backups behind: no run reaches its sidecar any
// more, and no other host prunes the storage path it uploaded to, unless they share it. The operator
// never deletes them for it, as the backups of a removed shard may be all that is left of its data;
// the host is recorded as unpruned and warned about instead, until it rejoins and is pruned again.
//
// The Job of a run counts only for ranOn, the hosts the CronJob still ran on before this reconcile
// updated it, so a Job kept after a host left never records the host again. The hosts that left are
// told from the recorded hosts rather than from the CronJob, so a status write lost after the
// CronJob was updated does not lose them.
func (c *ScheduleController) recordRetentionHosts(
	chbs *api.ClickHouseBackupSchedule,
	cluster *api.ScheduleClusterStatus,
	jobs []*batch.Job,
	ranOn []string,
	hosts []string,
	out *announcements,
) error {
	var backedUp []string
	for _, job := range jobs {
		backedUp = append(backedUp, model.JobRetentionHosts(job)...)
	}
	recorded := slices.Clone(cluster.RetentionHosts)
	for _, host := range ranOn {
		if slices.Contains(backedUp, host) && !slices.Contains(recorded, host) {
			recorded = append(recorded, host)
		}
	}

	unpruned := cluster.UnprunedHosts
	var left []string
	if chbs.Spec.Retention.GetKeepLast() > 0 {
		for _, host := range recorded {
			if !slices.Contains(hosts, host) && !slices.Contains(left, host) && !slices.Contains(unpruned, host) {
				left = append(left, host)
			}
		}
	}
	// An unpruned host that rejoins still holds the backups of the runs before it left
	cluster.RetentionHosts = slices.DeleteFunc(slices.Clone(hosts), func(host string) bool {
		return !slices.Contains(recorded, host) && !slices.Contains(unpruned, host)
	})
	cluster.UnprunedHosts = slices.DeleteFunc(append(slices.Clone(unpruned), left...), func(host string) bool {
		return slices.Contains(hosts, host)
	})
	if len(left) == 0 {
		return nil
	}
	out.add(c.warning(chbs, a.EventActionReconcile, api.BackupReasonUnprunedHosts,
		"%s of cluster %s left the hosts the schedule backs up: retention no longer prunes the backups named "+
			"%s%s they made, which stay in the storage until deleted by hand, unless a host still backed up shares "+
			"their storage path",
		strings.Join(left, ", "), cluster.Name, model.BackupNamePrefix(chbs), api.BackupNameScheduleSuffix))
	return nil
}

// hold keeps the existing CronJobs while the schedule cannot be applied. An installation in the middle of a
// reconcile keeps its last hosts until the reconcile completes and refreshes them, and a schedule validated
// before the reconcile stays validated, its runs reported as ever; any other condition would only make
// scheduled backups fail, or run a spec the user replaced, so the CronJobs are suspended until the schedule
// is valid again. The clusters without a storage, whose CronJobs stay suspended meanwhile, are not
// counted as backed up, as when the schedule is applied. Through a reconcile the Ready condition follows the
// runs while a CronJob counted runs, and keeps a hold found before as well as a suspension of the spec;
// otherwise it tells the reconcile, as any other hold tells its cause. A restore hold found before is told
// anew from the restores read now, along with the CronJob of every cluster no restore holds any longer, which
// the reconcile keeps suspended until it completes, unless the spec suspends the schedule. A restore holds
// the CronJobs of its clusters through a reconcile all the same, as one of another installation that reads
// the backup of this one does not wait for its reconcile to complete: chi, the installation, is nil when the
// spec is invalid, and only a hold through a reconcile reads it.
func (c *ScheduleController) hold(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	chi *api.ClickHouseInstallation,
	owned map[string]*batch.CronJob,
	runs map[kubeTypes.UID][]*batch.Job,
	outcomes runOutcomes,
	wait *pending,
	withoutStorage []string,
	out *announcements,
) (ctrl.Result, error) {
	reconciling := wait.reason == api.BackupReasonCHINotReady
	if !reconciling || !apiMeta.IsStatusConditionTrue(chbs.Status.Conditions, api.BackupConditionValidated) {
		c.setCondition(chbs, api.BackupConditionValidated, meta.ConditionFalse, wait.reason, wait.message)
	}
	if len(owned) == 0 {
		c.setReady(chbs, meta.ConditionFalse, wait.reason, wait.message, out)
		return ctrl.Result{RequeueAfter: wait.requeue}, nil
	}

	uncounted := func(cluster string) bool {
		return slices.Contains(withoutStorage, cluster)
	}
	readRestores := reconciling && (chi != nil)
	restored := map[string]*pending{}
	if readRestores {
		var err error
		if restored, err = c.restoreHolds(ctx, c.Client, chbs, chi, sortedKeys(owned)); err != nil {
			return ctrl.Result{}, err
		}
	}
	// suspended are the counted clusters whose CronJobs are suspended
	var cronJobs, suspended []string
	running := false
	recheck := time.Duration(0)
	for _, name := range sortedKeys(owned) {
		cronJob := owned[name]
		// The CronJob of a cluster without a storage could only fail through a reconcile as well
		if !reconciling || uncounted(name) || (restored[name] != nil) {
			if err := c.suspendCronJob(ctx, cronJob); err != nil {
				return ctrl.Result{}, err
			}
		}
		cluster := scheduleCluster(chbs, name)
		delay, err := c.observeScheduledJobs(ctx, chbs, cluster, runs[cronJob.GetUID()], outcomes)
		if err != nil {
			return ctrl.Result{}, err
		}
		recheck = shorterDelay(recheck, delay)
		c.syncStatus(chbs, cluster, cronJob)
		if uncounted(name) {
			continue
		}
		cronJobs = append(cronJobs, cronJob.GetName())
		if cronJobSuspended(cronJob) {
			suspended = append(suspended, name)
		} else {
			running = true
		}
	}
	c.aggregate(chbs, slices.DeleteFunc(heldClusters(chbs, owned), uncounted), outcomes)
	// A hold of the schedule found before the reconcile, and a suspension of its spec, stay until the reconcile
	// completes and tells again; a hold for a cause the reconcile may remove meanwhile does not
	kept := keptHold(chbs)
	if readRestores && (kept != nil) && ((kept.reason == api.BackupReasonSuspendedForRestore) || (kept.reason == api.BackupReasonSuspendedForFailedRestore)) {
		// The restores read above tell what holds the schedule now. A CronJob a restore held that none holds any longer
		// stays suspended until the reconcile completes and resumes it, which its own hold tells, unless the spec
		// suspends the schedule anyway
		kept = nil
		for _, cluster := range suspended {
			if (restored[cluster] == nil) && !chbs.Spec.IsSuspended() {
				restored[cluster] = releasedThroughReconcile(chi.GetName(), owned[cluster].GetName())
			}
		}
	}
	kept = heldThroughReconcile(kept, combineHolds(slices.DeleteFunc(sortedKeys(owned), uncounted), restored))
	if reconciling && (running || (kept != nil) || chbs.Spec.IsSuspended()) {
		c.refreshReady(chbs, cronJobs, kept, out)
	} else {
		c.setReady(chbs, meta.ConditionFalse, wait.reason, wait.message, out)
	}
	return ctrl.Result{RequeueAfter: shorterDelay(wait.requeue, recheck)}, nil
}

// releasedThroughReconcile explains a hold of the CronJob of a cluster through a reconcile of the installation once no
// restore holds it any longer: it stays suspended until the reconcile completes and resumes it.
func releasedThroughReconcile(chiName, cronJob string) *pending {
	return &pending{
		reason: api.BackupReasonSuspendedForRestore,
		message: fmt.Sprintf("CronJob %s stays suspended until ClickHouseInstallation %s completes its reconcile: no restore "+
			"holds it any longer", cronJob, chiName),
		requeue:  pendingRequeueInterval,
		released: true,
	}
}

// heldClusters returns the clusters a held schedule backs up, as far as its CronJobs and its status tell:
// those of its CronJobs, and those of the entries it keeps but for the hosts retention left unpruned when
// a cluster left, which dropLeavingClusters tells by clearing their CronJob name: a cluster whose CronJob
// name another resource holds, or one awaiting garbage collection, has an entry and no CronJob, and still
// keeps the schedule from counting as backed up on every cluster.
func heldClusters(chbs *api.ClickHouseBackupSchedule, owned map[string]*batch.CronJob) []string {
	clusters := sortedKeys(owned)
	for _, cluster := range chbs.Status.Clusters {
		if slices.Contains(clusters, cluster.Name) || ((cluster.CronJobName == "") && (len(cluster.UnprunedHosts) > 0)) {
			continue
		}
		clusters = append(clusters, cluster.Name)
	}
	return clusters
}

// heldThroughReconcile returns the hold a schedule tells through a reconcile of its installation: one found before
// that guards a shared prefix or needs action, which the schedule tells first as when it applies its CronJobs, else a
// restore hold found now, else the one found before.
func heldThroughReconcile(kept, restored *pending) *pending {
	if (kept != nil) && ((kept.reason == api.BackupReasonSharesSourcePrefix) || holdNeedsAction(kept.reason)) {
		return kept
	}
	return cmpPending(restored, kept)
}

// keptHold returns the hold the Ready condition of a schedule records, nil when it records none.
func keptHold(chbs *api.ClickHouseBackupSchedule) *pending {
	ready := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionReady)
	if (ready == nil) || !holdReason(ready.Reason) {
		return nil
	}
	return &pending{reason: ready.Reason, message: ready.Message}
}

// holdReason reports whether a reason of the Ready condition of a schedule tells a hold.
func holdReason(reason string) bool {
	switch reason {
	case api.BackupReasonSuspendedForRestore, api.BackupReasonSuspendedForFailedRestore, api.BackupReasonSharesSourcePrefix,
		api.BackupReasonNameConflict, api.BackupReasonWaitingForGarbageCollection:
		return true
	}
	return false
}

// holdNeedsAction reports whether a hold of a schedule with the given reason lasts until somebody acts:
// deletes a failed restore, renames one of two resources whose names collide, or deletes a CronJob no
// resource controls.
func holdNeedsAction(reason string) bool {
	return (reason == api.BackupReasonSuspendedForFailedRestore) || (reason == api.BackupReasonNameConflict)
}

// restoreHolds tells, of every given cluster, why a restore holds the scheduled backups of the
// schedule's installation there, nil when nothing does: a restore of the cluster, of every cluster, or
// of access entities, which belong to every cluster, validated to start or running, or the Job of one
// that is still running; a restore that failed and may have left the cluster partly restored, until it
// is deleted; and the cluster waiting to be recovered from the backup spec.backup.recovery names, until
// its recovery restore exists, which then holds it as any restore does. reader tells where the
// restores and Jobs are read.
func (c *ScheduleController) restoreHolds(
	ctx context.Context,
	reader client.Reader,
	chbs *api.ClickHouseBackupSchedule,
	chi *api.ClickHouseInstallation,
	clusters []string,
) (map[string]*pending, error) {
	chiName := chbs.Spec.ClickHouseInstallation
	restores := &api.ClickHouseRestoreList{}
	if err := reader.List(ctx, restores, client.InNamespace(chbs.GetNamespace())); err != nil {
		return nil, err
	}
	inProgress := map[string]*pending{}
	failed := map[string]*pending{}
	for i := range restores.Items {
		restore := &restores.Items[i]
		if restore.LocalSource() == chiName {
			// The retention of a run here could delete the backup another installation restores from the
			// storage of this one: the schedules of the clusters it reads are held while it may read
			for _, cluster := range sourceClustersRead(restore, clusters) {
				inProgress[cluster] = cmpPending(inProgress[cluster], sourceRestoreInProgress(chiName, cluster, restore))
			}
			continue
		}
		if (restore.Spec.ClickHouseInstallation != chiName) || !holdsSchedules(restore) {
			continue
		}
		held := heldCluster(restore)
		for _, cluster := range clusters {
			// A restore of every cluster restores this one
			restored := cmp.Or(restore.TargetCluster(), cluster)
			switch {
			case (held != "") && (held != cluster):
			case restore.Status.Phase == api.OperationPhaseFailed:
				failed[cluster] = cmpPending(failed[cluster], failedRestoreHold(chiName, cluster, restored, restore.GetName()))
			default:
				inProgress[cluster] = cmpPending(inProgress[cluster], restoreInProgress(chiName, cluster, restored))
			}
		}
	}

	recovering := map[string]*pending{}
	if chi.GetAnnotations()[api.AnnotationRecoveredFrom] == "" {
		// The recovery starts only once every cluster it recovers has a storage, which the hold tells
		waitsFor := ""
		if lacking := model.RecoveryClusters(model.RecoveriesWithoutStorage(chi.GetAncestorT())); len(lacking) > 0 {
			waitsFor = fmt.Sprintf(", once backup is enabled with a storage on %s", strings.Join(lacking, ", "))
		}
		for _, recovery := range model.Recoveries(chi.GetAncestorT()) {
			restore := model.BuildRecoveryRestore(chi, recovery)
			// A recovery that can never start, and one whose restore was validated, or refused, and holds as
			// any restore does, hold nothing here
			if !slices.Contains(clusters, recovery.Cluster) || (restore.Spec.Validate() != nil) || recoveryRestoreDecided(restores, chi, restore) {
				continue
			}
			recovering[recovery.Cluster] = &pending{
				reason: api.BackupReasonSuspendedForRestore,
				message: fmt.Sprintf("cluster %s of ClickHouseInstallation %s waits to be restored from backup %s%s",
					recovery.Cluster, chiName, recovery.BackupName, waitsFor),
				requeue: targetRequeueInterval,
			}
		}
	}

	active, err := c.activeJobsOf(ctx, reader, chbs.GetNamespace(), model.CHIJobsSelector(chiName, api.ClickHouseRestoreCRDResourceKind))
	if err != nil {
		return nil, err
	}
	holds := map[string]*pending{}
	for _, cluster := range clusters {
		hold := cmpPending(inProgress[cluster], failed[cluster], recovering[cluster])
		if (hold == nil) && slices.ContainsFunc(active, func(job *batch.Job) bool { return model.ObjectCluster(job) == cluster }) {
			hold = restoreInProgress(chiName, cluster, cluster)
		}
		if hold != nil {
			holds[cluster] = hold
		}
	}
	return holds, nil
}

// sourceClustersRead returns the clusters of an installation a restore of another one reads the backup of,
// from those given: every cluster it restores, as the clusters of a backup are read under the names of those
// it restores, once it was validated and until it finished there, but while it waits for the installation it
// restores, which may never come to run it: the schedules of another installation are held only for a restore
// that may start, or runs.
func sourceClustersRead(chr *api.ClickHouseRestore, clusters []string) []string {
	if chr.Status.Phase.IsTerminal() {
		return nil
	}
	var read []string
	for _, cluster := range chr.Status.Clusters {
		if slices.Contains(clusters, cluster.Name) && !waitsForTarget(cluster) && !cluster.Phase.IsTerminal() {
			read = append(read, cluster.Name)
		}
	}
	return read
}

// waitsForTarget reports whether an operation waits on a cluster, before its Job exists, for its installation itself:
// to exist, to complete its reconcile or to be started again.
func waitsForTarget(cluster api.OperationClusterStatus) bool {
	switch {
	case cluster.JobName != "":
		return false
	case (cluster.Reason == api.BackupReasonCHINotFound) || (cluster.Reason == api.BackupReasonCHINotReady):
		return true
	}
	return cluster.Reason == api.BackupReasonCHIStopped
}

// sourceRestoreInProgress explains a hold of the schedules of a cluster by a restore of another installation that
// reads the backup of the cluster.
func sourceRestoreInProgress(chiName, cluster string, chr *api.ClickHouseRestore) *pending {
	return &pending{
		reason: api.BackupReasonSuspendedForRestore,
		message: fmt.Sprintf("ClickHouseRestore %s restores ClickHouseInstallation %s from the backup %s of cluster %s of "+
			"ClickHouseInstallation %s, which the retention of a run here could delete", chr.GetName(),
			chr.Spec.ClickHouseInstallation, chr.Spec.BackupName, cluster, chiName),
		requeue: pendingRequeueInterval,
	}
}

// restoreInProgress explains a hold of the schedules of a cluster by a restore in progress of restored: the
// cluster itself, or another cluster whose restore restores the access entities of every cluster.
func restoreInProgress(chiName, cluster, restored string) *pending {
	message := fmt.Sprintf("a restore of cluster %s of ClickHouseInstallation %s is in progress", cluster, chiName)
	if restored != cluster {
		message = fmt.Sprintf("a restore of cluster %s of ClickHouseInstallation %s, which restores the access entities of every "+
			"cluster, is in progress", restored, chiName)
	}
	return &pending{reason: api.BackupReasonSuspendedForRestore, message: message, requeue: pendingRequeueInterval}
}

// failedRestoreHold explains a hold of the schedules of a cluster by a restore of restored that failed, as
// restoreInProgress does for one in progress, until it is deleted.
func failedRestoreHold(chiName, cluster, restored, restore string) *pending {
	left := "cluster " + cluster
	if restored != cluster {
		left = fmt.Sprintf("cluster %s, and the access entities of every cluster,", restored)
	}
	return &pending{
		reason: api.BackupReasonSuspendedForFailedRestore,
		message: fmt.Sprintf("ClickHouseRestore %s of ClickHouseInstallation %s failed and may have left %s partly restored; "+
			"scheduled backups stay suspended while that restore remains", restore, chiName, left),
		requeue: targetRequeueInterval,
	}
}

// cmpPending returns the first of the given waits that is set.
func cmpPending(waits ...*pending) *pending {
	for _, wait := range waits {
		if wait != nil {
			return wait
		}
	}
	return nil
}

// recoveryRestoreDecided reports whether the restore that recovers a cluster of the installation exists,
// controlled by the installation, and was validated, which makes it hold the cluster as any restore
// does, or refused.
func recoveryRestoreDecided(restores *api.ClickHouseRestoreList, chi *api.ClickHouseInstallation, recovery *api.ClickHouseRestore) bool {
	return slices.ContainsFunc(restores.Items, func(restore api.ClickHouseRestore) bool {
		return (restore.GetName() == recovery.GetName()) && meta.IsControlledBy(&restore, chi) &&
			((len(restore.Status.Clusters) > 0) || restore.Status.Phase.IsTerminal())
	})
}

// combineHolds returns what holds the schedule on the given clusters, nil when nothing does: a hold
// that needs somebody to act comes first, the clusters held tell the message. The hold returned is
// released only when every hold it combines is.
func combineHolds(clusters []string, holds map[string]*pending) *pending {
	var combined *pending
	var messages []string
	released := true
	for _, cluster := range clusters {
		hold := holds[cluster]
		if hold == nil {
			continue
		}
		if !slices.Contains(messages, hold.message) {
			messages = append(messages, hold.message)
		}
		if (combined == nil) || (holdNeedsAction(hold.reason) && !holdNeedsAction(combined.reason)) {
			combined = &pending{reason: hold.reason, requeue: hold.requeue}
		}
		combined.requeue = shorterDelay(combined.requeue, hold.requeue)
		released = released && hold.released
	}
	if combined != nil {
		combined.message = strings.Join(messages, "; ")
		combined.released = released
	}
	return combined
}

// ownedCronJobs returns the CronJobs of the schedule by the cluster each runs the schedule on.
func (c *ScheduleController) ownedCronJobs(ctx context.Context, chbs *api.ClickHouseBackupSchedule) (map[string]*batch.CronJob, error) {
	cronJobs := &batch.CronJobList{}
	if err := c.List(ctx, cronJobs, client.InNamespace(chbs.GetNamespace()), client.MatchingLabelsSelector{
		Selector: model.OwnedJobsSelector(api.ClickHouseBackupScheduleCRDResourceKind, chbs.GetName()),
	}); err != nil {
		return nil, err
	}
	owned := map[string]*batch.CronJob{}
	for i := range cronJobs.Items {
		cronJob := &cronJobs.Items[i]
		if !meta.IsControlledBy(cronJob, chbs) {
			continue
		}
		if cluster, found := cronJobCluster(chbs, cronJob); found {
			owned[cluster] = cronJob
		}
	}
	return owned, nil
}

// cronJobCluster returns the cluster a CronJob of the schedule runs the schedule on, as its cluster label tells, or,
// when a user stripped or changed that, the one of the Jobs it starts, or else one the status of the schedule
// records: the name the schedule gives the CronJob of the cluster confirms it, false when none does. A label of the
// wrong cluster would have the CronJob deleted as the one of a cluster the schedule no longer backs up, or held as
// another, and only a schedule that applies its CronJobs labels it again.
func cronJobCluster(chbs *api.ClickHouseBackupSchedule, cronJob *batch.CronJob) (string, bool) {
	clusters := []string{model.ObjectCluster(cronJob), model.ObjectCluster(&cronJob.Spec.JobTemplate)}
	for _, cluster := range chbs.Status.Clusters {
		clusters = append(clusters, cluster.Name)
	}
	for _, cluster := range clusters {
		if cronJob.GetName() == model.CronJobName(chbs, cluster) {
			return cluster, true
		}
	}
	return "", false
}

func (c *ScheduleController) getCronJob(ctx context.Context, chbs *api.ClickHouseBackupSchedule, cluster string) (*batch.CronJob, error) {
	cronJob := &batch.CronJob{}
	// The cache holds only the CronJobs stamped as the operator stamps its own: one made by hand would be created again
	// and again otherwise, which the API server refuses as it exists already
	err := c.getConfirmed(ctx, kubeTypes.NamespacedName{Namespace: chbs.GetNamespace(), Name: model.CronJobName(chbs, cluster)}, cronJob)
	switch {
	case apiErrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return cronJob, nil
}

// applyCronJob creates the CronJob, or updates it when the spec it was built from changed, or when a user stripped
// or changed a label the operator stamps on it or on the Jobs it starts: the cache selects both by those, a restore
// that waits for the CronJob to be suspended selects it, and one that checks for a backup running selects the Jobs.
// Labels a user added to the CronJob stay. A CronJob the operator suspends records when, which a restore waits on.
func (c *ScheduleController) applyCronJob(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	existing *batch.CronJob,
	desired *batch.CronJob,
) (*batch.CronJob, error) {
	suspend := cronJobSuspended(desired)
	if existing == nil {
		if suspend {
			recordSuspension(desired)
		}
		if err := ctrl.SetControllerReference(chbs, desired, c.scheme); err != nil {
			return nil, err
		}
		if err := c.Create(ctx, desired); err != nil {
			return nil, err
		}
		log.V(1).M(c.meta(chbs)).F().Info("created CronJob %s", desired.GetName())
		return desired, nil
	}

	version := desired.GetAnnotations()[model.AnnotationBackupObjectVersion]
	_, recorded := suspendedAt(existing)
	labeled := hasLabels(existing, desired.GetLabels()) && hasLabels(&existing.Spec.JobTemplate, desired.Spec.JobTemplate.GetLabels())
	if (existing.GetAnnotations()[model.AnnotationBackupObjectVersion] == version) && (recorded == suspend) && labeled {
		return existing, nil
	}
	updated := existing.DeepCopy()
	updated.SetLabels(util.MergeStringMapsOverwrite(updated.GetLabels(), desired.GetLabels()))
	meta.SetMetaDataAnnotation(&updated.ObjectMeta, model.AnnotationBackupObjectVersion, version)
	updated.Spec = desired.Spec
	switch {
	case !suspend:
		delete(updated.Annotations, model.AnnotationCronJobSuspendedAt)
	case !recorded || !cronJobSuspended(existing):
		recordSuspension(updated)
	}
	if err := c.Update(ctx, updated); err != nil {
		return nil, err
	}
	log.V(1).M(c.meta(chbs)).F().Info("updated CronJob %s", updated.GetName())
	return updated, nil
}

// hasLabels reports whether obj carries every one of labels.
func hasLabels(obj meta.Object, labels map[string]string) bool {
	return k8sLabels.SelectorFromSet(labels).Matches(k8sLabels.Set(obj.GetLabels()))
}

// suspendCronJob suspends a CronJob outside of the spec it was built from. The recorded version
// is dropped with it, so the next valid build replaces the CronJob even when nothing else
// changed. A suspended CronJob without a record of when, which a restore would wait on forever, is
// recorded as suspended now.
func (c *ScheduleController) suspendCronJob(ctx context.Context, cronJob *batch.CronJob) error {
	_, recorded := suspendedAt(cronJob)
	if cronJobSuspended(cronJob) && recorded {
		return nil
	}
	base := cronJob.DeepCopy()
	if !cronJobSuspended(cronJob) {
		cronJob.Spec.Suspend = ptr.To(true)
		delete(cronJob.Annotations, model.AnnotationBackupObjectVersion)
	}
	recordSuspension(cronJob)
	return c.Patch(ctx, cronJob, client.MergeFrom(base))
}

// cronJobSuspended reports whether a CronJob is suspended.
func cronJobSuspended(cronJob *batch.CronJob) bool {
	return ptr.Deref(cronJob.Spec.Suspend, false)
}

// recordSuspension records on a CronJob that the operator suspends it now, to the nanosecond: a record
// rounded to the second would let a restore start up to a second before the suspension is as old as it
// waits for.
func recordSuspension(cronJob *batch.CronJob) {
	meta.SetMetaDataAnnotation(&cronJob.ObjectMeta, model.AnnotationCronJobSuspendedAt, meta.Now().UTC().Format(time.RFC3339Nano))
}

// suspendedAt returns when the operator recorded that it suspended a CronJob, false when it has no
// record of it, or one it cannot read, which is as good as none.
func suspendedAt(cronJob *batch.CronJob) (time.Time, bool) {
	value, found := cronJob.GetAnnotations()[model.AnnotationCronJobSuspendedAt]
	if !found {
		return time.Time{}, false
	}
	suspended, err := time.Parse(time.RFC3339Nano, value)
	return suspended, err == nil
}

// syncStatus takes over what the CronJob of a cluster tells. A success it knows of and the schedule
// does not, of a run whose Job is gone before it was seen, records no backup name: the one recorded is
// older.
func (c *ScheduleController) syncStatus(chbs *api.ClickHouseBackupSchedule, cluster *api.ScheduleClusterStatus, cronJob *batch.CronJob) {
	cluster.CronJobName = cronJob.GetName()
	cluster.Storage = model.ObjectStorage(cronJob)
	cluster.LastScheduleTime = cronJob.Status.LastScheduleTime.DeepCopy()
	if succeeded := cronJob.Status.LastSuccessfulTime; later(succeeded, cluster.LastSuccessfulTime) {
		recordSuccess(cluster, *succeeded, "")
	}
}

// later reports whether a time is set and after another one, which may be unset.
func later(t, than *meta.Time) bool {
	return (t != nil) && ((than == nil) || t.After(than.Time))
}

// lastBackupFailed reports whether the last finished scheduled run failed on a cluster.
func lastBackupFailed(chbs *api.ClickHouseBackupSchedule) bool {
	return apiMeta.IsStatusConditionFalse(chbs.Status.Conditions, api.BackupConditionLastBackupSucceeded)
}

// lastBackupWithoutData returns the LastBackupSucceeded condition of a schedule whose last finished
// runs made backups that hold no data, nil otherwise.
func lastBackupWithoutData(chbs *api.ClickHouseBackupSchedule) *meta.Condition {
	lastRun := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionLastBackupSucceeded)
	if (lastRun == nil) || (lastRun.Reason != api.BackupReasonCompletedWithoutData) {
		return nil
	}
	return lastRun
}

// recordSuccess records a successful run of a cluster that finished at the given time, and the
// backup it made, unless a later success is recorded already.
func recordSuccess(cluster *api.ScheduleClusterStatus, finished meta.Time, backupName string) {
	if later(cluster.LastSuccessfulTime, &finished) {
		return
	}
	cluster.LastSuccessfulTime = &finished
	cluster.LastBackupName = backupName
}

// recordFailure records a failed run of a cluster that finished at the given time, unless a later
// failure is recorded already.
func recordFailure(cluster *api.ScheduleClusterStatus, finished meta.Time, message string) {
	if !later(&finished, cluster.LastFailureTime) {
		return
	}
	cluster.LastFailureTime = &finished
	cluster.LastFailureMessage = message
}

func (c *ScheduleController) setCondition(chbs *api.ClickHouseBackupSchedule, conditionType string, status meta.ConditionStatus, reason, message string) {
	setCondition(&chbs.Status.Conditions, chbs.GetGeneration(), conditionType, status, reason, message)
}

// refreshReady records whether scheduled backups run, as the runs of the CronJobs tell, unless hold
// holds the schedule. A released hold is recorded unannounced: it follows a restore hold, which the
// Ready condition told already, and the CronJobs are announced resumed once they resume.
func (c *ScheduleController) refreshReady(chbs *api.ClickHouseBackupSchedule, cronJobs []string, hold *pending, out *announcements) {
	withoutData := lastBackupWithoutData(chbs)
	switch {
	case (hold != nil) && hold.released:
		c.setCondition(chbs, api.BackupConditionReady, meta.ConditionFalse, hold.reason, hold.message)
	case hold != nil:
		c.setReady(chbs, meta.ConditionFalse, hold.reason, hold.message, out)
	case chbs.Spec.IsSuspended():
		c.setReady(chbs, meta.ConditionFalse, api.BackupReasonSuspended, "the schedule is suspended", out)
	case lastBackupFailed(chbs):
		lastRun := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionLastBackupSucceeded)
		c.setReady(chbs, meta.ConditionFalse, api.BackupReasonLastBackupFailed, fmt.Sprintf(
			"the last scheduled backup failed, %s the schedule on: %s", cronJobsRun(cronJobs), lastRun.Message), out)
	case withoutData != nil:
		c.setReady(chbs, meta.ConditionTrue, api.BackupReasonLastBackupWithoutData, fmt.Sprintf(
			"the last scheduled backup holds no data, %s the schedule on: %s", cronJobsRun(cronJobs), withoutData.Message), out)
	default:
		c.setReady(chbs, meta.ConditionTrue, api.BackupReasonScheduled, fmt.Sprintf("%s the schedule", cronJobsRun(cronJobs)), out)
	}
}

// cronJobsRun names the CronJobs that run a schedule as the subject of a sentence.
func cronJobsRun(cronJobs []string) string {
	if len(cronJobs) == 1 {
		return fmt.Sprintf("CronJob %s runs", cronJobs[0])
	}
	return fmt.Sprintf("CronJobs %s run", strings.Join(cronJobs, ", "))
}

// cronJobsCreatedMessage tells the CronJobs a schedule created.
func cronJobsCreatedMessage(cronJobs []string) string {
	if len(cronJobs) == 1 {
		return fmt.Sprintf("CronJob %s created", cronJobs[0])
	}
	return fmt.Sprintf("CronJobs %s created", strings.Join(cronJobs, ", "))
}

// setReady records whether scheduled backups run, announcing a suspension for a restore or for the
// backup name prefix, and the resumption of scheduled backups after it.
func (c *ScheduleController) setReady(chbs *api.ClickHouseBackupSchedule, status meta.ConditionStatus, reason, message string, out *announcements) {
	previous := ""
	if condition := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionReady); condition != nil {
		previous = condition.Reason
	}
	c.setCondition(chbs, api.BackupConditionReady, status, reason, message)

	switch {
	case previous == reason:
	case reason == api.BackupReasonSuspendedForRestore:
		out.add(c.info(chbs, a.EventActionUpdate, reason, "scheduled backups suspended: %s", message))
	case (reason == api.BackupReasonSuspendedForFailedRestore) || (reason == api.BackupReasonSharesSourcePrefix):
		out.add(c.warning(chbs, a.EventActionUpdate, reason, "scheduled backups suspended: %s", message))
	case reason == api.BackupReasonNameConflict:
		out.add(c.warning(chbs, a.EventActionUpdate, reason, "%s", message))
	case !runsSchedule(reason):
	case holdReason(previous):
		out.add(c.info(chbs, a.EventActionUpdate, reason, "scheduled backups resumed"))
	}
}

// runsSchedule reports whether the CronJobs run the schedule, as the Ready condition of the schedule
// with the given reason tells.
func runsSchedule(reason string) bool {
	switch reason {
	case api.BackupReasonScheduled, api.BackupReasonLastBackupFailed, api.BackupReasonLastBackupWithoutData:
		return true
	}
	return false
}

// runOutcome is how a finished run of a cluster went, as its Job tells.
type runOutcome struct {
	finished meta.Time
	failed   bool
	// reason tells why a failed run failed
	reason string
	// noData tells why the backup a successful run made holds no data, "" when it holds data
	noData string
}

// runOutcomes holds the newest finished run of every cluster a reconcile saw.
type runOutcomes map[string]runOutcome

func (o runOutcomes) add(cluster string, outcome runOutcome) {
	if previous, found := o[cluster]; !found || outcome.finished.After(previous.finished.Time) {
		o[cluster] = outcome
	}
}

// observeScheduledJobs counts and announces the runs of the CronJob of a cluster. A pod that stays
// pending changes nothing on its Job, so for a run whose pod has not started yet it also returns
// when that run is to be checked again, zero when no run waits for its pod.
func (c *ScheduleController) observeScheduledJobs(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	cluster *api.ScheduleClusterStatus,
	jobs []*batch.Job,
	outcomes runOutcomes,
) (time.Duration, error) {
	var recheck time.Duration
	for _, job := range jobs {
		if err := c.observeScheduledJob(ctx, chbs, cluster, job, outcomes); err != nil {
			return 0, err
		}
		if (stateOf(job) == jobActive) && !jobPodStarted(job) {
			recheck = shorterDelay(recheck, untilStartTimeout(job)+startTimeoutRecheckDelay)
		}
	}
	return recheck, nil
}

// scheduledRuns returns the Jobs of the runs of the schedule's CronJobs, by the CronJob that created
// each.
func (c *ScheduleController) scheduledRuns(ctx context.Context, chbs *api.ClickHouseBackupSchedule) (map[kubeTypes.UID][]*batch.Job, error) {
	jobs := &batch.JobList{}
	if err := c.List(ctx, jobs, client.InNamespace(chbs.GetNamespace()), client.MatchingLabelsSelector{
		Selector: model.OwnedJobsSelector(api.ClickHouseBackupScheduleCRDResourceKind, chbs.GetName()),
	}); err != nil {
		return nil, err
	}
	runs := map[kubeTypes.UID][]*batch.Job{}
	for i := range jobs.Items {
		if owner := meta.GetControllerOf(&jobs.Items[i]); owner != nil {
			runs[owner.UID] = append(runs[owner.UID], &jobs.Items[i])
		}
	}
	return runs, nil
}

// shorterDelay returns the shorter of two requeue delays, zero meaning none.
func shorterDelay(x, y time.Duration) time.Duration {
	if (x == 0) || ((y > 0) && (y < x)) {
		return y
	}
	return x
}

// observeScheduledJob records and reports a phase of a scheduled run once. The run is settled as the
// Job of a one-off operation is, a run whose pod never started stopped. The outcome of a finished
// run goes to the status, then the phase is recorded on the Job, guarded by its resourceVersion,
// and reported only once that write succeeded; whichever reconcile loses the race reports nothing.
//
// The status may lose the outcome to a write conflict after the Job has recorded it, so the outcome
// of a run reported before is recorded again whenever the status misses it.
func (c *ScheduleController) observeScheduledJob(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	cluster *api.ScheduleClusterStatus,
	job *batch.Job,
	outcomes runOutcomes,
) error {
	observed := api.OperationPhase(job.GetAnnotations()[model.AnnotationScheduledJobObserved])
	if observed.IsTerminal() {
		return c.recordAgain(ctx, chbs, cluster, job, observed, outcomes)
	}
	state, err := c.settle(ctx, chbs, job)
	if err != nil {
		return err
	}
	var phase api.OperationPhase
	switch state {
	case jobSucceeded:
		phase = api.OperationPhaseCompleted
	case jobFailed:
		phase = api.OperationPhaseFailed
	default:
		phase = api.OperationPhaseRunning
	}
	if observed == phase {
		return nil
	}
	// The outcome is taken before the record below replaces the Job with what the API server stored,
	// where a stop the cluster refused left no trace.
	reason := failureReason(job)
	failed := jobFailureTime(job)
	withoutData := false
	var message string
	var completed func()
	switch phase {
	case api.OperationPhaseCompleted:
		if withoutData, err = c.backupHoldsNoData(ctx, job); err != nil {
			return err
		}
		completed = c.recordCompletion(chbs, cluster, job, withoutData, outcomes)
	case api.OperationPhaseFailed:
		failure, err := c.describeFailure(ctx, job, reason)
		if err != nil {
			return err
		}
		message = failure.message
		recordFailure(cluster, failed, message)
		outcomes.add(cluster.Name, runOutcome{finished: failed, failed: true, reason: reason})
	}

	base := job.DeepCopy()
	meta.SetMetaDataAnnotation(&job.ObjectMeta, model.AnnotationScheduledJobObserved, string(phase))
	if withoutData {
		meta.SetMetaDataAnnotation(&job.ObjectMeta, model.AnnotationScheduledJobWithoutData, strconv.FormatBool(true))
	}
	if err := c.Patch(ctx, job, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apiErrors.IsConflict(err) || apiErrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	namespace, chiName, clusterName := chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, cluster.Name
	if observed == "" {
		metrics.BackupStarted(ctx, namespace, chiName, clusterName)
	}
	switch phase {
	case api.OperationPhaseCompleted:
		metrics.BackupCompleted(ctx, namespace, chiName, clusterName, jobDuration(job), jobCompletionTime(job).Time)
		completed()
	case api.OperationPhaseFailed:
		metrics.BackupFailed(ctx, namespace, chiName, clusterName, failed.Time)
		c.warning(chbs, a.EventActionReconcile, reason, "scheduled backup of cluster %s failed: %s", clusterName, message)()
	}
	return nil
}

// recordCompletion records a successful scheduled run of a cluster in the status, and returns its
// announcement: a warning when withoutData tells that the backup the run made holds no data.
func (c *ScheduleController) recordCompletion(
	chbs *api.ClickHouseBackupSchedule,
	cluster *api.ScheduleClusterStatus,
	job *batch.Job,
	withoutData bool,
	outcomes runOutcomes,
) func() {
	backupName := model.ScheduledBackupName(job)
	noData := ""
	if withoutData {
		noData = scheduledNoDataCause(job)
	}
	finished := jobCompletionTime(job)
	recordSuccess(cluster, finished, backupName)
	outcomes.add(cluster.Name, runOutcome{finished: finished, noData: noData})
	message := fmt.Sprintf("scheduled backup Job %s completed", job.GetName())
	if backupName != "" {
		message = fmt.Sprintf("scheduled backup %s completed in Job %s", backupName, job.GetName())
	}
	if noData == "" {
		return c.info(chbs, a.EventActionReconcile, api.BackupReasonCompleted, "%s", message)
	}
	return c.warning(chbs, a.EventActionReconcile, api.BackupReasonCompletedWithoutData, "%s, but the backup holds no data: %s", message, noData)
}

// scheduledNoDataCause tells why the backup a scheduled run made holds no data, and that retention
// does not count it among the backups with data it keeps, as the Job of the run was built.
func scheduledNoDataCause(job *batch.Job) string {
	cause := noDataCause(job)
	if keep := model.JobKeepLast(job); keep > 0 {
		return fmt.Sprintf("%s; it does not count toward the %d backups with data that retention keeps", cause, keep)
	}
	return cause
}

// observedWithoutData reports whether a scheduled run was reported completed with a backup that
// holds no data.
func observedWithoutData(job *batch.Job) bool {
	_, observed := job.GetAnnotations()[model.AnnotationScheduledJobWithoutData]
	return observed
}

// recordAgain records the outcome of a run reported before once more, unless the status has it
// already: the status write that recorded it may have been lost to a conflict after the Job
// recorded it. The Job also records whether the backup of a run reported completed holds no data,
// so its pod is not asked again. A run reported failed whose Job has not failed by itself, as a
// stop the cluster refused leaves it, is left as reported.
func (c *ScheduleController) recordAgain(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	cluster *api.ScheduleClusterStatus,
	job *batch.Job,
	observed api.OperationPhase,
	outcomes runOutcomes,
) error {
	switch {
	case observed == api.OperationPhaseCompleted:
		c.recordCompletion(chbs, cluster, job, observedWithoutData(job), outcomes)
	case (stateOf(job) != jobFailed) && !jobStopped(job):
	default:
		finished := jobFailureTime(job)
		reason := failureReason(job)
		outcomes.add(cluster.Name, runOutcome{finished: finished, failed: true, reason: reason})
		if !later(&finished, cluster.LastFailureTime) {
			return nil
		}
		failure, err := c.describeFailure(ctx, job, reason)
		if err != nil {
			return err
		}
		recordFailure(cluster, finished, failure.message)
	}
	return nil
}

// aggregate adds the clusters the schedule backs up up into its own status: when it last ran, the
// oldest of their last successes, by when every cluster had a backup, the newest backup every cluster
// made, the last failure on any cluster, and the LastBackupSucceeded condition, which a failure on any
// cluster turns false. outcomes holds the newest finished runs the reconcile saw, which tell the
// reason of a failure and whether a backup holds data.
func (c *ScheduleController) aggregate(chbs *api.ClickHouseBackupSchedule, clusters []string, outcomes runOutcomes) {
	var lastSchedule, lastFailure, oldestSuccess *meta.Time
	failureMessage := ""
	succeeded := 0
	lastRunFailed := false
	var backupNames []string
	for i := range chbs.Status.Clusters {
		cluster := &chbs.Status.Clusters[i]
		if later(cluster.LastScheduleTime, lastSchedule) {
			lastSchedule = cluster.LastScheduleTime
		}
		if later(cluster.LastFailureTime, lastFailure) {
			lastFailure, failureMessage = cluster.LastFailureTime, cluster.LastFailureMessage
		}
		if !slices.Contains(clusters, cluster.Name) {
			continue
		}
		if cluster.LastSuccessfulTime != nil {
			succeeded++
			if (oldestSuccess == nil) || cluster.LastSuccessfulTime.Before(oldestSuccess) {
				oldestSuccess = cluster.LastSuccessfulTime
			}
		}
		lastRunFailed = lastRunFailed || later(cluster.LastFailureTime, cluster.LastSuccessfulTime)
		if !slices.Contains(backupNames, cluster.LastBackupName) {
			backupNames = append(backupNames, cluster.LastBackupName)
		}
	}
	everyClusterSucceeded := (len(clusters) > 0) && (succeeded == len(clusters))
	chbs.Status.LastScheduleTime = lastSchedule.DeepCopy()
	chbs.Status.LastFailureTime = lastFailure.DeepCopy()
	chbs.Status.LastFailureMessage = failureMessage
	chbs.Status.LastSuccessfulTime = nil
	if everyClusterSucceeded {
		chbs.Status.LastSuccessfulTime = oldestSuccess.DeepCopy()
	}
	// The runs of one slot make one backup name on every cluster: a name every cluster made last is the
	// newest backup of every cluster. While the runs of a slot complete one after the other the one
	// before stays, as every cluster made it. A cluster that has no success yet, having joined the
	// schedule, never made it; while one whose last run failed stays behind, the retention of the others
	// prunes it once they made enough newer ones: no backup is known to be complete then.
	switch {
	case !everyClusterSucceeded:
		chbs.Status.LastBackupName = ""
	case len(backupNames) == 1:
		chbs.Status.LastBackupName = backupNames[0]
	case lastRunFailed:
		chbs.Status.LastBackupName = ""
	}
	c.setLastRun(chbs, clusters, outcomes)
}

// setLastRun records in the LastBackupSucceeded condition how the last finished runs of the clusters
// the schedule backs up went: false once the last run of any of them failed, true once each of them
// succeeded last, with or without data.
func (c *ScheduleController) setLastRun(chbs *api.ClickHouseBackupSchedule, clusters []string, outcomes runOutcomes) {
	var failed, succeeded []*api.ScheduleClusterStatus
	for _, name := range clusters {
		cluster := findScheduleCluster(chbs, name)
		switch {
		case cluster == nil:
		case later(cluster.LastFailureTime, cluster.LastSuccessfulTime):
			failed = append(failed, cluster)
		case cluster.LastSuccessfulTime != nil:
			succeeded = append(succeeded, cluster)
		}
	}
	var messages []string
	if len(failed) > 0 {
		newest := failed[0]
		for _, cluster := range failed {
			messages = append(messages, clusterMessage(len(clusters), cluster.Name, cluster.LastFailureMessage))
			if later(cluster.LastFailureTime, newest.LastFailureTime) {
				newest = cluster
			}
		}
		reason := api.BackupReasonJobFailed
		if outcome, found := outcomes[newest.Name]; found && outcome.failed && outcome.finished.Equal(newest.LastFailureTime) {
			reason = outcome.reason
		} else if lastRun := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionLastBackupSucceeded); (lastRun != nil) && (lastRun.Status == meta.ConditionFalse) {
			reason = lastRun.Reason
		}
		c.setCondition(chbs, api.BackupConditionLastBackupSucceeded, meta.ConditionFalse, reason, strings.Join(messages, "; "))
		return
	}
	if len(succeeded) == 0 {
		return
	}
	withoutData := true
	for _, cluster := range succeeded {
		noData := ""
		if outcome, found := outcomes[cluster.Name]; found && !outcome.failed && outcome.finished.Equal(cluster.LastSuccessfulTime) {
			noData = outcome.noData
		}
		withoutData = withoutData && (noData != "")
		messages = append(messages, clusterMessage(len(clusters), cluster.Name, successMessage(cluster, noData)))
	}
	reason := api.BackupReasonCompleted
	if withoutData {
		reason = api.BackupReasonCompletedWithoutData
	}
	c.setCondition(chbs, api.BackupConditionLastBackupSucceeded, meta.ConditionTrue, reason, strings.Join(messages, "; "))
}

// successMessage tells the last successful run of a cluster, and why the backup it made holds no data
// when noData tells.
func successMessage(cluster *api.ScheduleClusterStatus, noData string) string {
	finished := cluster.LastSuccessfulTime.UTC().Format(time.RFC3339)
	message := fmt.Sprintf("a scheduled backup completed at %s", finished)
	if cluster.LastBackupName != "" {
		message = fmt.Sprintf("scheduled backup %s completed at %s", cluster.LastBackupName, finished)
	}
	if noData != "" {
		message = fmt.Sprintf("%s, but the backup holds no data: %s", message, noData)
	}
	return message
}

// findScheduleCluster returns the status of the schedule on a cluster, nil when it has none.
func findScheduleCluster(chbs *api.ClickHouseBackupSchedule, name string) *api.ScheduleClusterStatus {
	for i := range chbs.Status.Clusters {
		if chbs.Status.Clusters[i].Name == name {
			return &chbs.Status.Clusters[i]
		}
	}
	return nil
}

// scheduleCluster returns the status of the schedule on a cluster, added when it has none.
func scheduleCluster(chbs *api.ClickHouseBackupSchedule, name string) *api.ScheduleClusterStatus {
	if cluster := findScheduleCluster(chbs, name); cluster != nil {
		return cluster
	}
	chbs.Status.Clusters = append(chbs.Status.Clusters, api.ScheduleClusterStatus{Name: name})
	return &chbs.Status.Clusters[len(chbs.Status.Clusters)-1]
}

// orderClusters orders the status of the schedule by cluster: the clusters it backs up in their order,
// then the ones it no longer backs up as they were.
func orderClusters(chbs *api.ClickHouseBackupSchedule, clusters []string) {
	slices.SortStableFunc(chbs.Status.Clusters, func(x, y api.ScheduleClusterStatus) int {
		return clusterRank(clusters, x.Name) - clusterRank(clusters, y.Name)
	})
}

// clusterRank ranks a cluster by its position among the given clusters, after all of them when it is
// not one.
func clusterRank(clusters []string, cluster string) int {
	if index := slices.Index(clusters, cluster); index >= 0 {
		return index
	}
	return len(clusters)
}

// sortedKeys returns the keys of a map in order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// SchedulesForCHI maps an installation to the schedules that back it up, so their CronJobs
// follow its topology.
func (c *ScheduleController) SchedulesForCHI(ctx context.Context, obj client.Object) []reconcile.Request {
	return c.schedules(ctx, obj.GetNamespace(), func(chbs *api.ClickHouseBackupSchedule) bool {
		return chbs.Spec.ClickHouseInstallation == obj.GetName()
	})
}

// SchedulesForRestore maps a restore to the schedules of its installation, which it holds, and to those of the
// installation of its namespace whose backup it reads, which it holds as well.
func (c *ScheduleController) SchedulesForRestore(ctx context.Context, obj client.Object) []reconcile.Request {
	chr, ok := obj.(*api.ClickHouseRestore)
	if !ok {
		return nil
	}
	return c.schedules(ctx, obj.GetNamespace(), func(chbs *api.ClickHouseBackupSchedule) bool {
		return (chbs.Spec.ClickHouseInstallation == chr.Spec.ClickHouseInstallation) || (chbs.Spec.ClickHouseInstallation == chr.LocalSource())
	})
}

// SchedulesForJob maps a Job to the schedules it concerns: a restore Job holds the schedules of
// its installation, and a scheduled backup Job reports to its schedule.
func (c *ScheduleController) SchedulesForJob(ctx context.Context, obj client.Object) []reconcile.Request {
	labels := obj.GetLabels()
	switch labels[model.LabelBackupCRKind] {
	case api.ClickHouseRestoreCRDResourceKind:
		return c.schedules(ctx, obj.GetNamespace(), func(chbs *api.ClickHouseBackupSchedule) bool {
			return model.LabelValue(chbs.Spec.ClickHouseInstallation) == labels[model.LabelBackupCHI]
		})
	case api.ClickHouseBackupScheduleCRDResourceKind:
		return c.schedules(ctx, obj.GetNamespace(), func(chbs *api.ClickHouseBackupSchedule) bool {
			return model.LabelValue(chbs.GetName()) == labels[model.LabelBackupCRName]
		})
	}
	return nil
}

func (c *ScheduleController) schedules(ctx context.Context, namespace string, match func(*api.ClickHouseBackupSchedule) bool) []reconcile.Request {
	list := &api.ClickHouseBackupScheduleList{}
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		log.V(1).M(namespace, "").F().Warning("unable to list schedules: %v", err)
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		if match(&list.Items[i]) {
			requests = append(requests, reconcile.Request{NamespacedName: kubeTypes.NamespacedName{
				Namespace: list.Items[i].GetNamespace(),
				Name:      list.Items[i].GetName(),
			}})
		}
	}
	return requests
}
