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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

const (
	defaultSuccessfulRunsHistoryLimit = 3
	defaultFailedRunsHistoryLimit     = 1
)

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

func (c *ScheduleController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if c.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}

	cur := &api.ClickHouseBackupSchedule{}
	if err := c.Get(ctx, req.NamespacedName, cur); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if deleting(cur) {
		return c.finalize(ctx, cur)
	}
	if err := c.addFinalizer(ctx, cur, scheduleFinalizer); err != nil {
		return ignoreConflict(err)
	}
	next := cur.DeepCopy()
	var out announcements
	c.applyTaskID(next, &out)
	result, err := c.reconcile(ctx, next, &out)
	if err != nil {
		return ctrl.Result{}, err
	}
	c.refreshEntries(next)
	c.refreshPhase(next, &out)
	next.Status.Complete()
	if delay := c.refreshNextScheduleTime(next); delay > 0 {
		result.RequeueAfter = shorterDelay(result.RequeueAfter, delay)
	}
	return c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), result, out)
}

func (c *ScheduleController) refreshPhase(chbs *api.ClickHouseBackupSchedule, out *announcements) {
	previous := chbs.Status.Phase
	validated := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionValidated)
	lastRun := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionLastBackupSucceeded)
	ready := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionReady)
	phase, reason, message := api.SchedulePhaseActive, reasonScheduleActive, "the schedule runs"
	switch {
	case chbs.Spec.IsSuspended():
		phase, reason, message = api.SchedulePhaseSuspended, api.BackupReasonSuspended, "the schedule is suspended"
	case (validated != nil) && (validated.Status == meta.ConditionFalse) && abortsSchedule(validated.Reason):
		phase, reason, message = api.SchedulePhaseAborted, validated.Reason, validated.Message
	case (ready != nil) && (ready.Status == meta.ConditionFalse) && (ready.Reason == api.BackupReasonSharesSourcePrefix):
		phase, reason, message = api.SchedulePhaseAborted, ready.Reason, ready.Message
	case (lastRun != nil) && (lastRun.Status == meta.ConditionFalse):
		phase, reason, message = api.SchedulePhaseAborted, api.BackupReasonLastBackupFailed, lastRun.Message
	case ready != nil:
		reason, message = ready.Reason, ready.Message
	}
	chbs.Status.Phase, chbs.Status.Reason, chbs.Status.Message = phase, reason, message
	if previous == phase {
		return
	}
	switch phase {
	case api.SchedulePhaseAborted:
		out.add(c.warning(chbs, a.EventActionUpdate, reasonScheduleAborted, "schedule aborted (%s): %s", reason, message))
	case api.SchedulePhaseSuspended:
		out.add(c.info(chbs, a.EventActionUpdate, reasonScheduleSuspended, "schedule suspended"))
	default:
		out.add(c.info(chbs, a.EventActionUpdate, reasonScheduleActive, "schedule active: %s", message))
	}
}

func (c *ScheduleController) refreshEntries(chbs *api.ClickHouseBackupSchedule) {
	location, err := chbs.Spec.GetLocation()
	if (err != nil) || (len(chbs.Spec.Schedules) == 0) {
		return
	}
	type observed struct {
		scheduled, completed time.Time
		backupName           string
	}
	byPeriod := map[api.BackupPeriod]*observed{}
	for _, scheduled := range append(slices.Clone(chbs.Status.Active), chbs.Status.RecentRuns...) {
		slot := scheduled.Slot.Time
		for _, entry := range model.MatchingEntries(chbs.Spec.Schedules, slot.In(location)) {
			seen := byPeriod[entry.Period]
			if seen == nil {
				seen = &observed{}
				byPeriod[entry.Period] = seen
			}
			if slot.After(seen.scheduled) {
				seen.scheduled = slot
			}
			if (scheduled.Phase == api.OperationPhaseCompleted) && slot.After(seen.completed) {
				seen.completed, seen.backupName = slot, scheduled.BackupName
			}
		}
	}
	var entries []api.ScheduleEntryStatus
	for _, entry := range chbs.Spec.Schedules {
		status := api.ScheduleEntryStatus{Period: entry.Period}
		if recorded := chbs.Status.GetEntry(entry.Period); recorded != nil {
			status = *recorded.DeepCopy()
		}
		if seen := byPeriod[entry.Period]; seen != nil {
			if scheduled := (meta.Time{Time: seen.scheduled.UTC()}); later(&scheduled, status.LastScheduleTime) {
				status.LastScheduleTime = &scheduled
			}
			if seen.backupName != "" {
				status.LastBackupName = seen.backupName
			}
		}
		entries = append(entries, status)
	}
	chbs.Status.Entries = entries
}

func (c *ScheduleController) reconcile(ctx context.Context, chbs *api.ClickHouseBackupSchedule, out *announcements) (ctrl.Result, error) {
	recheck, err := c.observeActive(ctx, chbs, out)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := chbs.Spec.Validate(); err != nil {
		reportScheduleOutcome(chbs)
		c.setCondition(chbs, api.BackupConditionValidated, meta.ConditionFalse, api.BackupReasonInvalidSpec, err.Error())
		c.setReady(chbs, meta.ConditionFalse, api.BackupReasonInvalidSpec, err.Error(), out)
		return ctrl.Result{RequeueAfter: recheck}, nil
	}
	if changeID := model.TrackScheduleRetention(chbs, c.clock.Now()); changeID != "" {
		out.add(c.warning(chbs, a.EventActionReconcile, api.BackupReasonRetentionHeld,
			"the entries or the time zone of the schedule changed: retention keeps every backup the schedule kept before, "+
				"as status.retention.previous records it, and lists in status.clusters[].heldBackups those it would delete now, "+
				"each until it expires as the schedule before would have deleted it; keep those to keep longer in "+
				"spec.retention.keepBackups, or release the change at once with spec.retention.releaseHeld: %s", changeID))
	}
	if expired := model.ExpireScheduleRetention(chbs, c.clock.Now()); len(expired) > 0 {
		out.add(c.info(chbs, a.EventActionReconcile, api.BackupReasonRetentionHeldExpired,
			"every backup held for the change(s) %s of the schedule has expired as the schedule before would have deleted it: "+
				"retention no longer holds them, and status.retention.previous drops the change(s)", strings.Join(expired, ", ")))
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
		reconciling := wait.reason == api.BackupReasonCHINotReady
		if !reconciling || !apiMeta.IsStatusConditionTrue(chbs.Status.Conditions, api.BackupConditionValidated) {
			c.setCondition(chbs, api.BackupConditionValidated, meta.ConditionFalse, wait.reason, wait.message)
		}
		c.setReady(chbs, meta.ConditionFalse, wait.reason, wait.message, out)
		return ctrl.Result{RequeueAfter: shorterDelay(wait.requeue, recheck)}, nil
	}
	if err := clickHouseAccess(); err != nil {
		hold := noClickHouseAccess()
		c.setCondition(chbs, api.BackupConditionValidated, meta.ConditionFalse, hold.reason, hold.message)
		c.setReady(chbs, meta.ConditionFalse, hold.reason, hold.message, out)
		return ctrl.Result{RequeueAfter: shorterDelay(hold.requeue, recheck)}, nil
	}

	clusters := target.topology.ClusterNames()
	refusal, err := c.refusedPaths(ctx, chbs, target, clusters)
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		c.setCondition(chbs, api.BackupConditionValidated, meta.ConditionFalse, api.BackupReasonStoragePathConflict, refusal)
		c.setReady(chbs, meta.ConditionFalse, api.BackupReasonStoragePathConflict, refusal, out)
		return ctrl.Result{RequeueAfter: shorterDelay(topologyRecheckInterval, recheck)}, nil
	}
	guard := sharesSourcePrefix(chbs, target.chi)
	holds, err := c.restoreHolds(ctx, c.Client, chbs, target.chi, clusters)
	if err != nil {
		return ctrl.Result{}, err
	}
	c.validated(chbs, target, out)
	for _, name := range clusters {
		topology, err := target.topology.ForCluster(name)
		if err != nil {
			return ctrl.Result{}, err
		}
		cluster := scheduleCluster(chbs, name)
		cluster.ConfigSecret = topology.ConfigSecret()
		c.recordRetentionHosts(chbs, cluster, topology.Hosts(chbs.Spec.GetReplicaSelection()), out)
	}
	c.dropLeavingClusters(chbs, clusters, out)
	orderClusters(chbs, clusters)

	hold := guard
	if hold == nil {
		hold = combineHolds(clusters, holds)
	}
	if !chbs.Spec.IsSuspended() && (guard == nil) {
		delay, err := c.runDueSlot(ctx, chbs, target, clusters, holds, out)
		if err != nil {
			return ctrl.Result{}, err
		}
		recheck = shorterDelay(recheck, delay)
	}
	c.aggregate(chbs, clusters)
	c.refreshReady(chbs, hold, out)
	if hold != nil {
		return ctrl.Result{RequeueAfter: shorterDelay(hold.requeue, recheck)}, nil
	}
	return ctrl.Result{RequeueAfter: shorterDelay(recheck, topologyRecheckInterval)}, nil
}

func (c *ScheduleController) refreshReady(chbs *api.ClickHouseBackupSchedule, hold *pending, out *announcements) {
	withoutData := lastBackupWithoutData(chbs)
	switch {
	case hold != nil:
		c.setReady(chbs, meta.ConditionFalse, hold.reason, hold.message, out)
	case chbs.Spec.IsSuspended():
		c.setReady(chbs, meta.ConditionFalse, api.BackupReasonSuspended, "the schedule is suspended", out)
	case lastBackupFailed(chbs):
		lastRun := apiMeta.FindStatusCondition(chbs.Status.Conditions, api.BackupConditionLastBackupSucceeded)
		c.setReady(chbs, meta.ConditionFalse, api.BackupReasonLastBackupFailed, fmt.Sprintf(
			"the last scheduled backup failed, the schedule runs on: %s", lastRun.Message), out)
	case withoutData != nil:
		c.setReady(chbs, meta.ConditionTrue, api.BackupReasonLastBackupWithoutData, fmt.Sprintf(
			"the last scheduled backup holds no data, the schedule runs on: %s", withoutData.Message), out)
	default:
		c.setReady(chbs, meta.ConditionTrue, api.BackupReasonScheduled, "the schedule runs", out)
	}
}

func dueSlot(chbs *api.ClickHouseBackupSchedule, now time.Time) time.Time {
	location, err := chbs.Spec.GetLocation()
	if err != nil {
		return time.Time{}
	}
	var latest time.Time
	for _, entry := range api.ResolveScheduleTimes(chbs.Spec.Schedules) {
		if slot := model.LatestSlot(entry, location, now); slot.After(latest) {
			latest = slot
		}
	}
	return latest
}

// missedSlotsMax caps missedSlots, so a minute entry of a long-stopped schedule is not walked slot by slot.
const missedSlotsMax = 10000

func missedSlots(chbs *api.ClickHouseBackupSchedule, after, slot time.Time) (int, time.Time) {
	location, err := chbs.Spec.GetLocation()
	if err != nil {
		return 0, time.Time{}
	}
	entries := api.ResolveScheduleTimes(chbs.Spec.Schedules)
	if created := chbs.GetCreationTimestamp().Time; after.Before(created) {
		after = created.Add(-time.Nanosecond)
	}
	count, first := 0, time.Time{}
	for count < missedSlotsMax {
		var next time.Time
		for _, entry := range entries {
			if candidate := model.NextSlot(entry, location, after); !candidate.IsZero() && (next.IsZero() || candidate.Before(next)) {
				next = candidate
			}
		}
		if next.IsZero() || !next.Before(slot) {
			break
		}
		if count == 0 {
			first = next
		}
		count++
		after = next
	}
	return count, first
}

func (c *ScheduleController) warnMissedSlots(
	chbs *api.ClickHouseBackupSchedule,
	cluster *api.ScheduleClusterStatus,
	name string,
	slot time.Time,
	out *announcements,
) {
	if cluster.LastScheduleTime == nil {
		return
	}
	count, first := missedSlots(chbs, cluster.LastScheduleTime.Time, slot)
	if count == 0 {
		return
	}
	more := ""
	if count >= missedSlotsMax {
		more = " or more"
	}
	out.add(c.warning(chbs, a.EventActionReconcile, reasonScheduledRunSkipped,
		"%d%s slot(s) of cluster %s from %s were missed, as the operator did not run or a run of the cluster was in "+
			"progress or held: only the latest due slot %s runs, the missed ones are not made up",
		count, more, name, first.UTC().Format(time.RFC3339), slot.UTC().Format(time.RFC3339)))
}

func (c *ScheduleController) runDueSlot(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	target *target,
	clusters []string,
	holds map[string]*pending,
	out *announcements,
) (time.Duration, error) {
	now := c.clock.Now()
	slot := dueSlot(chbs, now)
	if slot.IsZero() || slot.Before(chbs.GetCreationTimestamp().Time) {
		return 0, nil
	}
	recheck := time.Duration(0)
	for _, name := range clusters {
		cluster := scheduleCluster(chbs, name)
		switch {
		case (cluster.LastScheduleTime != nil) && !slot.After(cluster.LastScheduleTime.Time):
			continue
		case activeRun(chbs, name) != nil:
			continue
		case holds[name] != nil:
			continue
		}
		c.warnMissedSlots(chbs, cluster, name, slot, out)
		if deadline := chbs.Spec.StartingDeadlineSeconds; (deadline != nil) && (now.Sub(slot) > time.Duration(*deadline)*time.Second) {
			cluster.LastScheduleTime = &meta.Time{Time: slot.UTC()}
			out.add(c.warning(chbs, a.EventActionReconcile, reasonScheduledRunSkipped,
				"the run of slot %s on cluster %s did not start within startingDeadlineSeconds of the slot, and is skipped",
				slot.UTC().Format(time.RFC3339), name))
			continue
		}
		delay, err := c.startScheduledRun(ctx, chbs, target, name, slot, out)
		if err != nil {
			return 0, err
		}
		recheck = shorterDelay(recheck, delay)
	}
	return recheck, nil
}

func scheduledRunKey(chbs *api.ClickHouseBackupSchedule, cluster string, slot time.Time) runKey {
	return runKeyOf(chbs, api.ClickHouseBackupScheduleCRDResourceKind, cluster,
		api.BackupRunOperationBackup+api.BackupRunOperation("@"+strconv.FormatInt(slot.Unix(), 10)))
}

func (c *ScheduleController) refusedPaths(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	target *target,
	clusters []string,
) (string, error) {
	resolve := c.settingResolver(ctx, chbs.GetNamespace())
	for _, name := range clusters {
		topology, err := target.topology.ForCluster(name)
		if err != nil {
			return "", err
		}
		spec := model.ScheduledRunSpec(chbs, topology, c.clock.Now())
		if err := spec.CheckPaths(topology, chbs.Spec.GetReplicaSelection(), resolve); err != nil {
			return "", err
		}
		if refusal := refusedPaths(spec); refusal != "" {
			return refusal, nil
		}
	}
	return "", nil
}

// startScheduledRun reads the restores again from the API server, so a just-validated restore holds the run.
func (c *ScheduleController) startScheduledRun(
	ctx context.Context,
	chbs *api.ClickHouseBackupSchedule,
	target *target,
	cluster string,
	slot time.Time,
	out *announcements,
) (time.Duration, error) {
	topology, err := target.topology.ForCluster(cluster)
	if err != nil {
		return 0, err
	}
	key := scheduledRunKey(chbs, cluster, slot)
	var spec *model.RunSpec
	gate := &startGate{check: func(ctx context.Context) (*pending, error) {
		holds, err := c.restoreHolds(ctx, c.apiReader, chbs, target.chi, []string{cluster})
		if err != nil {
			return nil, err
		}
		return holds[cluster], nil
	}}
	run, wait, err := c.startRun(ctx, chbs, chbs.Spec.ClickHouseInstallation, key, gate, func() (*model.RunSpec, error) {
		spec = model.ScheduledRunSpec(chbs, topology, slot)
		if err := spec.CheckPaths(topology, chbs.Spec.GetReplicaSelection(), c.settingResolver(ctx, chbs.GetNamespace())); err != nil {
			return nil, err
		}
		return spec, nil
	})
	switch {
	case err != nil:
		return 0, err
	case wait != nil:
		return wait.requeue, nil
	}
	backupName, err := model.ScheduledBackupName(spec.Prefix, spec.Naming, model.SlotTag(slot, chbs.Spec.TimeZone))
	if err != nil {
		backupName = ""
	}
	scheduled := api.ScheduledRun{Slot: meta.Time{Time: slot.UTC()}, BackupName: backupName}
	scheduled.Name = cluster
	startOn(&scheduled.OperationClusterStatus, run, spec.BackupHosts, spec.ConfigSecret, c.clock.Now())
	chbs.Status.Active = append(chbs.Status.Active, scheduled)
	status := scheduleCluster(chbs, cluster)
	status.LastScheduleTime = &meta.Time{Time: slot.UTC()}
	namespace, chiName := chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation
	out.add(c.info(chbs, a.EventActionCreate, reasonScheduledBackupStarted, "scheduled backup %s of cluster %s started, slot %s",
		backupName, cluster, slot.UTC().Format(time.RFC3339)))
	out.add(func() { metrics.BackupStarted(context.Background(), namespace, chiName, cluster) })
	return runningRequeueInterval, nil
}

func activeRun(chbs *api.ClickHouseBackupSchedule, cluster string) *api.ScheduledRun {
	for i := range chbs.Status.Active {
		if chbs.Status.Active[i].Name == cluster {
			return &chbs.Status.Active[i]
		}
	}
	return nil
}

func (c *ScheduleController) observeActive(ctx context.Context, chbs *api.ClickHouseBackupSchedule, out *announcements) (time.Duration, error) {
	var recheck time.Duration
	var active []api.ScheduledRun
	var targets *targetResolver
	for i := range chbs.Status.Active {
		scheduled := chbs.Status.Active[i].DeepCopy()
		key := scheduledRunKey(chbs, scheduled.Name, scheduled.Slot.Time)
		run := runs.get(key)
		if run == nil {
			if targets == nil {
				targets = c.newTargetResolver(ctx, chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, false)
			}
			var err error
			run, err = c.resumeRun(chbs, chbs.Spec.ClickHouseInstallation, key, &scheduled.OperationClusterStatus, func() (*model.RunSpec, error) {
				target, wait, err := targets.forCluster(scheduled.Name)
				switch {
				case err != nil:
					return nil, err
				case wait != nil:
					return nil, fmt.Errorf("%w: %s", errTargetGone, wait.message)
				}
				return model.ScheduledRunSpec(chbs, target.topology, scheduled.Slot.Time), nil
			})
			switch {
			case errors.Is(err, errNoClickHouseAccess) || errors.Is(err, errTargetGone):
				c.finishRun(chbs, scheduled, runOutcome{failed: true, message: fmt.Sprintf("the run cannot resume: %v", err)}, out)
				continue
			case err != nil:
				return 0, err
			}
		}
		outcome, finished := observeOn(&scheduled.OperationClusterStatus, run)
		if !finished {
			active = append(active, *scheduled)
			recheck = runningRequeueInterval
			continue
		}
		out.add(func() { runs.forget(key) })
		c.finishRun(chbs, scheduled, outcome, out)
	}
	chbs.Status.Active = active
	return recheck, nil
}

func (c *ScheduleController) finishRun(chbs *api.ClickHouseBackupSchedule, scheduled *api.ScheduledRun, outcome runOutcome, out *announcements) {
	cluster := scheduleCluster(chbs, scheduled.Name)
	namespace, chiName, clusterName := chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, scheduled.Name
	if outcome.backupName != "" {
		scheduled.BackupName = outcome.backupName
	}
	if outcome.failed {
		failOn(&scheduled.OperationClusterStatus, outcomeReason(outcome, api.BackupRunOperationBackup), outcome.message)
		recordFailure(cluster, *scheduled.CompletionTime, outcome.message)
		failed := scheduled.CompletionTime.Time
		reason := scheduled.Reason
		out.add(func() { metrics.BackupFailed(context.Background(), namespace, chiName, clusterName, failed) })
		out.add(c.warning(chbs, a.EventActionReconcile, reasonScheduledBackupFailed, "scheduled backup %s of cluster %s failed (%s): %s",
			scheduled.BackupName, clusterName, reason, outcome.message))
	} else {
		reason, message := api.BackupReasonCompleted, "the backup completed"
		if outcome.noData != "" {
			reason, message = api.BackupReasonCompletedWithoutData, "the backup completed, but holds no data: "+outcome.noData
		}
		completeOn(&scheduled.OperationClusterStatus, reason, message)
		if len(outcome.hostsUsed) > 0 {
			scheduled.Hosts = outcome.hostsUsed
		}
		recordSuccess(cluster, *scheduled.CompletionTime, scheduled.BackupName)
		cluster.HeldBackups = outcome.held
		if outcome.series != nil {
			chbs.Status.Backups = api.ReplaceStoredBackups(chbs.Status.Backups, clusterName, outcome.series)
			cluster.BackupsListTime = scheduled.CompletionTime.DeepCopy()
		}
		for _, host := range scheduled.Hosts {
			if !slices.Contains(cluster.RetentionHosts, host) {
				cluster.RetentionHosts = append(cluster.RetentionHosts, host)
			}
		}
		completion := scheduled.CompletionTime.Time
		duration := time.Duration(0)
		if scheduled.StartTime != nil {
			duration = completion.Sub(scheduled.StartTime.Time)
		}
		out.add(func() {
			metrics.BackupCompleted(context.Background(), namespace, chiName, clusterName, duration, completion)
		})
		if outcome.noData == "" {
			out.add(c.info(chbs, a.EventActionReconcile, reasonScheduledBackupSucceeded, "scheduled backup %s of cluster %s completed",
				scheduled.BackupName, clusterName))
		} else {
			out.add(c.warning(chbs, a.EventActionReconcile, api.BackupReasonCompletedWithoutData,
				"scheduled backup %s of cluster %s completed, but the backup holds no data: %s", scheduled.BackupName, clusterName,
				outcome.noData))
		}
	}
	chbs.Status.RecentRuns = trimRecentRuns(append([]api.ScheduledRun{*scheduled}, chbs.Status.RecentRuns...),
		int(ptr.Deref(chbs.Spec.SuccessfulRunsHistoryLimit, defaultSuccessfulRunsHistoryLimit)),
		int(ptr.Deref(chbs.Spec.FailedRunsHistoryLimit, defaultFailedRunsHistoryLimit)))
}

func trimRecentRuns(recent []api.ScheduledRun, successful, failed int) []api.ScheduledRun {
	var kept []api.ScheduledRun
	succeeded, failures := 0, 0
	for _, scheduled := range recent {
		if scheduled.Phase == api.OperationPhaseCompleted {
			if succeeded++; succeeded > successful {
				continue
			}
		} else if failures++; failures > failed {
			continue
		}
		scheduled.Run = nil
		kept = append(kept, scheduled)
	}
	return kept
}

// The operator never deletes them, because the backups of a removed shard can be the last copy of its data.
func (c *ScheduleController) recordRetentionHosts(chbs *api.ClickHouseBackupSchedule, cluster *api.ScheduleClusterStatus, hosts []string, out *announcements) {
	var left []string
	for _, host := range cluster.RetentionHosts {
		if !slices.Contains(hosts, host) && !slices.Contains(cluster.UnprunedHosts, host) {
			left = append(left, host)
		}
	}
	cluster.RetentionHosts = slices.DeleteFunc(slices.Clone(cluster.RetentionHosts), func(host string) bool {
		return !slices.Contains(hosts, host)
	})
	for _, host := range cluster.UnprunedHosts {
		if slices.Contains(hosts, host) && !slices.Contains(cluster.RetentionHosts, host) {
			cluster.RetentionHosts = append(cluster.RetentionHosts, host)
		}
	}
	cluster.UnprunedHosts = slices.DeleteFunc(append(slices.Clone(cluster.UnprunedHosts), left...), func(host string) bool {
		return slices.Contains(hosts, host)
	})
	if len(left) == 0 {
		return
	}
	out.add(c.warning(chbs, a.EventActionReconcile, api.BackupReasonUnprunedHosts,
		"%s of cluster %s left the hosts the schedule backs up: retention no longer prunes the backups named %s%s they made, "+
			"which stay in the storage until deleted by hand, unless a host still backed up shares their storage path",
		strings.Join(left, ", "), cluster.Name, model.BackupNamePrefix(chbs), api.BackupNameScheduleSuffix))
}

func (c *ScheduleController) dropLeavingClusters(chbs *api.ClickHouseBackupSchedule, clusters []string, out *announcements) {
	var kept []api.ScheduleClusterStatus
	for i := range chbs.Status.Clusters {
		cluster := chbs.Status.Clusters[i]
		if !slices.Contains(clusters, cluster.Name) && (activeRun(chbs, cluster.Name) == nil) {
			c.recordRetentionHosts(chbs, &cluster, nil, out)
			if len(cluster.UnprunedHosts) == 0 {
				continue
			}
		}
		kept = append(kept, cluster)
	}
	chbs.Status.Clusters = kept
	var names []string
	for _, cluster := range kept {
		names = append(names, cluster.Name)
	}
	chbs.Status.Backups = api.KeepStoredBackupsOf(chbs.Status.Backups, names)
}

func (c *ScheduleController) aggregate(chbs *api.ClickHouseBackupSchedule, clusters []string) {
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
	switch {
	case !everyClusterSucceeded:
		chbs.Status.LastBackupName = ""
	case len(backupNames) == 1:
		chbs.Status.LastBackupName = backupNames[0]
	case lastRunFailed:
		chbs.Status.LastBackupName = ""
	}
	c.setLastRun(chbs, clusters)
}

func lastFinished(chbs *api.ClickHouseBackupSchedule, cluster string) *api.ScheduledRun {
	for i := range chbs.Status.RecentRuns {
		if chbs.Status.RecentRuns[i].Name == cluster {
			return &chbs.Status.RecentRuns[i]
		}
	}
	return nil
}

func (c *ScheduleController) setLastRun(chbs *api.ClickHouseBackupSchedule, clusters []string) {
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
		reason := api.BackupReasonFailed
		if run := lastFinished(chbs, newest.Name); (run != nil) && failedPhase(run.Phase) {
			reason = run.Reason
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
		if run := lastFinished(chbs, cluster.Name); (run != nil) && (run.Reason == api.BackupReasonCompletedWithoutData) {
			noData = strings.TrimPrefix(run.Message, "the backup completed, but holds no data: ")
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

func abortsSchedule(reason string) bool {
	switch reason {
	case api.BackupReasonInvalidSpec, api.BackupReasonCHINotFound, api.BackupReasonCHIStopped, api.BackupReasonClusterNotFound,
		api.BackupReasonBackupDisabled, api.BackupReasonNoBackupStorage, api.BackupReasonNoTargetHosts,
		api.BackupReasonNoClickHouseAccess, api.BackupReasonStoragePathConflict:
		return true
	}
	return false
}

func (c *ScheduleController) refreshNextScheduleTime(chbs *api.ClickHouseBackupSchedule) time.Duration {
	chbs.Status.NextScheduleTime = nil
	location, err := chbs.Spec.GetLocation()
	aborted := (chbs.Status.Phase == api.SchedulePhaseAborted) && (chbs.Status.Reason != api.BackupReasonLastBackupFailed)
	if (chbs.Status.Phase == api.SchedulePhaseSuspended) || (err != nil) || aborted {
		return 0
	}
	now := c.clock.Now()
	next := model.NextScheduleTime(chbs.Spec.Schedules, location, now)
	if next.IsZero() {
		return 0
	}
	chbs.Status.NextScheduleTime = &meta.Time{Time: next.UTC()}
	return next.Sub(now) + dueRecheckDelay
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
				inProgress[cluster] = cmp.Or(inProgress[cluster], sourceRestoreInProgress(chiName, cluster, restore))
			}
			continue
		}
		if (restore.Spec.ClickHouseInstallation != chiName) || !holdsSchedules(restore) {
			continue
		}
		held := heldCluster(restore)
		for _, cluster := range clusters {
			switch {
			case (held != "") && (held != cluster):
			case failedPhase(restore.Status.Phase):
				failed[cluster] = cmp.Or(failed[cluster], failedRestoreHold(chiName, cluster, restore.GetName()))
			default:
				inProgress[cluster] = cmp.Or(inProgress[cluster], restoreInProgress(chiName, cluster))
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

	holds := map[string]*pending{}
	for _, cluster := range clusters {
		hold := cmp.Or(inProgress[cluster], failed[cluster], recovering[cluster])
		if (hold == nil) && runs.unfinished(api.ClickHouseRestoreCRDResourceKind, chbs.GetNamespace(), chiName, cluster) {
			hold = restoreInProgress(chiName, cluster)
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

func waitsForTarget(cluster api.OperationClusterStatus) bool {
	return (cluster.Run == nil) && slices.Contains(
		[]string{api.BackupReasonCHINotFound, api.BackupReasonCHINotReady, api.BackupReasonCHIStopped}, cluster.Reason)
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

func restoreInProgress(chiName, cluster string) *pending {
	return &pending{
		reason:  api.BackupReasonSuspendedForRestore,
		message: fmt.Sprintf("a restore of cluster %s of ClickHouseInstallation %s is in progress", cluster, chiName),
		requeue: pendingRequeueInterval,
	}
}

func failedRestoreHold(chiName, cluster, restore string) *pending {
	return &pending{
		reason: api.BackupReasonSuspendedForFailedRestore,
		message: fmt.Sprintf("ClickHouseRestore %s of ClickHouseInstallation %s failed and may have left cluster %s partly restored; "+
			"scheduled backups stay suspended while that restore remains", restore, chiName, cluster),
		requeue: targetRequeueInterval,
	}
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
// that needs somebody to act comes first, the clusters held tell the message.
func combineHolds(clusters []string, holds map[string]*pending) *pending {
	var combined *pending
	var messages []string
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
	}
	if combined != nil {
		combined.message = strings.Join(messages, "; ")
	}
	return combined
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

func holdReason(reason string) bool {
	switch reason {
	case api.BackupReasonSuspendedForRestore, api.BackupReasonSuspendedForFailedRestore, api.BackupReasonSharesSourcePrefix,
		api.BackupReasonNameConflict, api.BackupReasonWaitingForGarbageCollection:
		return true
	}
	return false
}

func holdNeedsAction(reason string) bool {
	return (reason == api.BackupReasonSuspendedForFailedRestore) || (reason == api.BackupReasonNameConflict)
}

func runsSchedule(reason string) bool {
	switch reason {
	case api.BackupReasonScheduled, api.BackupReasonLastBackupFailed, api.BackupReasonLastBackupWithoutData:
		return true
	}
	return false
}

// shorterDelay returns the shorter of two requeue delays, zero meaning none.
func shorterDelay(x, y time.Duration) time.Duration {
	if (x == 0) || ((y > 0) && (y < x)) {
		return y
	}
	return x
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
