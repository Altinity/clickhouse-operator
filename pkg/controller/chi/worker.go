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

package chi

import (
	"context"
	"errors"
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilRuntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/altinity/queue"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chiConfig "github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	normalizerCommon "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// FinalizerName specifies name of the finalizer to be used with CHI
const FinalizerName = "finalizer.clickhouseinstallation.altinity.com"

// worker represents worker thread which runs reconcile tasks
type worker struct {
	c *Controller
	a common.Announcer
	//queue workqueue.RateLimitingInterface
	queue         queue.PriorityQueue
	normalizer    *normalizer.Normalizer
	schemer       *schemer.ClusterSchemer
	start         time.Time
	task          *common.Task
	stsReconciler *statefulset.StatefulSetReconciler
}

// newWorker
// func (c *Controller) newWorker(q workqueue.RateLimitingInterface) *worker {
func (c *Controller) newWorker(q queue.PriorityQueue, sys bool) *worker {
	start := time.Now()
	if !sys {
		start = start.Add(api.DefaultReconcileThreadsWarmup)
	}
	kind := "ClickHouseInstallation"
	generateName := "chop-chi-"
	component := componentName

	announcer := common.NewAnnouncer(
		common.NewEventEmitter(c.kube.Event(), kind, generateName, component),
		c.kube.CRStatus(),
	)

	return &worker{
		c:     c,
		a:     announcer,
		queue: q,
		normalizer: normalizer.New(func(namespace, name string) (*core.Secret, error) {
			return c.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, controller.NewGetOptions())
		}),
		schemer: nil,
		start:   start,
		task:    nil,
	}
}

func configGeneratorOptions(chi *api.ClickHouseInstallation) *chiConfig.GeneratorOptions {
	return &chiConfig.GeneratorOptions{
		Users:          chi.GetSpecT().Configuration.Users,
		Profiles:       chi.GetSpecT().Configuration.Profiles,
		Quotas:         chi.GetSpecT().Configuration.Quotas,
		Settings:       chi.GetSpecT().Configuration.Settings,
		Files:          chi.GetSpecT().Configuration.Files,
		DistributedDDL: chi.GetSpecT().Defaults.DistributedDDL,
	}
}

// newContext creates new reconcile task
func (w *worker) newTask(chi *api.ClickHouseInstallation) {
	w.task = common.NewTask(
		commonCreator.NewCreator(
			chi,
			managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeClickHouse, chi, configGeneratorOptions(chi)),
			managers.NewContainerManager(managers.ContainerManagerTypeClickHouse),
			managers.NewTagManager(managers.TagManagerTypeClickHouse, chi),
			managers.NewProbeManager(managers.ProbeManagerTypeClickHouse),
			managers.NewServiceManager(managers.ServiceManagerTypeClickHouse),
			managers.NewVolumeManager(managers.VolumeManagerTypeClickHouse),
			managers.NewConfigMapManager(managers.ConfigMapManagerTypeClickHouse),
			managers.NewNameManager(managers.NameManagerTypeClickHouse),
		),
	)

	w.stsReconciler = statefulset.NewStatefulSetReconciler(
		w.a,
		w.task,
		poller.NewHostStatefulSetPoller(poller.NewStatefulSetPoller(w.c.kube), w.c.kube, w.c.labeler),
		w.c.namer,
		storage.NewStorageReconciler(w.task, w.c.namer, w.c.kube.Storage()),
		w.c.kube,
		w.c,
	)
}

// timeToStart specifies time that operator does not accept changes
const timeToStart = 1 * time.Minute

// isJustStarted checks whether worked just started
func (w *worker) isJustStarted() bool {
	return time.Since(w.start) < timeToStart
}

func (w *worker) isConfigurationChangeRequiresReboot(host *api.Host) bool {
	return model.IsConfigurationChangeRequiresReboot(host)
}

// shouldForceRestartHost checks whether cluster requires hosts restart
func (w *worker) shouldForceRestartHost(host *api.Host) bool {
	// RollingUpdate purpose is to always shut the host down.
	// It is such an interesting policy.
	if host.GetCR().IsRollingUpdate() {
		w.a.V(1).M(host).F().Info("RollingUpdate requires force restart. Host: %s", host.GetName())
		return true
	}

	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusNew {
		w.a.V(1).M(host).F().Info("Host is new, no restart applicable. Host: %s", host.GetName())
		return false
	}

	if (host.GetReconcileAttributes().GetStatus() == api.ObjectStatusSame) && !host.HasAncestor() {
		w.a.V(1).M(host).F().Info("Host already exists, but has no ancestor, no restart applicable. Host: %s", host.GetName())
		return false
	}

	// For some configuration changes we have to force restart host
	if w.isConfigurationChangeRequiresReboot(host) {
		w.a.V(1).M(host).F().Info("Config change(s) require host restart. Host: %s", host.GetName())
		return true
	}

	podIsCrushed := false
	// pod.Status.ContainerStatuses[0].State.Waiting.Reason
	if pod, err := w.c.kube.Pod().Get(host); err == nil {
		if len(pod.Status.ContainerStatuses) > 0 {
			if pod.Status.ContainerStatuses[0].State.Waiting != nil {
				if pod.Status.ContainerStatuses[0].State.Waiting.Reason == "CrashLoopBackOff" {
					podIsCrushed = true
				}
			}
		}
	}

	if host.Runtime.Version.IsUnknown() && podIsCrushed {
		w.a.V(1).M(host).F().Info("Host with unknown version and in CrashLoopBackOff should be restarted. It most likely is unable to start due to bad config. Host: %s", host.GetName())
		return true
	}

	w.a.V(1).M(host).F().Info("Host restart is not required. Host: %s", host.GetName())
	return false
}

// run is an endless work loop, expected to be run in a thread
func (w *worker) run() {
	w.a.V(2).S().P()
	defer w.a.V(2).E().P()

	// For system thread let's wait its 'official start time', thus giving it time to bootstrap
	util.WaitContextDoneUntil(context.Background(), w.start)

	// Events loop
	for {
		// Get() blocks until it can return an item
		item, ctx, ok := w.queue.Get()
		if !ok {
			w.a.Info("shutdown request")
			return
		}

		//item, shut := w.queue.Get()
		//task := context.Background()
		//if shut {
		//	w.a.Info("shutdown request")
		//	return
		//}

		if err := w.processItem(ctx, item); err != nil {
			// Item not processed
			// this code cannot return an error and needs to indicate error has been ignored
			utilRuntime.HandleError(err)
		}

		// Forget indicates that an item is finished being retried.  Doesn't matter whether its for perm failing
		// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
		// still have to call `Done` on the queue.
		//w.queue.Forget(item)

		// Remove item from processing set when processing completed
		w.queue.Done(item)
	}
}

func (w *worker) processReconcileCHI(ctx context.Context, cmd *cmd_queue.ReconcileCHI) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		return w.updateCHI(ctx, nil, cmd.New)
	case cmd_queue.ReconcileUpdate:
		return w.updateCHI(ctx, cmd.Old, cmd.New)
	case cmd_queue.ReconcileDelete:
		return w.discoveryAndDeleteCHI(ctx, cmd.Old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileCHIT(cmd *cmd_queue.ReconcileCHIT) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		return w.addChit(cmd.New)
	case cmd_queue.ReconcileUpdate:
		return w.updateChit(cmd.Old, cmd.New)
	case cmd_queue.ReconcileDelete:
		return w.deleteChit(cmd.Old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileChopConfig(cmd *cmd_queue.ReconcileChopConfig) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		return w.c.addChopConfig(cmd.New)
	case cmd_queue.ReconcileUpdate:
		return w.c.updateChopConfig(cmd.Old, cmd.New)
	case cmd_queue.ReconcileDelete:
		return w.c.deleteChopConfig(cmd.Old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileEndpoints(ctx context.Context, cmd *cmd_queue.ReconcileEndpoints) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileUpdate:
		return w.updateEndpoints(ctx, cmd.Old, cmd.New)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcilePod(ctx context.Context, cmd *cmd_queue.ReconcilePod) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		w.a.V(1).M(cmd.New).F().Info("Add Pod. %s/%s", cmd.New.Namespace, cmd.New.Name)
		metrics.PodAdd(ctx)
		return nil
	case cmd_queue.ReconcileUpdate:
		//ignore
		//w.a.V(1).M(cmd.new).F().Info("Update Pod. %s/%s", cmd.new.Namespace, cmd.new.Name)
		//metricsPodUpdate(ctx)
		return nil
	case cmd_queue.ReconcileDelete:
		w.a.V(1).M(cmd.Old).F().Info("Delete Pod. %s/%s", cmd.Old.Namespace, cmd.Old.Name)
		metrics.PodDelete(ctx)
		return nil
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processDropDns(ctx context.Context, cmd *cmd_queue.DropDns) error {
	if chi, err := w.createCHIFromObjectMeta(cmd.Initiator, false, normalizerCommon.NewOptions()); err == nil {
		w.a.V(2).M(cmd.Initiator).Info("flushing DNS for CHI %s", chi.Name)
		_ = w.ensureClusterSchemer(chi.FirstHost()).CHIDropDnsCache(ctx, chi)
	} else {
		w.a.M(cmd.Initiator).F().Error("unable to find CHI by %v err: %v", cmd.Initiator.GetLabels(), err)
	}
	return nil
}

// processItem processes one work item according to its type
func (w *worker) processItem(ctx context.Context, item interface{}) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(3).S().P()
	defer w.a.V(3).E().P()

	switch cmd := item.(type) {
	case *cmd_queue.ReconcileCHI:
		return w.processReconcileCHI(ctx, cmd)
	case *cmd_queue.ReconcileCHIT:
		return w.processReconcileCHIT(cmd)
	case *cmd_queue.ReconcileChopConfig:
		return w.processReconcileChopConfig(cmd)
	case *cmd_queue.ReconcileEndpoints:
		return w.processReconcileEndpoints(ctx, cmd)
	case *cmd_queue.ReconcilePod:
		return w.processReconcilePod(ctx, cmd)
	case *cmd_queue.DropDns:
		return w.processDropDns(ctx, cmd)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// normalize
func (w *worker) normalize(c *api.ClickHouseInstallation) *api.ClickHouseInstallation {

	chi, err := w.normalizer.CreateTemplated(c, normalizerCommon.NewOptions())
	if err != nil {
		w.a.WithEvent(chi, common.EventActionReconcile, common.EventReasonReconcileFailed).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to normalize CHI 1: %v", err)
	}

	ips := w.c.getPodsIPs(chi)
	w.a.V(1).M(chi).Info("IPs of the CHI normalizer %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
	opts := normalizerCommon.NewOptions()
	opts.DefaultUserAdditionalIPs = ips

	chi, err = w.normalizer.CreateTemplated(c, opts)
	if err != nil {
		w.a.WithEvent(chi, common.EventActionReconcile, common.EventReasonReconcileFailed).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to normalize CHI 2: %v", err)
	}

	return chi
}

// ensureFinalizer
func (w *worker) ensureFinalizer(ctx context.Context, chi *api.ClickHouseInstallation) bool {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return false
	}

	// In case CHI is being deleted already, no need to meddle with finalizers
	if !chi.GetDeletionTimestamp().IsZero() {
		return false
	}

	// Finalizer can already be listed in CHI, do nothing in this case
	if util.InArray(FinalizerName, chi.GetFinalizers()) {
		w.a.V(2).M(chi).F().Info("finalizer already installed")
		return false
	}

	// No finalizer found - need to install it

	if err := w.c.installFinalizer(ctx, chi); err != nil {
		w.a.V(1).M(chi).F().Error("unable to install finalizer. err: %v", err)
		return false
	}

	w.a.V(3).M(chi).F().Info("finalizer installed")
	return true
}

// updateEndpoints updates endpoints
func (w *worker) updateEndpoints(ctx context.Context, old, new *core.Endpoints) error {

	if chi, err := w.createCHIFromObjectMeta(new.GetObjectMeta(), false, normalizerCommon.NewOptions()); err == nil {
		w.a.V(1).M(chi).Info("updating endpoints for CHI-1 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-1 update endpoints %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
		opts := normalizerCommon.NewOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(new.GetObjectMeta(), false, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-1")

			// TODO unify with finalize reconcile
			w.newTask(chi)
			w.reconcileConfigMapCommonUsers(ctx, chi)
			w.c.updateCHIObjectStatus(ctx, chi, types.UpdateStatusOptions{
				TolerateAbsence: true,
				CopyStatusOptions: types.CopyStatusOptions{
					Normalized: true,
				},
			})
		} else {
			w.a.M(new.GetObjectMeta()).F().Error("internal unable to find CHI by %v err: %v", new.GetLabels(), err)
		}
	} else {
		w.a.M(new.GetObjectMeta()).F().Error("external unable to find CHI by %v err %v", new.GetLabels(), err)
	}
	return nil
}

// updateCHI sync CHI which was already created earlier
func (w *worker) updateCHI(ctx context.Context, old, new *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(3).M(new).S().P()
	defer w.a.V(3).M(new).E().P()

	update := (old != nil) && (new != nil)

	if update && (old.GetResourceVersion() == new.GetResourceVersion()) {
		// No need to react
		w.a.V(3).M(new).F().Info("ResourceVersion did not change: %s", new.GetResourceVersion())
		return nil
	}

	w.a.V(1).M(new).S().P()
	defer w.a.V(1).M(new).E().P()

	if w.ensureFinalizer(context.Background(), new) {
		w.a.M(new).F().Info("finalizer installed, let's restart reconcile cycle. CHI: %s/%s", new.Namespace, new.Name)
		w.a.M(new).F().Info("---------------------------------------------------------------------")
		return nil
	} else {
		w.a.M(new).F().Info("finalizer in place, proceed to reconcile cycle. CHI: %s/%s", new.Namespace, new.Name)
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if w.deleteCHI(ctx, old, new) {
		// CHI is being deleted
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if w.isCHIProcessedOnTheSameIP(new) {
		// First minute after restart do not reconcile already reconciled generations
		w.a.V(1).M(new).F().Info("Will not reconcile known generation after restart. Generation %d", new.Generation)
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// CHI is being reconciled
	return w.reconcileCHI(ctx, old, new)
}

// isCHIProcessedOnTheSameIP checks whether it is just a restart of the operator on the same IP
func (w *worker) isCHIProcessedOnTheSameIP(chi *api.ClickHouseInstallation) bool {
	ip, _ := chop.Get().ConfigManager.GetRuntimeParam(deployment.OPERATOR_POD_IP)
	operatorIpIsTheSame := ip == chi.Status.GetCHOpIP()
	log.V(1).Info("Operator IPs to process CHI: %s. Previous: %s Cur: %s", chi.Name, chi.Status.GetCHOpIP(), ip)

	if !operatorIpIsTheSame {
		// Operator has restarted on the different IP address.
		// We may need to reconcile config files
		log.V(1).Info("Operator IPs are different. Operator was restarted on another IP since previous reconcile of the CHI: %s", chi.Name)
		return false
	}

	log.V(1).Info("Operator IPs are the same as on previous reconcile of the CHI: %s", chi.Name)
	return w.isCleanRestart(chi)
}

// isCleanRestart checks whether it is just a restart of the operator and CHI has no changes since last processed
func (w *worker) isCleanRestart(chi *api.ClickHouseInstallation) bool {
	// Clean restart may be only in case operator has just recently started
	if !w.isJustStarted() {
		log.V(1).Info("Operator is not just started. May not be clean restart")
		return false
	}

	log.V(1).Info("Operator just started. May be clean restart")

	// Migration support
	// Do we have have previously completed CHI?
	// In case no - this means that CHI has either not completed or we are migrating from
	// such a version of the operator, where there is no completed CHI at all
	noCompletedCHI := !chi.HasAncestor()
	// Having status completed and not having completed CHI suggests we are migrating operator version
	statusIsCompleted := chi.Status.GetStatus() == api.StatusCompleted
	if noCompletedCHI && statusIsCompleted {
		// In case of a restart - assume that normalized is already completed
		chi.SetAncestor(chi.GetTarget())
	}

	// Check whether anything has changed in CHI spec
	// In case the generation is the same as already completed - it is clean restart
	generationIsOk := false
	// However, completed CHI still can be missing, for example, in newly requested CHI
	if chi.HasAncestor() {
		generationIsOk = chi.Generation == chi.GetAncestor().GetGeneration()
		log.V(1).Info(
			"CHI %s has ancestor. Generations. Prev: %d Cur: %d Generation is the same: %t",
			chi.Name,
			chi.GetAncestor().GetGeneration(),
			chi.Generation,
			generationIsOk,
		)
	} else {
		log.V(1).Info("CHI %s has NO ancestor, meaning reconcile cycle was never completed.", chi.Name)
	}

	log.V(1).Info("Is CHI %s clean on operator restart: %t", chi.Name, generationIsOk)
	return generationIsOk
}

// areUsableOldAndNew checks whether there are old and new usable
func (w *worker) areUsableOldAndNew(old, new *api.ClickHouseInstallation) bool {
	if old == nil {
		return false
	}
	if new == nil {
		return false
	}
	return true
}

// isAfterFinalizerInstalled checks whether we are just installed finalizer
func (w *worker) isAfterFinalizerInstalled(old, new *api.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	finalizerIsInstalled := len(old.Finalizers) == 0 && len(new.Finalizers) > 0
	return w.isGenerationTheSame(old, new) && finalizerIsInstalled
}

// isGenerationTheSame checks whether old ans new CHI have the same generation
func (w *worker) isGenerationTheSame(old, new *api.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	return old.Generation == new.Generation
}

// excludeStoppedCHIFromMonitoring excludes stopped CHI from monitoring
func (w *worker) excludeStoppedCHIFromMonitoring(chi *api.ClickHouseInstallation) {
	if !chi.IsStopped() {
		// No need to exclude non-stopped CHI
		return
	}

	w.a.V(1).
		WithEvent(chi, common.EventActionReconcile, common.EventReasonReconcileInProgress).
		WithStatusAction(chi).
		M(chi).F().
		Info("exclude CHI from monitoring")
	w.c.deleteWatch(chi)
}

// addCHIToMonitoring adds CHI to monitoring
func (w *worker) addCHIToMonitoring(chi *api.ClickHouseInstallation) {
	if chi.IsStopped() {
		// No need to add stopped CHI
		return
	}

	w.a.V(1).
		WithEvent(chi, common.EventActionReconcile, common.EventReasonReconcileInProgress).
		WithStatusAction(chi).
		M(chi).F().
		Info("add CHI to monitoring")
	w.c.updateWatch(chi)
}

func (w *worker) markReconcileStart(ctx context.Context, chi *api.ClickHouseInstallation, ap *action_plan.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	chi.EnsureStatus().ReconcileStart(ap.GetRemovedHostsNum())
	_ = w.c.updateCHIObjectStatus(ctx, chi, types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			MainFields: true,
		},
	})

	w.a.V(1).
		WithEvent(chi, common.EventActionReconcile, common.EventReasonReconcileStarted).
		WithStatusAction(chi).
		WithStatusActions(chi).
		M(chi).F().
		Info("reconcile started, task id: %s", chi.GetSpecT().GetTaskID())
	w.a.V(2).M(chi).F().Info("action plan\n%s\n", ap.String())
}

func (w *worker) finalizeReconcileAndMarkCompleted(ctx context.Context, _chi *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.a.V(1).M(_chi).F().S().Info("finalize reconcile")

	// Update CHI object
	if chi, err := w.createCHIFromObjectMeta(_chi, true, normalizerCommon.NewOptions()); err == nil {
		w.a.V(1).M(chi).Info("updating endpoints for CHI-2 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-2 finalize reconcile %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
		opts := normalizerCommon.NewOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(_chi, true, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-2")
			chi.SetAncestor(chi.GetTarget())
			chi.SetTarget(nil)
			chi.EnsureStatus().ReconcileComplete()
			// TODO unify with update endpoints
			w.newTask(chi)
			w.reconcileConfigMapCommonUsers(ctx, chi)
			w.c.updateCHIObjectStatus(ctx, chi, types.UpdateStatusOptions{
				CopyStatusOptions: types.CopyStatusOptions{
					WholeStatus: true,
				},
			})
		} else {
			w.a.M(_chi).F().Error("internal unable to find CHI by %v err: %v", _chi.GetLabels(), err)
		}
	} else {
		w.a.M(_chi).F().Error("external unable to find CHI by %v err %v", _chi.GetLabels(), err)
	}

	w.a.V(1).
		WithEvent(_chi, common.EventActionReconcile, common.EventReasonReconcileCompleted).
		WithStatusAction(_chi).
		WithStatusActions(_chi).
		M(_chi).F().
		Info("reconcile completed successfully, task id: %s", _chi.GetSpecT().GetTaskID())
}

func (w *worker) markReconcileCompletedUnsuccessfully(ctx context.Context, chi *api.ClickHouseInstallation, err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	switch {
	case err == nil:
		chi.EnsureStatus().ReconcileComplete()
	case errors.Is(err, common.ErrCRUDAbort):
		chi.EnsureStatus().ReconcileAbort()
	}
	w.c.updateCHIObjectStatus(ctx, chi, types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			MainFields: true,
		},
	})

	w.a.V(1).
		WithEvent(chi, common.EventActionReconcile, common.EventReasonReconcileFailed).
		WithStatusAction(chi).
		WithStatusActions(chi).
		M(chi).F().
		Warning("reconcile completed UNSUCCESSFULLY, task id: %s", chi.GetSpecT().GetTaskID())
}

func (w *worker) walkHosts(ctx context.Context, chi *api.ClickHouseInstallation, ap *action_plan.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	existingObjects := w.c.discovery(ctx, chi)
	ap.WalkAdded(
		// Walk over added clusters
		func(cluster api.ICluster) {
			cluster.WalkHosts(func(host *api.Host) error {

				// Name of the StatefulSet for this host
				name := w.c.namer.Name(interfaces.NameStatefulSet, host)
				// Have we found this StatefulSet
				found := false

				existingObjects.WalkStatefulSet(func(meta meta.Object) {
					if name == meta.GetName() {
						// StatefulSet of this host already exist
						found = true
					}
				})

				if found {
					// StatefulSet of this host already exist, we can't ADD it for sure
					// It looks like FOUND is the most correct approach
					host.GetReconcileAttributes().SetFound()
					w.a.V(1).M(chi).Info("Add host as FOUND via cluster. Host was found as sts. Host: %s", host.GetName())
				} else {
					// StatefulSet of this host does not exist, looks like we need to ADD it
					host.GetReconcileAttributes().SetAdd()
					w.a.V(1).M(chi).Info("Add host as ADD via cluster. Host was not found as sts. Host: %s", host.GetName())
				}

				return nil
			})
		},
		// Walk over added shards
		func(shard api.IShard) {
			// Mark all hosts of the shard as newly added
			shard.WalkHosts(func(host *api.Host) error {
				host.GetReconcileAttributes().SetAdd()
				w.a.V(1).M(chi).Info("Add host as ADD via shard. Host: %s", host.GetName())
				return nil
			})
		},
		// Walk over added hosts
		func(host *api.Host) {
			host.GetReconcileAttributes().SetAdd()
			w.a.V(1).M(chi).Info("Add host as ADD via host. Host: %s", host.GetName())
		},
	)

	ap.WalkModified(
		func(cluster api.ICluster) {
		},
		func(shard api.IShard) {
		},
		func(host *api.Host) {
			w.a.V(1).M(chi).Info("Add host as MODIFIED via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetModify()
		},
	)

	chi.WalkHosts(func(host *api.Host) error {
		switch {
		case host.GetReconcileAttributes().IsAdd():
			// Already added
			return nil
		case host.GetReconcileAttributes().IsModify():
			// Already modified
			return nil
		default:
			// Not clear yet
			w.a.V(1).M(chi).Info("Add host as FOUND via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetFound()
		}
		return nil
	})

	chi.WalkHosts(func(host *api.Host) error {
		switch {
		case host.GetReconcileAttributes().IsAdd():
			w.a.M(host).Info("ADD host: %s", host.Runtime.Address.CompactString())
		case host.GetReconcileAttributes().IsModify():
			w.a.M(host).Info("MODIFY host: %s", host.Runtime.Address.CompactString())
		case host.GetReconcileAttributes().IsFound():
			w.a.M(host).Info("FOUND host: %s", host.Runtime.Address.CompactString())
		default:
			w.a.M(host).Info("UNKNOWN host: %s", host.Runtime.Address.CompactString())
		}
		return nil
	})
}

// getRemoteServersGeneratorOptions build base set of RemoteServersOptions
// which are applied on each of `remote_servers` reconfiguration during reconcile cycle
func (w *worker) getRemoteServersGeneratorOptions() *chiConfig.RemoteServersOptions {
	// Base chiModel.RemoteServersOptions specifies to exclude:
	// 1. all newly added hosts
	// 2. all explicitly excluded hosts
	return chiConfig.NewRemoteServersOptions().ExcludeReconcileAttributes(
		api.NewChiHostReconcileAttributes().
			SetAdd().
			SetExclude(),
	)
}

// options build FilesGeneratorOptionsClickHouse
func (w *worker) options() *chiConfig.FilesGeneratorOptionsClickHouse {
	opts := w.getRemoteServersGeneratorOptions()
	w.a.Info("RemoteServersOptions: %s", opts)
	return chiConfig.NewConfigFilesGeneratorOptionsClickHouse().SetRemoteServersOptions(opts)
}

// createCHIFromObjectMeta
func (w *worker) createCHIFromObjectMeta(meta meta.Object, isCHI bool, options *normalizerCommon.Options) (*api.ClickHouseInstallation, error) {
	w.a.V(3).M(meta).S().P()
	defer w.a.V(3).M(meta).E().P()

	chi, err := w.c.GetCHIByObjectMeta(meta, isCHI)
	if err != nil {
		return nil, err
	}

	chi, err = w.normalizer.CreateTemplated(chi, options)
	if err != nil {
		return nil, err
	}

	return chi, nil
}
