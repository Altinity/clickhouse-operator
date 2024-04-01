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
	"time"

	"github.com/juliangruber/go-intersect"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilRuntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/altinity/queue"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// FinalizerName specifies name of the finalizer to be used with CHI
const FinalizerName = "finalizer.clickhouseinstallation.altinity.com"

// worker represents worker thread which runs reconcile tasks
type worker struct {
	c *Controller
	a Announcer
	//queue workqueue.RateLimitingInterface
	queue      queue.PriorityQueue
	normalizer *normalizer.Normalizer
	schemer    *schemer.ClusterSchemer
	start      time.Time
	task       task
}

// task represents context of a worker. This also can be called "a reconcile task"
type task struct {
	creator            *chiCreator.Creator
	registryReconciled *model.Registry
	registryFailed     *model.Registry
	cmUpdate           time.Time
	start              time.Time
}

// newTask creates new context
func newTask(creator *chiCreator.Creator) task {
	return task{
		creator:            creator,
		registryReconciled: model.NewRegistry(),
		registryFailed:     model.NewRegistry(),
		cmUpdate:           time.Time{},
		start:              time.Now(),
	}
}

// newWorker
// func (c *Controller) newWorker(q workqueue.RateLimitingInterface) *worker {
func (c *Controller) newWorker(q queue.PriorityQueue, sys bool) *worker {
	start := time.Now()
	if !sys {
		start = start.Add(api.DefaultReconcileThreadsWarmup)
	}
	return &worker{
		c:     c,
		a:     NewAnnouncer().WithController(c),
		queue: q,
		normalizer: normalizer.NewNormalizer(func(namespace, name string) (*core.Secret, error) {
			return c.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, controller.NewGetOptions())
		}),
		schemer: nil,
		start:   start,
	}
}

// newContext creates new reconcile task
func (w *worker) newTask(chi *api.ClickHouseInstallation) {
	w.task = newTask(chiCreator.NewCreator(chi))
}

// timeToStart specifies time that operator does not accept changes
const timeToStart = 1 * time.Minute

// isJustStarted checks whether worked just started
func (w *worker) isJustStarted() bool {
	return time.Since(w.start) < timeToStart
}

func (w *worker) isConfigurationChangeRequiresReboot(host *api.ChiHost) bool {
	return model.IsConfigurationChangeRequiresReboot(host)
}

// shouldForceRestartHost checks whether cluster requires hosts restart
func (w *worker) shouldForceRestartHost(host *api.ChiHost) bool {
	// RollingUpdate purpose is to always shut the host down.
	// It is such an interesting policy.
	if host.GetCHI().IsRollingUpdate() {
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
	if pod, err := w.c.getPod(host); err == nil {
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

func (w *worker) processReconcileCHI(ctx context.Context, cmd *ReconcileCHI) error {
	switch cmd.cmd {
	case reconcileAdd:
		return w.updateCHI(ctx, nil, cmd.new)
	case reconcileUpdate:
		return w.updateCHI(ctx, cmd.old, cmd.new)
	case reconcileDelete:
		return w.discoveryAndDeleteCHI(ctx, cmd.old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileCHIT(cmd *ReconcileCHIT) error {
	switch cmd.cmd {
	case reconcileAdd:
		return w.addChit(cmd.new)
	case reconcileUpdate:
		return w.updateChit(cmd.old, cmd.new)
	case reconcileDelete:
		return w.deleteChit(cmd.old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileChopConfig(cmd *ReconcileChopConfig) error {
	switch cmd.cmd {
	case reconcileAdd:
		return w.c.addChopConfig(cmd.new)
	case reconcileUpdate:
		return w.c.updateChopConfig(cmd.old, cmd.new)
	case reconcileDelete:
		return w.c.deleteChopConfig(cmd.old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileEndpoints(ctx context.Context, cmd *ReconcileEndpoints) error {
	switch cmd.cmd {
	case reconcileUpdate:
		return w.updateEndpoints(ctx, cmd.old, cmd.new)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcilePod(ctx context.Context, cmd *ReconcilePod) error {
	switch cmd.cmd {
	case reconcileAdd:
		w.a.V(1).M(cmd.new).F().Info("Add Pod. %s/%s", cmd.new.Namespace, cmd.new.Name)
		metricsPodAdd(ctx)
		return nil
	case reconcileUpdate:
		//ignore
		//w.a.V(1).M(cmd.new).F().Info("Update Pod. %s/%s", cmd.new.Namespace, cmd.new.Name)
		//metricsPodUpdate(ctx)
		return nil
	case reconcileDelete:
		w.a.V(1).M(cmd.old).F().Info("Delete Pod. %s/%s", cmd.old.Namespace, cmd.old.Name)
		metricsPodDelete(ctx)
		return nil
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processDropDns(ctx context.Context, cmd *DropDns) error {
	if chi, err := w.createCHIFromObjectMeta(cmd.initiator, false, normalizer.NewOptions()); err == nil {
		w.a.V(2).M(cmd.initiator).Info("flushing DNS for CHI %s", chi.Name)
		_ = w.ensureClusterSchemer(chi.FirstHost()).CHIDropDnsCache(ctx, chi)
	} else {
		w.a.M(cmd.initiator).F().Error("unable to find CHI by %v err: %v", cmd.initiator.Labels, err)
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
	case *ReconcileCHI:
		return w.processReconcileCHI(ctx, cmd)
	case *ReconcileCHIT:
		return w.processReconcileCHIT(cmd)
	case *ReconcileChopConfig:
		return w.processReconcileChopConfig(cmd)
	case *ReconcileEndpoints:
		return w.processReconcileEndpoints(ctx, cmd)
	case *ReconcilePod:
		return w.processReconcilePod(ctx, cmd)
	case *DropDns:
		return w.processDropDns(ctx, cmd)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// normalize
func (w *worker) normalize(c *api.ClickHouseInstallation) *api.ClickHouseInstallation {

	chi, err := w.normalizer.CreateTemplatedCHI(c, normalizer.NewOptions())
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to normalize CHI 1: %v", err)
	}

	ips := w.c.getPodsIPs(chi)
	w.a.V(1).M(chi).Info("IPs of the CHI normalizer %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
	opts := normalizer.NewOptions()
	opts.DefaultUserAdditionalIPs = ips

	chi, err = w.normalizer.CreateTemplatedCHI(c, opts)
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
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
	if !chi.ObjectMeta.DeletionTimestamp.IsZero() {
		return false
	}

	// Finalizer can already be listed in CHI, do nothing in this case
	if util.InArray(FinalizerName, chi.ObjectMeta.Finalizers) {
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

	if chi, err := w.createCHIFromObjectMeta(&new.ObjectMeta, false, normalizer.NewOptions()); err == nil {
		w.a.V(1).M(chi).Info("updating endpoints for CHI-1 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-1 update endpoints %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
		opts := normalizer.NewOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(&new.ObjectMeta, false, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-1")

			// TODO unify with finalize reconcile
			w.newTask(chi)
			w.reconcileCHIConfigMapUsers(ctx, chi)
			w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
				TolerateAbsence: true,
				CopyCHIStatusOptions: api.CopyCHIStatusOptions{
					Normalized: true,
				},
			})
		} else {
			w.a.M(&new.ObjectMeta).F().Error("internal unable to find CHI by %v err: %v", new.Labels, err)
		}
	} else {
		w.a.M(&new.ObjectMeta).F().Error("external unable to find CHI by %v err %v", new.Labels, err)
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

	if update && (old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion) {
		// No need to react
		w.a.V(3).M(new).F().Info("ResourceVersion did not change: %s", new.ObjectMeta.ResourceVersion)
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
		generationIsOk = chi.Generation == chi.GetAncestor().Generation
		log.V(1).Info(
			"CHI %s has ancestor. Generations. Prev: %d Cur: %d Generation is the same: %t",
			chi.Name,
			chi.GetAncestor().Generation,
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

// logCHI writes a CHI into the log
func (w *worker) logCHI(name string, chi *api.ClickHouseInstallation) {
	w.a.V(1).M(chi).Info(
		"logCHI %s start--------------------------------------------:\n%s\nlogCHI %s end--------------------------------------------",
		name,
		name,
		chi.YAML(api.CopyCHIOptions{SkipStatus: true, SkipManagedFields: true}),
	)
}

// logActionPlan logs action plan
func (w *worker) logActionPlan(ap *model.ActionPlan) {
	w.a.Info(
		"ActionPlan start---------------------------------------------:\n%s\nActionPlan end---------------------------------------------",
		ap,
	)
}

// logOldAndNew writes old and new CHIs into the log
func (w *worker) logOldAndNew(name string, old, new *api.ClickHouseInstallation) {
	w.logCHI(name+" old", old)
	w.logCHI(name+" new", new)
}

func (w *worker) waitForIPAddresses(ctx context.Context, chi *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}
	if chi.IsStopped() {
		// No need to wait for stopped CHI
		return
	}
	w.a.V(1).M(chi).F().S().Info("wait for IP addresses to be assigned to all pods")
	start := time.Now()
	w.c.poll(ctx, chi, func(c *api.ClickHouseInstallation, e error) bool {
		if len(c.Status.GetPodIPs()) >= len(c.Status.GetPods()) {
			// Stop polling
			w.a.V(1).M(c).Info("all IP addresses are in place")
			return false
		}
		if time.Now().Sub(start) > 1*time.Minute {
			// Stop polling
			w.a.V(1).M(c).Warning("not all IP addresses are in place but time has elapsed")
			return false
		}
		// Continue polling
		w.a.V(1).M(c).Warning("still waiting - not all IP addresses are in place yet")
		return true
	})
}

// excludeStoppedCHIFromMonitoring excludes stopped CHI from monitoring
func (w *worker) excludeStoppedCHIFromMonitoring(chi *api.ClickHouseInstallation) {
	if !chi.IsStopped() {
		// No need to exclude non-stopped CHI
		return
	}

	w.a.V(1).
		WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
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
		WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(chi).
		M(chi).F().
		Info("add CHI to monitoring")
	w.c.updateWatch(chi)
}

func (w *worker) markReconcileStart(ctx context.Context, chi *api.ClickHouseInstallation, ap *model.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	chi.EnsureStatus().ReconcileStart(ap.GetRemovedHostsNum())
	_ = w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		CopyCHIStatusOptions: api.CopyCHIStatusOptions{
			MainFields: true,
		},
	})

	w.a.V(1).
		WithEvent(chi, eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(chi).
		WithStatusActions(chi).
		M(chi).F().
		Info("reconcile started, task id: %s", chi.Spec.GetTaskID())
	w.a.V(2).M(chi).F().Info("action plan\n%s\n", ap.String())
}

func (w *worker) finalizeReconcileAndMarkCompleted(ctx context.Context, _chi *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.a.V(1).M(_chi).F().S().Info("finalize reconcile")

	// Update CHI object
	if chi, err := w.createCHIFromObjectMeta(&_chi.ObjectMeta, true, normalizer.NewOptions()); err == nil {
		w.a.V(1).M(chi).Info("updating endpoints for CHI-2 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-2 finalize reconcile %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
		opts := normalizer.NewOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(&_chi.ObjectMeta, true, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-2")
			chi.SetAncestor(chi.GetTarget())
			chi.SetTarget(nil)
			chi.EnsureStatus().ReconcileComplete()
			// TODO unify with update endpoints
			w.newTask(chi)
			w.reconcileCHIConfigMapUsers(ctx, chi)
			w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
				CopyCHIStatusOptions: api.CopyCHIStatusOptions{
					WholeStatus: true,
				},
			})
		} else {
			w.a.M(&_chi.ObjectMeta).F().Error("internal unable to find CHI by %v err: %v", _chi.Labels, err)
		}
	} else {
		w.a.M(&_chi.ObjectMeta).F().Error("external unable to find CHI by %v err %v", _chi.Labels, err)
	}

	w.a.V(1).
		WithEvent(_chi, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusAction(_chi).
		WithStatusActions(_chi).
		M(_chi).F().
		Info("reconcile completed successfully, task id: %s", _chi.Spec.GetTaskID())
}

func (w *worker) markReconcileCompletedUnsuccessfully(ctx context.Context, chi *api.ClickHouseInstallation, err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	switch {
	case err == nil:
		chi.EnsureStatus().ReconcileComplete()
	case errors.Is(err, errCRUDAbort):
		chi.EnsureStatus().ReconcileAbort()
	}
	w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		CopyCHIStatusOptions: api.CopyCHIStatusOptions{
			MainFields: true,
		},
	})

	w.a.V(1).
		WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
		WithStatusAction(chi).
		WithStatusActions(chi).
		M(chi).F().
		Warning("reconcile completed UNSUCCESSFULLY, task id: %s", chi.Spec.GetTaskID())
}

func (w *worker) walkHosts(ctx context.Context, chi *api.ClickHouseInstallation, ap *model.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	objs := w.c.discovery(ctx, chi)
	ap.WalkAdded(
		// Walk over added clusters
		func(cluster *api.Cluster) {
			cluster.WalkHosts(func(host *api.ChiHost) error {

				// Name of the StatefulSet for this host
				name := model.CreateStatefulSetName(host)
				// Have we found this StatefulSet
				found := false

				objs.WalkStatefulSet(func(meta meta.ObjectMeta) {
					if name == meta.Name {
						// StatefulSet of this host already exist
						found = true
					}
				})

				if found {
					// StatefulSet of this host already exist, we can't ADD it for sure
					// It looks like FOUND is the most correct approach
					host.GetReconcileAttributes().SetFound()
					w.a.V(1).M(chi).Info("Add host as FOUND. Host was found as sts %s", host.GetName())
				} else {
					// StatefulSet of this host does not exist, looks like we need to ADD it
					host.GetReconcileAttributes().SetAdd()
					w.a.V(1).M(chi).Info("Add host as ADD. Host was not found as sts %s", host.GetName())
				}

				return nil
			})
		},
		// Walk over added shards
		func(shard *api.ChiShard) {
			// Mark all hosts of the shard as newly added
			shard.WalkHosts(func(host *api.ChiHost) error {
				host.GetReconcileAttributes().SetAdd()
				return nil
			})
		},
		// Walk over added hosts
		func(host *api.ChiHost) {
			host.GetReconcileAttributes().SetAdd()
		},
	)

	ap.WalkModified(
		func(cluster *api.Cluster) {
		},
		func(shard *api.ChiShard) {
		},
		func(host *api.ChiHost) {
			host.GetReconcileAttributes().SetModify()
		},
	)

	chi.WalkHosts(func(host *api.ChiHost) error {
		switch {
		case host.GetReconcileAttributes().IsAdd():
			// Already added
			return nil
		case host.GetReconcileAttributes().IsModify():
			// Already modified
			return nil
		default:
			// Not clear yet
			host.GetReconcileAttributes().SetFound()
		}
		return nil
	})

	chi.WalkHosts(func(host *api.ChiHost) error {
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

// baseRemoteServersGeneratorOptions build base set of RemoteServersGeneratorOptions
// which are applied on each of `remote_servers` reconfiguration during reconcile cycle
func (w *worker) baseRemoteServersGeneratorOptions() *model.RemoteServersGeneratorOptions {
	opts := model.NewRemoteServersGeneratorOptions()
	opts.ExcludeReconcileAttributes(
		api.NewChiHostReconcileAttributes().SetAdd(),
	)

	return opts
}

// options build ClickHouseConfigFilesGeneratorOptions
func (w *worker) options(excludeHosts ...*api.ChiHost) *model.ClickHouseConfigFilesGeneratorOptions {
	// Stringify
	str := ""
	for _, host := range excludeHosts {
		str += fmt.Sprintf("name: '%s' sts: '%s'", host.GetName(), host.Runtime.Address.StatefulSet)
	}

	opts := w.baseRemoteServersGeneratorOptions().ExcludeHosts(excludeHosts...)
	w.a.Info("RemoteServersGeneratorOptions: %s, excluded host(s): %s", opts, str)
	return model.NewClickHouseConfigFilesGeneratorOptions().SetRemoteServersGeneratorOptions(opts)
}

// prepareHostStatefulSetWithStatus prepares host's StatefulSet status
func (w *worker) prepareHostStatefulSetWithStatus(ctx context.Context, host *api.ChiHost, shutdown bool) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.prepareDesiredStatefulSet(host, shutdown)
	host.GetReconcileAttributes().SetStatus(w.getStatefulSetStatus(host))
}

// prepareDesiredStatefulSet prepares desired StatefulSet
func (w *worker) prepareDesiredStatefulSet(host *api.ChiHost, shutdown bool) {
	host.Runtime.DesiredStatefulSet = w.task.creator.CreateStatefulSet(host, shutdown)
}

type migrateTableOptions struct {
	forceMigrate bool
	dropReplica  bool
}

func (o *migrateTableOptions) ForceMigrate() bool {
	if o == nil {
		return false
	}
	return o.forceMigrate
}

func (o *migrateTableOptions) DropReplica() bool {
	if o == nil {
		return false
	}
	return o.dropReplica
}

type migrateTableOptionsArr []*migrateTableOptions

// NewMigrateTableOptionsArr creates new migrateTableOptions array
func NewMigrateTableOptionsArr(opts ...*migrateTableOptions) (res migrateTableOptionsArr) {
	return append(res, opts...)
}

// First gets first option
func (a migrateTableOptionsArr) First() *migrateTableOptions {
	if len(a) > 0 {
		return a[0]
	}
	return nil
}

// migrateTables
func (w *worker) migrateTables(ctx context.Context, host *api.ChiHost, opts ...*migrateTableOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if !w.shouldMigrateTables(host, opts...) {
		w.a.V(1).
			M(host).F().
			Info(
				"No need to add tables on host %d to shard %d in cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	// Need to migrate tables

	if w.shouldDropReplica(host, opts...) {
		w.a.V(1).
			M(host).F().
			Info(
				"Need to drop replica on host %d to shard %d in cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		w.dropReplica(ctx, host, &dropReplicaOptions{forceDrop: true})
	}

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info(
			"Adding tables on shard/host:%d/%d cluster:%s",
			host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName)

	err := w.ensureClusterSchemer(host).HostCreateTables(ctx, host)
	if err == nil {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Info("Tables added successfully on shard/host:%d/%d cluster:%s",
				host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName)
		host.GetCHI().EnsureStatus().PushHostTablesCreated(model.CreateFQDN(host))
	} else {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Error("ERROR add tables added successfully on shard/host:%d/%d cluster:%s err:%v",
				host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName, err)
	}
	return err
}

// shouldMigrateTables
func (w *worker) shouldMigrateTables(host *api.ChiHost, opts ...*migrateTableOptions) bool {
	o := NewMigrateTableOptionsArr(opts...).First()

	// Deal with special cases in order of priority
	switch {
	case host.IsStopped():
		// Stopped host is not able to receive any data, migration is inapplicable
		return false

	case o.ForceMigrate():
		// Force migration requested
		return true

	case model.HostHasTablesCreated(host):
		// This host is listed as having tables created already, no need to migrate again
		return false

	case model.HostIsNewOne(host):
		// CHI is new, all hosts were added
		return false
	}

	// In all the rest cases - perform migration
	return true
}

// shouldDropTables
func (w *worker) shouldDropReplica(host *api.ChiHost, opts ...*migrateTableOptions) bool {
	o := NewMigrateTableOptionsArr(opts...).First()

	// Deal with special cases
	switch {
	case o.DropReplica():
		return true

	}

	return false
}

// excludeHost excludes host from ClickHouse clusters if required
func (w *worker) excludeHost(ctx context.Context, host *api.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).M(host).F().S().Info("exclude host start")
	defer log.V(1).M(host).F().E().Info("exclude host end")

	if !w.shouldExcludeHost(host) {
		return nil
	}

	w.a.V(1).
		M(host).F().
		Info("Exclude from cluster host %d shard %d cluster %s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	_ = w.excludeHostFromService(ctx, host)
	w.excludeHostFromClickHouseCluster(ctx, host)
	return nil
}

// completeQueries wait for running queries to complete
func (w *worker) completeQueries(ctx context.Context, host *api.ChiHost) error {
	log.V(1).M(host).F().S().Info("complete queries start")
	defer log.V(1).M(host).F().E().Info("complete queries end")

	if w.shouldWaitQueries(host) {
		return w.waitHostNoActiveQueries(ctx, host)
	}

	return nil
}

// shouldIncludeHost determines whether host to be included into cluster after reconciling
func (w *worker) shouldIncludeHost(host *api.ChiHost) bool {
	switch {
	case host.IsStopped():
		// No need to include stopped host
		return false
	}
	return true
}

// includeHost includes host back back into ClickHouse clusters
func (w *worker) includeHost(ctx context.Context, host *api.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if !w.shouldIncludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to include into cluster host %d shard %d cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	w.a.V(1).
		M(host).F().
		Info("Include into cluster host %d shard %d cluster %s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	w.includeHostIntoClickHouseCluster(ctx, host)
	_ = w.includeHostIntoService(ctx, host)

	return nil
}

// excludeHostFromService
func (w *worker) excludeHostFromService(ctx context.Context, host *api.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.deleteLabelReadyPod(ctx, host)
	_ = w.c.deleteAnnotationReadyService(ctx, host)
	return nil
}

// includeHostIntoService
func (w *worker) includeHostIntoService(ctx context.Context, host *api.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.appendLabelReadyOnPod(ctx, host)
	_ = w.c.appendAnnotationReadyOnService(ctx, host)
	return nil
}

// excludeHostFromClickHouseCluster excludes host from ClickHouse configuration
func (w *worker) excludeHostFromClickHouseCluster(ctx context.Context, host *api.ChiHost) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Remove host from cluster config and wait for ClickHouse to pick-up the change
	if w.shouldWaitExcludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("going to exclude host %d shard %d cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

		// Specify in options to exclude host from ClickHouse config file
		options := w.options(host)
		_ = w.reconcileCHIConfigMapCommon(ctx, host.GetCHI(), options)
		// Wait for ClickHouse to pick-up the change
		_ = w.waitHostNotInCluster(ctx, host)
	}
}

// includeHostIntoClickHouseCluster includes host into ClickHouse configuration
func (w *worker) includeHostIntoClickHouseCluster(ctx context.Context, host *api.ChiHost) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Add host to the cluster config
	options := w.options()
	_ = w.reconcileCHIConfigMapCommon(ctx, host.GetCHI(), options)

	if !w.shouldWaitIncludeHost(host) {
		return
	}
	// Wait for ClickHouse to pick-up the change
	_ = w.waitHostInCluster(ctx, host)
}

// shouldExcludeHost determines whether host to be excluded from cluster before reconciling
func (w *worker) shouldExcludeHost(host *api.ChiHost) bool {
	switch {
	case host.IsStopped():
		w.a.V(1).
			M(host).F().
			Info("Host is stopped, no need to exclude stopped host. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	case host.GetShard().HostsCount() == 1:
		w.a.V(1).
			M(host).F().
			Info("Host is the only host in the shard (means no replication), no need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	case w.shouldForceRestartHost(host):
		w.a.V(1).
			M(host).F().
			Info("Host should be restarted, need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	case host.GetReconcileAttributes().GetStatus() == api.ObjectStatusNew:
		w.a.V(1).
			M(host).F().
			Info("Host is new, no need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	case host.GetReconcileAttributes().GetStatus() == api.ObjectStatusSame:
		w.a.V(1).
			M(host).F().
			Info("Host is the same, would not be updated, no need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	}

	w.a.V(1).
		M(host).F().
		Info("Host should be excluded. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	return true
}

// shouldWaitExcludeHost determines whether reconciler should wait for the host to be excluded from cluster
func (w *worker) shouldWaitExcludeHost(host *api.ChiHost) bool {
	// Check CHI settings
	switch {
	case host.GetCHI().GetReconciling().IsReconcilingPolicyWait():
		w.a.V(1).
			M(host).F().
			Info("IsReconcilingPolicyWait() need to exclude host %d shard %d cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	case host.GetCHI().GetReconciling().IsReconcilingPolicyNoWait():
		w.a.V(1).
			M(host).F().
			Info("IsReconcilingPolicyNoWait() need NOT to exclude host %d shard %d cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	}

	w.a.V(1).
		M(host).F().
		Info("fallback to operator's settings. host %d shard %d cluster %s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
	return chop.Config().Reconcile.Host.Wait.Exclude.Value()
}

// shouldWaitQueries determines whether reconciler should wait for the host to complete running queries
func (w *worker) shouldWaitQueries(host *api.ChiHost) bool {
	switch {
	case host.GetReconcileAttributes().GetStatus() == api.ObjectStatusNew:
		w.a.V(1).
			M(host).F().
			Info("No need to wait for queries to complete, host is a new one. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	case chop.Config().Reconcile.Host.Wait.Queries.Value():
		w.a.V(1).
			M(host).F().
			Info("Will wait for queries to complete according to CHOp config 'reconcile.host.wait.queries' setting. "+
				"Host is not yet in the cluster. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	case host.GetCHI().GetReconciling().IsReconcilingPolicyWait():
		w.a.V(1).
			M(host).F().
			Info("Will wait for queries to complete according to CHI 'reconciling.policy' setting. "+
				"Host is not yet in the cluster. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	}

	w.a.V(1).
		M(host).F().
		Info("Will NOT wait for queries to complete on the host. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
	return false
}

// shouldWaitIncludeHost determines whether reconciler should wait for the host to be included into cluster
func (w *worker) shouldWaitIncludeHost(host *api.ChiHost) bool {
	status := host.GetReconcileAttributes().GetStatus()
	switch {
	case status == api.ObjectStatusNew:
		return false
	case status == api.ObjectStatusSame:
		// The same host was not modified and no need to wait it to be included - it already is
		return false
	case host.GetShard().HostsCount() == 1:
		// No need to wait one-host-shard
		return false
	case host.GetCHI().GetReconciling().IsReconcilingPolicyWait():
		// Check CHI settings - explicitly requested to wait
		return true
	case host.GetCHI().GetReconciling().IsReconcilingPolicyNoWait():
		// Check CHI settings - explicitly requested to not wait
		return false
	}

	// Fallback to operator's settings
	return chop.Config().Reconcile.Host.Wait.Include.Value()
}

// waitHostInCluster
func (w *worker) waitHostInCluster(ctx context.Context, host *api.ChiHost) error {
	return w.c.pollHost(ctx, host, nil, w.ensureClusterSchemer(host).IsHostInCluster)
}

// waitHostNotInCluster
func (w *worker) waitHostNotInCluster(ctx context.Context, host *api.ChiHost) error {
	return w.c.pollHost(ctx, host, nil, func(ctx context.Context, host *api.ChiHost) bool {
		return !w.ensureClusterSchemer(host).IsHostInCluster(ctx, host)
	})
}

// waitHostNoActiveQueries
func (w *worker) waitHostNoActiveQueries(ctx context.Context, host *api.ChiHost) error {
	return w.c.pollHost(ctx, host, nil, func(ctx context.Context, host *api.ChiHost) bool {
		n, _ := w.ensureClusterSchemer(host).HostActiveQueriesNum(ctx, host)
		return n <= 1
	})
}

// createCHIFromObjectMeta
func (w *worker) createCHIFromObjectMeta(objectMeta *meta.ObjectMeta, isCHI bool, options *normalizer.Options) (*api.ClickHouseInstallation, error) {
	w.a.V(3).M(objectMeta).S().P()
	defer w.a.V(3).M(objectMeta).E().P()

	chi, err := w.c.GetCHIByObjectMeta(objectMeta, isCHI)
	if err != nil {
		return nil, err
	}

	chi, err = w.normalizer.CreateTemplatedCHI(chi, options)
	if err != nil {
		return nil, err
	}

	return chi, nil
}

// updateConfigMap
func (w *worker) updateConfigMap(ctx context.Context, chi *api.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	updatedConfigMap, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(ctx, configMap, controller.NewUpdateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)
		if updatedConfigMap.ResourceVersion != configMap.ResourceVersion {
			w.task.cmUpdate = time.Now()
		}
	} else {
		w.a.WithEvent(chi, eventActionUpdate, eventReasonUpdateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Update ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// createConfigMap
func (w *worker) createConfigMap(ctx context.Context, chi *api.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Create(ctx, configMap, controller.NewCreateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Create ConfigMap %s/%s", configMap.Namespace, configMap.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Create ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// updateService
func (w *worker) updateService(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	curService *core.Service,
	targetService *core.Service,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if curService.Spec.Type != targetService.Spec.Type {
		return fmt.Errorf(
			"just recreate the service in case of service type change '%s'=>'%s'",
			curService.Spec.Type, targetService.Spec.Type)
	}

	// Updating a Service is a complicated business

	newService := targetService.DeepCopy()

	// spec.resourceVersion is required in order to update an object
	newService.ResourceVersion = curService.ResourceVersion

	//
	// Migrate ClusterIP to the new service
	//
	// spec.clusterIP field is immutable, need to use already assigned value
	// From https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service
	// Kubernetes assigns this Service an IP address (sometimes called the “cluster IP”), which is used by the Service proxies
	// See also https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// You can specify your own cluster IP address as part of a Service creation request. To do this, set the .spec.clusterIP
	newService.Spec.ClusterIP = curService.Spec.ClusterIP

	//
	// Migrate existing ports to the new service for NodePort and LoadBalancer services
	//
	// The port on each node on which this service is exposed when type=NodePort or LoadBalancer.
	// Usually assigned by the system. If specified, it will be allocated to the service if unused
	// or else creation of the service will fail.
	// Default is to auto-allocate a port if the ServiceType of this Service requires one.
	// More info: https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport

	// !!! IMPORTANT !!!
	// No changes in service type is allowed.
	// Already exposed port details can not be changed.

	serviceTypeIsNodePort := (curService.Spec.Type == core.ServiceTypeNodePort) && (newService.Spec.Type == core.ServiceTypeNodePort)
	serviceTypeIsLoadBalancer := (curService.Spec.Type == core.ServiceTypeLoadBalancer) && (newService.Spec.Type == core.ServiceTypeLoadBalancer)
	if serviceTypeIsNodePort || serviceTypeIsLoadBalancer {
		for i := range newService.Spec.Ports {
			newPort := &newService.Spec.Ports[i]
			for j := range curService.Spec.Ports {
				curPort := &curService.Spec.Ports[j]
				if newPort.Port == curPort.Port {
					// Already have this port specified - reuse all internals,
					// due to limitations with auto-assigned values
					*newPort = *curPort
					w.a.M(chi).F().Info("reuse Port %d values", newPort.Port)
					break
				}
			}
		}
	}

	//
	// Migrate HealthCheckNodePort to the new service
	//
	// spec.healthCheckNodePort field is used with ExternalTrafficPolicy=Local only and is immutable within ExternalTrafficPolicy=Local
	// In case ExternalTrafficPolicy is changed it seems to be irrelevant
	// https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/#preserving-the-client-source-ip
	curExternalTrafficPolicyTypeLocal := curService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal
	newExternalTrafficPolicyTypeLocal := newService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal
	if curExternalTrafficPolicyTypeLocal && newExternalTrafficPolicyTypeLocal {
		newService.Spec.HealthCheckNodePort = curService.Spec.HealthCheckNodePort
	}

	//
	// Migrate LoadBalancerClass to the new service
	//
	// This field can only be set when creating or updating a Service to type 'LoadBalancer'.
	// Once set, it can not be changed. This field will be wiped when a service is updated to a non 'LoadBalancer' type.
	if curService.Spec.LoadBalancerClass != nil {
		newService.Spec.LoadBalancerClass = curService.Spec.LoadBalancerClass
	}

	//
	// Migrate labels, annotations and finalizers to the new service
	//
	newService.ObjectMeta.Labels = util.MergeStringMapsPreserve(newService.ObjectMeta.Labels, curService.ObjectMeta.Labels)
	newService.ObjectMeta.Annotations = util.MergeStringMapsPreserve(newService.ObjectMeta.Annotations, curService.ObjectMeta.Annotations)
	newService.ObjectMeta.Finalizers = util.MergeStringArrays(newService.ObjectMeta.Finalizers, curService.ObjectMeta.Finalizers)

	//
	// And only now we are ready to actually update the service with new version of the service
	//

	_, err := w.c.kubeClient.CoreV1().Services(newService.Namespace).Update(ctx, newService, controller.NewUpdateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update Service success: %s/%s", newService.Namespace, newService.Name)
	} else {
		w.a.M(chi).F().Error("Update Service fail: %s/%s failed with error %v", newService.Namespace, newService.Name)
	}

	return err
}

// createService
func (w *worker) createService(ctx context.Context, chi *api.ClickHouseInstallation, service *core.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().Services(service.Namespace).Create(ctx, service, controller.NewCreateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("OK Create Service: %s/%s", service.Namespace, service.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED Create Service: %s/%s err: %v", service.Namespace, service.Name, err)
	}

	return err
}

// createSecret
func (w *worker) createSecret(ctx context.Context, chi *api.ClickHouseInstallation, secret *core.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, controller.NewCreateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Create Secret %s/%s", secret.Namespace, secret.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Create Secret %s/%s failed with error %v", secret.Namespace, secret.Name, err)
	}

	return err
}

// getStatefulSetStatus gets StatefulSet status
func (w *worker) getStatefulSetStatus(host *api.ChiHost) api.ObjectStatus {
	meta := host.Runtime.DesiredStatefulSet.ObjectMeta
	w.a.V(2).M(meta).S().Info(util.NamespaceNameString(meta))
	defer w.a.V(2).M(meta).E().Info(util.NamespaceNameString(meta))

	curStatefulSet, err := w.c.getStatefulSet(&meta, false)
	switch {
	case curStatefulSet != nil:
		w.a.V(2).M(meta).Info("Have StatefulSet available, try to perform label-based comparison for %s/%s", meta.Namespace, meta.Name)
		return w.getObjectStatusFromMetas(curStatefulSet.ObjectMeta, meta)

	case apiErrors.IsNotFound(err):
		// StatefulSet is not found at the moment.
		// However, it may be just deleted
		w.a.V(2).M(meta).Info("No cur StatefulSet available and it is not found. Either new one or deleted for %s/%s", meta.Namespace, meta.Name)
		if host.IsNewOne() {
			w.a.V(2).M(meta).Info("No cur StatefulSet available and it is not found and is a new one. New one for %s/%s", meta.Namespace, meta.Name)
			return api.ObjectStatusNew
		}
		w.a.V(1).M(meta).Warning("No cur StatefulSet available but host has an ancestor. Found deleted StatefulSet. for %s/%s", meta.Namespace, meta.Name)
		return api.ObjectStatusModified

	default:
		w.a.V(2).M(meta).Warning("Have no StatefulSet available, nor it is not found for %s/%s err: %v", meta.Namespace, meta.Name, err)
		return api.ObjectStatusUnknown
	}
}

// getObjectStatusFromMetas gets StatefulSet status from cur and new meta infos
func (w *worker) getObjectStatusFromMetas(curMeta, newMeta meta.ObjectMeta) api.ObjectStatus {
	// Try to perform label-based version comparison
	curVersion, curHasLabel := model.GetObjectVersion(curMeta)
	newVersion, newHasLabel := model.GetObjectVersion(newMeta)

	if !curHasLabel || !newHasLabel {
		w.a.M(newMeta).F().Warning(
			"Not enough labels to compare objects, can not say for sure what exactly is going on. Object: %s",
			util.NamespaceNameString(newMeta),
		)
		return api.ObjectStatusUnknown
	}

	//
	// We have both set of labels, can compare them
	//

	if curVersion == newVersion {
		w.a.M(newMeta).F().Info(
			"cur and new objects are equal based on object version label. Update of the object is not required. Object: %s",
			util.NamespaceNameString(newMeta),
		)
		return api.ObjectStatusSame
	}

	w.a.M(newMeta).F().Info(
		"cur and new objects ARE DIFFERENT based on object version label: Update of the object is required. Object: %s",
		util.NamespaceNameString(newMeta),
	)

	return api.ObjectStatusModified
}

// createStatefulSet
func (w *worker) createStatefulSet(ctx context.Context, host *api.ChiHost, register bool) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	statefulSet := host.Runtime.DesiredStatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(statefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(statefulSet.ObjectMeta))

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Create StatefulSet %s/%s - started", statefulSet.Namespace, statefulSet.Name)

	action := w.c.createStatefulSet(ctx, host)

	if register {
		host.GetCHI().EnsureStatus().HostAdded()
		_ = w.c.updateCHIObjectStatus(ctx, host.GetCHI(), UpdateCHIStatusOptions{
			CopyCHIStatusOptions: api.CopyCHIStatusOptions{
				MainFields: true,
			},
		})
	}

	switch action {
	case nil:
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Info("Create StatefulSet %s/%s - completed", statefulSet.Namespace, statefulSet.Name)
		return nil
	case errCRUDAbort:
		w.a.WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.GetCHI()).
			WithStatusError(host.GetCHI()).
			M(host).F().
			Error("Create StatefulSet %s/%s - failed with error %v", statefulSet.Namespace, statefulSet.Name, action)
		return action
	case errCRUDIgnore:
		w.a.WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Warning("Create StatefulSet %s/%s - error ignored", statefulSet.Namespace, statefulSet.Name)
		return nil
	case errCRUDRecreate:
		w.a.V(1).M(host).Warning("Got recreate action. Ignore and continue for now")
		return nil
	case errCRUDUnexpectedFlow:
		w.a.V(1).M(host).Warning("Got unexpected flow action. Ignore and continue for now")
		return nil
	}

	w.a.V(1).M(host).Warning("Got unexpected flow. This is strange. Ignore and continue for now")
	return nil
}

// waitConfigMapPropagation
func (w *worker) waitConfigMapPropagation(ctx context.Context, host *api.ChiHost) bool {
	// No need to wait for ConfigMap propagation on stopped host
	if host.IsStopped() {
		w.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - on stopped host")
		return false
	}

	// No need to wait on unchanged ConfigMap
	if w.task.cmUpdate.IsZero() {
		w.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - no changes in ConfigMap")
		return false
	}

	// What timeout is expected to be enough for ConfigMap propagation?
	// In case timeout is not specified, no need to wait
	timeout := host.GetCHI().GetReconciling().GetConfigMapPropagationTimeoutDuration()
	if timeout == 0 {
		w.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - not applicable")
		return false
	}

	// How much time has elapsed since last ConfigMap update?
	// May be there is not need to wait already
	elapsed := time.Now().Sub(w.task.cmUpdate)
	if elapsed >= timeout {
		w.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - already elapsed. %s/%s", elapsed, timeout)
		return false
	}

	// Looks like we need to wait for Configmap propagation, after all
	wait := timeout - elapsed
	w.a.V(1).M(host).F().Info("Wait for ConfigMap propagation for %s %s/%s", wait, elapsed, timeout)
	if util.WaitContextDoneOrTimeout(ctx, wait) {
		log.V(2).Info("task is done")
		return true
	}

	return false
}

// updateStatefulSet
func (w *worker) updateStatefulSet(ctx context.Context, host *api.ChiHost, register bool) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Helpers
	newStatefulSet := host.Runtime.DesiredStatefulSet
	curStatefulSet := host.Runtime.CurStatefulSet

	w.a.V(2).M(host).S().Info(newStatefulSet.Name)
	defer w.a.V(2).M(host).E().Info(newStatefulSet.Name)

	namespace := newStatefulSet.Namespace
	name := newStatefulSet.Name

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Update StatefulSet(%s/%s) - started", namespace, name)

	if w.waitConfigMapPropagation(ctx, host) {
		log.V(2).Info("task is done")
		return nil
	}

	action := errCRUDRecreate
	if k8s.IsStatefulSetReady(curStatefulSet) {
		action = w.c.updateStatefulSet(ctx, curStatefulSet, newStatefulSet, host)
	}

	switch action {
	case nil:
		if register {
			host.GetCHI().EnsureStatus().HostUpdated()
			_ = w.c.updateCHIObjectStatus(ctx, host.GetCHI(), UpdateCHIStatusOptions{
				CopyCHIStatusOptions: api.CopyCHIStatusOptions{
					MainFields: true,
				},
			})
		}
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Info("Update StatefulSet(%s/%s) - completed", namespace, name)
		return nil
	case errCRUDAbort:
		w.a.V(1).M(host).Info("Update StatefulSet(%s/%s) - got abort. Abort", namespace, name)
		return errCRUDAbort
	case errCRUDIgnore:
		w.a.V(1).M(host).Info("Update StatefulSet(%s/%s) - got ignore. Ignore", namespace, name)
		return nil
	case errCRUDRecreate:
		w.a.WithEvent(host.GetCHI(), eventActionUpdate, eventReasonUpdateInProgress).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Info("Update StatefulSet(%s/%s) switch from Update to Recreate", namespace, name)
		w.dumpStatefulSetDiff(host, curStatefulSet, newStatefulSet)
		return w.recreateStatefulSet(ctx, host, register)
	case errCRUDUnexpectedFlow:
		w.a.V(1).M(host).Warning("Got unexpected flow action. Ignore and continue for now")
		return nil
	}

	w.a.V(1).M(host).Warning("Got unexpected flow. This is strange. Ignore and continue for now")
	return nil
}

// recreateStatefulSet
func (w *worker) recreateStatefulSet(ctx context.Context, host *api.ChiHost, register bool) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.deleteStatefulSet(ctx, host)
	_ = w.reconcilePVCs(ctx, host, api.DesiredStatefulSet)
	return w.createStatefulSet(ctx, host, register)
}

// applyPVCResourcesRequests
func (w *worker) applyPVCResourcesRequests(
	pvc *core.PersistentVolumeClaim,
	template *api.VolumeClaimTemplate,
) bool {
	return w.applyResourcesList(pvc.Spec.Resources.Requests, template.Spec.Resources.Requests)
}

// applyResourcesList
func (w *worker) applyResourcesList(
	curResourceList core.ResourceList,
	desiredResourceList core.ResourceList,
) bool {
	// Prepare lists of resource names
	var curResourceNames []core.ResourceName
	for resourceName := range curResourceList {
		curResourceNames = append(curResourceNames, resourceName)
	}
	var desiredResourceNames []core.ResourceName
	for resourceName := range desiredResourceList {
		desiredResourceNames = append(desiredResourceNames, resourceName)
	}

	resourceNames := intersect.Simple(curResourceNames, desiredResourceNames)
	updated := false
	for _, resourceName := range resourceNames.([]interface{}) {
		updated = updated || w.applyResource(curResourceList, desiredResourceList, resourceName.(core.ResourceName))
	}
	return updated
}

// applyResource
func (w *worker) applyResource(
	curResourceList core.ResourceList,
	desiredResourceList core.ResourceList,
	resourceName core.ResourceName,
) bool {
	if (curResourceList == nil) || (desiredResourceList == nil) {
		// Nowhere or nothing to apply
		return false
	}

	var ok bool
	var curResourceQuantity resource.Quantity
	var desiredResourceQuantity resource.Quantity

	if curResourceQuantity, ok = curResourceList[resourceName]; !ok {
		// No such resource in target list
		return false
	}

	if desiredResourceQuantity, ok = desiredResourceList[resourceName]; !ok {
		// No such resource in desired list
		return false
	}

	if curResourceQuantity.Equal(desiredResourceQuantity) {
		// No need to apply
		return false
	}

	// Update resource
	curResourceList[resourceName] = desiredResourceList[resourceName]
	return true
}

func (w *worker) ensureClusterSchemer(host *api.ChiHost) *schemer.ClusterSchemer {
	if w == nil {
		return nil
	}
	// Make base cluster connection params
	clusterConnectionParams := clickhouse.NewClusterConnectionParamsFromCHOpConfig(chop.Config())
	// Adjust base cluster connection params with per-host props
	switch clusterConnectionParams.Scheme {
	case api.ChSchemeAuto:
		switch {
		case api.IsPortAssigned(host.HTTPPort):
			clusterConnectionParams.Scheme = "http"
			clusterConnectionParams.Port = int(host.HTTPPort)
		case api.IsPortAssigned(host.HTTPSPort):
			clusterConnectionParams.Scheme = "https"
			clusterConnectionParams.Port = int(host.HTTPSPort)
		}
	case api.ChSchemeHTTP:
		clusterConnectionParams.Port = int(host.HTTPPort)
	case api.ChSchemeHTTPS:
		clusterConnectionParams.Port = int(host.HTTPSPort)
	}
	w.schemer = schemer.NewClusterSchemer(clusterConnectionParams, host.Runtime.Version)

	return w.schemer
}
