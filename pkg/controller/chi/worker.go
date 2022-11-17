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
	"fmt"
	"time"

	"github.com/juliangruber/go-intersect"
	"gopkg.in/d4l3k/messagediff.v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/altinity/queue"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
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
	normalizer *chopmodel.Normalizer
	schemer    *chopmodel.Schemer
	start      time.Time
	ctx        workerContext
}

type workerContext struct {
	creator            *chopmodel.Creator
	registryReconciled *chopmodel.Registry
	registryFailed     *chopmodel.Registry
	cmUpdate           time.Time
}

func newWorkerContext(creator *chopmodel.Creator) workerContext {
	return workerContext{
		creator:            creator,
		registryReconciled: chopmodel.NewRegistry(),
		registryFailed:     chopmodel.NewRegistry(),
		cmUpdate:           time.Time{},
	}
}

// newWorker
//func (c *Controller) newWorker(q workqueue.RateLimitingInterface) *worker {
func (c *Controller) newWorker(q queue.PriorityQueue, sys bool) *worker {
	start := time.Now()
	if !sys {
		start = start.Add(chiv1.DefaultReconcileThreadsWarmup)
	}
	return &worker{
		c:          c,
		a:          NewAnnouncer().WithController(c),
		queue:      q,
		normalizer: chopmodel.NewNormalizer(c.kubeClient),
		schemer: chopmodel.NewSchemer(
			chop.Config().ClickHouse.Access.Scheme,
			chop.Config().ClickHouse.Access.Username,
			chop.Config().ClickHouse.Access.Password,
			chop.Config().ClickHouse.Access.RootCA,
			chop.Config().ClickHouse.Access.Port,
		),
		start: start,
	}
}

func (w *worker) newContext(chi *chiv1.ClickHouseInstallation) {
	w.ctx = newWorkerContext(chopmodel.NewCreator(chi))
}

// run is an endless work loop, expected to be run in a thread
func (w *worker) run() {
	w.a.V(2).S().P()
	defer w.a.V(2).E().P()

	util.WaitContextDoneUntil(context.Background(), w.start)

	for {
		// Get() blocks until it can return an item
		item, ctx, ok := w.queue.Get()
		if !ok {
			w.a.Info("shutdown request")
			return
		}

		//item, shut := w.queue.Get()
		//ctx := context.Background()
		//if shut {
		//	w.a.Info("shutdown request")
		//	return
		//}

		if err := w.processItem(ctx, item); err != nil {
			// Item not processed
			// this code cannot return an error and needs to indicate error has been ignored
			utilruntime.HandleError(err)
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
	utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileCHIT(cmd *ReconcileCHIT) error {
	switch cmd.cmd {
	case reconcileAdd:
		return w.c.addChit(cmd.new)
	case reconcileUpdate:
		return w.c.updateChit(cmd.old, cmd.new)
	case reconcileDelete:
		return w.c.deleteChit(cmd.old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
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
	utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileEndpoints(ctx context.Context, cmd *ReconcileEndpoints) error {
	switch cmd.cmd {
	case reconcileUpdate:
		return w.updateEndpoints(ctx, cmd.old, cmd.new)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processDropDns(ctx context.Context, cmd *DropDns) error {
	if chi, err := w.createCHIFromObjectMeta(cmd.initiator, false, chopmodel.NewNormalizerOptions()); err == nil {
		w.a.V(2).M(cmd.initiator).Info("flushing DNS for CHI %s", chi.Name)
		_ = w.schemer.CHIDropDnsCache(ctx, chi)
	} else {
		w.a.M(cmd.initiator).F().Error("unable to find CHI by %v err: %v", cmd.initiator.Labels, err)
	}
	return nil
}

// processItem processes one work item according to its type
func (w *worker) processItem(ctx context.Context, item interface{}) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
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
	case *DropDns:
		return w.processDropDns(ctx, cmd)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilruntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// normalize
func (w *worker) normalize(c *chiv1.ClickHouseInstallation) *chiv1.ClickHouseInstallation {
	w.a.V(3).M(c).S().P()
	defer w.a.V(3).M(c).E().P()

	chi, err := w.normalizer.CreateTemplatedCHI(c, chopmodel.NewNormalizerOptions())
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to normalize CHI 1: %v", err)
	}

	ips := w.c.getPodsIPs(chi)
	w.a.V(1).M(chi).Info("IPs of the CHI %s/%s: %v", chi.Namespace, chi.Name, ips)
	opts := chopmodel.NewNormalizerOptions()
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
func (w *worker) ensureFinalizer(ctx context.Context, chi *chiv1.ClickHouseInstallation) bool {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
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

	if chi, err := w.createCHIFromObjectMeta(&new.ObjectMeta, false, chopmodel.NewNormalizerOptions()); err == nil {
		w.a.V(2).M(chi).Info("updating endpoints for CHI-1 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-1 %v", ips)
		opts := chopmodel.NewNormalizerOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(&new.ObjectMeta, false, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-1")
			w.newContext(chi)
			w.reconcileCHIConfigMapUsers(ctx, chi)
			w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
				TolerateAbsence: true,
				CopyCHIStatusOptions: chiv1.CopyCHIStatusOptions{
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
func (w *worker) updateCHI(ctx context.Context, old, new *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
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
		w.a.M(new).F().Info("finalizer installed, let's restart reconcile cycle")
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if w.deleteCHI(ctx, old, new) {
		// CHI is being deleted
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if w.isCleanRestartOnTheSameIP(new) {
		// First minute after restart do not reconcile already reconciled generations
		w.a.V(1).M(new).F().Info("Will not reconcile known generation after restart. Generation %d", new.Generation)
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// CHI is being reconciled
	return w.reconcileCHI(ctx, old, new)
}

// isCleanRestartOnTheSameIP checks whether it is just a restart of the operator on the same IP
func (w *worker) isCleanRestartOnTheSameIP(chi *chiv1.ClickHouseInstallation) bool {
	ip, _ := chop.Get().ConfigManager.GetRuntimeParam(chiv1.OPERATOR_POD_IP)
	ipIsTheSame := ip == chi.Status.GetCHOpIP()
	return w.isCleanRestart(chi) && ipIsTheSame
}

// isCleanRestart checks whether it is just a restart of the operator and CHI has no changes since last processed
func (w *worker) isCleanRestart(chi *chiv1.ClickHouseInstallation) bool {
	// Operator just recently started
	timeIsOk := time.Since(w.start) < 1*time.Minute

	if !timeIsOk {
		// Need to have operator recently started
		return false
	}

	// Migration support
	// Do we have have previously completed CHI?
	// In case no - this means that CHI has either not completed or we are migrating from
	// such a version of the operator, where there is no completed CHI at all
	noCompletedCHI := chi.Status.GetNormalizedCHICompleted() == nil
	// Having status completed and not having completed CHI suggests we are migrating operator version
	statusIsCompleted := chi.Status.GetStatus() == chiv1.StatusCompleted
	if noCompletedCHI && statusIsCompleted {
		// In case of a restart - assume that normalized is already completed
		chi.EnsureStatus().NormalizedCHICompleted = chi.Status.GetNormalizedCHI()
	}

	// Check whether anything has changed in CHI spec
	// In case the generation is the same as already completed - it is clean restart
	generationIsOk := false
	// However, NormalizedCHICompleted still can be missing, for example, in newly requested CHI
	if chi.Status.GetNormalizedCHICompleted() != nil {
		generationIsOk = chi.Generation == chi.Status.GetNormalizedCHICompleted().Generation
	}

	return generationIsOk
}

// areUsableOldAndNew checks whether there are old and new usable
func (w *worker) areUsableOldAndNew(old, new *chiv1.ClickHouseInstallation) bool {
	if old == nil {
		return false
	}
	if new == nil {
		return false
	}
	return true
}

// isAfterFinalizerInstalled checks whether we are just installed finalizer
func (w *worker) isAfterFinalizerInstalled(old, new *chiv1.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	finalizerIsInstalled := len(old.Finalizers) == 0 && len(new.Finalizers) > 0
	return w.isGenerationTheSame(old, new) && finalizerIsInstalled
}

// isGenerationTheSame checks whether old ans new CHI have the same generation
func (w *worker) isGenerationTheSame(old, new *chiv1.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	return old.Generation == new.Generation
}

// logCHI writes a CHI into the log
func (w *worker) logCHI(name string, chi *chiv1.ClickHouseInstallation) {
	w.a.V(2).M(chi).Info(
		"%s CHI start--------------------------------------------:\n%s\n%s CHI end--------------------------------------------",
		name,
		name,
		chi.YAML(true, true),
	)
}

// logActionPlan logs action plan
func (w *worker) logActionPlan(ap *chopmodel.ActionPlan) {
	w.a.Info(
		"ActionPlan start---------------------------------------------:\n%s\nActionPlan end---------------------------------------------",
		ap,
	)
}

// logOldAndNew writes old and new CHIs into the log
func (w *worker) logOldAndNew(name string, old, new *chiv1.ClickHouseInstallation) {
	w.logCHI(name+" old", old)
	w.logCHI(name+" new", new)
}

// reconcileCHI run reconcile cycle for a CHI
func (w *worker) reconcileCHI(ctx context.Context, old, new *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.logOldAndNew("non-normalized yet", old, new)

	switch {
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-1")
	case w.isGenerationTheSame(old, new):
		w.a.M(new).F().Info("isGenerationTheSame() - nothing to do here, exit")
		return nil
	}

	w.a.M(new).S().P()
	defer w.a.M(new).E().P()

	if new.Status.GetNormalizedCHICompleted() != nil {
		w.a.M(new).F().Info("has NormalizedCHICompleted, use it as a base for reconcile")
		old = new.Status.GetNormalizedCHICompleted()
	}

	old = w.normalize(old)
	new = w.normalize(new)
	w.logOldAndNew("normalized", old, new)

	actionPlan := chopmodel.NewActionPlan(old, new)
	w.logActionPlan(actionPlan)

	switch {
	case actionPlan.HasActionsToDo():
		w.a.M(new).F().Info("ActionPlan has actions - continue reconcile")
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-2")
	default:
		w.a.M(new).F().Info("ActionPlan has no actions and not finalizer - nothing to do")
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.newContext(new)
	w.markReconcileStart(ctx, new, actionPlan)
	w.excludeStopped(new)
	w.walkHosts(ctx, new, actionPlan)

	if err := w.reconcile(ctx, new); err != nil {
		w.a.WithEvent(new, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(new).
			M(new).F().
			Error("FAILED update: %v", err)
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Post-process added items
	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(new).
		M(new).F().
		Info("remove items scheduled for deletion")
	w.clear(ctx, new)
	w.dropReplicas(ctx, new, actionPlan)
	w.includeStopped(new)
	w.waitForIPAddresses(ctx, new)
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}
	w.markReconcileComplete(ctx, new)

	return nil
}

func (w *worker) waitForIPAddresses(ctx context.Context, chi *chiv1.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}
	start := time.Now()
	w.c.poll(ctx, chi, func(c *chiv1.ClickHouseInstallation, e error) bool {
		if len(c.Status.GetPodIPS()) >= len(c.Status.GetPods()) {
			// Stop polling
			w.a.V(1).M(c).Info("all IP addresses are in place")
			return false
		}
		if time.Now().Sub(start) > 1*time.Minute {
			// Stop polling
			w.a.V(1).M(c).Warning("Not all IP addresses are in place but time has elapsed")
			return false
		}
		// Continue polling
		w.a.V(1).M(c).Warning("Not all IP addresses are in place")
		return true
	})
}

func (w *worker) excludeStopped(chi *chiv1.ClickHouseInstallation) {
	// Exclude stopped CHI from monitoring
	if chi.IsStopped() {
		w.a.V(1).
			WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
			WithStatusAction(chi).
			M(chi).F().
			Info("exclude CHI from monitoring")
		w.c.deleteWatch(chi.Namespace, chi.Name)
	}
}

func (w *worker) includeStopped(chi *chiv1.ClickHouseInstallation) {
	if !chi.IsStopped() {
		w.a.V(1).
			WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
			WithStatusAction(chi).
			M(chi).F().
			Info("add CHI to monitoring")
		w.c.updateWatch(chi.Namespace, chi.Name, chopmodel.CreateFQDNs(chi, nil, false))
	}
}

func (w *worker) clear(ctx context.Context, chi *chiv1.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	// Remove deleted items
	objs := w.c.discovery(ctx, chi)
	need := w.ctx.registryReconciled
	w.a.V(1).M(chi).F().Info("Reconciled objects:\n%s", w.ctx.registryReconciled)
	w.a.V(1).M(chi).F().Info("Existing objects:\n%s", objs)
	objs.Subtract(need)
	w.a.V(1).M(chi).F().Info("Non-reconciled objects:\n%s", objs)
	if w.purge(ctx, chi, objs, w.ctx.registryFailed) > 0 {
		w.c.enqueueObject(NewDropDns(&chi.ObjectMeta))
		util.WaitContextDoneOrTimeout(ctx, 1*time.Minute)
	}

	w.a.V(1).
		WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(chi).
		M(chi).F().
		Info("remove items scheduled for deletion")
}

func (w *worker) dropReplicas(ctx context.Context, chi *chiv1.ClickHouseInstallation, ap *chopmodel.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	w.a.V(1).M(chi).F().S().Info("drop replicas based on AP")
	cnt := 0
	ap.WalkRemoved(
		func(cluster *chiv1.ChiCluster) {
		},
		func(shard *chiv1.ChiShard) {
		},
		func(host *chiv1.ChiHost) {
			var run *chiv1.ChiHost
			if shard := host.GetShard(); shard != nil {
				run = shard.FirstHost()
			}

			_ = w.dropReplica(ctx, run, host)
			cnt++
		},
	)
	w.a.V(1).M(chi).F().E().Info("processed replicas: %d", cnt)
}

func (w *worker) markReconcileStart(ctx context.Context, chi *chiv1.ClickHouseInstallation, ap *chopmodel.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	chi.EnsureStatus().ReconcileStart(ap.GetRemovedHostsNum())
	_ = w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		CopyCHIStatusOptions: chiv1.CopyCHIStatusOptions{
			MainFields: true,
		},
	})

	w.a.V(1).
		WithEvent(chi, eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(chi).
		WithStatusActions(chi).
		M(chi).F().
		Info("reconcile started")
	w.a.V(2).M(chi).F().Info("action plan\n%s\n", ap.String())
}

func (w *worker) markReconcileComplete(ctx context.Context, _chi *chiv1.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	// Update CHI object
	if chi, err := w.createCHIFromObjectMeta(&_chi.ObjectMeta, true, chopmodel.NewNormalizerOptions()); err == nil {
		w.a.V(2).M(chi).Info("updating endpoints for CHI-2 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-2 %v", ips)
		opts := chopmodel.NewNormalizerOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(&_chi.ObjectMeta, true, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-2")
			chi.EnsureStatus().NormalizedCHICompleted = chi.Status.GetNormalizedCHI()
			chi.EnsureStatus().NormalizedCHI = nil
			chi.EnsureStatus().ReconcileComplete()
			w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
				CopyCHIStatusOptions: chiv1.CopyCHIStatusOptions{
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
		Info("reconcile completed")
}

func (w *worker) walkHosts(ctx context.Context, chi *chiv1.ClickHouseInstallation, ap *chopmodel.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	objs := w.c.discovery(ctx, chi)
	ap.WalkAdded(
		func(cluster *chiv1.ChiCluster) {
			cluster.WalkHosts(func(host *chiv1.ChiHost) error {

				// Name of the StatefulSet for this host
				name := chopmodel.CreateStatefulSetName(host)
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
					(&host.ReconcileAttributes).SetFound()
					w.a.V(1).M(chi).Info("Add host as FOUND. Host was found as sts %s", host.Name)
				} else {
					// StatefulSet of this host does not exist, looks like we need to ADD it
					(&host.ReconcileAttributes).SetAdd()
					w.a.V(1).M(chi).Info("Add host as ADD. Host was not found as sts %s", host.Name)
				}

				return nil
			})
		},
		func(shard *chiv1.ChiShard) {
			shard.WalkHosts(func(host *chiv1.ChiHost) error {
				(&host.ReconcileAttributes).SetAdd()
				return nil
			})
		},
		func(host *chiv1.ChiHost) {
			(&host.ReconcileAttributes).SetAdd()
		},
	)

	ap.WalkModified(
		func(cluster *chiv1.ChiCluster) {
		},
		func(shard *chiv1.ChiShard) {
		},
		func(host *chiv1.ChiHost) {
			(&host.ReconcileAttributes).SetModify()
		},
	)

	chi.WalkHosts(func(host *chiv1.ChiHost) error {
		if host.ReconcileAttributes.IsAdd() {
			// Already added
			return nil
		}
		if host.ReconcileAttributes.IsModify() {
			// Already modified
			return nil
		}
		// Not clear yet
		(&host.ReconcileAttributes).SetFound()
		return nil
	})

	chi.WalkHosts(func(host *chiv1.ChiHost) error {
		if host.ReconcileAttributes.IsAdd() {
			w.a.M(host).Info("ADD host: %s", host.Address.CompactString())
			return nil
		}
		if host.ReconcileAttributes.IsModify() {
			w.a.M(host).Info("MODIFY host: %s", host.Address.CompactString())
			return nil
		}
		if host.ReconcileAttributes.IsFound() {
			w.a.M(host).Info("FOUND host: %s", host.Address.CompactString())
			return nil
		}
		w.a.M(host).Info("UNTOUCHED host: %s", host.Address.CompactString())
		return nil
	})
}

func purgeStatefulSet(chi *chiv1.ClickHouseInstallation, reconcileFailedObjs *chopmodel.Registry, m meta.ObjectMeta) bool {
	if reconcileFailedObjs.HasStatefulSet(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetStatefulSet() == chiv1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetStatefulSet() == chiv1.ObjectsCleanupDelete
}

func purgePVC(chi *chiv1.ClickHouseInstallation, reconcileFailedObjs *chopmodel.Registry, m meta.ObjectMeta) bool {
	if reconcileFailedObjs.HasPVC(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetPVC() == chiv1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetPVC() == chiv1.ObjectsCleanupDelete
}

func purgeConfigMap(chi *chiv1.ClickHouseInstallation, reconcileFailedObjs *chopmodel.Registry, m meta.ObjectMeta) bool {
	if reconcileFailedObjs.HasConfigMap(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetConfigMap() == chiv1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetConfigMap() == chiv1.ObjectsCleanupDelete
}

func purgeService(chi *chiv1.ClickHouseInstallation, reconcileFailedObjs *chopmodel.Registry, m meta.ObjectMeta) bool {
	if reconcileFailedObjs.HasService(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetService() == chiv1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetService() == chiv1.ObjectsCleanupDelete
}

func purgeSecret(chi *chiv1.ClickHouseInstallation, reconcileFailedObjs *chopmodel.Registry, m meta.ObjectMeta) bool {
	if reconcileFailedObjs.HasSecret(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetSecret() == chiv1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetSecret() == chiv1.ObjectsCleanupDelete
}

// purge
func (w *worker) purge(
	ctx context.Context,
	chi *chiv1.ClickHouseInstallation,
	reg *chopmodel.Registry,
	reconcileFailedObjs *chopmodel.Registry,
) (cnt int) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return cnt
	}

	reg.Walk(func(entityType chopmodel.EntityType, m meta.ObjectMeta) {
		switch entityType {
		case chopmodel.StatefulSet:
			if purgeStatefulSet(chi, reconcileFailedObjs, m) {
				w.a.V(1).M(m).F().Info("Delete StatefulSet %s/%s", m.Namespace, m.Name)
				if err := w.c.kubeClient.AppsV1().StatefulSets(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
					w.a.V(1).M(m).F().Error("FAILED to delete StatefulSet %s/%s, err: %v", m.Namespace, m.Name, err)
				}
				cnt++
			}
		case chopmodel.PVC:
			if purgePVC(chi, reconcileFailedObjs, m) {
				if chopmodel.GetReclaimPolicy(m) == chiv1.PVCReclaimPolicyDelete {
					w.a.V(1).M(m).F().Info("Delete PVC %s/%s", m.Namespace, m.Name)
					if err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
						w.a.V(1).M(m).F().Error("FAILED to delete PVC %s/%s, err: %v", m.Namespace, m.Name, err)
					}
				}
			}
		case chopmodel.ConfigMap:
			if purgeConfigMap(chi, reconcileFailedObjs, m) {
				w.a.V(1).M(m).F().Info("Delete ConfigMap %s/%s", m.Namespace, m.Name)
				if err := w.c.kubeClient.CoreV1().ConfigMaps(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
					w.a.V(1).M(m).F().Error("FAILED to delete ConfigMap %s/%s, err: %v", m.Namespace, m.Name, err)
				}
			}
		case chopmodel.Service:
			if purgeService(chi, reconcileFailedObjs, m) {
				w.a.V(1).M(m).F().Info("Delete Service %s/%s", m.Namespace, m.Name)
				if err := w.c.kubeClient.CoreV1().Services(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
					w.a.V(1).M(m).F().Error("FAILED to delete Service %s/%s, err: %v", m.Namespace, m.Name, err)
				}
			}
		case chopmodel.Secret:
			if purgeSecret(chi, reconcileFailedObjs, m) {
				w.a.V(1).M(m).F().Info("Delete Secret %s/%s", m.Namespace, m.Name)
				if err := w.c.kubeClient.CoreV1().Secrets(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
					w.a.V(1).M(m).F().Error("FAILED to delete Secret %s/%s, err: %v", m.Namespace, m.Name, err)
				}
			}
		}
	})
	return cnt
}

// reconcile reconciles ClickHouseInstallation
func (w *worker) reconcile(ctx context.Context, chi *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	w.createPodDisruptionBudget(ctx, chi)
	return chi.WalkTillError(
		ctx,
		w.reconcileCHIAuxObjectsPreliminary,
		w.reconcileCluster,
		w.reconcileShard,
		w.reconcileHost,
		w.reconcileCHIAuxObjectsFinal,
	)
}

// baseRemoteServersGeneratorOptions build base set of RemoteServersGeneratorOptions
// which are applied on each remote_serves reconfiguration during reconcile cycle
func (w *worker) baseRemoteServersGeneratorOptions() *chopmodel.RemoteServersGeneratorOptions {
	opts := chopmodel.NewRemoteServersGeneratorOptions()
	opts.ExcludeReconcileAttributes(
		chiv1.NewChiHostReconcileAttributes().SetAdd(),
	)

	return opts
}

// options build ClickHouseConfigFilesGeneratorOptions
func (w *worker) options(excludeHosts ...*chiv1.ChiHost) *chopmodel.ClickHouseConfigFilesGeneratorOptions {
	// Stringify
	str := ""
	for _, host := range excludeHosts {
		str += fmt.Sprintf("name: '%s' sts: '%s'", host.Name, host.Address.StatefulSet)
	}

	opts := w.baseRemoteServersGeneratorOptions().ExcludeHosts(excludeHosts...)
	w.a.Info("RemoteServersGeneratorOptions: %s, excluded host(s): %s", opts, str)
	return chopmodel.NewClickHouseConfigFilesGeneratorOptions().SetRemoteServersGeneratorOptions(opts)
}

// reconcileCHIAuxObjectsPreliminary reconciles CHI preliminary in order to ensure that ConfigMaps are in place
func (w *worker) reconcileCHIAuxObjectsPreliminary(ctx context.Context, chi *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// 1. CHI Service
	if chi.IsStopped() {
		// Stopped cluster must have no entry point
		_ = w.c.deleteServiceCHI(ctx, chi)
	} else {
		if service := w.ctx.creator.CreateServiceCHI(); service != nil {
			if err := w.reconcileService(ctx, chi, service); err != nil {
				// Service not reconciled
				w.ctx.registryFailed.RegisterService(service.ObjectMeta)
				return err
			}
			w.ctx.registryReconciled.RegisterService(service.ObjectMeta)
		}
	}

	// 2. CHI common ConfigMap without added hosts
	options := w.options()
	if err := w.reconcileCHIConfigMapCommon(ctx, chi, options); err != nil {
		w.a.F().Error("failed to reconcile config map common. err: %v", err)
	}
	// 3. CHI users ConfigMap
	if err := w.reconcileCHIConfigMapUsers(ctx, chi); err != nil {
		w.a.F().Error("failed to reconcile config map users. err: %v", err)
	}

	return nil
}

// reconcileCHIAuxObjectsFinal reconciles CHI global objects
func (w *worker) reconcileCHIAuxObjectsFinal(ctx context.Context, chi *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// CHI ConfigMaps with update
	return w.reconcileCHIConfigMapCommon(ctx, chi, nil)
}

// reconcileCHIConfigMaps reconciles all CHI's ConfigMaps
/*
func (w *worker) reconcileCHIConfigMaps(
	ctx context.Context,
	chi *chiv1.ClickHouseInstallation,
	options *chopmodel.ClickHouseConfigFilesGeneratorOptions,
	update bool,
) error {
	if util.IsContextDone(ctx) {
		return nil
	}

	if err := w.reconcileCHIConfigMapCommon(ctx, chi, options, update); err != nil {
		return err
	}

	if err := w.reconcileCHIConfigMapUsers(ctx, chi, options, update); err != nil {
		return err
	}

	return nil
}
*/

// reconcileCHIConfigMapCommon reconciles all CHI's common ConfigMap
func (w *worker) reconcileCHIConfigMapCommon(
	ctx context.Context,
	chi *chiv1.ClickHouseInstallation,
	options *chopmodel.ClickHouseConfigFilesGeneratorOptions,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.ctx.creator.CreateConfigMapCHICommon(options)
	err := w.reconcileConfigMap(ctx, chi, configMapCommon)
	if err == nil {
		w.ctx.registryReconciled.RegisterConfigMap(configMapCommon.ObjectMeta)
	} else {
		w.ctx.registryFailed.RegisterConfigMap(configMapCommon.ObjectMeta)
	}
	return err
}

// reconcileCHIConfigMapUsers reconciles all CHI's users ConfigMap
// ConfigMap common for all users resources in CHI
func (w *worker) reconcileCHIConfigMapUsers(ctx context.Context, chi *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := w.ctx.creator.CreateConfigMapCHICommonUsers()
	err := w.reconcileConfigMap(ctx, chi, configMapUsers)
	if err == nil {
		w.ctx.registryReconciled.RegisterConfigMap(configMapUsers.ObjectMeta)
	} else {
		w.ctx.registryFailed.RegisterConfigMap(configMapUsers.ObjectMeta)
	}
	return err
}

// reconcileHostConfigMap reconciles host's personal ConfigMap
func (w *worker) reconcileHostConfigMap(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// ConfigMap for a host
	configMap := w.ctx.creator.CreateConfigMapHost(host)
	err := w.reconcileConfigMap(ctx, host.CHI, configMap)
	if err == nil {
		w.ctx.registryReconciled.RegisterConfigMap(configMap.ObjectMeta)
	} else {
		w.ctx.registryFailed.RegisterConfigMap(configMap.ObjectMeta)
		return err
	}

	//replicatedObjectNames, replicatedCreateSQLs, distributedObjectNames, distributedCreateSQLs := w.schemer.hostCreateTablesSQLs(ctx, host)
	//names := append(replicatedObjectNames, distributedObjectNames...)
	//sql := append(replicatedCreateSQLs, distributedCreateSQLs...)
	//
	// ConfigMap for a host
	//configMap = w.creator.CreateConfigMapHostMigration(host, w.creator.MakeConfigMapData(names, sql))
	//err = w.reconcileConfigMap(ctx, host.CHI, configMap)
	//if err == nil {
	//	w.registryReconciled.RegisterConfigMap(configMap.ObjectMeta)
	//} else {
	//	w.registryFailed.RegisterConfigMap(configMap.ObjectMeta)
	//	return err
	//}

	return nil
}

// prepareHostStatefulSetWithStatus prepares host's StatefulSet status
func (w *worker) prepareHostStatefulSetWithStatus(ctx context.Context, host *chiv1.ChiHost, shutdown bool) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	// StatefulSet for a host
	_ = w.ctx.creator.CreateStatefulSet(host, shutdown)
	(&host.ReconcileAttributes).SetStatus(w.getStatefulSetStatus(host.StatefulSet.ObjectMeta))
}

// reconcileHostStatefulSet reconciles host's StatefulSet
func (w *worker) reconcileHostStatefulSet(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// RollingUpdate is made with always shutting the host down.
	// It is such an interesting policy.
	// We'll do it via replicas: 0 in StatefulSet.
	if host.CHI.IsRollingUpdate() {
		w.prepareHostStatefulSetWithStatus(ctx, host, true)
		_ = w.reconcileStatefulSet(ctx, host)
		// At this moment StatefulSet has 0 replicas.
		// First stage of RollingUpdate completed.
	}

	// We are in place, where we can  reconcile StatefulSet to desired configuration.
	w.prepareHostStatefulSetWithStatus(ctx, host, false)
	err := w.reconcileStatefulSet(ctx, host)
	if err == nil {
		w.ctx.registryReconciled.RegisterStatefulSet(host.StatefulSet.ObjectMeta)
	} else {
		w.ctx.registryFailed.RegisterStatefulSet(host.StatefulSet.ObjectMeta)
		if err == errIgnore {
			err = nil
		}
	}
	return err
}

// reconcileHostService reconciles host's Service
func (w *worker) reconcileHostService(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}
	service := w.ctx.creator.CreateServiceHost(host)
	if service == nil {
		// This is not a problem, service may be omitted
		return nil
	}
	err := w.reconcileService(ctx, host.CHI, service)
	if err == nil {
		w.ctx.registryReconciled.RegisterService(service.ObjectMeta)
	} else {
		w.ctx.registryFailed.RegisterService(service.ObjectMeta)
	}
	return err
}

// reconcileCluster reconciles Cluster, excluding nested shards
func (w *worker) reconcileCluster(ctx context.Context, cluster *chiv1.ChiCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	// Add Cluster's Service
	if service := w.ctx.creator.CreateServiceCluster(cluster); service != nil {
		if err := w.reconcileService(ctx, cluster.CHI, service); err == nil {
			w.ctx.registryReconciled.RegisterService(service.ObjectMeta)
		} else {
			w.ctx.registryFailed.RegisterService(service.ObjectMeta)
		}
	}

	// Add Cluster's Auto Secret
	if cluster.Secret.Source() == chiv1.ClusterSecretSourceAuto {
		if secret := w.ctx.creator.CreateClusterSecret(chopmodel.CreateClusterAutoSecretName(cluster)); secret != nil {
			if err := w.reconcileSecret(ctx, cluster.CHI, secret); err == nil {
				w.ctx.registryReconciled.RegisterSecret(secret.ObjectMeta)
			} else {
				w.ctx.registryFailed.RegisterSecret(secret.ObjectMeta)
			}
		}
	}

	return nil
}

// reconcileShard reconciles Shard, excluding nested replicas
func (w *worker) reconcileShard(ctx context.Context, shard *chiv1.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	// Add Shard's Service
	service := w.ctx.creator.CreateServiceShard(shard)
	if service == nil {
		// This is not a problem, ServiceShard may be omitted
		return nil
	}
	err := w.reconcileService(ctx, shard.CHI, service)
	if err == nil {
		w.ctx.registryReconciled.RegisterService(service.ObjectMeta)
	} else {
		w.ctx.registryFailed.RegisterService(service.ObjectMeta)
	}
	return err
}

// reconcileHost reconciles ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Reconcile Host %s started", host.Name)

	// Create artifacts
	w.prepareHostStatefulSetWithStatus(ctx, host, false)

	if err := w.excludeHost(ctx, host); err != nil {
		return err
	}

	if err := w.reconcileHostConfigMap(ctx, host); err != nil {
		return err
	}
	//w.reconcilePersistentVolumes(ctx, host)
	_ = w.reconcilePVCs(ctx, host)
	if err := w.reconcileHostStatefulSet(ctx, host); err != nil {
		return err
	}
	_ = w.reconcilePVCs(ctx, host)

	_ = w.reconcileHostService(ctx, host)

	host.ReconcileAttributes.UnsetAdd()

	_ = w.migrateTables(ctx, host)

	if err := w.includeHost(ctx, host); err != nil {
		// If host is not ready - fallback
		return err
	}

	w.a.V(1).
		WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Reconcile Host %s completed", host.Name)

	return nil
}

// migrateTables
func (w *worker) migrateTables(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if !w.shouldMigrateTables(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to add tables on host %d to shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return nil
	}

	// Need to migrate tables

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Adding tables on shard/host:%d/%d cluster:%s", host.Address.ShardIndex, host.Address.ReplicaIndex, host.Address.ClusterName)
	err := w.schemer.HostCreateTables(ctx, host)
	if err != nil {
		w.a.M(host).F().Error("ERROR create tables on host %s. err: %v", host.Name, err)
	}
	return err
}

// shouldMigrateTables
func (w *worker) shouldMigrateTables(host *chiv1.ChiHost) bool {
	switch {
	case host.GetCHI().IsStopped():
		// Stopped host is not able to receive data
		return false
	case host.ReconcileAttributes.GetStatus() == chiv1.StatefulSetStatusSame:
		// No need to migrate on the same host
		return false
	}
	return true
}

// Exclude host from ClickHouse clusters if required
func (w *worker) excludeHost(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if w.shouldExcludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("Exclude from cluster host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)

		_ = w.excludeHostFromService(ctx, host)
		w.excludeHostFromClickHouseCluster(ctx, host)
		_ = w.waitHostNoActiveQueries(ctx, host)
	}
	return nil
}

// shouldIncludeHost determines whether host to be included into cluster after reconciling
func (w *worker) shouldIncludeHost(host *chiv1.ChiHost) bool {
	switch {
	case host.GetCHI().IsStopped():
		// No need to include stopped host
		return false
	}
	return true
}

// includeHost includes host back back into ClickHouse clusters
func (w *worker) includeHost(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if w.shouldIncludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("Include into cluster host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)

		w.includeHostIntoClickHouseCluster(ctx, host)
		_ = w.includeHostIntoService(ctx, host)
	}

	return nil
}

// excludeHostFromService
func (w *worker) excludeHostFromService(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_ = w.c.deleteLabelReadyPod(ctx, host)
	_ = w.c.deleteAnnotationReadyService(ctx, host)
	return nil
}

// includeHostIntoService
func (w *worker) includeHostIntoService(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_ = w.c.appendLabelReadyPod(ctx, host)
	_ = w.c.appendAnnotationReadyService(ctx, host)
	return nil
}

// excludeHostFromClickHouseCluster excludes host from ClickHouse configuration
func (w *worker) excludeHostFromClickHouseCluster(ctx context.Context, host *chiv1.ChiHost) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	// Remove host from cluster config and wait for ClickHouse to pick-up the change
	if w.shouldWaitExcludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("going to exclude host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)

		// Specify in options to exclude host from ClickHouse config file
		options := w.options(host)
		_ = w.reconcileCHIConfigMapCommon(ctx, host.GetCHI(), options)
		// Wait for ClickHouse to pick-up the change
		_ = w.waitHostNotInCluster(ctx, host)
	}
}

// includeHostIntoClickHouseCluster includes host into ClickHouse configuration
func (w *worker) includeHostIntoClickHouseCluster(ctx context.Context, host *chiv1.ChiHost) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	// Add host to the cluster config
	options := w.options()
	_ = w.reconcileCHIConfigMapCommon(ctx, host.GetCHI(), options)
	// Wait for ClickHouse to pick-up the change
	if w.shouldWaitIncludeHost(host) {
		_ = w.waitHostInCluster(ctx, host)
	}
}

// shouldExcludeHost determines whether host to be excluded from cluster before reconciling
func (w *worker) shouldExcludeHost(host *chiv1.ChiHost) bool {
	status := host.ReconcileAttributes.GetStatus()
	switch {
	case host.GetCHI().IsStopped():
		w.a.V(1).
			M(host).F().
			Info("No need to exclude stopped host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return false
	case host.GetCHI().IsRollingUpdate():
		w.a.V(1).
			M(host).F().
			Info("While rolling update host would be restarted host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return true
	case status == chiv1.StatefulSetStatusNew:
		w.a.V(1).
			M(host).F().
			Info("Nothing to exclude, host is not yet in the cluster host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return false
	case status == chiv1.StatefulSetStatusSame:
		w.a.V(1).
			M(host).F().
			Info("The same host would not be updated host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return false
	case host.GetShard().HostsCount() == 1:
		w.a.V(1).
			M(host).F().
			Info("In case shard where current host is located has only one host (means no replication), no need to exclude host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return false
	}

	w.a.V(1).
		M(host).F().
		Info("host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)

	return true
}

// shouldWaitExcludeHost determines whether reconciler should wait for host to be excluded from cluster
func (w *worker) shouldWaitExcludeHost(host *chiv1.ChiHost) bool {
	// Check CHI settings
	switch {
	case host.GetCHI().GetReconciling().IsReconcilingPolicyWait():
		w.a.V(1).
			M(host).F().
			Info("IsReconcilingPolicyWait() need to exclude host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return true
	case host.GetCHI().GetReconciling().IsReconcilingPolicyNoWait():
		w.a.V(1).
			M(host).F().
			Info("IsReconcilingPolicyNoWait() need NOT to exclude host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return false
	}

	w.a.V(1).
		M(host).F().
		Info("fallback to operator's settings. host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
	return chop.Config().Reconcile.Host.Wait.Exclude.Value()
}

// shouldWaitIncludeHost determines whether reconciler should wait for host to be included into cluster
func (w *worker) shouldWaitIncludeHost(host *chiv1.ChiHost) bool {
	status := host.ReconcileAttributes.GetStatus()
	switch {
	case status == chiv1.StatefulSetStatusNew:
		return false
	case status == chiv1.StatefulSetStatusSame:
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
func (w *worker) waitHostInCluster(ctx context.Context, host *chiv1.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, w.schemer.IsHostInCluster)
}

// waitHostNotInCluster
func (w *worker) waitHostNotInCluster(ctx context.Context, host *chiv1.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, func(ctx context.Context, host *chiv1.ChiHost) bool {
		return !w.schemer.IsHostInCluster(ctx, host)
	})
}

// waitHostNoActiveQueries
func (w *worker) waitHostNoActiveQueries(ctx context.Context, host *chiv1.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, func(ctx context.Context, host *chiv1.ChiHost) bool {
		n, _ := w.schemer.HostActiveQueriesNum(ctx, host)
		return n <= 1
	})
}

// createPodDisruptionBudget creates PodDisruptionBudget
func (w *worker) createPodDisruptionBudget(ctx context.Context, chi *chiv1.ClickHouseInstallation) {
	pdb, err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(chi.Namespace).Create(ctx, w.ctx.creator.NewPodDisruptionBudget(), newCreateOptions())
	if err != nil {
		log.V(1).Warning("unable to create PDB %v", err)
		return
	}
	log.V(1).Info("PDB created %s/%s", pdb.Namespace, pdb.Name)
}

// deletePodDisruptionBudget deletes PodDisruptionBudget
func (w *worker) deletePodDisruptionBudget(ctx context.Context, chi *chiv1.ClickHouseInstallation) {
	_ = w.c.kubeClient.PolicyV1().PodDisruptionBudgets(chi.Namespace).Delete(ctx, chi.Name, newDeleteOptions())
}

// deleteCHI
func (w *worker) deleteCHI(ctx context.Context, old, new *chiv1.ClickHouseInstallation) bool {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return false
	}

	// Do we have pending request for CHI to be deleted?
	if new.ObjectMeta.DeletionTimestamp.IsZero() {
		// CHI is not being deleted and operator has not deleted anything.
		return false
	}

	w.a.V(3).M(new).S().P()
	defer w.a.V(3).M(new).E().P()

	// Ok, we have pending request for CHI to be deleted.
	// However, we need to decide, should CHI's child resources be deleted or not.
	// There is a curious situation, when CRD is deleted and k8s starts to delete all resources of the type,
	// described by CRD being deleted. This is may be unexpected and very painful situation,
	// so in this case we should agree to delete CHI itself, but has to keep all CHI's child resources.

	var clear bool
	crd, err := w.c.extClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, "clickhouseinstallations.clickhouse.altinity.com", newGetOptions())
	if err == nil {
		if crd.ObjectMeta.DeletionTimestamp.IsZero() {
			// CRD is not being deleted and operator can delete all child resources.
			w.a.V(1).M(new).F().Info("CRD %s/%s is not being deleted, operator will delete child resources", crd.Namespace, crd.Name)
			clear = true
		} else {
			// CRD is being deleted. This may be a mistake, operator should not delete data
			w.a.V(1).M(new).F().Info("CRD %s/%s BEING DELETED, operator will NOT delete child resources", crd.Namespace, crd.Name)
			clear = false
		}
	} else {
		w.a.V(1).M(new).F().Error("unable to get CRD, got error: %v ", err)
		w.a.V(1).M(new).F().Info("will delete chi %s/%s", new.Namespace, new.Name)
		clear = true
	}

	if clear {
		cur, err := w.c.chopClient.ClickhouseV1().ClickHouseInstallations(new.Namespace).Get(ctx, new.Name, newGetOptions())
		if cur == nil {
			return false
		}
		if err != nil {
			return false
		}

		if !util.InArray(FinalizerName, new.ObjectMeta.Finalizers) {
			// No finalizer found, unexpected behavior
			return false
		}

		_ = w.deleteCHIProtocol(ctx, new)
	} else {
		new.Attributes.SkipOwnerRef = true
		_ = w.reconcileCHI(ctx, old, new)
	}

	// We need to uninstall finalizer in order to allow k8s to delete CHI resource
	w.a.V(2).M(new).F().Info("uninstall finalizer")
	if err := w.c.uninstallFinalizer(ctx, new); err != nil {
		w.a.V(1).M(new).F().Error("unable to uninstall finalizer: err:%v", err)
	}

	// CHI's child resources were deleted
	return true
}

// discoveryAndDeleteCHI deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) discoveryAndDeleteCHI(ctx context.Context, chi *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	objs := w.c.discovery(ctx, chi)
	if objs.NumStatefulSet() > 0 {
		chi.WalkHosts(func(host *chiv1.ChiHost) error {
			_ = w.schemer.HostSyncTables(ctx, host)
			return nil
		})
	}
	w.purge(ctx, chi, objs, nil)
	return nil
}

// deleteCHIProtocol deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) deleteCHIProtocol(ctx context.Context, chi *chiv1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	var err error
	chi, err = w.normalizer.CreateTemplatedCHI(chi, chopmodel.NewNormalizerOptions())
	if err != nil {
		w.a.WithEvent(chi, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(chi).
			M(chi).F().
			Error("Delete CHI failed - unable to normalize: %q", err)
		return err
	}

	// Announce delete procedure
	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(chi).
		M(chi).F().
		Info("Delete CHI started")

	chi.EnsureStatus().DeleteStart()
	if err := w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		TolerateAbsence: true,
		CopyCHIStatusOptions: chiv1.CopyCHIStatusOptions{
			MainFields: true,
		},
	}); err != nil {
		w.a.V(1).M(chi).F().Error("UNABLE to write normalized CHI. err:%q", err)
		return nil
	}

	// Start delete protocol

	w.deletePodDisruptionBudget(ctx, chi)

	// Exclude this CHI from monitoring
	w.c.deleteWatch(chi.Namespace, chi.Name)

	// Delete Service
	_ = w.c.deleteServiceCHI(ctx, chi)

	chi.WalkHosts(func(host *chiv1.ChiHost) error {
		_ = w.schemer.HostSyncTables(ctx, host)
		return nil
	})

	// Delete all clusters
	chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		return w.deleteCluster(ctx, chi, cluster)
	})

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Delete ConfigMap(s)
	_ = w.c.deleteConfigMapsCHI(ctx, chi)

	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(chi).
		M(chi).F().
		Info("Delete CHI completed")

	return nil
}

// canDropReplica
func (w *worker) canDropReplica(host *chiv1.ChiHost) (can bool) {
	can = true
	w.c.walkDiscoveredPVCs(host, func(pvc *core.PersistentVolumeClaim) {
		// Replica's state has to be kept in Zookeeper for retained volumes.
		// ClickHouse expects to have state of the non-empty replica in-place when replica rejoins.
		if chopmodel.GetReclaimPolicy(pvc.ObjectMeta) == chiv1.PVCReclaimPolicyRetain {
			w.a.V(1).F().Info("PVC %s/%s blocks drop replica. Reclaim policy: %s", chiv1.PVCReclaimPolicyRetain.String())
			can = false
		}
	})
	return can
}

// dropReplica
func (w *worker) dropReplica(ctx context.Context, hostToRun, hostToDrop *chiv1.ChiHost) error {
	if (hostToRun == nil) || (hostToDrop == nil) {
		w.a.V(1).F().Error("FAILED to drop replica. hostToRun:%s, hostToDrop:%s", hostToRun.GetName(), hostToDrop.GetName())
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}
	if !w.canDropReplica(hostToDrop) {
		w.a.V(1).F().Warning("UNABLE to drop replica. hostToRun:%s, hostToDrop:%s", hostToRun.GetName(), hostToDrop.GetName())
		return nil
	}

	err := w.schemer.HostDropReplica(ctx, hostToRun, hostToDrop)

	if err == nil {
		w.a.V(1).
			WithEvent(hostToRun.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(hostToRun.CHI).
			M(hostToRun).F().
			Info("Drop replica host %s in cluster %s", hostToDrop.Name, hostToDrop.Address.ClusterName)
	} else {
		w.a.WithEvent(hostToRun.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(hostToRun.CHI).
			M(hostToRun).F().
			Error("FAILED to drop replica on host %s with error %v", hostToDrop.Name, err)
	}

	return err
}

// deleteTables
// chi is the new CHI in which there will be no more this tabled
func (w *worker) deleteTables(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if !chopmodel.HostCanDeleteAllPVCs(host) {
		return nil
	}
	err := w.schemer.HostDropTables(ctx, host)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Deleted tables on host %s replica %d to shard %d in cluster %s",
				host.Name, host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			M(host).F().
			Error("FAILED to delete tables on host %s with error %v", host.Name, err)
	}

	return err
}

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
// chi is the new CHI in which there will be no more this host
func (w *worker) deleteHost(ctx context.Context, chi *chiv1.ClickHouseInstallation, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(host).S().Info(host.Address.HostName)
	defer w.a.V(2).M(host).E().Info(host.Address.HostName)

	w.a.V(1).
		WithEvent(host.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Delete host %s/%s - started", host.Address.ClusterName, host.Name)

	if _, err := w.c.getStatefulSet(host); err != nil {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Delete host %s/%s - completed StatefulSet not found - already deleted? err: %v",
				host.Address.ClusterName, host.Name, err)
		return nil
	}

	// Each host consists of
	// 1. User-level objects - tables on the host
	//    We need to delete tables on the host in order to clean Zookeeper data.
	//    If just delete tables, Zookeeper will still keep track of non-existent tables
	// 2. Kubernetes-level objects - such as StatefulSet, PVC(s), ConfigMap(s), Service(s)
	// Need to delete all these items

	var err error
	_ = w.deleteTables(ctx, host)
	err = w.c.deleteHost(ctx, host)

	// When deleting the whole CHI (not particular host), CHI may already be unavailable, so update CHI tolerantly
	chi.EnsureStatus().DeletedHostsCount++
	_ = w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		TolerateAbsence: true,
		CopyCHIStatusOptions: chiv1.CopyCHIStatusOptions{
			MainFields: true,
		},
	})

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Delete host %s/%s - completed", host.Address.ClusterName, host.Name)
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			M(host).F().
			Error("FAILED Delete host %s/%s - completed", host.Address.ClusterName, host.Name)
	}

	return err
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
// chi is the new CHI in which there will be no more this shard
func (w *worker) deleteShard(ctx context.Context, chi *chiv1.ClickHouseInstallation, shard *chiv1.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(shard.CHI).
		M(shard).F().
		Info("Delete shard %s/%s - started", shard.Address.Namespace, shard.Name)

	// Delete Shard Service
	_ = w.c.deleteServiceShard(ctx, shard)

	// Delete all replicas
	shard.WalkHosts(func(host *chiv1.ChiHost) error {
		return w.deleteHost(ctx, chi, host)
	})

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(shard.CHI).
		M(shard).F().
		Info("Delete shard %s/%s - completed", shard.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
// chi is the new CHI in which there will be no more this cluster
func (w *worker) deleteCluster(ctx context.Context, chi *chiv1.ClickHouseInstallation, cluster *chiv1.ChiCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(cluster.CHI).
		M(cluster).F().
		Info("Delete cluster %s/%s - started", cluster.Address.Namespace, cluster.Name)

	// Delete Cluster Service
	_ = w.c.deleteServiceCluster(ctx, cluster)

	// Delete Cluster's Auto Secret
	if cluster.Secret.Source() == chiv1.ClusterSecretSourceAuto {
		// Delete Cluster Secret
		_ = w.c.deleteSecretCluster(ctx, cluster)
	}

	// Delete all shards
	cluster.WalkShards(func(index int, shard *chiv1.ChiShard) error {
		return w.deleteShard(ctx, chi, shard)
	})

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(cluster.CHI).
		M(cluster).F().
		Info("Delete cluster %s/%s - completed", cluster.Address.Namespace, cluster.Name)

	return nil
}

// createCHIFromObjectMeta
func (w *worker) createCHIFromObjectMeta(objectMeta *meta.ObjectMeta, isCHI bool, options *chopmodel.NormalizerOptions) (*chiv1.ClickHouseInstallation, error) {
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
func (w *worker) updateConfigMap(ctx context.Context, chi *chiv1.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	updatedConfigMap, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(ctx, configMap, newUpdateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)
		if updatedConfigMap.ResourceVersion != configMap.ResourceVersion {
			w.ctx.cmUpdate = time.Now()
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
func (w *worker) createConfigMap(ctx context.Context, chi *chiv1.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Create(ctx, configMap, newCreateOptions())
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

// reconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) reconcileConfigMap(
	ctx context.Context,
	chi *chiv1.ClickHouseInstallation,
	configMap *core.ConfigMap,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// Check whether this object already exists in k8s
	curConfigMap, err := w.c.getConfigMap(&configMap.ObjectMeta, true)

	if curConfigMap != nil {
		// We have ConfigMap - try to update it
		err = w.updateConfigMap(ctx, chi, configMap)
	}

	if apierrors.IsNotFound(err) {
		// ConfigMap not found - even during Update process - try to create it
		err = w.createConfigMap(ctx, chi, configMap)
	}

	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to reconcile ConfigMap: %s CHI: %s ", configMap.Name, chi.Name)
	}

	return err
}

// updateService
func (w *worker) updateService(
	ctx context.Context,
	chi *chiv1.ClickHouseInstallation,
	curService *core.Service,
	newService *core.Service,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Updating a Service is a complicated business

	// spec.resourceVersion is required in order to update object
	newService.ResourceVersion = curService.ResourceVersion

	// The port on each node on which this service is exposed when type=NodePort or LoadBalancer.
	// Usually assigned by the system. If specified, it will be allocated to the service
	// if unused or else creation of the service will fail.
	// Default is to auto-allocate a port if the ServiceType of this Service requires one.
	// More info: https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport
	if ((curService.Spec.Type == core.ServiceTypeNodePort) && (newService.Spec.Type == core.ServiceTypeNodePort)) ||
		((curService.Spec.Type == core.ServiceTypeLoadBalancer) && (newService.Spec.Type == core.ServiceTypeLoadBalancer)) {
		// No changes in service type and service type assumes NodePort to be allocated.
		// !!! IMPORTANT !!!
		// The same exposed port details can not be changed. This is important limitation
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

	// spec.clusterIP field is immutable, need to use already assigned value
	// From https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service
	// Kubernetes assigns this Service an IP address (sometimes called the “cluster IP”), which is used by the Service proxies
	// See also https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// You can specify your own cluster IP address as part of a Service creation request. To do this, set the .spec.clusterIP
	newService.Spec.ClusterIP = curService.Spec.ClusterIP

	// spec.healthCheckNodePort field is used with ExternalTrafficPolicy=Local only and is immutable within ExternalTrafficPolicy=Local
	// In case ExternalTrafficPolicy is changed it seems to be irrelevant
	// https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/#preserving-the-client-source-ip
	if (curService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal) &&
		(newService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal) {
		newService.Spec.HealthCheckNodePort = curService.Spec.HealthCheckNodePort
	}

	newService.ObjectMeta.Labels = util.MergeStringMapsPreserve(newService.ObjectMeta.Labels, curService.ObjectMeta.Labels)
	newService.ObjectMeta.Annotations = util.MergeStringMapsPreserve(newService.ObjectMeta.Annotations, curService.ObjectMeta.Annotations)
	newService.ObjectMeta.Finalizers = util.MergeStringArrays(newService.ObjectMeta.Finalizers, curService.ObjectMeta.Finalizers)

	// And only now we are ready to actually update the service with new version of the service
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}
	_, err := w.c.kubeClient.CoreV1().Services(newService.Namespace).Update(ctx, newService, newUpdateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update Service %s/%s", newService.Namespace, newService.Name)
	} else {
		w.a.WithEvent(chi, eventActionUpdate, eventReasonUpdateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Update Service %s/%s failed with error %v", newService.Namespace, newService.Name, err)
	}

	return err
}

// createService
func (w *worker) createService(ctx context.Context, chi *chiv1.ClickHouseInstallation, service *core.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().Services(service.Namespace).Create(ctx, service, newCreateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Create Service %s/%s", service.Namespace, service.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Create Service %s/%s failed with error %v", service.Namespace, service.Name, err)
	}

	return err
}

// reconcileService reconciles core.Service
func (w *worker) reconcileService(ctx context.Context, chi *chiv1.ClickHouseInstallation, service *core.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().Info(service.Name)
	defer w.a.V(2).M(chi).E().Info(service.Name)

	// Check whether this object already exists
	curService, err := w.c.getService(service)

	if curService != nil {
		// We have Service - try to update it
		err = w.updateService(ctx, chi, curService, service)
	}

	if err != nil {
		// Service not found or not updated. Try to recreate
		_ = w.c.deleteServiceIfExists(ctx, service.Namespace, service.Name)
		err = w.createService(ctx, chi, service)
	}

	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to reconcile Service: %s CHI: %s ", service.Name, chi.Name)
	}

	return err
}

// createSecret
func (w *worker) createSecret(ctx context.Context, chi *chiv1.ClickHouseInstallation, secret *core.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, newCreateOptions())
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

// reconcileSecret reconciles core.Secret
func (w *worker) reconcileSecret(ctx context.Context, chi *chiv1.ClickHouseInstallation, secret *core.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().Info(secret.Name)
	defer w.a.V(2).M(chi).E().Info(secret.Name)

	// Check whether this object already exists
	if _, err := w.c.getSecret(secret); err == nil {
		// We have Secret - try to update it
		return nil
	}

	// Secret not found or broken. Try to recreate
	_ = w.c.deleteSecretIfExists(ctx, secret.Namespace, secret.Name)
	err := w.createSecret(ctx, chi, secret)
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to reconcile Secret: %s CHI: %s ", secret.Name, chi.Name)
	}

	return err
}

// getStatefulSetStatus
func (w *worker) getStatefulSetStatus(meta meta.ObjectMeta) chiv1.StatefulSetStatus {
	w.a.V(2).M(meta).S().Info(util.NamespaceNameString(meta))
	defer w.a.V(2).M(meta).E().Info(util.NamespaceNameString(meta))

	// Check whether this object already exists in k8s
	curStatefulSet, err := w.c.getStatefulSet(&meta, false)

	if curStatefulSet != nil {
		// Try to perform label-based comparison
		curLabel, curHasLabel := w.ctx.creator.GetObjectVersion(curStatefulSet.ObjectMeta)
		newLabel, newHasLabel := w.ctx.creator.GetObjectVersion(meta)
		if curHasLabel && newHasLabel {
			if curLabel == newLabel {
				w.a.M(meta).F().Info(
					"cur and new StatefulSets ARE EQUAL based on labels. No reconcile is required for: %s",
					util.NamespaceNameString(meta),
				)
				return chiv1.StatefulSetStatusSame
			}
			//if diff, equal := messagediff.DeepDiff(curStatefulSet.Spec, statefulSet.Spec); equal {
			//	w.a.Info("INFO StatefulSet ARE EQUAL based on diff no reconcile is actually needed")
			//	//					return chop.StatefulSetStatusSame
			//} else {
			//	w.a.Info("INFO StatefulSet ARE DIFFERENT based on diff reconcile is required: a:%v m:%v r:%v", diff.Added, diff.Modified, diff.Removed)
			//	//					return chop.StatefulSetStatusModified
			//}
			w.a.M(meta).F().Info(
				"cur and new StatefulSets ARE DIFFERENT based on labels. Reconcile is required for: %s",
				util.NamespaceNameString(meta),
			)
			return chiv1.StatefulSetStatusModified
		}
		// No labels to compare, we can not say for sure what exactly is going on
		return chiv1.StatefulSetStatusUnknown
	}

	// No cur StatefulSet available

	if apierrors.IsNotFound(err) {
		return chiv1.StatefulSetStatusNew
	}

	return chiv1.StatefulSetStatusUnknown
}

// reconcileStatefulSet reconciles apps.StatefulSet
func (w *worker) reconcileStatefulSet(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	newStatefulSet := host.StatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))

	if host.ReconcileAttributes.GetStatus() == chiv1.StatefulSetStatusSame {
		defer w.a.V(2).M(host).F().Info("no need to reconcile the same StatefulSet %s", util.NamespaceNameString(newStatefulSet.ObjectMeta))
		return nil
	}

	// Check whether this object already exists in k8s
	var err error
	host.CurStatefulSet, err = w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	if host.CurStatefulSet != nil {
		// We have StatefulSet - try to update it
		err = w.updateStatefulSet(ctx, host)
	}

	if apierrors.IsNotFound(err) {
		// StatefulSet not found - even during Update process - try to create it
		err = w.createStatefulSet(ctx, host)
	}

	if err != nil {
		w.a.WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			M(host).F().
			Error("FAILED to reconcile StatefulSet: %s CHI: %s ", newStatefulSet.Name, host.CHI.Name)
	}

	// Host has to know current StatefulSet and Pod
	host.CurStatefulSet, _ = w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	return err
}

// createStatefulSet
func (w *worker) createStatefulSet(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	statefulSet := host.StatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(statefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(statefulSet.ObjectMeta))

	w.a.V(1).
		WithEvent(host.CHI, eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Create StatefulSet %s/%s - started", statefulSet.Namespace, statefulSet.Name)

	err := w.c.createStatefulSet(ctx, host)

	host.CHI.EnsureStatus().AddedHostsCount++
	_ = w.c.updateCHIObjectStatus(ctx, host.CHI, UpdateCHIStatusOptions{
		CopyCHIStatusOptions: chiv1.CopyCHIStatusOptions{
			MainFields: true,
		},
	})

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Create StatefulSet %s/%s - completed", statefulSet.Namespace, statefulSet.Name)
	} else if err == errIgnore {
		w.a.WithEvent(host.CHI, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.CHI).
			M(host).F().
			Warning("Create StatefulSet %s/%s - error ignored", statefulSet.Namespace, statefulSet.Name)
	} else {
		w.a.WithEvent(host.CHI, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			M(host).F().
			Error("Create StatefulSet %s/%s - failed with error %v", statefulSet.Namespace, statefulSet.Name, err)
	}

	return err
}

// waitConfigMapPropagation
func (w *worker) waitConfigMapPropagation(ctx context.Context, host *chiv1.ChiHost) bool {
	// No need to wait for ConfigMap propagation on stopped host
	if host.GetCHI().IsStopped() {
		w.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - on stopped host")
		return false
	}

	// No need to wait on unchanged ConfigMap
	if w.ctx.cmUpdate.IsZero() {
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
	elapsed := time.Now().Sub(w.ctx.cmUpdate)
	if elapsed >= timeout {
		w.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - already elapsed. %s/%s", elapsed, timeout)
		return false
	}

	// Looks like we need to wait for Configmap propagation, after all
	wait := timeout - elapsed
	w.a.V(1).M(host).F().Info("Wait for ConfigMap propagation for %s %s/%s", wait, elapsed, timeout)
	if util.WaitContextDoneOrTimeout(ctx, wait) {
		log.V(2).Info("ctx is done")
		return true
	}

	return false
}

// updateStatefulSet
func (w *worker) updateStatefulSet(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Helpers
	newStatefulSet := host.StatefulSet
	curStatefulSet := host.CurStatefulSet

	w.a.V(2).M(host).S().Info(newStatefulSet.Name)
	defer w.a.V(2).M(host).E().Info(newStatefulSet.Name)

	namespace := newStatefulSet.Namespace
	name := newStatefulSet.Name

	w.a.V(1).
		WithEvent(host.CHI, eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Update StatefulSet(%s/%s) - started", namespace, name)

	if w.waitConfigMapPropagation(ctx, host) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if chopmodel.IsStatefulSetReady(curStatefulSet) {
		err := w.c.updateStatefulSet(ctx, curStatefulSet, newStatefulSet, host)
		if err == nil {
			host.CHI.EnsureStatus().UpdatedHostsCount++
			_ = w.c.updateCHIObjectStatus(ctx, host.CHI, UpdateCHIStatusOptions{
				CopyCHIStatusOptions: chiv1.CopyCHIStatusOptions{
					MainFields: true,
				},
			})
			w.a.V(1).
				WithEvent(host.CHI, eventActionUpdate, eventReasonUpdateCompleted).
				WithStatusAction(host.CHI).
				M(host).F().
				Info("Update StatefulSet(%s/%s) - completed", namespace, name)
			return nil
		}

		w.a.WithEvent(host.CHI, eventActionUpdate, eventReasonUpdateInProgress).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Update StatefulSet(%s/%s) switch from Update to Recreate", namespace, name)

		w.a.V(2).
			M(host).F().
			Error("Update StatefulSet(%s/%s) - failed with error\n---\n%v\n--\nContinue with recreate", namespace, name, err)
		diff, equal := messagediff.DeepDiff(curStatefulSet.Spec, newStatefulSet.Spec)
		w.a.V(2).M(host).Info("StatefulSet.Spec diff:")
		w.a.V(2).M(host).Info(util.MessageDiffString(diff, equal))
	}

	return w.recreateStatefulSet(ctx, host)
}

// recreateStatefulSet
func (w *worker) recreateStatefulSet(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_ = w.c.deleteStatefulSet(ctx, host)
	_ = w.reconcilePVCs(ctx, host)
	host.StatefulSet = host.DesiredStatefulSet
	return w.createStatefulSet(ctx, host)
}

// reconcilePersistentVolumes
func (w *worker) reconcilePersistentVolumes(ctx context.Context, host *chiv1.ChiHost) {
	if util.IsContextDone(ctx) {
		return
	}

	w.c.walkPVs(host, func(pv *core.PersistentVolume) {
		pv = w.ctx.creator.PreparePersistentVolume(pv, host)
		_, _ = w.c.updatePersistentVolume(ctx, pv)
	})
}

// reconcilePVCs
func (w *worker) reconcilePVCs(ctx context.Context, host *chiv1.ChiHost) error {
	if util.IsContextDone(ctx) {
		return nil
	}

	namespace := host.Address.Namespace
	w.a.V(2).M(host).S().Info("host %s/%s", namespace, host.Name)
	defer w.a.V(2).M(host).E().Info("host %s/%s", namespace, host.Name)

	host.WalkVolumeMounts(func(volumeMount *core.VolumeMount) {
		if util.IsContextDone(ctx) {
			return
		}

		volumeClaimTemplateName := volumeMount.Name
		volumeClaimTemplate, ok := host.CHI.GetVolumeClaimTemplate(volumeClaimTemplateName)
		if !ok {
			// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
			return
		}

		pvcName := chopmodel.CreatePVCName(host, volumeMount, volumeClaimTemplate)
		w.a.V(2).M(host).Info("reconcile volumeMount (%s/%s/%s/%s) - start", namespace, host.Name, volumeMount.Name, pvcName)
		defer w.a.V(2).M(host).Info("reconcile volumeMount (%s/%s/%s/%s) - end", namespace, host.Name, volumeMount.Name, pvcName)

		pvc, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, newGetOptions())
		if err != nil {
			if apierrors.IsNotFound(err) {
				// This is not an error per se, means PVC is not created (yet)?
				if w.ctx.creator.OperatorShouldCreatePVC(host, volumeClaimTemplate) {
					pvc = w.ctx.creator.CreatePVC(pvcName, host, &volumeClaimTemplate.Spec)
				} else {
					// Not created and we are not expected to create PVC by ourselves
					return
				}
			} else {
				// Any non-NotFound API error - unable to proceed
				w.a.M(host).F().Error("ERROR unable to get PVC(%s/%s) err: %v", namespace, pvcName, err)
				return
			}
		}

		pvc, err = w.reconcilePVC(ctx, pvc, host, volumeClaimTemplate)
		if err != nil {
			w.a.M(host).F().Error("ERROR unable to reconcile PVC(%s/%s) err: %v", namespace, pvcName, err)
			w.ctx.registryFailed.RegisterPVC(pvc.ObjectMeta)
			return
		}

		w.ctx.registryReconciled.RegisterPVC(pvc.ObjectMeta)
	})

	return nil
}

// reconcilePVC
func (w *worker) reconcilePVC(
	ctx context.Context,
	pvc *core.PersistentVolumeClaim,
	host *chiv1.ChiHost,
	template *chiv1.ChiVolumeClaimTemplate,
) (*core.PersistentVolumeClaim, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil, fmt.Errorf("ctx is done")
	}

	w.applyPVCResourcesRequests(pvc, template)
	pvc = w.ctx.creator.PreparePersistentVolumeClaim(pvc, host, template)
	return w.c.updatePersistentVolumeClaim(ctx, pvc)
}

// applyPVCResourcesRequests
func (w *worker) applyPVCResourcesRequests(
	pvc *core.PersistentVolumeClaim,
	template *chiv1.ChiVolumeClaimTemplate,
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
