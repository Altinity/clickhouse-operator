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
	coreV1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilRuntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/altinity/queue"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopModel "github.com/altinity/clickhouse-operator/pkg/model"
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
	normalizer *chopModel.Normalizer
	schemer    *chopModel.Schemer
	start      time.Time
	task       task
}

// task represents context of a worker. This also can be called "a reconcile task"
type task struct {
	creator            *chopModel.Creator
	registryReconciled *chopModel.Registry
	registryFailed     *chopModel.Registry
	cmUpdate           time.Time
	start              time.Time
}

// newTask creates new context
func newTask(creator *chopModel.Creator) task {
	return task{
		creator:            creator,
		registryReconciled: chopModel.NewRegistry(),
		registryFailed:     chopModel.NewRegistry(),
		cmUpdate:           time.Time{},
		start:              time.Now(),
	}
}

// newWorker
// func (c *Controller) newWorker(q workqueue.RateLimitingInterface) *worker {
func (c *Controller) newWorker(q queue.PriorityQueue, sys bool) *worker {
	start := time.Now()
	if !sys {
		start = start.Add(chiV1.DefaultReconcileThreadsWarmup)
	}
	return &worker{
		c:          c,
		a:          NewAnnouncer().WithController(c),
		queue:      q,
		normalizer: chopModel.NewNormalizer(c.kubeClient),
		schemer: chopModel.NewSchemer(
			chop.Config().ClickHouse.Access.Scheme,
			chop.Config().ClickHouse.Access.Username,
			chop.Config().ClickHouse.Access.Password,
			chop.Config().ClickHouse.Access.RootCA,
			chop.Config().ClickHouse.Access.Port,
		),
		start: start,
	}
}

// newContext creates new reconcile task
func (w *worker) newTask(chi *chiV1.ClickHouseInstallation) {
	w.task = newTask(chopModel.NewCreator(chi))
}

// timeToStart specifies time that operator does not accept changes
const timeToStart = 1 * time.Minute

// isJustStarted checks whether worked just started
func (w *worker) isJustStarted() bool {
	return time.Since(w.start) < timeToStart
}

func (w *worker) isConfigurationChangeRequiresReboot(host *chiV1.ChiHost) bool {
	return chopModel.IsConfigurationChangeRequiresReboot(host)
}

// shouldForceRestartHost checks whether cluster requires hosts restart
func (w *worker) shouldForceRestartHost(host *chiV1.ChiHost) bool {
	// RollingUpdate purpose is to always shut the host down.
	// It is such an interesting policy.
	if host.GetCHI().IsRollingUpdate() {
		w.a.V(1).M(host).F().Info("RollingUpdate requires force restart. Host: %s", host.GetName())
		return true
	}

	if host.GetReconcileAttributes().GetStatus() == chiV1.StatefulSetStatusNew {
		w.a.V(1).M(host).F().Info("Host is new, no restart applicable. Host: %s", host.GetName())
		return false
	}

	if host.GetReconcileAttributes().GetStatus() == chiV1.StatefulSetStatusSame && !host.HasAncestor() {
		w.a.V(1).M(host).F().Info("Host already exists, but has no ancestor, no restart applicable. Host: %s", host.GetName())
		return false
	}

	// For some configuration changes we have to force restart host
	if w.isConfigurationChangeRequiresReboot(host) {
		w.a.V(1).M(host).F().Info("Config changes requires force restart. Host: %s", host.GetName())
		return true
	}

	w.a.V(1).M(host).F().Info("Force restart is not required. Host: %s", host.GetName())
	return false
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
		return w.c.addChit(cmd.new)
	case reconcileUpdate:
		return w.c.updateChit(cmd.old, cmd.new)
	case reconcileDelete:
		return w.c.deleteChit(cmd.old)
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

func (w *worker) processDropDns(ctx context.Context, cmd *DropDns) error {
	if chi, err := w.createCHIFromObjectMeta(cmd.initiator, false, chopModel.NewNormalizerOptions()); err == nil {
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
	case *DropDns:
		return w.processDropDns(ctx, cmd)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// normalize
func (w *worker) normalize(c *chiV1.ClickHouseInstallation) *chiV1.ClickHouseInstallation {
	w.a.V(3).M(c).S().P()
	defer w.a.V(3).M(c).E().P()

	chi, err := w.normalizer.CreateTemplatedCHI(c, chopModel.NewNormalizerOptions())
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to normalize CHI 1: %v", err)
	}

	ips := w.c.getPodsIPs(chi)
	w.a.V(1).M(chi).Info("IPs of the CHI %s/%s: %v", chi.Namespace, chi.Name, ips)
	opts := chopModel.NewNormalizerOptions()
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
func (w *worker) ensureFinalizer(ctx context.Context, chi *chiV1.ClickHouseInstallation) bool {
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
func (w *worker) updateEndpoints(ctx context.Context, old, new *coreV1.Endpoints) error {

	if chi, err := w.createCHIFromObjectMeta(&new.ObjectMeta, false, chopModel.NewNormalizerOptions()); err == nil {
		w.a.V(2).M(chi).Info("updating endpoints for CHI-1 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-1 %v", ips)
		opts := chopModel.NewNormalizerOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(&new.ObjectMeta, false, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-1")
			w.newTask(chi)
			w.reconcileCHIConfigMapUsers(ctx, chi)
			w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
				TolerateAbsence: true,
				CopyCHIStatusOptions: chiV1.CopyCHIStatusOptions{
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
func (w *worker) updateCHI(ctx context.Context, old, new *chiV1.ClickHouseInstallation) error {
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
		w.a.M(new).F().Info("finalizer installed, let's restart reconcile cycle")
		return nil
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

	if w.isCleanRestartOnTheSameIP(new) {
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

// isCleanRestartOnTheSameIP checks whether it is just a restart of the operator on the same IP
func (w *worker) isCleanRestartOnTheSameIP(chi *chiV1.ClickHouseInstallation) bool {
	ip, _ := chop.Get().ConfigManager.GetRuntimeParam(chiV1.OPERATOR_POD_IP)
	operatorIpIsTheSame := ip == chi.Status.GetCHOpIP()
	log.V(1).Info("Operator IPs. Previous: %s Cur: %s", chi.Status.GetCHOpIP(), ip)

	if !operatorIpIsTheSame {
		// Operator has restarted on the different IP address.
		// We may need to reconcile config files
		log.V(1).Info("Operator IPs are different. Is NOT restart on the same IP")
		return false
	}

	log.V(1).Info("Operator IPs are the same. It is restart on the same IP")
	return w.isCleanRestart(chi)
}

// isCleanRestart checks whether it is just a restart of the operator and CHI has no changes since last processed
func (w *worker) isCleanRestart(chi *chiV1.ClickHouseInstallation) bool {
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
	statusIsCompleted := chi.Status.GetStatus() == chiV1.StatusCompleted
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
func (w *worker) areUsableOldAndNew(old, new *chiV1.ClickHouseInstallation) bool {
	if old == nil {
		return false
	}
	if new == nil {
		return false
	}
	return true
}

// isAfterFinalizerInstalled checks whether we are just installed finalizer
func (w *worker) isAfterFinalizerInstalled(old, new *chiV1.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	finalizerIsInstalled := len(old.Finalizers) == 0 && len(new.Finalizers) > 0
	return w.isGenerationTheSame(old, new) && finalizerIsInstalled
}

// isGenerationTheSame checks whether old ans new CHI have the same generation
func (w *worker) isGenerationTheSame(old, new *chiV1.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	return old.Generation == new.Generation
}

// logCHI writes a CHI into the log
func (w *worker) logCHI(name string, chi *chiV1.ClickHouseInstallation) {
	w.a.V(2).M(chi).Info(
		"%s CHI start--------------------------------------------:\n%s\n%s CHI end--------------------------------------------",
		name,
		name,
		chi.YAML(chiV1.CopyCHIOptions{SkipStatus: true, SkipManagedFields: true}),
	)
}

// logActionPlan logs action plan
func (w *worker) logActionPlan(ap *chopModel.ActionPlan) {
	w.a.Info(
		"ActionPlan start---------------------------------------------:\n%s\nActionPlan end---------------------------------------------",
		ap,
	)
}

// logOldAndNew writes old and new CHIs into the log
func (w *worker) logOldAndNew(name string, old, new *chiV1.ClickHouseInstallation) {
	w.logCHI(name+" old", old)
	w.logCHI(name+" new", new)
}

func (w *worker) waitForIPAddresses(ctx context.Context, chi *chiV1.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}
	start := time.Now()
	w.c.poll(ctx, chi, func(c *chiV1.ClickHouseInstallation, e error) bool {
		if len(c.Status.GetPodIPs()) >= len(c.Status.GetPods()) {
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

// excludeStopped excludes stopped CHI from monitoring
func (w *worker) excludeStopped(chi *chiV1.ClickHouseInstallation) {
	if chi.IsStopped() {
		w.a.V(1).
			WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
			WithStatusAction(chi).
			M(chi).F().
			Info("exclude CHI from monitoring")
		w.c.deleteWatch(chi.Namespace, chi.Name)
	}
}

// includeStopped includes previously stopped CHI into monitoring
func (w *worker) includeStopped(chi *chiV1.ClickHouseInstallation) {
	if !chi.IsStopped() {
		w.a.V(1).
			WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
			WithStatusAction(chi).
			M(chi).F().
			Info("add CHI to monitoring")
		w.c.updateWatch(chi.Namespace, chi.Name, chopModel.CreateFQDNs(chi, nil, false))
	}
}

func (w *worker) markReconcileStart(ctx context.Context, chi *chiV1.ClickHouseInstallation, ap *chopModel.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	chi.EnsureStatus().ReconcileStart(ap.GetRemovedHostsNum())
	_ = w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		CopyCHIStatusOptions: chiV1.CopyCHIStatusOptions{
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

func (w *worker) markReconcileComplete(ctx context.Context, _chi *chiV1.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Update CHI object
	if chi, err := w.createCHIFromObjectMeta(&_chi.ObjectMeta, true, chopModel.NewNormalizerOptions()); err == nil {
		w.a.V(2).M(chi).Info("updating endpoints for CHI-2 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-2 %v", ips)
		opts := chopModel.NewNormalizerOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(&_chi.ObjectMeta, true, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-2")
			chi.SetAncestor(chi.GetTarget())
			chi.SetTarget(nil)
			chi.EnsureStatus().ReconcileComplete()
			w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
				CopyCHIStatusOptions: chiV1.CopyCHIStatusOptions{
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
		Info("reconcile completed, task id: %s", _chi.Spec.GetTaskID())
}

func (w *worker) walkHosts(ctx context.Context, chi *chiV1.ClickHouseInstallation, ap *chopModel.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	objs := w.c.discovery(ctx, chi)
	ap.WalkAdded(
		func(cluster *chiV1.Cluster) {
			cluster.WalkHosts(func(host *chiV1.ChiHost) error {

				// Name of the StatefulSet for this host
				name := chopModel.CreateStatefulSetName(host)
				// Have we found this StatefulSet
				found := false

				objs.WalkStatefulSet(func(meta metaV1.ObjectMeta) {
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
		func(shard *chiV1.ChiShard) {
			shard.WalkHosts(func(host *chiV1.ChiHost) error {
				host.GetReconcileAttributes().SetAdd()
				return nil
			})
		},
		func(host *chiV1.ChiHost) {
			host.GetReconcileAttributes().SetAdd()
		},
	)

	ap.WalkModified(
		func(cluster *chiV1.Cluster) {
		},
		func(shard *chiV1.ChiShard) {
		},
		func(host *chiV1.ChiHost) {
			host.GetReconcileAttributes().SetModify()
		},
	)

	chi.WalkHosts(func(host *chiV1.ChiHost) error {
		if host.GetReconcileAttributes().IsAdd() {
			// Already added
			return nil
		}
		if host.GetReconcileAttributes().IsModify() {
			// Already modified
			return nil
		}
		// Not clear yet
		host.GetReconcileAttributes().SetFound()
		return nil
	})

	chi.WalkHosts(func(host *chiV1.ChiHost) error {
		if host.GetReconcileAttributes().IsAdd() {
			w.a.M(host).Info("ADD host: %s", host.Address.CompactString())
			return nil
		}
		if host.GetReconcileAttributes().IsModify() {
			w.a.M(host).Info("MODIFY host: %s", host.Address.CompactString())
			return nil
		}
		if host.GetReconcileAttributes().IsFound() {
			w.a.M(host).Info("FOUND host: %s", host.Address.CompactString())
			return nil
		}
		w.a.M(host).Info("UNTOUCHED host: %s", host.Address.CompactString())
		return nil
	})
}

// baseRemoteServersGeneratorOptions build base set of RemoteServersGeneratorOptions
// which are applied on each of `remote_servers` reconfiguration during reconcile cycle
func (w *worker) baseRemoteServersGeneratorOptions() *chopModel.RemoteServersGeneratorOptions {
	opts := chopModel.NewRemoteServersGeneratorOptions()
	opts.ExcludeReconcileAttributes(
		chiV1.NewChiHostReconcileAttributes().SetAdd(),
	)

	return opts
}

// options build ClickHouseConfigFilesGeneratorOptions
func (w *worker) options(excludeHosts ...*chiV1.ChiHost) *chopModel.ClickHouseConfigFilesGeneratorOptions {
	// Stringify
	str := ""
	for _, host := range excludeHosts {
		str += fmt.Sprintf("name: '%s' sts: '%s'", host.GetName(), host.Address.StatefulSet)
	}

	opts := w.baseRemoteServersGeneratorOptions().ExcludeHosts(excludeHosts...)
	w.a.Info("RemoteServersGeneratorOptions: %s, excluded host(s): %s", opts, str)
	return chopModel.NewClickHouseConfigFilesGeneratorOptions().SetRemoteServersGeneratorOptions(opts)
}

// prepareHostStatefulSetWithStatus prepares host's StatefulSet status
func (w *worker) prepareHostStatefulSetWithStatus(ctx context.Context, host *chiV1.ChiHost, shutdown bool) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.prepareDesiredStatefulSet(host, shutdown)
	host.GetReconcileAttributes().SetStatus(w.getStatefulSetStatus(host.DesiredStatefulSet.ObjectMeta))
}

// prepareDesiredStatefulSet prepares desired StatefulSet
func (w *worker) prepareDesiredStatefulSet(host *chiV1.ChiHost, shutdown bool) {
	host.DesiredStatefulSet = w.task.creator.CreateStatefulSet(host, shutdown)
}

// migrateTables
func (w *worker) migrateTables(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
	host.GetCHI().EnsureStatus().PushHostTablesCreated(chopModel.CreateFQDN(host))
	if err == nil {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Info("Tables added successfully on shard/host:%d/%d cluster:%s", host.Address.ShardIndex, host.Address.ReplicaIndex, host.Address.ClusterName)
	} else {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Error("ERROR add tables added successfully on shard/host:%d/%d cluster:%s err:%v", host.Address.ShardIndex, host.Address.ReplicaIndex, host.Address.ClusterName, err)
	}
	return err
}

// shouldMigrateTables
func (w *worker) shouldMigrateTables(host *chiV1.ChiHost) bool {
	switch {
	case host.GetCHI().IsStopped():
		// Stopped host is not able to receive any data
		return false

	case util.InArray(chopModel.CreateFQDN(host), host.GetCHI().EnsureStatus().GetHostsWithTablesCreated()):
		// This host is listed as having tables created already
		return false

	default:
		return true
	}
}

// excludeHost excludes host from ClickHouse clusters if required
func (w *worker) excludeHost(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
func (w *worker) shouldIncludeHost(host *chiV1.ChiHost) bool {
	switch {
	case host.GetCHI().IsStopped():
		// No need to include stopped host
		return false
	}
	return true
}

// includeHost includes host back back into ClickHouse clusters
func (w *worker) includeHost(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
func (w *worker) excludeHostFromService(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.deleteLabelReadyPod(ctx, host)
	_ = w.c.deleteAnnotationReadyService(ctx, host)
	return nil
}

// includeHostIntoService
func (w *worker) includeHostIntoService(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.appendLabelReadyPod(ctx, host)
	_ = w.c.appendAnnotationReadyService(ctx, host)
	return nil
}

// excludeHostFromClickHouseCluster excludes host from ClickHouse configuration
func (w *worker) excludeHostFromClickHouseCluster(ctx context.Context, host *chiV1.ChiHost) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
func (w *worker) includeHostIntoClickHouseCluster(ctx context.Context, host *chiV1.ChiHost) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
func (w *worker) shouldExcludeHost(host *chiV1.ChiHost) bool {
	switch {
	case host.GetCHI().IsStopped():
		w.a.V(1).
			M(host).F().
			Info("No need to exclude stopped host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return false
	case w.shouldForceRestartHost(host):
		w.a.V(1).
			M(host).F().
			Info("While rolling update host would be restarted host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return true
	case host.GetReconcileAttributes().GetStatus() == chiV1.StatefulSetStatusNew:
		w.a.V(1).
			M(host).F().
			Info("Nothing to exclude, host is not yet in the cluster host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return false
	case host.GetReconcileAttributes().GetStatus() == chiV1.StatefulSetStatusSame:
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
func (w *worker) shouldWaitExcludeHost(host *chiV1.ChiHost) bool {
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
func (w *worker) shouldWaitIncludeHost(host *chiV1.ChiHost) bool {
	status := host.GetReconcileAttributes().GetStatus()
	switch {
	case status == chiV1.StatefulSetStatusNew:
		return false
	case status == chiV1.StatefulSetStatusSame:
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
func (w *worker) waitHostInCluster(ctx context.Context, host *chiV1.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, w.schemer.IsHostInCluster)
}

// waitHostNotInCluster
func (w *worker) waitHostNotInCluster(ctx context.Context, host *chiV1.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, func(ctx context.Context, host *chiV1.ChiHost) bool {
		return !w.schemer.IsHostInCluster(ctx, host)
	})
}

// waitHostNoActiveQueries
func (w *worker) waitHostNoActiveQueries(ctx context.Context, host *chiV1.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, func(ctx context.Context, host *chiV1.ChiHost) bool {
		n, _ := w.schemer.HostActiveQueriesNum(ctx, host)
		return n <= 1
	})
}

// createCHIFromObjectMeta
func (w *worker) createCHIFromObjectMeta(objectMeta *metaV1.ObjectMeta, isCHI bool, options *chopModel.NormalizerOptions) (*chiV1.ClickHouseInstallation, error) {
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
func (w *worker) updateConfigMap(ctx context.Context, chi *chiV1.ClickHouseInstallation, configMap *coreV1.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
func (w *worker) createConfigMap(ctx context.Context, chi *chiV1.ClickHouseInstallation, configMap *coreV1.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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

// updateService
func (w *worker) updateService(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	curService *coreV1.Service,
	newService *coreV1.Service,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
	if ((curService.Spec.Type == coreV1.ServiceTypeNodePort) && (newService.Spec.Type == coreV1.ServiceTypeNodePort)) ||
		((curService.Spec.Type == coreV1.ServiceTypeLoadBalancer) && (newService.Spec.Type == coreV1.ServiceTypeLoadBalancer)) {
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
	if (curService.Spec.ExternalTrafficPolicy == coreV1.ServiceExternalTrafficPolicyTypeLocal) &&
		(newService.Spec.ExternalTrafficPolicy == coreV1.ServiceExternalTrafficPolicyTypeLocal) {
		newService.Spec.HealthCheckNodePort = curService.Spec.HealthCheckNodePort
	}

	newService.ObjectMeta.Labels = util.MergeStringMapsPreserve(newService.ObjectMeta.Labels, curService.ObjectMeta.Labels)
	newService.ObjectMeta.Annotations = util.MergeStringMapsPreserve(newService.ObjectMeta.Annotations, curService.ObjectMeta.Annotations)
	newService.ObjectMeta.Finalizers = util.MergeStringArrays(newService.ObjectMeta.Finalizers, curService.ObjectMeta.Finalizers)

	// And only now we are ready to actually update the service with new version of the service
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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
func (w *worker) createService(ctx context.Context, chi *chiV1.ClickHouseInstallation, service *coreV1.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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

// createSecret
func (w *worker) createSecret(ctx context.Context, chi *chiV1.ClickHouseInstallation, secret *coreV1.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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

// getStatefulSetStatus
func (w *worker) getStatefulSetStatus(meta metaV1.ObjectMeta) chiV1.StatefulSetStatus {
	w.a.V(2).M(meta).S().Info(util.NamespaceNameString(meta))
	defer w.a.V(2).M(meta).E().Info(util.NamespaceNameString(meta))

	// Check whether this object already exists in k8s
	curStatefulSet, err := w.c.getStatefulSet(&meta, false)

	if curStatefulSet != nil {
		// Try to perform label-based comparison
		curLabel, curHasLabel := chopModel.GetObjectVersion(curStatefulSet.ObjectMeta)
		newLabel, newHasLabel := chopModel.GetObjectVersion(meta)
		if curHasLabel && newHasLabel {
			if curLabel == newLabel {
				w.a.M(meta).F().Info(
					"cur and new StatefulSets ARE EQUAL based on labels. No StatefulSet reconcile is required for: %s",
					util.NamespaceNameString(meta),
				)
				return chiV1.StatefulSetStatusSame
			}
			//if diff, equal := messagediff.DeepDiff(curStatefulSet.Spec, statefulSet.Spec); equal {
			//	w.a.Info("INFO StatefulSet ARE EQUAL based on diff no reconcile is actually needed")
			//	//					return chop.StatefulSetStatusSame
			//} else {
			//	w.a.Info("INFO StatefulSet ARE DIFFERENT based on diff reconcile is required: a:%v m:%v r:%v", diff.Added, diff.Modified, diff.Removed)
			//	//					return chop.StatefulSetStatusModified
			//}
			w.a.M(meta).F().Info(
				"cur and new StatefulSets ARE DIFFERENT based on labels. StatefulSet reconcile is required for: %s",
				util.NamespaceNameString(meta),
			)
			return chiV1.StatefulSetStatusModified
		}
		// No labels to compare, we can not say for sure what exactly is going on
		return chiV1.StatefulSetStatusUnknown
	}

	// No cur StatefulSet available

	if apiErrors.IsNotFound(err) {
		return chiV1.StatefulSetStatusNew
	}

	return chiV1.StatefulSetStatusUnknown
}

// createStatefulSet
func (w *worker) createStatefulSet(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	statefulSet := host.DesiredStatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(statefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(statefulSet.ObjectMeta))

	w.a.V(1).
		WithEvent(host.CHI, eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Create StatefulSet %s/%s - started", statefulSet.Namespace, statefulSet.Name)

	err := w.c.createStatefulSet(ctx, host)

	host.CHI.EnsureStatus().HostAdded()
	_ = w.c.updateCHIObjectStatus(ctx, host.CHI, UpdateCHIStatusOptions{
		CopyCHIStatusOptions: chiV1.CopyCHIStatusOptions{
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
func (w *worker) waitConfigMapPropagation(ctx context.Context, host *chiV1.ChiHost) bool {
	// No need to wait for ConfigMap propagation on stopped host
	if host.GetCHI().IsStopped() {
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
func (w *worker) updateStatefulSet(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Helpers
	newStatefulSet := host.DesiredStatefulSet
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
		log.V(2).Info("task is done")
		return nil
	}

	if chopModel.IsStatefulSetReady(curStatefulSet) {
		err := w.c.updateStatefulSet(ctx, curStatefulSet, newStatefulSet, host)
		if err == nil {
			host.CHI.EnsureStatus().HostUpdated()
			_ = w.c.updateCHIObjectStatus(ctx, host.CHI, UpdateCHIStatusOptions{
				CopyCHIStatusOptions: chiV1.CopyCHIStatusOptions{
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
func (w *worker) recreateStatefulSet(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.deleteStatefulSet(ctx, host)
	_ = w.reconcilePVCs(ctx, host)
	//host.StatefulSet = host.DesiredStatefulSet
	return w.createStatefulSet(ctx, host)
}

// applyPVCResourcesRequests
func (w *worker) applyPVCResourcesRequests(
	pvc *coreV1.PersistentVolumeClaim,
	template *chiV1.ChiVolumeClaimTemplate,
) bool {
	return w.applyResourcesList(pvc.Spec.Resources.Requests, template.Spec.Resources.Requests)
}

// applyResourcesList
func (w *worker) applyResourcesList(
	curResourceList coreV1.ResourceList,
	desiredResourceList coreV1.ResourceList,
) bool {
	// Prepare lists of resource names
	var curResourceNames []coreV1.ResourceName
	for resourceName := range curResourceList {
		curResourceNames = append(curResourceNames, resourceName)
	}
	var desiredResourceNames []coreV1.ResourceName
	for resourceName := range desiredResourceList {
		desiredResourceNames = append(desiredResourceNames, resourceName)
	}

	resourceNames := intersect.Simple(curResourceNames, desiredResourceNames)
	updated := false
	for _, resourceName := range resourceNames.([]interface{}) {
		updated = updated || w.applyResource(curResourceList, desiredResourceList, resourceName.(coreV1.ResourceName))
	}
	return updated
}

// applyResource
func (w *worker) applyResource(
	curResourceList coreV1.ResourceList,
	desiredResourceList coreV1.ResourceList,
	resourceName coreV1.ResourceName,
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
