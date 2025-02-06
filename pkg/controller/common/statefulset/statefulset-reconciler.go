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

package statefulset

import (
	"context"
	"strings"
	"time"

	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type Reconciler struct {
	a    a.Announcer
	task *common.Task

	hostSTSPoller IHostStatefulSetPoller
	namer         interfaces.INameManager
	labeler       interfaces.ILabeler
	storage       *storage.Reconciler

	cr  interfaces.IKubeCR
	sts interfaces.IKubeSTS

	fallback fallback
}

func NewReconciler(
	a a.Announcer,
	task *common.Task,
	hostSTSPoller IHostStatefulSetPoller,
	namer interfaces.INameManager,
	labeler interfaces.ILabeler,
	storage *storage.Reconciler,
	kube interfaces.IKube,
	fallback fallback,
) *Reconciler {
	return &Reconciler{
		a:    a,
		task: task,

		hostSTSPoller: hostSTSPoller,
		namer:         namer,
		labeler:       labeler,
		storage:       storage,

		cr:  kube.CR(),
		sts: kube.STS(),

		fallback: fallback,
	}
}

// PrepareHostStatefulSetWithStatus prepares host's StatefulSet status
func (r *Reconciler) PrepareHostStatefulSetWithStatus(ctx context.Context, host *api.Host, shutdown bool) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	r.prepareDesiredStatefulSet(host, shutdown)
	host.GetReconcileAttributes().SetStatus(r.getStatefulSetStatus(host))
}

// prepareDesiredStatefulSet prepares desired StatefulSet
func (r *Reconciler) prepareDesiredStatefulSet(host *api.Host, shutdown bool) {
	host.Runtime.DesiredStatefulSet = r.task.Creator().CreateStatefulSet(host, shutdown)
}

// getStatefulSetStatus gets StatefulSet status
func (r *Reconciler) getStatefulSetStatus(host *api.Host) api.ObjectStatus {
	new := host.Runtime.DesiredStatefulSet
	r.a.V(2).M(new).S().Info(util.NamespaceNameString(new))
	defer r.a.V(2).M(new).E().Info(util.NamespaceNameString(new))

	r.a.V(2).M(new).Info("host sts preamble: ancestor: %t cnt: %d added: %d",
		host.HasAncestor(),
		host.GetCR().IEnsureStatus().GetHostsCount(),
		host.GetCR().IEnsureStatus().GetHostsAddedCount(),
	)

	cur, err := r.sts.Get(context.TODO(), new)
	switch {
	case cur != nil:
		r.a.V(1).M(new).Info("Have StatefulSet available, try to perform label-based comparison for sts: %s", util.NamespaceNameString(new))
		return common.GetObjectStatusFromMetas(r.labeler, cur, new)

	case apiErrors.IsNotFound(err):
		// StatefulSet is not found at the moment.
		// However, it may be just deleted
		r.a.V(1).M(new).Info("No cur StatefulSet available and the reason is - not found. Either new one or a deleted sts: %s", util.NamespaceNameString(new))
		if host.HasAncestor() {
			r.a.V(1).M(new).Warning("No cur StatefulSet available but host has an ancestor. Found deleted sts. for: %s", util.NamespaceNameString(new))
			return api.ObjectStatusModified
		}
		r.a.V(1).M(new).Info("No cur StatefulSet available and it is not found and is a new one. New sts: %s", util.NamespaceNameString(new))
		return api.ObjectStatusNew

	default:
		r.a.V(1).M(new).Warning("Have no StatefulSet available, nor it is not found. sts: %s err: %v", util.NamespaceNameString(new), err)
		return api.ObjectStatusUnknown
	}
}

// ReconcileStatefulSet reconciles StatefulSet of a host
func (r *Reconciler) ReconcileStatefulSet(
	ctx context.Context,
	host *api.Host,
	register bool,
	opts *ReconcileOptions,
) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	newStatefulSet := host.Runtime.DesiredStatefulSet

	r.a.V(2).M(host).S().Info(util.NamespaceNameString(newStatefulSet))
	defer r.a.V(2).M(host).E().Info(util.NamespaceNameString(newStatefulSet))

	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusSame {
		r.a.V(2).M(host).F().Info("No need to reconcile THE SAME StatefulSet: %s", util.NamespaceNameString(newStatefulSet))
		if register {
			host.GetCR().IEnsureStatus().HostUnchanged()
			_ = r.cr.StatusUpdate(ctx, host.GetCR(), types.UpdateStatusOptions{
				CopyStatusOptions: types.CopyStatusOptions{
					CopyStatusFieldGroup: types.CopyStatusFieldGroup{
						FieldGroupMain: true,
					},
				},
			})
		}
		return nil
	}

	// Check whether this object already exists in k8s
	host.Runtime.CurStatefulSet, err = r.sts.Get(ctx, newStatefulSet)

	// Report diff to trace
	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusModified {
		r.a.V(1).M(host).F().Info("Need to reconcile MODIFIED StatefulSet: %s", util.NamespaceNameString(newStatefulSet))
		common.DumpStatefulSetDiff(host, host.Runtime.CurStatefulSet, newStatefulSet)
	}

	switch {
	case opts.IsForceRecreate():
		// Force recreate prevails over all other requests
		r.recreateStatefulSet(ctx, host, register, opts)
	default:
		// We have (or had in the past) StatefulSet - try to update|recreate it
		err = r.updateStatefulSet(ctx, host, register, opts)
	}

	if apiErrors.IsNotFound(err) {
		// StatefulSet not found - even during Update process - try to create it
		err = r.createStatefulSet(ctx, host, register, opts)
	}

	// Host has to know current StatefulSet and Pod
	host.Runtime.CurStatefulSet, _ = r.sts.Get(ctx, newStatefulSet)

	return err
}

// recreateStatefulSet
func (r *Reconciler) recreateStatefulSet(ctx context.Context, host *api.Host, register bool, opts *ReconcileOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	r.a.V(2).M(host).S().Info(util.NamespaceNameString(host.GetCR()))
	defer r.a.V(2).M(host).E().Info(util.NamespaceNameString(host.GetCR()))

	_ = r.doDeleteStatefulSet(ctx, host)
	_ = r.storage.ReconcilePVCs(ctx, host, api.DesiredStatefulSet)
	return r.createStatefulSet(ctx, host, register, opts)
}

// copyKubectlAnnotationsBeforeUpdate copies kubectl annotations from old to new statefulset as they are set by external kubectl
// operations and should be preserved during statefulset update
func (r *Reconciler) copyKubectlAnnotationsBeforeUpdate(old, new *apps.StatefulSet) {
	if old == nil || new == nil {
		return
	}

	oldMeta := old.Spec.Template.ObjectMeta
	newMeta := new.Spec.Template.ObjectMeta

	for k, v := range oldMeta.Annotations {
		if strings.HasPrefix(k, "kubectl.kubernetes.io/") {
			if newMeta.Annotations == nil {
				newMeta.Annotations = make(map[string]string)
			}

			newMeta.Annotations[k] = v
		}
	}
}

// updateStatefulSet
func (r *Reconciler) updateStatefulSet(ctx context.Context, host *api.Host, register bool, opts *ReconcileOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Helpers
	newStatefulSet := host.Runtime.DesiredStatefulSet.DeepCopy()
	curStatefulSet := host.Runtime.CurStatefulSet

	r.copyKubectlAnnotationsBeforeUpdate(curStatefulSet, newStatefulSet)

	r.a.V(2).M(host).S().Info(newStatefulSet.Name)
	defer r.a.V(2).M(host).E().Info(newStatefulSet.Name)

	namespace := newStatefulSet.Namespace
	name := newStatefulSet.Name

	r.a.V(1).
		WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateStarted).
		WithAction(host.GetCR()).
		M(host).F().
		Info("Update StatefulSet(%s) - started", util.NamespaceNameString(newStatefulSet))

	if r.waitForConfigMapPropagation(ctx, host) {
		log.V(2).Info("task is done")
		return nil
	}

	action := common.ErrCRUDRecreate
	if k8s.IsStatefulSetReady(curStatefulSet) {
		action = r.doUpdateStatefulSet(ctx, curStatefulSet, newStatefulSet, host)
	}

	switch action {
	case nil:
		if register {
			host.GetCR().IEnsureStatus().HostUpdated()
			_ = r.cr.StatusUpdate(ctx, host.GetCR(), types.UpdateStatusOptions{
				CopyStatusOptions: types.CopyStatusOptions{
					CopyStatusFieldGroup: types.CopyStatusFieldGroup{
						FieldGroupMain: true,
					},
				},
			})
		}
		r.a.V(1).
			WithEvent(host.GetCR(), a.EventActionUpdate, a.EventReasonUpdateCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Update StatefulSet(%s/%s) - completed", namespace, name)
		return nil
	case common.ErrCRUDAbort:
		r.a.V(1).M(host).Info("Update StatefulSet(%s/%s) - got abort. Abort", namespace, name)
		return common.ErrCRUDAbort
	case common.ErrCRUDIgnore:
		r.a.V(1).M(host).Info("Update StatefulSet(%s/%s) - got ignore. Ignore", namespace, name)
		return nil
	case common.ErrCRUDRecreate:
		r.a.WithEvent(host.GetCR(), a.EventActionUpdate, a.EventReasonUpdateInProgress).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Update StatefulSet(%s/%s) switch from Update to Recreate", namespace, name)
		common.DumpStatefulSetDiff(host, curStatefulSet, newStatefulSet)
		return r.recreateStatefulSet(ctx, host, register, opts)
	case common.ErrCRUDUnexpectedFlow:
		r.a.V(1).M(host).Warning("Got unexpected flow action. Ignore and continue for now")
		return nil
	}

	r.a.V(1).M(host).Warning("Got unexpected flow. This is strange. Ignore and continue for now")
	return nil
}

// createStatefulSet
func (r *Reconciler) createStatefulSet(ctx context.Context, host *api.Host, register bool, opts *ReconcileOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	statefulSet := host.Runtime.DesiredStatefulSet

	r.a.V(2).M(host).S().Info(util.NamespaceNameString(statefulSet.GetObjectMeta()))
	defer r.a.V(2).M(host).E().Info(util.NamespaceNameString(statefulSet.GetObjectMeta()))

	r.a.V(1).
		WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateStarted).
		WithAction(host.GetCR()).
		M(host).F().
		Info("Create StatefulSet: %s - started", util.NamespaceNameString(statefulSet))

	action := r.doCreateStatefulSet(ctx, host, opts)

	if register {
		host.GetCR().IEnsureStatus().HostAdded()
		_ = r.cr.StatusUpdate(ctx, host.GetCR(), types.UpdateStatusOptions{
			CopyStatusOptions: types.CopyStatusOptions{
				CopyStatusFieldGroup: types.CopyStatusFieldGroup{
					FieldGroupMain: true,
				},
			},
		})
	}

	switch action {
	case nil:
		r.a.V(1).
			WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Create StatefulSet: %s - completed", util.NamespaceNameString(statefulSet))
		return nil
	case common.ErrCRUDAbort:
		r.a.WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateFailed).
			WithAction(host.GetCR()).
			WithError(host.GetCR()).
			M(host).F().
			Error("Create StatefulSet: %s - failed with error: %v", util.NamespaceNameString(statefulSet), action)
		return action
	case common.ErrCRUDIgnore:
		r.a.WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateFailed).
			WithAction(host.GetCR()).
			M(host).F().
			Warning("Create StatefulSet: %s - error ignored", util.NamespaceNameString(statefulSet))
		return nil
	case common.ErrCRUDRecreate:
		r.a.V(1).M(host).Warning("Got recreate action. Ignore and continue for now")
		return nil
	case common.ErrCRUDUnexpectedFlow:
		r.a.V(1).M(host).Warning("Got unexpected flow action. Ignore and continue for now")
		return nil
	}

	r.a.V(1).M(host).Warning("Got unexpected flow. This is strange. Ignore and continue for now")
	return nil
}

// waitForConfigMapPropagation
func (r *Reconciler) waitForConfigMapPropagation(ctx context.Context, host *api.Host) bool {
	// No need to wait for ConfigMap propagation on stopped host
	if host.IsStopped() {
		r.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - host is stopped")
		return false
	}

	// No need to wait on unchanged ConfigMap
	if r.task.CmUpdate().IsZero() {
		r.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - no changes in ConfigMap")
		return false
	}

	// What timeout is expected to be enough for ConfigMap propagation?
	// In case timeout is not specified, no need to wait
	if !host.GetCR().GetReconciling().HasConfigMapPropagationTimeout() {
		r.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - not applicable due to missing timeout value")
		return false
	}

	timeout := host.GetCR().GetReconciling().GetConfigMapPropagationTimeoutDuration()

	// How much time has elapsed since last ConfigMap update?
	// May be there is no need to wait already
	elapsed := time.Now().Sub(r.task.CmUpdate())
	if elapsed >= timeout {
		r.a.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - already elapsed. [elapsed/timeout: %s/%s]", elapsed, timeout)
		return false
	}

	// Looks like we need to wait for Configmap propagation, after all
	wait := timeout - elapsed
	r.a.V(1).M(host).F().Info("Going to wait for ConfigMap propagation for: %s [elapsed/timeout: %s/%s]", wait, elapsed, timeout)
	if util.WaitContextDoneOrTimeout(ctx, wait) {
		log.V(2).Info("task is done")
		return true
	}

	r.a.V(1).M(host).F().Info("Wait completed for: %s  of timeout: %s]", wait, timeout)
	return false
}

// createStatefulSet is an internal function, used in reconcileStatefulSet only
func (r *Reconciler) doCreateStatefulSet(ctx context.Context, host *api.Host, opts *ReconcileOptions) common.ErrorCRUD {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).M(host).F().P()
	statefulSet := host.Runtime.DesiredStatefulSet

	log.V(1).Info("Create StatefulSet %s", util.NamespaceNameString(statefulSet))
	if _, err := r.sts.Create(ctx, statefulSet); err != nil {
		log.V(1).M(host).F().Error("StatefulSet create failed. err: %v", err)
		return common.ErrCRUDRecreate
	}

	if opts.IsDoNotWait() {
		// StatefulSet created, do not wait until host is ready, go by
		log.V(1).M(host).F().Info("Will NOT wait for StatefulSet to be ready, consider it is created successfully")
	} else {
		// StatefulSet created, wait until host is ready
		if err := r.hostSTSPoller.WaitHostStatefulSetReady(ctx, host); err != nil {
			log.V(1).M(host).F().Error("StatefulSet create wait failed. err: %v", err)
			return r.fallback.OnStatefulSetCreateFailed(ctx, host)
		}
		log.V(2).M(host).F().Info("Target generation reached, StatefulSet created successfully")
	}

	return nil
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (r *Reconciler) doUpdateStatefulSet(
	ctx context.Context,
	oldStatefulSet *apps.StatefulSet,
	newStatefulSet *apps.StatefulSet,
	host *api.Host,
) common.ErrorCRUD {
	log.V(2).M(host).F().P()

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Apply newStatefulSet and wait for Generation to change
	updatedStatefulSet, err := r.sts.Update(ctx, newStatefulSet)
	if err != nil {
		log.V(1).M(host).F().Error("StatefulSet update failed. err: %v", err)
		log.V(1).M(host).F().Error("%s", dumpDiff(oldStatefulSet, newStatefulSet))
		return common.ErrCRUDRecreate
	}

	// After calling "Update()"
	// 1. ObjectMeta.Generation is target generation
	// 2. Status.ObservedGeneration may be <= ObjectMeta.Generation

	if updatedStatefulSet.Generation == oldStatefulSet.Generation {
		// Generation is not updated - no changes in .spec section were made
		log.V(2).M(host).F().Info("no generation change")
		return nil
	}

	log.V(1).M(host).F().Info("generation change %d=>%d", oldStatefulSet.Generation, updatedStatefulSet.Generation)

	if err := r.hostSTSPoller.WaitHostStatefulSetReady(ctx, host); err != nil {
		log.V(1).M(host).F().Error("StatefulSet update wait failed. err: %v", err)
		return r.fallback.OnStatefulSetUpdateFailed(ctx, oldStatefulSet, host, r.sts)
	}

	log.V(2).M(host).F().Info("Target generation reached, StatefulSet updated successfully")
	return nil
}

// deleteStatefulSet gracefully deletes StatefulSet through zeroing Pod's count
func (r *Reconciler) doDeleteStatefulSet(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// IMPORTANT
	// StatefulSets do not provide any guarantees on the termination of pods when a StatefulSet is deleted.
	// To achieve ordered and graceful termination of the pods in the StatefulSet,
	// it is possible to scale the StatefulSet down to 0 prior to deletion.

	name := r.namer.Name(interfaces.NameStatefulSet, host)
	namespace := host.Runtime.Address.Namespace
	log.V(1).M(host).F().Info("%s/%s", namespace, name)

	var err error
	host.Runtime.CurStatefulSet, err = r.sts.Get(ctx, host)
	if err != nil {
		// Unable to fetch cur StatefulSet, but this is not necessarily an error yet
		if apiErrors.IsNotFound(err) {
			log.V(1).M(host).Info("NEUTRAL not found StatefulSet %s/%s", namespace, name)
		} else {
			log.V(1).M(host).F().Error("FAIL get StatefulSet %s/%s err:%v", namespace, name, err)
		}
		return err
	}

	// Scale StatefulSet down to 0 pods count.
	// This is the proper and graceful way to delete StatefulSet
	var zero int32 = 0
	host.Runtime.CurStatefulSet.Spec.Replicas = &zero
	if _, err := r.sts.Update(ctx, host.Runtime.CurStatefulSet); err != nil {
		log.V(1).M(host).Error("UNABLE to update StatefulSet %s/%s", namespace, name)
		return err
	}

	// Wait until StatefulSet scales down to 0 pods count.
	_ = r.hostSTSPoller.WaitHostStatefulSetReady(ctx, host)

	// And now delete empty StatefulSet
	if err := r.sts.Delete(ctx, namespace, name); err == nil {
		log.V(1).M(host).Info("OK delete StatefulSet %s/%s", namespace, name)
		//		r.hostSTSPoller.WaitHostStatefulSetDeleted(host)
	} else if apiErrors.IsNotFound(err) {
		log.V(1).M(host).Info("NEUTRAL not found StatefulSet %s/%s", namespace, name)
	} else {
		log.V(1).M(host).F().Error("FAIL delete StatefulSet %s/%s err: %v", namespace, name, err)
	}

	return nil
}
