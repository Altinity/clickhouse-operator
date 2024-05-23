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
	"github.com/altinity/clickhouse-operator/pkg/controller/common"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// prepareHostStatefulSetWithStatus prepares host's StatefulSet status
func (w *worker) prepareHostStatefulSetWithStatus(ctx context.Context, host *api.Host, shutdown bool) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.prepareDesiredStatefulSet(host, shutdown)
	host.GetReconcileAttributes().SetStatus(w.getStatefulSetStatus(host))
}

// getStatefulSetStatus gets StatefulSet status
func (w *worker) getStatefulSetStatus(host *api.Host) api.ObjectStatus {
	meta := host.Runtime.DesiredStatefulSet.GetObjectMeta()
	w.a.V(2).M(meta).S().Info(util.NamespaceNameString(meta))
	defer w.a.V(2).M(meta).E().Info(util.NamespaceNameString(meta))

	curStatefulSet, err := w.c.getStatefulSet(meta)
	switch {
	case curStatefulSet != nil:
		w.a.V(2).M(meta).Info("Have StatefulSet available, try to perform label-based comparison for %s/%s", meta.GetNamespace(), meta.GetName())
		return common.GetObjectStatusFromMetas(curStatefulSet.GetObjectMeta(), meta)

	case apiErrors.IsNotFound(err):
		// StatefulSet is not found at the moment.
		// However, it may be just deleted
		w.a.V(2).M(meta).Info("No cur StatefulSet available and it is not found. Either new one or deleted for %s/%s", meta.GetNamespace(), meta.GetName())
		if host.IsNewOne() {
			w.a.V(2).M(meta).Info("No cur StatefulSet available and it is not found and is a new one. New one for %s/%s", meta.GetNamespace(), meta.GetName())
			return api.ObjectStatusNew
		}
		w.a.V(1).M(meta).Warning("No cur StatefulSet available but host has an ancestor. Found deleted StatefulSet. for %s/%s", meta.GetNamespace(), meta.GetName())
		return api.ObjectStatusModified

	default:
		w.a.V(2).M(meta).Warning("Have no StatefulSet available, nor it is not found for %s/%s err: %v", meta.GetNamespace(), meta.GetName(), err)
		return api.ObjectStatusUnknown
	}
}

// prepareDesiredStatefulSet prepares desired StatefulSet
func (w *worker) prepareDesiredStatefulSet(host *api.Host, shutdown bool) {
	host.Runtime.DesiredStatefulSet = w.task.Creator.CreateStatefulSet(host, shutdown)
}

// reconcileStatefulSet reconciles StatefulSet of a host
func (w *worker) reconcileStatefulSet(
	ctx context.Context,
	host *api.Host,
	register bool,
	opts ...*reconcileHostStatefulSetOptions,
) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	newStatefulSet := host.Runtime.DesiredStatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(newStatefulSet.GetObjectMeta()))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(newStatefulSet.GetObjectMeta()))

	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusSame {
		w.a.V(2).M(host).F().Info("No need to reconcile THE SAME StatefulSet: %s", util.NamespaceNameString(newStatefulSet.GetObjectMeta()))
		if register {
			host.GetCR().EnsureStatus().HostUnchanged()
			_ = w.c.updateCHIObjectStatus(ctx, host.GetCR(), UpdateCHIStatusOptions{
				CopyCHIStatusOptions: api.CopyCHIStatusOptions{
					MainFields: true,
				},
			})
		}
		return nil
	}

	// Check whether this object already exists in k8s
	host.Runtime.CurStatefulSet, err = w.c.getStatefulSet(newStatefulSet.GetObjectMeta())

	// Report diff to trace
	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusModified {
		w.a.V(1).M(host).F().Info("Need to reconcile MODIFIED StatefulSet: %s", util.NamespaceNameString(newStatefulSet.GetObjectMeta()))
		w.dumpStatefulSetDiff(host, host.Runtime.CurStatefulSet, newStatefulSet)
	}

	opt := NewReconcileHostStatefulSetOptionsArr(opts...).First()
	switch {
	case opt.ForceRecreate():
		// Force recreate prevails over all other requests
		w.recreateStatefulSet(ctx, host, register)
	default:
		// We have (or had in the past) StatefulSet - try to update|recreate it
		err = w.updateStatefulSet(ctx, host, register)
	}

	if apiErrors.IsNotFound(err) {
		// StatefulSet not found - even during Update process - try to create it
		err = w.createStatefulSet(ctx, host, register)
	}

	// Host has to know current StatefulSet and Pod
	host.Runtime.CurStatefulSet, _ = w.c.getStatefulSet(newStatefulSet.GetObjectMeta())

	return err
}

// recreateStatefulSet
func (w *worker) recreateStatefulSet(ctx context.Context, host *api.Host, register bool) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.deleteStatefulSet(ctx, host)
	_ = common.NewStorageReconciler(w.task, w.c.namer, NewKubePVCClickHouse(w.c.kubeClient)).ReconcilePVCs(ctx, host, api.DesiredStatefulSet)
	return w.createStatefulSet(ctx, host, register)
}

// updateStatefulSet
func (w *worker) updateStatefulSet(ctx context.Context, host *api.Host, register bool) error {
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
		WithEvent(host.GetCR(), eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.GetCR()).
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
			host.GetCR().EnsureStatus().HostUpdated()
			_ = w.c.updateCHIObjectStatus(ctx, host.GetCR(), UpdateCHIStatusOptions{
				CopyCHIStatusOptions: api.CopyCHIStatusOptions{
					MainFields: true,
				},
			})
		}
		w.a.V(1).
			WithEvent(host.GetCR(), eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(host.GetCR()).
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
		w.a.WithEvent(host.GetCR(), eventActionUpdate, eventReasonUpdateInProgress).
			WithStatusAction(host.GetCR()).
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

// createStatefulSet
func (w *worker) createStatefulSet(ctx context.Context, host *api.Host, register bool) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	statefulSet := host.Runtime.DesiredStatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(statefulSet.GetObjectMeta()))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(statefulSet.GetObjectMeta()))

	w.a.V(1).
		WithEvent(host.GetCR(), eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.GetCR()).
		M(host).F().
		Info("Create StatefulSet %s/%s - started", statefulSet.Namespace, statefulSet.Name)

	action := w.c.createStatefulSet(ctx, host)

	if register {
		host.GetCR().EnsureStatus().HostAdded()
		_ = w.c.updateCHIObjectStatus(ctx, host.GetCR(), UpdateCHIStatusOptions{
			CopyCHIStatusOptions: api.CopyCHIStatusOptions{
				MainFields: true,
			},
		})
	}

	switch action {
	case nil:
		w.a.V(1).
			WithEvent(host.GetCR(), eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.GetCR()).
			M(host).F().
			Info("Create StatefulSet %s/%s - completed", statefulSet.Namespace, statefulSet.Name)
		return nil
	case errCRUDAbort:
		w.a.WithEvent(host.GetCR(), eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.GetCR()).
			WithStatusError(host.GetCR()).
			M(host).F().
			Error("Create StatefulSet %s/%s - failed with error %v", statefulSet.Namespace, statefulSet.Name, action)
		return action
	case errCRUDIgnore:
		w.a.WithEvent(host.GetCR(), eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.GetCR()).
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
