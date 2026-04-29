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

package chk

import (
	"context"
	"time"

	apiExtensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Controller reconciles a ClickHouseKeeper object
type Controller struct {
	client.Client
	// APIReader is a non-cached client.Reader for direct API-server reads.
	// It is used by the STS client to avoid stale-cache races in the forced-restart path.
	APIReader client.Reader
	Scheme    *apiMachinery.Scheme
	ExtClient apiExtensions.Interface

	namer interfaces.INameManager
	kube  interfaces.IKube
	//labeler    *Labeler
	//pvcDeleter *volume.PVCDeleter
}

func (c *Controller) new() {
	c.namer = managers.NewNameManager(managers.NameManagerTypeKeeper)
	c.kube = kube.NewAdapter(c.Client, c.APIReader, c.namer)
	//labeler:                 NewLabeler(kube),
	//pvcDeleter :=              volume.NewPVCDeleter(managers.NewNameManager(managers.NameManagerTypeKeeper))
}

func (c *Controller) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return ctrl.Result{}, nil
	}

	// Guard: skip namespaces outside the configured watch list.
	// keeperPredicate() filters direct CHK events, but reconcile requests triggered by owned
	// StatefulSet changes bypass it. This guard catches all paths including those.
	if !chop.Config().IsNamespaceWatched(req.Namespace) {
		log.V(2).Info("skip reconcile, namespace '%s' is not watched", req.Namespace)
		return ctrl.Result{}, nil
	}

	// Fetch the ClickHouseKeeper instance
	new := &apiChk.ClickHouseKeeperInstallation{}
	if err := c.Client.Get(ctx, req.NamespacedName, new); err != nil {
		if apiErrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		// Return and requeue
		return ctrl.Result{}, err
	}

	c.new()
	w := c.newWorker()

	if w.ensureFinalizer(ctx, new) {
		// Finalizer just installed; controller-runtime will re-reconcile
		return ctrl.Result{}, nil
	}

	if w.deleteCHK(ctx, new) {
		// CHK is being deleted
		return ctrl.Result{}, nil
	}

	if new.Spec.Suspend.Value() {
		log.V(2).M(new).F().Info("CR is suspended, skip reconcile")
		return ctrl.Result{}, nil
	}

	w.reconcileCR(context.TODO(), nil, new)

	return ctrl.Result{}, nil
}

// installFinalizer adds the operator finalizer to the CHK CR.
func (c *Controller) installFinalizer(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	cur := &apiChk.ClickHouseKeeperInstallation{}
	if err := c.Client.Get(ctx, kubeTypes.NamespacedName{Namespace: chk.Namespace, Name: chk.Name}, cur); err != nil {
		return err
	}
	if util.InArray(FinalizerName, cur.GetFinalizers()) {
		return nil
	}
	base := cur.DeepCopy()
	cur.SetFinalizers(append(cur.GetFinalizers(), FinalizerName))
	return c.Client.Patch(ctx, cur, client.MergeFrom(base))
}

// uninstallFinalizer removes the operator finalizer from the CHK CR, allowing k8s to delete it.
func (c *Controller) uninstallFinalizer(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	cur := &apiChk.ClickHouseKeeperInstallation{}
	if err := c.Client.Get(ctx, kubeTypes.NamespacedName{Namespace: chk.Namespace, Name: chk.Name}, cur); err != nil {
		return err
	}
	base := cur.DeepCopy()
	cur.SetFinalizers(util.RemoveFromArray(FinalizerName, cur.GetFinalizers()))
	return c.Client.Patch(ctx, cur, client.MergeFrom(base))
}

func (c *Controller) poll(ctx context.Context, cr api.ICustomResource, f func(c *apiChk.ClickHouseKeeperInstallation, e error) bool) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Poll is aborted. cr: %s ", cr.GetName())
		return
	}

	namespace, name := util.NamespaceName(cr)

	for {
		cur, err := c.kube.CR().Get(ctx, namespace, name)
		chk, ok := cur.(*apiChk.ClickHouseKeeperInstallation)
		if !ok || chk == nil {
			if apiErrors.IsNotFound(err) {
				return
			}
			log.V(1).Info("poll Get error for %s: %v", cr.GetName(), err)
			if util.IsContextDone(ctx) {
				log.V(1).Info("Poll is aborted. Cr: %s ", cr.GetName())
				return
			}
			time.Sleep(15 * time.Second)
			continue
		}
		if f(chk, err) {
			// Continue polling
			if util.IsContextDone(ctx) {
				log.V(1).Info("Poll is aborted. Cr: %s ", cr.GetName())
				return
			}
			time.Sleep(15 * time.Second)
		} else {
			// Stop polling
			return
		}
	}
}

func ShouldEnqueue(cr *apiChk.ClickHouseKeeperInstallation) bool {
	ns := cr.GetNamespace()
	if !chop.Config().IsNamespaceWatched(ns) {
		log.V(2).M(cr).Info("skip enqueue, namespace '%s' is not watched or is in deny list", ns)
		return false
	}

	return true
}
