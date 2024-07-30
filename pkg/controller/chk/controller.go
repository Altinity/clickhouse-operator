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

	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlUtil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/volume"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Controller reconciles a ClickHouseKeeper object
type Controller struct {
	client.Client
	Scheme *apiMachinery.Scheme

	namer interfaces.INameManager
	kube  interfaces.IKube
	//labeler    *Labeler
	pvcDeleter *volume.PVCDeleter
}

type reconcileFunc func(cluster *apiChk.ClickHouseKeeperInstallation) error

func (c *Controller) new() {
	c.namer = managers.NewNameManager(managers.NameManagerTypeKeeper)
	c.kube = kube.NewKeeper(c.Client, c.namer)
	//labeler:                 NewLabeler(kube),
	//pvcDeleter :=              volume.NewPVCDeleter(managers.NewNameManager(managers.NameManagerTypeKeeper))

}

func (c *Controller) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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

	w.reconcileCHK(context.TODO(), nil, new)

	//// Fetch the ClickHouseKeeper instance
	//dummy := &apiChk.ClickHouseKeeperInstallation{}
	//if err := c.Client.Get(ctx, req.NamespacedName, dummy); err != nil {
	//	if apiErrors.IsNotFound(err) {
	//		// Request object not found, could have been deleted after reconcile request.
	//		// Owned objects are automatically garbage collected.
	//		// For additional cleanup logic use finalizers.
	//		// Return and don't requeue
	//		return ctrl.Result{}, nil
	//	}
	//	// Return and requeue
	//	return ctrl.Result{}, err
	//}

	// Move to worker?!
	if err := c.reconcileClusterStatus(new); err != nil {
		log.V(1).Error("Error during reconcile status. f: %s err: %s", getFunctionName(c.reconcileClusterStatus), err)
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (c *Controller) reconcile(
	owner meta.Object,
	cur client.Object,
	new client.Object,
	name string,
	updater func(cur, new client.Object) error,
) (err error) {
	// TODO unify approach with CHI - set OWNER REFERENCE
	if err = ctrlUtil.SetControllerReference(owner, new, c.Scheme); err != nil {
		return err
	}

	err = c.Client.Get(context.TODO(), getNamespacedName(new), cur)
	if err != nil && apiErrors.IsNotFound(err) {
		log.V(1).Info("Creating new " + name)

		if err = c.Client.Create(context.TODO(), new); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if updater == nil {
			log.V(1).Info("Updater not provided")
		} else {
			log.V(1).Info("Updating existing " + name)
			if err = updater(cur, new); err != nil {
				return err
			}
			if err = c.Client.Update(context.TODO(), cur); err != nil {
				return err
			}
		}
	}
	return nil
}
