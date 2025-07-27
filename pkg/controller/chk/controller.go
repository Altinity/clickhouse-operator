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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Controller reconciles a ClickHouseKeeper object
type Controller struct {
	client.Client
	Scheme *apiMachinery.Scheme

	namer interfaces.INameManager
	kube  interfaces.IKube
	//labeler    *Labeler
	//pvcDeleter *volume.PVCDeleter
}

func (c *Controller) new() {
	c.namer = managers.NewNameManager(managers.NameManagerTypeKeeper)
	c.kube = kube.NewAdapter(c.Client, c.namer)
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

	if new.Spec.Suspend.Value() {
		log.V(2).M(new).F().Info("CR is suspended, skip reconcile")
		return ctrl.Result{}, nil
	}

	w.reconcileCR(context.TODO(), nil, new)

	return ctrl.Result{}, nil
}

func (c *Controller) poll(ctx context.Context, cr api.ICustomResource, f func(c *apiChk.ClickHouseKeeperInstallation, e error) bool) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Poll is aborted. cr: %s ", cr.GetName())
		return
	}

	namespace, name := util.NamespaceName(cr)

	for {
		cur, err := c.kube.CR().Get(ctx, namespace, name)
		if f(cur.(*apiChk.ClickHouseKeeperInstallation), err) {
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
