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
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	chkNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Reconciler reconciles a ClickHouseKeeper object
type Reconciler struct {
	client.Client
	Scheme        *apiMachinery.Scheme
	task          *common.Task
	stsReconciler *statefulset.StatefulSetReconciler
}

type reconcileFunc func(cluster *apiChk.ClickHouseKeeperInstallation) error

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return ctrl.Result{}, nil
	}

	var old, new *apiChk.ClickHouseKeeperInstallation

	// Fetch the ClickHouseKeeper instance
	new = &apiChk.ClickHouseKeeperInstallation{}
	if err := r.Get(ctx, req.NamespacedName, new); err != nil {
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

	if new.HasAncestor() {
		log.V(2).M(new).F().Info("has ancestor, use it as a base for reconcile. CHK: %s/%s", new.Namespace, new.Name)
		old = new.GetAncestor()
	} else {
		log.V(2).M(new).F().Info("has NO ancestor, use empty CHK as a base for reconcile. CHK: %s/%s", new.Namespace, new.Name)
		old = nil
	}

	log.V(2).M(new).F().Info("Normalized OLD CHK: %s/%s", new.Namespace, new.Name)
	old = r.normalize(old)

	log.V(2).M(new).F().Info("Normalized NEW CHK %s/%s", new.Namespace, new.Name)
	new = r.normalize(new)
	new.SetAncestor(old)

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return ctrl.Result{}, nil
	}

	r.reconcileInit(new)
	if old.GetGeneration() != new.GetGeneration() {
		for _, f := range []reconcileFunc{
			r.reconcileConfigMap,
			r.reconcileStatefulSet,
			r.reconcileClientService,
			r.reconcileHeadlessService,
			r.reconcilePodDisruptionBudget,
		} {
			if err := f(new); err != nil {
				log.V(1).Error("Error during reconcile. f: %s err: %s", getFunctionName(f), err)
				return reconcile.Result{}, err
			}
		}
	}

	// Fetch the ClickHouseKeeper instance
	dummy := &apiChk.ClickHouseKeeperInstallation{}
	if err := r.Get(ctx, req.NamespacedName, dummy); err != nil {
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

	if err := r.reconcileClusterStatus(new); err != nil {
		log.V(1).Error("Error during reconcile status. f: %s err: %s", getFunctionName(r.reconcileClusterStatus), err)
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *Reconciler) reconcile(
	owner meta.Object,
	cur client.Object,
	new client.Object,
	name string,
	updater func(cur, new client.Object) error,
) (err error) {
	// TODO unify approach with CHI - set OWNER REFERENCE
	if err = ctrlUtil.SetControllerReference(owner, new, r.Scheme); err != nil {
		return err
	}

	err = r.Client.Get(context.TODO(), getNamespacedName(new), cur)
	if err != nil && apiErrors.IsNotFound(err) {
		log.V(1).Info("Creating new " + name)

		if err = r.Client.Create(context.TODO(), new); err != nil {
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
			if err = r.Client.Update(context.TODO(), cur); err != nil {
				return err
			}
		}
	}
	return nil
}

// normalize
func (r *Reconciler) normalize(c *apiChk.ClickHouseKeeperInstallation) *apiChk.ClickHouseKeeperInstallation {
	chk, err := chkNormalizer.New().CreateTemplated(c, normalizer.NewOptions())
	if err != nil {
		log.V(1).
			M(chk).F().
			Error("FAILED to normalize CHK: %v", err)
	}
	return chk
}

func configGeneratorOptions(chk *apiChk.ClickHouseKeeperInstallation) *chkConfig.GeneratorOptions {
	return &chkConfig.GeneratorOptions{
		RaftPort:      chk.GetSpec().GetRaftPort(),
		ReplicasCount: 1,
	}
}

func (r *Reconciler) reconcileInit(chk *apiChk.ClickHouseKeeperInstallation) {

	namer := managers.NewNameManager(managers.NameManagerTypeKeeper)
	kube := kube.NewKeeper(r.Client, namer)
	//pvcDeleter :=              volume.NewPVCDeleter(managers.NewNameManager(managers.NameManagerTypeKeeper))
	announcer := common.NewAnnouncer(nil, kube.CRStatus())

	r.task = common.NewTask(
		commonCreator.NewCreator(
			chk,
			managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeKeeper, chk, configGeneratorOptions(chk)),
			managers.NewContainerManager(managers.ContainerManagerTypeKeeper),
			managers.NewTagManager(managers.TagManagerTypeKeeper, chk),
			managers.NewProbeManager(managers.ProbeManagerTypeKeeper),
			managers.NewServiceManager(managers.ServiceManagerTypeKeeper),
			managers.NewVolumeManager(managers.VolumeManagerTypeKeeper),
			managers.NewConfigMapManager(managers.ConfigMapManagerTypeKeeper),
			managers.NewNameManager(managers.NameManagerTypeKeeper),
		),
	)

	r.stsReconciler = statefulset.NewStatefulSetReconciler(
		announcer,
		r.task,
		poller.NewHostStatefulSetPoller(poller.NewStatefulSetPoller(kube), kube, nil),
		namer,
		storage.NewStorageReconciler(r.task, namer, kube.Storage()),
		kube,
		statefulset.NewDefaultFallback(),
	)
}
