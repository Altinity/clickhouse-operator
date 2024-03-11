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
	"fmt"
	"time"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlUtil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	//	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chk"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ReconcileTime is the delay between reconciliations
const ReconcileTime = 30 * time.Second

// ChkReconciler reconciles a ClickHouseKeeper object
type ChkReconciler struct {
	client.Client
	Scheme *apiMachinery.Scheme
}

type reconcileFunc func(cluster *apiChk.ClickHouseKeeperInstallation) error

func (r *ChkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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

func (r *ChkReconciler) reconcileConfigMap(chk *apiChk.ClickHouseKeeperInstallation) error {
	return r.reconcile(
		chk,
		&core.ConfigMap{},
		model.CreateConfigMap(chk),
		"ConfigMap",
		func(curObject, newObject client.Object) error {
			cur, ok1 := curObject.(*core.ConfigMap)
			new, ok2 := newObject.(*core.ConfigMap)
			if !ok1 || !ok2 {
				return fmt.Errorf("unable to cast")
			}
			cur.Data = new.Data
			cur.BinaryData = new.BinaryData
			return nil
		},
	)
}

func (r *ChkReconciler) reconcileStatefulSet(chk *apiChk.ClickHouseKeeperInstallation) error {
	return r.reconcile(
		chk,
		&apps.StatefulSet{},
		model.CreateStatefulSet(chk),
		"StatefulSet",
		func(curObject, newObject client.Object) error {
			cur, ok1 := curObject.(*apps.StatefulSet)
			new, ok2 := newObject.(*apps.StatefulSet)
			if !ok1 || !ok2 {
				return fmt.Errorf("unable to cast")
			}
			markPodRestartedNow(new)
			cur.Spec.Replicas = new.Spec.Replicas
			cur.Spec.Template = new.Spec.Template
			cur.Spec.UpdateStrategy = new.Spec.UpdateStrategy
			return nil
		},
	)
}

func (r *ChkReconciler) reconcileClientService(chk *apiChk.ClickHouseKeeperInstallation) error {
	return r.reconcile(
		chk,
		&core.Service{},
		model.CreateClientService(chk),
		"Client Service",
		func(curObject, newObject client.Object) error {
			cur, ok1 := curObject.(*core.Service)
			new, ok2 := newObject.(*core.Service)
			if !ok1 || !ok2 {
				return fmt.Errorf("unable to cast")
			}
			cur.Spec.Ports = new.Spec.Ports
			cur.Spec.Type = new.Spec.Type
			cur.SetAnnotations(new.GetAnnotations())
			return nil
		},
	)
}

func (r *ChkReconciler) reconcileHeadlessService(chk *apiChk.ClickHouseKeeperInstallation) error {
	return r.reconcile(
		chk,
		&core.Service{},
		model.CreateHeadlessService(chk),
		"Headless Service",
		func(curObject, newObject client.Object) error {
			cur, ok1 := curObject.(*core.Service)
			new, ok2 := newObject.(*core.Service)
			if !ok1 || !ok2 {
				return fmt.Errorf("unable to cast")
			}
			cur.Spec.Ports = new.Spec.Ports
			cur.Spec.Type = new.Spec.Type
			cur.SetAnnotations(new.GetAnnotations())
			return nil
		},
	)
}

func (r *ChkReconciler) reconcilePodDisruptionBudget(chk *apiChk.ClickHouseKeeperInstallation) error {
	return r.reconcile(
		chk,
		&policy.PodDisruptionBudget{},
		model.CreatePodDisruptionBudget(chk),
		"PodDisruptionBudget",
		nil,
	)
}

func (r *ChkReconciler) reconcile(
	chk *apiChk.ClickHouseKeeperInstallation,
	cur client.Object,
	new client.Object,
	name string,
	updater func(cur, new client.Object) error,
) (err error) {
	if err = ctrlUtil.SetControllerReference(chk, new, r.Scheme); err != nil {
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

func (r *ChkReconciler) reconcileClusterStatus(chk *apiChk.ClickHouseKeeperInstallation) (err error) {
	readyMembers, err := r.getReadyPods(chk)
	if err != nil {
		return err
	}

	for {
		// Fetch the latest ClickHouseKeeper instance again
		cur := &apiChk.ClickHouseKeeperInstallation{}
		if err := r.Get(context.TODO(), getNamespacedName(chk), cur); err != nil {
			log.V(1).Error("Error: not found %s err: %s", chk.Name, err)
			return err
		}

		if cur.GetStatus() == nil {
			cur.Status = cur.EnsureStatus()
		}
		cur.Status.Replicas = int32(model.GetReplicasCount(chk))

		cur.Status.ReadyReplicas = []apiChi.ChiZookeeperNode{}
		for _, readyOne := range readyMembers {
			cur.Status.ReadyReplicas = append(cur.Status.ReadyReplicas,
				apiChi.ChiZookeeperNode{
					Host:   fmt.Sprintf("%s.%s.svc.cluster.local", readyOne, chk.Namespace),
					Port:   int32(chk.Spec.GetClientPort()),
					Secure: apiChi.NewStringBool(false),
				})
		}

		log.V(2).Info("ReadyReplicas: " + fmt.Sprintf("%v", cur.Status.ReadyReplicas))

		if len(readyMembers) == model.GetReplicasCount(chk) {
			cur.Status.Status = "Completed"
		} else {
			cur.Status.Status = "In progress"
		}

		cur.Status.NormalizedCHK = nil
		cur.Status.NormalizedCHKCompleted = chk.DeepCopy()
		cur.Status.NormalizedCHKCompleted.ObjectMeta.ManagedFields = nil
		cur.Status.NormalizedCHKCompleted.Status = nil

		if err := r.Status().Update(context.TODO(), cur); err != nil {
			log.V(1).Error("err: %s", err.Error())
		} else {
			return nil
		}
	}
}

// normalize
func (r *ChkReconciler) normalize(c *apiChk.ClickHouseKeeperInstallation) *apiChk.ClickHouseKeeperInstallation {
	chk, err := model.NewNormalizer().CreateTemplatedCHK(c, model.NewNormalizerOptions())
	if err != nil {
		log.V(1).
			M(chk).F().
			Error("FAILED to normalize CHI 1: %v", err)
	}
	return chk
}
