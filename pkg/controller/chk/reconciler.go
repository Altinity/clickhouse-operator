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

	"github.com/go-logr/logr"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlUtil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chk"
)

// ReconcileTime is the delay between reconciliations
const ReconcileTime = 30 * time.Second

var log = logf.Log.WithName("ctrl_chk")

// ChkReconciler reconciles a ClickHouseKeeper object
type ChkReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *apiMachinery.Scheme
}

type reconcileFunc func(cluster *apiChk.ClickHouseKeeperInstallation) error

func (r *ChkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log = log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)

	// Fetch the ClickHouseKeeper instance
	chk := &apiChk.ClickHouseKeeperInstallation{}
	if err := r.Get(ctx, req.NamespacedName, chk); err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile
			// request. Owned objects are automatically garbage collected. For
			// additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	saved_chksum := chk.Annotations["saved_chksum"]
	chksum, err := getCheckSum(chk)
	if err != nil {
		return ctrl.Result{}, err
	}

	if saved_chksum != chksum {
		r.Log.Info("Reconciling ClickHouseKeeper")

		for _, f := range []reconcileFunc{
			r.reconcileConfigMap,
			r.reconcileStatefulSet,
			r.reconcileClientService,
			r.reconcileHeadlessService,
			r.reconcilePodDisruptionBudget,
		} {
			if err := f(chk); err != nil {
				r.Log.Error(err, "Wrong when reconciling "+getFunctionName(f))
				return reconcile.Result{}, err
			}
		}

		tmp := &apiChk.ClickHouseKeeperInstallation{}
		if err := r.Get(ctx, req.NamespacedName, tmp); err != nil {
			if errors.IsNotFound(err) {
				// Request object not found, could have been deleted after reconcile
				// request. Owned objects are automatically garbage collected. For
				// additional cleanup logic use finalizers.
				// Return and don't requeue
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}

		r.Log.Info("chksum: " + chksum)
		tmp.Annotations = chk.Annotations
		tmp.Annotations["saved_chksum"] = chksum
		r.Update(ctx, tmp)
	}

	if err := r.reconcileClusterStatus(chk); err != nil {
		r.Log.Error(err, "Wrong when reconciling "+getFunctionName(r.reconcileClusterStatus))
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
			if isReplicasChanged(chk) {
				setAnnotationLastAppliedConfiguration(chk)
				lastApplied := chk.Annotations["kubectl.kubernetes.io/last-applied-configuration"]
				r.Log.Info("kubectl.kubernetes.io/last-applied-configuration: " + lastApplied)
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
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating new " + name)

		if err = r.Client.Create(context.TODO(), new); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if updater == nil {
			r.Log.Info("Updater not provided")
		} else {
			r.Log.Info("Updating existing " + name)
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
		tmp := &apiChk.ClickHouseKeeperInstallation{}
		if err := r.Get(context.TODO(), getNamespacedName(chk), tmp); err != nil {
			r.Log.Error(err, chk.Name+" not found")
			return err
		}

		if tmp.Status == nil {
			tmp.Status = &apiChk.ChkStatus{
				Status: "In progress",
			}
		}
		tmp.Status.Replicas = chk.Spec.GetReplicas()

		tmp.Status.ReadyReplicas = []apiChi.ChiZookeeperNode{}
		for _, readyOne := range readyMembers {
			tmp.Status.ReadyReplicas = append(tmp.Status.ReadyReplicas,
				apiChi.ChiZookeeperNode{
					Host:   fmt.Sprintf("%s.%s.svc.cluster.local", readyOne, chk.Namespace),
					Port:   int32(chk.Spec.GetClientPort()),
					Secure: apiChi.NewStringBool(false),
				})
		}

		r.Log.Info("ReadyReplicas: " + fmt.Sprintf("%v", tmp.Status.ReadyReplicas))

		if len(readyMembers) == int(chk.Spec.GetReplicas()) {
			tmp.Status.Status = "Completed"
		} else {
			tmp.Status.Status = "In progress"
		}
		if err := r.Status().Update(context.TODO(), tmp); err != nil {
			r.Log.Info(err.Error())
		} else {
			return nil
		}
	}
}
