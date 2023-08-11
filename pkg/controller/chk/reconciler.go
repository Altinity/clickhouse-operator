package chk

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimachinery "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ReconcileTime is the delay between reconciliations
const ReconcileTime = 30 * time.Second

var log = logf.Log.WithName("ctrl_chk")

// ChkReconciler reconciles a ClickHouseKeeper object
type ChkReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *apimachinery.Scheme
}

type reconcileFunc func(cluster *v1alpha1.ClickHouseKeeper) error

func (r *ChkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log = log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)

	// Fetch the ClickHouseKeeper instance
	instance := &v1alpha1.ClickHouseKeeper{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile
			// request. Owned objects are automatically garbage collected. For
			// additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	saved_chksum := instance.Annotations["saved_chksum"]
	chksum, err := getCheckSum(instance)
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
			if err := f(instance); err != nil {
				r.Log.Error(err, "Wrong when reconciling "+getFunctionName(f))
				return reconcile.Result{}, err
			}
		}

		tmp := &v1alpha1.ClickHouseKeeper{}
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
		tmp.Annotations = instance.Annotations
		tmp.Annotations["saved_chksum"] = chksum
		r.Update(ctx, tmp)
	}

	if err := r.reconcileClusterStatus(instance); err != nil {
		r.Log.Error(err, "Wrong when reconciling "+getFunctionName(r.reconcileClusterStatus))
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *ChkReconciler) reconcileConfigMap(instance *v1alpha1.ClickHouseKeeper) (err error) {
	cm := createConfigMap(instance)
	if err = controllerutil.SetControllerReference(instance, cm, r.Scheme); err != nil {
		return err
	}
	foundCm := &corev1.ConfigMap{}
	err = r.Get(context.TODO(), types.NamespacedName{
		Name:      cm.Name,
		Namespace: cm.Namespace,
	}, foundCm)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating a new Config Map")

		if err = r.Create(context.TODO(), cm); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		r.Log.Info("Updating existing config-map")

		foundCm.Data = cm.Data
		foundCm.BinaryData = cm.BinaryData
		if err = r.Update(context.TODO(), foundCm); err != nil {
			return err
		}
	}
	return nil
}

func (r *ChkReconciler) reconcileStatefulSet(instance *v1alpha1.ClickHouseKeeper) (err error) {
	sts := createStatefulSet(instance)
	if err = controllerutil.SetControllerReference(instance, sts, r.Scheme); err != nil {
		return err
	}
	foundSts := &appsv1.StatefulSet{}
	err = r.Get(context.TODO(), types.NamespacedName{
		Name:      sts.Name,
		Namespace: sts.Namespace,
	}, foundSts)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating a new StatefulSet")

		if err = r.Create(context.TODO(), sts); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if isReplicasChanged(instance) {
			updateLastReplicas(instance)
			lastApplied := instance.Annotations["kubectl.kubernetes.io/last-applied-configuration"]
			r.Log.Info("kubectl.kubernetes.io/last-applied-configuration: " + lastApplied)
		}
		restartPods(sts)
		if err = r.updateStatefulSet(instance, foundSts, sts); err != nil {
			return err
		}
	}
	return nil
}

func (r *ChkReconciler) updateStatefulSet(instance *v1alpha1.ClickHouseKeeper, foundSts *appsv1.StatefulSet, sts *appsv1.StatefulSet) (err error) {
	r.Log.Info("Updating existing StatefulSet")

	foundSts.Spec.Replicas = sts.Spec.Replicas
	foundSts.Spec.Template = sts.Spec.Template
	foundSts.Spec.UpdateStrategy = sts.Spec.UpdateStrategy

	if err = r.Update(context.TODO(), foundSts); err != nil {
		return err
	}
	return nil
}

func (r *ChkReconciler) reconcileClientService(instance *v1alpha1.ClickHouseKeeper) (err error) {
	svc := createClientService(instance)
	if err = controllerutil.SetControllerReference(instance, svc, r.Scheme); err != nil {
		return err
	}
	foundSvc := &corev1.Service{}
	err = r.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating new client service")

		if err = r.Create(context.TODO(), svc); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		r.Log.Info("Updating existing client service")

		foundSvc.Spec.Ports = svc.Spec.Ports
		foundSvc.Spec.Type = svc.Spec.Type
		foundSvc.SetAnnotations(svc.GetAnnotations())

		if err = r.Update(context.TODO(), foundSvc); err != nil {
			return err
		}
	}
	return nil
}

func (r *ChkReconciler) reconcileHeadlessService(instance *v1alpha1.ClickHouseKeeper) (err error) {
	svc := createHeadlessService(instance)
	if err = controllerutil.SetControllerReference(instance, svc, r.Scheme); err != nil {
		return err
	}
	foundSvc := &corev1.Service{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating new headless service")

		if err = r.Client.Create(context.TODO(), svc); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		r.Log.Info("Updating existing headless service")

		foundSvc.Spec.Ports = svc.Spec.Ports
		foundSvc.Spec.Type = svc.Spec.Type
		foundSvc.SetAnnotations(svc.GetAnnotations())

		if err = r.Client.Update(context.TODO(), foundSvc); err != nil {
			return err
		}
	}
	return nil
}

func (r *ChkReconciler) reconcilePodDisruptionBudget(instance *v1alpha1.ClickHouseKeeper) (err error) {
	pdb := createPodDisruptionBudget(instance)
	if err = controllerutil.SetControllerReference(instance, pdb, r.Scheme); err != nil {
		return err
	}
	foundPdb := &policyv1.PodDisruptionBudget{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{
		Name:      pdb.Name,
		Namespace: pdb.Namespace,
	}, foundPdb)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating new pod-disruption-budget")
		if err = r.Client.Create(context.TODO(), pdb); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

func (r *ChkReconciler) reconcileClusterStatus(instance *v1alpha1.ClickHouseKeeper) (err error) {
	readyMembers, err := r.getReadyMembers(instance)
	if err != nil {
		return err
	}

	// Refetch the latest ClickHouseKeeper instance
	tmp := &v1alpha1.ClickHouseKeeper{}
	namespacedName := types.NamespacedName{
		Name:      instance.Name,
		Namespace: instance.Namespace,
	}
	for {
		if err := r.Get(context.TODO(), namespacedName, tmp); err != nil {
			r.Log.Error(err, instance.Name+" not found")
			return err
		}

		if tmp.Status == nil {
			tmp.Status = &v1alpha1.ChkStatus{
				Status: "In progress",
			}
		}
		tmp.Status.Replicas = instance.Spec.GetReplicas()

		tmp.Status.ReadyReplicas = []v1.ChiZookeeperNode{}
		for _, readyOne := range readyMembers {
			tmp.Status.ReadyReplicas = append(tmp.Status.ReadyReplicas, v1.ChiZookeeperNode{
				Host:   fmt.Sprintf("%s.%s.svc.cluster.local", readyOne, instance.Namespace),
				Port:   int32(instance.Spec.GetClientPort()),
				Secure: v1.NewStringBool(false),
			})
		}

		r.Log.Info("ReadyReplicas: " + fmt.Sprintf("%v", tmp.Status.ReadyReplicas))

		if len(readyMembers) == int(instance.Spec.GetReplicas()) {
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
