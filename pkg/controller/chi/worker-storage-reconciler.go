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

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/volume"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Comment out PV
// reconcilePersistentVolumes reconciles all PVs of a host
//func (w *worker) reconcilePersistentVolumes(ctx context.Context, host *api.Host) {
//	if util.IsContextDone(ctx) {
//		return
//	}
//
//	w.c.walkPVs(host, func(pv *core.PersistentVolume) {
//		pv = w.task.creator.PreparePersistentVolume(pv, host)
//		_, _ = w.c.updatePersistentVolume(ctx, pv)
//	})
//}

// reconcilePVCs reconciles all PVCs of a host
func (w *worker) reconcilePVCs(ctx context.Context, host *api.Host, which api.WhichStatefulSet) (res ErrorDataPersistence) {
	if util.IsContextDone(ctx) {
		return nil
	}

	namespace := host.Runtime.Address.Namespace
	w.a.V(2).M(host).S().Info("host %s/%s", namespace, host.GetName())
	defer w.a.V(2).M(host).E().Info("host %s/%s", namespace, host.GetName())

	host.WalkVolumeMounts(which, func(volumeMount *core.VolumeMount) {
		if util.IsContextDone(ctx) {
			return
		}
		if e := w.reconcilePVCFromVolumeMount(ctx, host, volumeMount); e != nil {
			if res == nil {
				res = e
			}
		}
	})

	return
}

func (w *worker) reconcilePVCFromVolumeMount(
	ctx context.Context,
	host *api.Host,
	volumeMount *core.VolumeMount,
) (
	res ErrorDataPersistence,
) {
	// Which PVC are we going to reconcile
	pvc, volumeClaimTemplate, isModelCreated, err := w.fetchPVC(ctx, host, volumeMount)
	if err != nil {
		// Unable to fetch or model PVC correctly.
		// May be volume is not built from VolumeClaimTemplate, it may be reference to ConfigMap
		return nil
	}

	// PVC available. Either fetched or not found and model created (from templates)

	pvcName := "pvc-name-unknown-pvc-not-exist"
	namespace := host.Runtime.Address.Namespace

	if pvc != nil {
		pvcName = pvc.Name
	}

	w.a.V(2).M(host).S().Info("reconcile volumeMount (%s/%s/%s/%s)", namespace, host.GetName(), volumeMount.Name, pvcName)
	defer w.a.V(2).M(host).E().Info("reconcile volumeMount (%s/%s/%s/%s)", namespace, host.GetName(), volumeMount.Name, pvcName)

	// Check scenario 1 - no PVC available
	// Such a PVC should be re-created
	if w.isLostPVC(pvc, isModelCreated, host) {
		// Looks like data loss detected
		w.a.V(1).M(host).Warning("PVC is either newly added to the host or was lost earlier (%s/%s/%s/%s)", namespace, host.GetName(), volumeMount.Name, pvcName)
		res = errPVCIsLost
	}

	// Check scenario 2 - PVC exists, but no PV available
	// Such a PVC should be deleted and re-created
	if w.isLostPV(pvc) {
		// This PVC has no PV available
		// Looks like data loss detected
		w.deletePVC(ctx, pvc)
		w.a.V(1).M(host).Info("deleted PVC with lost PV (%s/%s/%s/%s)", namespace, host.GetName(), volumeMount.Name, pvcName)

		// Refresh PVC model. Since PVC is just deleted refreshed model may not be fetched from the k8s,
		// but can be provided by the operator still
		pvc, volumeClaimTemplate, _, _ = w.fetchPVC(ctx, host, volumeMount)
		res = errPVCWithLostPVDeleted
	}

	// In any case - be PVC available or not - need to reconcile it

	switch pvcReconciled, err := w.reconcilePVC(ctx, pvc, host, volumeClaimTemplate); err {
	case errNilPVC:
		w.a.M(host).F().Error("Unable to reconcile nil PVC: %s/%s", namespace, pvcName)
	case nil:
		w.task.registryReconciled.RegisterPVC(pvcReconciled.GetObjectMeta())
	default:
		w.task.registryFailed.RegisterPVC(pvc.GetObjectMeta())
		w.a.M(host).F().Error("Unable to reconcile PVC: %s/%s err: %v", pvc.Namespace, pvc.Name, err)
	}

	// It still may return data loss errors
	return res
}

func (w *worker) isLostPVC(pvc *core.PersistentVolumeClaim, isJustCreated bool, host *api.Host) bool {
	if !host.HasData() {
		// No data to loose
		return false
	}

	// Now we assume that this PVC has had some data in the past, since tables were created on it

	if pvc == nil {
		// No PVC available at all, was it deleted?
		// Lost PVC
		return true
	}

	if isJustCreated {
		// PVC was just created by the operator, not fetched
		// Lost PVC
		return true
	}

	// PVC is in place
	return false
}

func (w *worker) isLostPV(pvc *core.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}

	return pvc.Status.Phase == core.ClaimLost
}

func (w *worker) fetchPVC(
	ctx context.Context,
	host *api.Host,
	volumeMount *core.VolumeMount,
) (
	pvc *core.PersistentVolumeClaim,
	vct *api.VolumeClaimTemplate,
	isModelCreated bool,
	err error,
) {
	namespace := host.Runtime.Address.Namespace

	volumeClaimTemplate, ok := volume.GetVolumeClaimTemplate(host, volumeMount)
	if !ok {
		// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
		return nil, nil, false, fmt.Errorf("unable to find VolumeClaimTemplate from volume mount")
	}
	pvcName := w.c.namer.Name(interfaces.NamePVCNameByVolumeClaimTemplate, host, volumeClaimTemplate)

	// We have a VolumeClaimTemplate for this VolumeMount
	// Treat it as persistent storage mount

	_pvc, e := w.c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, controller.NewGetOptions())
	if e == nil {
		w.a.V(2).M(host).Info("PVC (%s/%s/%s/%s) found", namespace, host.GetName(), volumeMount.Name, pvcName)
		return _pvc, volumeClaimTemplate, false, nil
	}

	// We have an error. PVC not fetched

	if !apiErrors.IsNotFound(e) {
		// In case of any non-NotFound API error - unable to proceed
		w.a.M(host).F().Error("ERROR unable to get PVC(%s/%s) err: %v", namespace, pvcName, e)
		return nil, nil, false, e
	}

	// We have NotFound error - PVC not found
	// This is not an error per se, means PVC is not created (yet)?
	w.a.V(2).M(host).Info("PVC (%s/%s/%s/%s) not found", namespace, host.GetName(), volumeMount.Name, pvcName)

	if volume.OperatorShouldCreatePVC(host, volumeClaimTemplate) {
		// Operator is in charge of PVCs
		// Create PVC model.
		pvc = w.task.creator.CreatePVC(pvcName, namespace, host, &volumeClaimTemplate.Spec)
		w.a.V(1).M(host).Info("PVC (%s/%s/%s/%s) model provided by the operator", namespace, host.GetName(), volumeMount.Name, pvcName)
		return pvc, volumeClaimTemplate, true, nil
	}

	// PVC is not available and the operator is not expected to create PVC
	w.a.V(1).M(host).Info("PVC (%s/%s/%s/%s) not found and model will not be provided by the operator", namespace, host.GetName(), volumeMount.Name, pvcName)
	return nil, volumeClaimTemplate, false, nil
}

var errNilPVC = fmt.Errorf("nil PVC, nothing to reconcile")

// reconcilePVC reconciles specified PVC
func (w *worker) reconcilePVC(
	ctx context.Context,
	pvc *core.PersistentVolumeClaim,
	host *api.Host,
	template *api.VolumeClaimTemplate,
) (*core.PersistentVolumeClaim, error) {
	if pvc == nil {
		w.a.V(2).M(host).F().Info("nil PVC, nothing to reconcile")
		return nil, errNilPVC
	}

	w.a.V(2).M(host).S().Info("reconcile PVC (%s/%s/%s)", pvc.Namespace, pvc.Name, host.GetName())
	defer w.a.V(2).M(host).E().Info("reconcile PVC (%s/%s/%s)", pvc.Namespace, pvc.Name, host.GetName())

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil, fmt.Errorf("task is done")
	}

	model.VolumeClaimTemplateApplyResourcesRequestsOnPVC(template, pvc)
	pvc = w.task.creator.AdjustPVC(pvc, host, template)
	return w.c.updatePersistentVolumeClaim(ctx, pvc)
}

func (w *worker) deletePVC(ctx context.Context, pvc *core.PersistentVolumeClaim) bool {
	w.a.V(1).M(pvc).F().S().Info("delete PVC with lost PV start: %s/%s", pvc.Namespace, pvc.Name)
	defer w.a.V(1).M(pvc).F().E().Info("delete PVC with lost PV end: %s/%s", pvc.Namespace, pvc.Name)

	w.a.V(2).M(pvc).F().Info("PVC with lost PV about to be deleted: %s/%s", pvc.Namespace, pvc.Name)
	w.c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, controller.NewDeleteOptions())

	for i := 0; i < 360; i++ {

		// Check availability
		w.a.V(2).M(pvc).F().Info("check PVC with lost PV availability: %s/%s", pvc.Namespace, pvc.Name)
		curPVC, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, controller.NewGetOptions())
		if err != nil {
			if apiErrors.IsNotFound(err) {
				// Not available - concider to bbe deleted
				w.a.V(1).M(pvc).F().Warning("PVC with lost PV was deleted: %s/%s", pvc.Namespace, pvc.Name)
				return true
			}
		}

		// PVC is not deleted (yet?). May be it has finalizers installed. Need to clean them.
		if len(curPVC.Finalizers) > 0 {
			w.a.V(2).M(pvc).F().Info("clean finalizers for PVC with lost PV: %s/%s", pvc.Namespace, pvc.Name)
			curPVC.Finalizers = nil
			w.c.updatePersistentVolumeClaim(ctx, curPVC)
		}
		time.Sleep(10 * time.Second)
	}

	return false
}
