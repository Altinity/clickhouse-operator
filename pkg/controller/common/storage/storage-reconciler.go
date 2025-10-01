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

package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/common/volume"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ErrorDataPersistence specifies errors of the PVCs and PVs
type ErrorDataPersistence error

var (
	ErrPVCWithLostPVDeleted ErrorDataPersistence = errors.New("pvc with lost pv deleted")
	ErrPVCIsLost            ErrorDataPersistence = errors.New("pvc is lost")
	ErrPVCIsMissed          ErrorDataPersistence = errors.New("pvc is missed")
)

func ErrIsDataLoss(err error) bool {
	switch err {
	case ErrPVCWithLostPVDeleted:
		return true
	case ErrPVCIsLost:
		return true
	}
	return false
}

func ErrIsVolumeMissed(err error) bool {
	switch err {
	case ErrPVCIsMissed:
		return true
	}
	return false
}

type Reconciler struct {
	task  *common.Task
	namer interfaces.INameManager
	pvc   interfaces.IKubeStoragePVC
}

func NewStorageReconciler(task *common.Task, namer interfaces.INameManager, pvc interfaces.IKubeStoragePVC) *Reconciler {
	return &Reconciler{
		task:  task,
		namer: namer,
		pvc:   pvc,
	}
}

// ReconcilePVCs reconciles all PVCs of a host
func (w *Reconciler) ReconcilePVCs(ctx context.Context, host *api.Host, which api.WhichStatefulSet) (res ErrorDataPersistence) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile PVC is aborted. Host: %s ", host.GetName())
		return nil
	}

	namespace := host.Runtime.Address.Namespace
	log.V(2).M(host).S().Info("host %s/%s", namespace, host.GetName())
	defer log.V(2).M(host).E().Info("host %s/%s", namespace, host.GetName())

	host.WalkVolumeMounts(which, func(volumeMount *core.VolumeMount) {
		if util.IsContextDone(ctx) {
			log.V(1).Info("Reconcile PVC is aborted. Host: %s ", host.GetName())
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

func (w *Reconciler) reconcilePVCFromVolumeMount(
	ctx context.Context,
	host *api.Host,
	volumeMount *core.VolumeMount,
) (
	reconcileError ErrorDataPersistence,
) {
	// Try to find VolumeClaimTemplate that is referenced by provided VolumeMount
	volumeClaimTemplate, found := volume.GetVolumeClaimTemplate(host, volumeMount)
	if !found {
		// No this VolumeMount is not a reference to VolumeClaimTemplate,
		// it may be a reference to, for example, ConfigMap to be mounted into Pod
		// Nothing to do here, we are looking for VolumeClaimTemplate(s)
		return nil
	}

	// Who is in charge of the PVC creation - the operator or the StatefulSet?
	shouldCHOPCreatePVC := volume.OperatorShouldCreatePVC(host, volumeClaimTemplate)

	// Name of the PVC to be used by a VolumeClaimTemplate within a Host
	pvcNamespace := host.Runtime.Address.Namespace
	pvcName := w.namer.Name(interfaces.NamePVCNameByVolumeClaimTemplate, host, volumeClaimTemplate)

	// Which PVC are we going to reconcile
	pvc, chopCreated, err := w.fetchOrCreatePVC(ctx, host, pvcNamespace, pvcName, volumeMount.Name, shouldCHOPCreatePVC, &volumeClaimTemplate.Spec)
	if err != nil {
		// Unable to fetch or create PVC correctly.
		return nil
	}

	// Beware PVC may be nil here still - in case it is managed by sts and not fetched

	log.V(2).M(host).S().Info("reconcile volumeMount (%s/%s/%s/%s)", pvcNamespace, host.GetName(), volumeMount.Name, pvcName)
	defer log.V(2).M(host).E().Info("reconcile volumeMount (%s/%s/%s/%s)", pvcNamespace, host.GetName(), volumeMount.Name, pvcName)

	// Check scenario 1 - no PVC available
	// Such a PVC should be re-created
	if w.isLostPVC(pvc, chopCreated, host) {
		// Looks like data loss detected
		log.V(1).M(host).Warning("PVC is either newly added to the host or was lost earlier (%s/%s/%s/%s)", pvcNamespace, host.GetName(), volumeMount.Name, pvcName)
		reconcileError = ErrPVCIsLost
	}

	// Check scenario 2 - PVC exists, but no PV available
	// Such a PVC should be deleted and re-created
	if w.isLostPV(pvc) {
		// This PVC has no PV available
		// Looks like data loss detected
		w.deletePVC(ctx, pvc)
		log.V(1).M(host).Info("deleted PVC with lost PV (%s/%s/%s/%s)", pvcNamespace, host.GetName(), volumeMount.Name, pvcName)

		// Refresh PVC model. Since PVC is just deleted refreshed model may not be fetched from the k8s,
		// but can be provided by the operator still
		pvc, _, _ = w.fetchOrCreatePVC(ctx, host, pvcNamespace, pvcName, volumeMount.Name, shouldCHOPCreatePVC, &volumeClaimTemplate.Spec)
		reconcileError = ErrPVCWithLostPVDeleted
	}

	if pvc == nil {
		log.M(host).F().Error("Unable to reconcile nil PVC: %s/%s", pvcNamespace, pvcName)
		reconcileError = ErrPVCIsMissed
		return reconcileError
	}

	if pvcReconciled, err := w.reconcilePVC(ctx, pvc, host, volumeClaimTemplate); err == nil {
		w.task.RegistryReconciled().RegisterPVC(pvcReconciled.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterPVC(pvc.GetObjectMeta())
		log.M(host).F().Error("Unable to reconcile PVC: %s err: %v", util.NamespacedName(pvc), err)
	}

	// It still may return data loss errors
	return reconcileError
}

func (w *Reconciler) fetchOrCreatePVC(
	ctx context.Context,
	host *api.Host,
	namespace, name string,
	volumeMountName string,
	operatorInCharge bool,
	pvcSpec *core.PersistentVolumeClaimSpec,
) (
	pvc *core.PersistentVolumeClaim,
	created bool,
	err error,
) {
	// Try to fetch existing PVC (if any)
	pvc, err = w.pvc.Get(ctx, namespace, name)

	switch {
	// PVC fetched - all is good
	case err == nil:
		// Existing PVC found, all is good
		// Nothing more to do here
		log.V(2).M(host).Info("PVC (%s/%s/%s/%s) found", namespace, host.GetName(), volumeMountName, name)
		return pvc, false, nil

	// PVC not fetched - and it does not exist
	case apiErrors.IsNotFound(err):
		// We have NotFound error - PVC not found - it does not exist
		// This is not an error per se, means PVC is not created (yet)?
		log.V(1).M(host).Info(
			"PVC (%s/%s/%s/%s) not found - operator should create the model: %t",
			namespace, host.GetName(), volumeMountName, name, operatorInCharge,
		)
		if operatorInCharge {
			// PVC is not available and the operator is in charge of the PVC
			// Create PVC model.
			log.V(1).M(host).Info(
				"PVC (%s/%s/%s/%s) model provided by the operator",
				namespace, host.GetName(), volumeMountName, name,
			)
			pvc = w.task.Creator().CreatePVC(name, namespace, host, pvcSpec)
			return pvc, true, nil
		} else {
			// PVC is not available and the operator is not in charge of the PVC
			// Do nothing
			log.V(1).M(host).Info(
				"PVC (%s/%s/%s/%s) not found and model will not be provided by the operator - expecting STS to provide PVC",
				namespace, host.GetName(), volumeMountName, name,
			)
			return nil, false, nil
		}

	// PVC not fetched - and we have some strange error
	default:
		// In case of any non-NotFound API error - unable to proceed
		log.M(host).F().Error("ERROR unable to get PVC(%s/%s) err: %v", namespace, name, err)
		return nil, false, err
	}
}

func (w *Reconciler) isLostPVC(pvc *core.PersistentVolumeClaim, chopCreated bool, host *api.Host) bool {
	// In case host has no data - there is no data to loose
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

	if chopCreated {
		// PVC was just created by the operator, not fetched
		// Lost PVC
		return true
	}

	// PVC is in place
	return false
}

func (w *Reconciler) isLostPV(pvc *core.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}

	return pvc.Status.Phase == core.ClaimLost
}

// reconcilePVC reconciles specified PVC
func (w *Reconciler) reconcilePVC(
	ctx context.Context,
	pvc *core.PersistentVolumeClaim,
	host *api.Host,
	template *api.VolumeClaimTemplate,
) (*core.PersistentVolumeClaim, error) {
	log.V(1).M(host).S().Info("reconcile PVC (%s/%s)", util.NamespacedName(pvc), host.GetName())
	defer log.V(1).M(host).E().Info("reconcile PVC (%s/%s)", util.NamespacedName(pvc), host.GetName())

	if util.IsContextDone(ctx) {
		log.V(1).Info("task is done")
		return nil, fmt.Errorf("task is done")
	}

	model.VolumeClaimTemplateApplyResourcesRequestsOnPVC(template, pvc)
	pvc = w.task.Creator().AdjustPVC(pvc, host, template)
	return w.pvc.UpdateOrCreate(ctx, pvc)
}

func (w *Reconciler) deletePVC(ctx context.Context, pvc *core.PersistentVolumeClaim) bool {
	log.V(1).M(pvc).F().S().Info("delete PVC with lost PV start: %s", util.NamespacedName(pvc))
	defer log.V(1).M(pvc).F().E().Info("delete PVC with lost PV end: %s", util.NamespacedName(pvc))

	log.V(2).M(pvc).F().Info("PVC with lost PV about to be deleted: %s", util.NamespacedName(pvc))
	w.pvc.Delete(ctx, pvc.Namespace, pvc.Name)

	for i := 0; i < 360; i++ {

		// Check availability
		log.V(2).M(pvc).F().Info("check PVC with lost PV availability: %s", util.NamespacedName(pvc))
		curPVC, err := w.pvc.Get(ctx, pvc.Namespace, pvc.Name)
		if err != nil {
			if apiErrors.IsNotFound(err) {
				// Not available - consider it to be deleted
				log.V(1).M(pvc).F().Warning("PVC with lost PV was deleted: %s", util.NamespacedName(pvc))
				return true
			}
		}

		// PVC is not deleted (yet?). May be it has finalizers installed. Need to clean them.
		if len(curPVC.Finalizers) > 0 {
			log.V(2).M(pvc).F().Info("clean finalizers for PVC with lost PV: %s", util.NamespacedName(pvc))
			curPVC.Finalizers = nil
			w.pvc.UpdateOrCreate(ctx, curPVC)
		}
		time.Sleep(10 * time.Second)
	}

	return false
}
