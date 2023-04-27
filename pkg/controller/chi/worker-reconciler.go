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
	coreV1 "k8s.io/api/core/v1"
	policyV1 "k8s.io/api/policy/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopModel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileCHI run reconcile cycle for a CHI
func (w *worker) reconcileCHI(ctx context.Context, old, new *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.logOldAndNew("non-normalized yet", old, new)

	switch {
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-1")
	case w.isGenerationTheSame(old, new):
		w.a.M(new).F().Info("isGenerationTheSame() - nothing to do here, exit")
		return nil
	}

	w.a.M(new).S().P()
	defer w.a.M(new).E().P()

	if new.HasAncestor() {
		w.a.M(new).F().Info("has ancestor, use it as a base for reconcile")
		old = new.GetAncestor()
	}

	old = w.normalize(old)
	new = w.normalize(new)
	new.SetAncestor(old)
	w.logOldAndNew("normalized", old, new)

	actionPlan := chopModel.NewActionPlan(old, new)
	w.logActionPlan(actionPlan)

	switch {
	case actionPlan.HasActionsToDo():
		w.a.M(new).F().Info("ActionPlan has actions - continue reconcile")
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-2")
	default:
		w.a.M(new).F().Info("ActionPlan has no actions and not finalizer - nothing to do")
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.newTask(new)
	w.markReconcileStart(ctx, new, actionPlan)
	w.excludeStopped(new)
	w.walkHosts(ctx, new, actionPlan)

	if err := w.reconcile(ctx, new); err != nil {
		w.a.WithEvent(new, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(new).
			M(new).F().
			Error("FAILED update: %v", err)
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Post-process added items
	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(new).
		M(new).F().
		Info("remove items scheduled for deletion")
	w.clean(ctx, new)
	w.dropReplicas(ctx, new, actionPlan)
	w.includeStopped(new)
	w.waitForIPAddresses(ctx, new)
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}
	w.markReconcileComplete(ctx, new)

	return nil
}

// reconcile reconciles ClickHouseInstallation
func (w *worker) reconcile(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	return chi.WalkTillError(
		ctx,
		w.reconcileCHIAuxObjectsPreliminary,
		w.reconcileCluster,
		w.reconcileShard,
		w.reconcileHost,
		w.reconcileCHIAuxObjectsFinal,
	)
}

// reconcileCHIAuxObjectsPreliminary reconciles CHI preliminary in order to ensure that ConfigMaps are in place
func (w *worker) reconcileCHIAuxObjectsPreliminary(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// CHI common ConfigMap without added hosts
	options := w.options()
	if err := w.reconcileCHIConfigMapCommon(ctx, chi, options); err != nil {
		w.a.F().Error("failed to reconcile config map common. err: %v", err)
	}
	// 3. CHI users ConfigMap
	if err := w.reconcileCHIConfigMapUsers(ctx, chi); err != nil {
		w.a.F().Error("failed to reconcile config map users. err: %v", err)
	}

	return nil
}

// reconcileCHIServicePreliminary runs first stage of CHI reconcile process
func (w *worker) reconcileCHIServicePreliminary(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if chi.IsStopped() {
		// Stopped CHI must have no entry point
		_ = w.c.deleteServiceCHI(ctx, chi)
	}
	return nil
}

// reconcileCHIServiceFinal runs second stage of CHI reconcile process
func (w *worker) reconcileCHIServiceFinal(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if chi.IsStopped() {
		// Stopped CHI must have no entry point
		return nil
	}

	// Create entry point for the whole CHI
	if service := w.task.creator.CreateServiceCHI(); service != nil {
		if err := w.reconcileService(ctx, chi, service); err != nil {
			// Service not reconciled
			w.task.registryFailed.RegisterService(service.ObjectMeta)
			return err
		}
		w.task.registryReconciled.RegisterService(service.ObjectMeta)
	}

	return nil
}

// reconcileCHIAuxObjectsFinal reconciles CHI global objects
func (w *worker) reconcileCHIAuxObjectsFinal(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// CHI ConfigMaps with update
	return w.reconcileCHIConfigMapCommon(ctx, chi, nil)
}

// reconcileCHIConfigMapCommon reconciles all CHI's common ConfigMap
func (w *worker) reconcileCHIConfigMapCommon(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	options *chopModel.ClickHouseConfigFilesGeneratorOptions,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.task.creator.CreateConfigMapCHICommon(options)
	err := w.reconcileConfigMap(ctx, chi, configMapCommon)
	if err == nil {
		w.task.registryReconciled.RegisterConfigMap(configMapCommon.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterConfigMap(configMapCommon.ObjectMeta)
	}
	return err
}

// reconcileCHIConfigMapUsers reconciles all CHI's users ConfigMap
// ConfigMap common for all users resources in CHI
func (w *worker) reconcileCHIConfigMapUsers(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := w.task.creator.CreateConfigMapCHICommonUsers()
	err := w.reconcileConfigMap(ctx, chi, configMapUsers)
	if err == nil {
		w.task.registryReconciled.RegisterConfigMap(configMapUsers.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterConfigMap(configMapUsers.ObjectMeta)
	}
	return err
}

// reconcileHostConfigMap reconciles host's personal ConfigMap
func (w *worker) reconcileHostConfigMap(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap for a host
	configMap := w.task.creator.CreateConfigMapHost(host)
	err := w.reconcileConfigMap(ctx, host.CHI, configMap)
	if err == nil {
		w.task.registryReconciled.RegisterConfigMap(configMap.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterConfigMap(configMap.ObjectMeta)
		return err
	}

	return nil
}

// getHostStatefulSetCurStatus gets StatefulSet current status
func (w *worker) getHostStatefulSetCurStatus(ctx context.Context, host *chiV1.ChiHost) string {
	version := "unknown"
	if host.GetReconcileAttributes().GetStatus() == chiV1.StatefulSetStatusNew {
		version = "not applicable"
	} else {
		if ver, e := w.schemer.HostVersion(ctx, host); e == nil {
			version = ver
			host.Version = chiV1.NewCHVersion(version)
		} else {
			version = "failed to query"
		}
	}
	host.CurStatefulSet, _ = w.c.getStatefulSet(host, false)
	return version
}

// reconcileHostStatefulSet reconciles host's StatefulSet
func (w *worker) reconcileHostStatefulSet(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	version := w.getHostStatefulSetCurStatus(ctx, host)
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(1).M(host).F().Info("Reconcile host %s. ClickHouse version: %s", host.GetName(), version)
	// In case we have to force-restart host
	// We'll do it via replicas: 0 in StatefulSet.
	if w.shouldForceRestartHost(host) {
		w.a.V(1).M(host).F().Info("Reconcile host %s. Shutting host down due to force restart", host.GetName())
		w.prepareHostStatefulSetWithStatus(ctx, host, true)
		_ = w.reconcileStatefulSet(ctx, host)
		// At this moment StatefulSet has 0 replicas.
		// First stage of RollingUpdate completed.
	}

	// We are in place, where we can  reconcile StatefulSet to desired configuration.
	w.a.V(1).M(host).F().Info("Reconcile host %s. Reconcile StatefulSet", host.GetName())
	w.prepareHostStatefulSetWithStatus(ctx, host, false)
	err := w.reconcileStatefulSet(ctx, host)
	if err == nil {
		w.task.registryReconciled.RegisterStatefulSet(host.DesiredStatefulSet.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterStatefulSet(host.DesiredStatefulSet.ObjectMeta)
		if err == errIgnore {
			err = nil
		}
	}
	return err
}

// reconcileHostService reconciles host's Service
func (w *worker) reconcileHostService(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}
	service := w.task.creator.CreateServiceHost(host)
	if service == nil {
		// This is not a problem, service may be omitted
		return nil
	}
	err := w.reconcileService(ctx, host.CHI, service)
	if err == nil {
		w.task.registryReconciled.RegisterService(service.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterService(service.ObjectMeta)
	}
	return err
}

// reconcileCluster reconciles Cluster, excluding nested shards
func (w *worker) reconcileCluster(ctx context.Context, cluster *chiV1.Cluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	// Add Cluster's Service
	if service := w.task.creator.CreateServiceCluster(cluster); service != nil {
		if err := w.reconcileService(ctx, cluster.CHI, service); err == nil {
			w.task.registryReconciled.RegisterService(service.ObjectMeta)
		} else {
			w.task.registryFailed.RegisterService(service.ObjectMeta)
		}
	}

	// Add Cluster's Auto Secret
	if cluster.Secret.Source() == chiV1.ClusterSecretSourceAuto {
		if secret := w.task.creator.CreateClusterSecret(chopModel.CreateClusterAutoSecretName(cluster)); secret != nil {
			if err := w.reconcileSecret(ctx, cluster.CHI, secret); err == nil {
				w.task.registryReconciled.RegisterSecret(secret.ObjectMeta)
			} else {
				w.task.registryFailed.RegisterSecret(secret.ObjectMeta)
			}
		}
	}

	pdb := w.task.creator.NewPodDisruptionBudget(cluster)
	if err := w.reconcilePDB(ctx, cluster, pdb); err == nil {
		w.task.registryReconciled.RegisterPDB(pdb.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterPDB(pdb.ObjectMeta)
	}

	return nil
}

// reconcileShard reconciles Shard, excluding nested replicas
func (w *worker) reconcileShard(ctx context.Context, shard *chiV1.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	// Add Shard's Service
	service := w.task.creator.CreateServiceShard(shard)
	if service == nil {
		// This is not a problem, ServiceShard may be omitted
		return nil
	}
	err := w.reconcileService(ctx, shard.CHI, service)
	if err == nil {
		w.task.registryReconciled.RegisterService(service.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterService(service.ObjectMeta)
	}
	return err
}

// reconcileHost reconciles ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()
	if host.IsFirst() {
		w.reconcileCHIServicePreliminary(ctx, host.CHI)
		defer w.reconcileCHIServiceFinal(ctx, host.CHI)
	}

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Reconcile Host %s started", host.GetName())

	// Create artifacts
	w.prepareHostStatefulSetWithStatus(ctx, host, false)

	if err := w.excludeHost(ctx, host); err != nil {
		return err
	}

	if err := w.reconcileHostConfigMap(ctx, host); err != nil {
		return err
	}
	//w.reconcilePersistentVolumes(task, host)
	_ = w.reconcilePVCs(ctx, host)
	if err := w.reconcileHostStatefulSet(ctx, host); err != nil {
		return err
	}
	_ = w.reconcilePVCs(ctx, host)

	_ = w.reconcileHostService(ctx, host)

	host.GetReconcileAttributes().UnsetAdd()

	_ = w.migrateTables(ctx, host)

	version, err := w.schemer.HostVersion(ctx, host)
	if err != nil {
		version = "unknown"
	}

	if err := w.includeHost(ctx, host); err != nil {
		// If host is not ready - fallback
		return err
	}

	w.a.V(1).
		WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Reconcile Host %s completed. ClickHouse version running: %s", host.GetName(), version)

	var (
		hostsCompleted int
		hostsCount     int
	)

	if host.CHI != nil && host.CHI.Status != nil {
		hostsCompleted = host.CHI.Status.GetHostsCompletedCount()
		hostsCount = host.CHI.Status.GetHostsCount()
	}

	w.a.V(1).
		WithEvent(host.CHI, eventActionProgress, eventReasonProgressHostsCompleted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("%s: %d of %d", eventReasonProgressHostsCompleted, hostsCompleted, hostsCount)

	return nil
}

// reconcilePDB reconciles PodDisruptionBudget
func (w *worker) reconcilePDB(ctx context.Context, cluster *chiV1.Cluster, pdb *policyV1.PodDisruptionBudget) error {
	cur, err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Get(ctx, pdb.Name, newGetOptions())
	switch {
	case err == nil:
		pdb.ResourceVersion = cur.ResourceVersion
		_, err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Update(ctx, pdb, newUpdateOptions())
		if err == nil {
			log.V(1).Info("PDB updated %s/%s", pdb.Namespace, pdb.Name)
		} else {
			log.Error("FAILED to update PDB %s/%s err: %v", pdb.Namespace, pdb.Name, err)
			return nil
		}
	case apiErrors.IsNotFound(err):
		_, err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Create(ctx, pdb, newCreateOptions())
		if err == nil {
			log.V(1).Info("PDB created %s/%s", pdb.Namespace, pdb.Name)
		} else {
			log.Error("FAILED create PDB %s/%s err: %v", pdb.Namespace, pdb.Name, err)
			return err
		}
	default:
		log.Error("FAILED get PDB %s/%s err: %v", pdb.Namespace, pdb.Name, err)
		return err
	}

	return nil
}

// reconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) reconcileConfigMap(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	configMap *coreV1.ConfigMap,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// Check whether this object already exists in k8s
	curConfigMap, err := w.c.getConfigMap(&configMap.ObjectMeta, true)

	if curConfigMap != nil {
		// We have ConfigMap - try to update it
		err = w.updateConfigMap(ctx, chi, configMap)
	}

	if apiErrors.IsNotFound(err) {
		// ConfigMap not found - even during Update process - try to create it
		err = w.createConfigMap(ctx, chi, configMap)
	}

	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to reconcile ConfigMap: %s CHI: %s ", configMap.Name, chi.Name)
	}

	return err
}

func (w *worker) hasService(ctx context.Context, chi *chiV1.ClickHouseInstallation, service *coreV1.Service) bool {
	// Check whether this object already exists
	curService, _ := w.c.getService(service)
	return curService != nil
}

// reconcileService reconciles core.Service
func (w *worker) reconcileService(ctx context.Context, chi *chiV1.ClickHouseInstallation, service *coreV1.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().Info(service.Name)
	defer w.a.V(2).M(chi).E().Info(service.Name)

	// Check whether this object already exists
	curService, err := w.c.getService(service)

	if curService != nil {
		// We have Service - try to update it
		err = w.updateService(ctx, chi, curService, service)
	}

	if err != nil {
		// Service not found or not updated. Try to recreate
		_ = w.c.deleteServiceIfExists(ctx, service.Namespace, service.Name)
		err = w.createService(ctx, chi, service)
	}

	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to reconcile Service: %s CHI: %s ", service.Name, chi.Name)
	}

	return err
}

// reconcileSecret reconciles core.Secret
func (w *worker) reconcileSecret(ctx context.Context, chi *chiV1.ClickHouseInstallation, secret *coreV1.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().Info(secret.Name)
	defer w.a.V(2).M(chi).E().Info(secret.Name)

	// Check whether this object already exists
	if _, err := w.c.getSecret(secret); err == nil {
		// We have Secret - try to update it
		return nil
	}

	// Secret not found or broken. Try to recreate
	_ = w.c.deleteSecretIfExists(ctx, secret.Namespace, secret.Name)
	err := w.createSecret(ctx, chi, secret)
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to reconcile Secret: %s CHI: %s ", secret.Name, chi.Name)
	}

	return err
}

// reconcileStatefulSet reconciles StatefulSet of a host
func (w *worker) reconcileStatefulSet(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	newStatefulSet := host.DesiredStatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))

	if host.GetReconcileAttributes().GetStatus() == chiV1.StatefulSetStatusSame {
		defer w.a.V(2).M(host).F().Info("no need to reconcile the same StatefulSet %s", util.NamespaceNameString(newStatefulSet.ObjectMeta))
		host.CHI.EnsureStatus().HostUnchanged()
		return nil
	}

	// Check whether this object already exists in k8s
	var err error
	host.CurStatefulSet, err = w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	if host.HasCurStatefulSet() {
		// We have StatefulSet - try to update it
		err = w.updateStatefulSet(ctx, host)
	}

	if apiErrors.IsNotFound(err) {
		// StatefulSet not found - even during Update process - try to create it
		err = w.createStatefulSet(ctx, host)
	}

	if err != nil {
		host.CHI.EnsureStatus().HostFailed()
		w.a.WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			M(host).F().
			Error("FAILED to reconcile StatefulSet: %s CHI: %s ", newStatefulSet.Name, host.CHI.Name)
	}

	// Host has to know current StatefulSet and Pod
	host.CurStatefulSet, _ = w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	return err
}

// reconcilePersistentVolumes reconciles all PVs of a host
func (w *worker) reconcilePersistentVolumes(ctx context.Context, host *chiV1.ChiHost) {
	if util.IsContextDone(ctx) {
		return
	}

	w.c.walkPVs(host, func(pv *coreV1.PersistentVolume) {
		pv = w.task.creator.PreparePersistentVolume(pv, host)
		_, _ = w.c.updatePersistentVolume(ctx, pv)
	})
}

// reconcilePVCs reconciles all PVCs of a host
func (w *worker) reconcilePVCs(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		return nil
	}

	namespace := host.Address.Namespace
	w.a.V(2).M(host).S().Info("host %s/%s", namespace, host.GetName())
	defer w.a.V(2).M(host).E().Info("host %s/%s", namespace, host.GetName())

	host.WalkVolumeMounts(chiV1.DesiredStatefulSet, func(volumeMount *coreV1.VolumeMount) {
		if util.IsContextDone(ctx) {
			return
		}

		volumeClaimTemplateName := volumeMount.Name
		volumeClaimTemplate, ok := host.CHI.GetVolumeClaimTemplate(volumeClaimTemplateName)
		if !ok {
			// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
			return
		}

		pvcName := chopModel.CreatePVCName(host, volumeMount, volumeClaimTemplate)
		w.a.V(2).M(host).Info("reconcile volumeMount (%s/%s/%s/%s) - start", namespace, host.GetName(), volumeMount.Name, pvcName)
		defer w.a.V(2).M(host).Info("reconcile volumeMount (%s/%s/%s/%s) - end", namespace, host.GetName(), volumeMount.Name, pvcName)

		pvc, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, newGetOptions())
		if err != nil {
			if apiErrors.IsNotFound(err) {
				// This is not an error per se, means PVC is not created (yet)?
				if w.task.creator.OperatorShouldCreatePVC(host, volumeClaimTemplate) {
					pvc = w.task.creator.CreatePVC(pvcName, host, &volumeClaimTemplate.Spec)
				} else {
					// Not created and we are not expected to create PVC by ourselves
					return
				}
			} else {
				// Any non-NotFound API error - unable to proceed
				w.a.M(host).F().Error("ERROR unable to get PVC(%s/%s) err: %v", namespace, pvcName, err)
				return
			}
		}

		pvc, err = w.reconcilePVC(ctx, pvc, host, volumeClaimTemplate)
		if err != nil {
			w.a.M(host).F().Error("ERROR unable to reconcile PVC(%s/%s) err: %v", namespace, pvcName, err)
			w.task.registryFailed.RegisterPVC(pvc.ObjectMeta)
			return
		}

		w.task.registryReconciled.RegisterPVC(pvc.ObjectMeta)
	})

	return nil
}

// reconcilePVC reconciles specified PVC
func (w *worker) reconcilePVC(
	ctx context.Context,
	pvc *coreV1.PersistentVolumeClaim,
	host *chiV1.ChiHost,
	template *chiV1.ChiVolumeClaimTemplate,
) (*coreV1.PersistentVolumeClaim, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil, fmt.Errorf("task is done")
	}

	w.applyPVCResourcesRequests(pvc, template)
	pvc = w.task.creator.PreparePersistentVolumeClaim(pvc, host, template)
	return w.c.updatePersistentVolumeClaim(ctx, pvc)
}
