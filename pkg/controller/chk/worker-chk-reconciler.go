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
	"errors"
	"sync"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileCR runs reconcile cycle for a Custom Resource
func (w *worker) reconcileCR(ctx context.Context, old, new *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.M(new).S().P()
	defer w.a.M(new).E().P()

	if new.HasAncestor() {
		log.V(2).M(new).F().Info("has ancestor, use it as a base for reconcile. CR: %s", util.NamespaceNameString(new))
		old = new.GetAncestorT()
	} else {
		log.V(2).M(new).F().Info("has NO ancestor, use empty base for reconcile. CR: %s", util.NamespaceNameString(new))
		old = nil
	}

	common.LogOldAndNew("non-normalized yet (native)", old, new)

	switch {
	case w.isGenerationTheSame(old, new):
		log.V(2).M(new).F().Info("isGenerationTheSame() - nothing to do here, exit")
		return nil
	}

	log.V(2).M(new).F().Info("Normalized OLD: %s", util.NamespaceNameString(new))
	old = w.normalize(old)

	log.V(2).M(new).F().Info("Normalized NEW: %s", util.NamespaceNameString(new))
	new = w.normalize(new)

	new.SetAncestor(old)
	common.LogOldAndNew("normalized", old, new)

	actionPlan := action_plan.NewActionPlan(old, new)
	common.LogActionPlan(actionPlan)

	switch {
	case actionPlan.HasActionsToDo():
		w.a.M(new).F().Info("ActionPlan has actions - continue reconcile")
	default:
		w.a.M(new).F().Info("ActionPlan has no actions and no need to install finalizer - nothing to do")
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.newTask(new)
	w.markReconcileStart(ctx, new, actionPlan)
	w.walkHosts(ctx, new, actionPlan)

	if err := w.reconcile(ctx, new); err != nil {
		// Something went wrong
		w.a.WithEvent(new, common.EventActionReconcile, common.EventReasonReconcileFailed).
			WithStatusError(new).
			M(new).F().
			Error("FAILED to reconcile CR %s, err: %v", util.NamespaceNameString(new), err)
		w.markReconcileCompletedUnsuccessfully(ctx, new, err)
		if errors.Is(err, common.ErrCRUDAbort) {
		}
	} else {
		// Reconcile successful
		// Post-process added items
		if util.IsContextDone(ctx) {
			log.V(2).Info("task is done")
			return nil
		}
		w.clean(ctx, new)
		w.waitForIPAddresses(ctx, new)
		w.finalizeReconcileAndMarkCompleted(ctx, new)
	}

	return nil
}

// reconcile reconciles Custom Resource
func (w *worker) reconcile(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	counters := api.NewHostReconcileAttributesCounters()
	cr.WalkHosts(func(host *api.Host) error {
		counters.Add(host.GetReconcileAttributes())
		return nil
	})

	if counters.AddOnly() {
		w.a.V(1).M(cr).Info("Enabling full fan-out mode. CHI: %s", util.NamespaceNameString(cr))
		ctx = context.WithValue(ctx, common.ReconcileShardsAndHostsOptionsCtxKey, &common.ReconcileShardsAndHostsOptions{
			FullFanOut: true,
		})
	}

	return cr.WalkTillError(
		ctx,
		w.reconcileCRAuxObjectsPreliminary,
		w.reconcileCluster,
		w.reconcileShardsAndHosts,
		w.reconcileCRAuxObjectsFinal,
	)
}

// reconcileCRAuxObjectsPreliminary reconciles CR preliminary in order to ensure that ConfigMaps are in place
func (w *worker) reconcileCRAuxObjectsPreliminary(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	// CR common ConfigMap without added hosts
	cr.GetRuntime().LockCommonConfig()
	if err := w.reconcileConfigMapCommon(ctx, cr, w.options()); err != nil {
		w.a.F().Error("failed to reconcile config map common. err: %v", err)
	}
	cr.GetRuntime().UnlockCommonConfig()

	// CR users ConfigMap - common for all hosts
	if err := w.reconcileConfigMapCommonUsers(ctx, cr); err != nil {
		w.a.F().Error("failed to reconcile config map users. err: %v", err)
	}

	return nil
}

// reconcileCRServicePreliminary runs first stage of CR reconcile process
func (w *worker) reconcileCRServicePreliminary(ctx context.Context, cr api.ICustomResource) error {
	if cr.IsStopped() {
		// Stopped CR must have no entry point
		_ = w.c.deleteServiceCR(ctx, cr)
	}
	return nil
}

// reconcileCRServiceFinal runs second stage of CR reconcile process
func (w *worker) reconcileCRServiceFinal(ctx context.Context, cr api.ICustomResource) error {
	if cr.IsStopped() {
		// Stopped CHI must have no entry point
		return nil
	}

	// Create entry point for the whole CHI
	if service := w.task.Creator().CreateService(interfaces.ServiceCR); service != nil {
		if err := w.reconcileService(ctx, cr, service); err != nil {
			// Service not reconciled
			w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
			return err
		}
		w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
	}

	return nil
}

// reconcileCRAuxObjectsFinal reconciles CR global objects
func (w *worker) reconcileCRAuxObjectsFinal(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	// CR ConfigMaps with update
	cr.GetRuntime().LockCommonConfig()
	err = w.reconcileConfigMapCommon(ctx, cr, nil)
	cr.GetRuntime().UnlockCommonConfig()
	return err
}

// reconcileConfigMapCommon reconciles common ConfigMap
func (w *worker) reconcileConfigMapCommon(
	ctx context.Context,
	cr api.ICustomResource,
	options *config.FilesGeneratorOptions,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.task.Creator().CreateConfigMap(interfaces.ConfigMapCommon, options)
	err := w.reconcileConfigMap(ctx, cr, configMapCommon)
	if err == nil {
		w.task.RegistryReconciled().RegisterConfigMap(configMapCommon.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterConfigMap(configMapCommon.GetObjectMeta())
	}
	return err
}

// reconcileConfigMapCommonUsers reconciles all CHI's users ConfigMap
// ConfigMap common for all users resources in CHI
func (w *worker) reconcileConfigMapCommonUsers(ctx context.Context, cr api.ICustomResource) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := w.task.Creator().CreateConfigMap(interfaces.ConfigMapCommonUsers)
	err := w.reconcileConfigMap(ctx, cr, configMapUsers)
	if err == nil {
		w.task.RegistryReconciled().RegisterConfigMap(configMapUsers.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterConfigMap(configMapUsers.GetObjectMeta())
	}
	return err
}

// reconcileConfigMapHost reconciles host's personal ConfigMap
func (w *worker) reconcileConfigMapHost(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap for a host
	configMap := w.task.Creator().CreateConfigMap(interfaces.ConfigMapHost, host)
	err := w.reconcileConfigMap(ctx, host.GetCR(), configMap)
	if err == nil {
		w.task.RegistryReconciled().RegisterConfigMap(configMap.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterConfigMap(configMap.GetObjectMeta())
		return err
	}

	return nil
}

// reconcileHostStatefulSet reconciles host's StatefulSet
func (w *worker) reconcileHostStatefulSet(ctx context.Context, host *api.Host, opts *statefulset.ReconcileOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).M(host).F().S().Info("reconcile StatefulSet start")
	defer log.V(1).M(host).F().E().Info("reconcile StatefulSet end")

	version := w.getHostSoftwareVersion(ctx, host)
	host.Runtime.CurStatefulSet, _ = w.c.kube.STS().Get(ctx, host)

	w.a.V(1).M(host).F().Info("Reconcile host: %s. App version: %s", host.GetName(), version)
	// In case we have to force-restart host
	// We'll do it via replicas: 0 in StatefulSet.
	if w.shouldForceRestartHost(host) {
		w.a.V(1).M(host).F().Info("Reconcile host: %s. Shutting host down due to force restart", host.GetName())
		w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, true)
		_ = w.stsReconciler.ReconcileStatefulSet(ctx, host, false, opts)
		metrics.HostReconcilesRestart(ctx, host.GetCR())
		// At this moment StatefulSet has 0 replicas.
		// First stage of RollingUpdate completed.
	}

	// We are in place, where we can  reconcile StatefulSet to desired configuration.
	w.a.V(1).M(host).F().Info("Reconcile host: %s. Reconcile StatefulSet", host.GetName())
	w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, false)
	err := w.stsReconciler.ReconcileStatefulSet(ctx, host, true, opts)
	if err == nil {
		w.task.RegistryReconciled().RegisterStatefulSet(host.Runtime.DesiredStatefulSet.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterStatefulSet(host.Runtime.DesiredStatefulSet.GetObjectMeta())
		if err == common.ErrCRUDIgnore {
			// Pretend nothing happened in case of ignore
			err = nil
		}

		host.GetCR().IEnsureStatus().HostFailed()
		w.a.WithEvent(host.GetCR(), common.EventActionReconcile, common.EventReasonReconcileFailed).
			WithStatusAction(host.GetCR()).
			WithStatusError(host.GetCR()).
			M(host).F().
			Error("FAILED to reconcile StatefulSet for host: %s", host.GetName())
	}

	return err
}

func (w *worker) getHostSoftwareVersion(ctx context.Context, host *api.Host) string {
	return "undefined"
}

// reconcileHostService reconciles host's Service
func (w *worker) reconcileHostService(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}
	service := w.task.Creator().CreateService(interfaces.ServiceHost, host)
	if service == nil {
		// This is not a problem, service may be omitted
		return nil
	}
	err := w.reconcileService(ctx, host.GetCR(), service)
	if err == nil {
		w.a.V(1).M(host).F().Info("DONE Reconcile service of the host: %s", host.GetName())
		w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
	} else {
		w.a.V(1).M(host).F().Warning("FAILED Reconcile service of the host: %s", host.GetName())
		w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
	}
	return err
}

// reconcileCluster reconciles ChkCluster, excluding nested shards
func (w *worker) reconcileCluster(ctx context.Context, cluster *apiChk.Cluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	// Add Cluster Service
	if service := w.task.Creator().CreateService(interfaces.ServiceCluster, cluster); service != nil {
		if err := w.reconcileService(ctx, cluster.GetRuntime().GetCR(), service); err == nil {
			w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
		} else {
			w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
		}
	}

	w.reconcileClusterSecret(ctx, cluster)

	pdb := w.task.Creator().CreatePodDisruptionBudget(cluster)
	if err := w.reconcilePDB(ctx, cluster, pdb); err == nil {
		w.task.RegistryReconciled().RegisterPDB(pdb.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterPDB(pdb.GetObjectMeta())
	}

	return nil
}

func (w *worker) reconcileClusterSecret(ctx context.Context, cluster *apiChk.Cluster) {
}

// getReconcileShardsWorkersNum calculates how many workers are allowed to be used for concurrent shard reconcile
func (w *worker) getReconcileShardsWorkersNum(shards []*apiChk.ChkShard, opts *common.ReconcileShardsAndHostsOptions) int {
	return 1
}

// reconcileShardsAndHosts reconciles shards and hosts of each shard
func (w *worker) reconcileShardsAndHosts(ctx context.Context, shards []*apiChk.ChkShard) error {
	// Sanity check - has to have shard(s)
	if len(shards) == 0 {
		return nil
	}

	log.V(1).F().S().Info("reconcileShardsAndHosts start")
	defer log.V(1).F().E().Info("reconcileShardsAndHosts end")

	// Try to fetch options
	opts, ok := ctx.Value(common.ReconcileShardsAndHostsOptionsCtxKey).(*common.ReconcileShardsAndHostsOptions)
	if ok {
		w.a.V(1).Info("found ReconcileShardsAndHostsOptionsCtxKey")
	} else {
		w.a.V(1).Info("not found ReconcileShardsAndHostsOptionsCtxKey, use empty opts")
		opts = &common.ReconcileShardsAndHostsOptions{}
	}

	// Which shard to start concurrent processing with
	var startShard int
	if opts.FullFanOut {
		// For full fan-out scenarios we'll start shards processing from the very beginning
		startShard = 0
		w.a.V(1).Info("full fan-out requested")
	} else {
		// For non-full fan-out scenarios, we'll process the first shard separately.
		// This gives us some early indicator on whether the reconciliation would fail,
		// and for large clusters it is a small price to pay before performing concurrent fan-out.
		w.a.V(1).Info("starting first shard separately")
		if err := w.reconcileShardWithHosts(ctx, shards[0]); err != nil {
			w.a.V(1).Warning("first shard failed, skipping rest of shards due to an error: %v", err)
			return err
		}

		// Since shard with 0 index is already done, we'll proceed with the 1-st
		startShard = 1
	}

	// Process shards using specified concurrency level while maintaining specified max concurrency percentage.
	// Loop over shards.
	workersNum := w.getReconcileShardsWorkersNum(shards, opts)
	w.a.V(1).Info("Starting rest of shards on workers: %d", workersNum)
	for startShardIndex := startShard; startShardIndex < len(shards); startShardIndex += workersNum {
		endShardIndex := startShardIndex + workersNum
		if endShardIndex > len(shards) {
			endShardIndex = len(shards)
		}
		concurrentlyProcessedShards := shards[startShardIndex:endShardIndex]

		// Processing error protected with mutex
		var err error
		var errLock sync.Mutex

		wg := sync.WaitGroup{}
		wg.Add(len(concurrentlyProcessedShards))
		// Launch shard concurrent processing
		for j := range concurrentlyProcessedShards {
			shard := concurrentlyProcessedShards[j]
			go func() {
				defer wg.Done()
				if e := w.reconcileShardWithHosts(ctx, shard); e != nil {
					errLock.Lock()
					err = e
					errLock.Unlock()
					return
				}
			}()
		}
		wg.Wait()
		if err != nil {
			w.a.V(1).Warning("Skipping rest of shards due to an error: %v", err)
			return err
		}
	}
	return nil
}

func (w *worker) reconcileShardWithHosts(ctx context.Context, shard api.IShard) error {
	if err := w.reconcileShard(ctx, shard); err != nil {
		return err
	}
	return shard.WalkHostsAbortOnError(func(host *api.Host) error {
		return w.reconcileHost(ctx, host)
	})
}

// reconcileShard reconciles specified shard, excluding nested replicas
func (w *worker) reconcileShard(ctx context.Context, shard api.IShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	err := w.reconcileShardService(ctx, shard)

	return err
}

func (w *worker) reconcileShardService(ctx context.Context, shard api.IShard) error {
	return nil
}

// reconcileHost reconciles specified ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()

	if host.IsFirst() {
		_ = w.reconcileCRServicePreliminary(ctx, host.GetCR())
		defer w.reconcileCRServiceFinal(ctx, host.GetCR())
	}

	// Create artifacts
	w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, false)

	if err := w.reconcileHostPrepare(ctx, host); err != nil {
		return err
	}
	if err := w.reconcileHostMain(ctx, host); err != nil {
		return err
	}
	// Host is now added and functional
	host.GetReconcileAttributes().UnsetAdd()
	if err := w.reconcileHostBootstrap(ctx, host); err != nil {
		return err
	}

	now := time.Now()
	hostsCompleted := 0
	hostsCount := 0
	host.GetCR().IEnsureStatus().HostCompleted()
	if host.GetCR() != nil && host.GetCR().GetStatus() != nil {
		hostsCompleted = host.GetCR().GetStatus().GetHostsCompletedCount()
		hostsCount = host.GetCR().GetStatus().GetHostsCount()
	}
	w.a.V(1).
		WithEvent(host.GetCR(), common.EventActionProgress, common.EventReasonProgressHostsCompleted).
		WithStatusAction(host.GetCR()).
		M(host).F().
		Info("[now: %s] %s: %d of %d", now, common.EventReasonProgressHostsCompleted, hostsCompleted, hostsCount)

	_ = w.c.updateCRObjectStatus(ctx, host.GetCR(), types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			MainFields: true,
		},
	})
	return nil
}

// reconcileHostPrepare reconciles specified ClickHouse host
func (w *worker) reconcileHostPrepare(ctx context.Context, host *api.Host) error {
	w.a.V(1).
		M(host).F().
		Info("Include host into cluster. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	w.includeHostIntoRaftCluster(ctx, host)
	return nil
}

// reconcileHostMain reconciles specified ClickHouse host
func (w *worker) reconcileHostMain(ctx context.Context, host *api.Host) error {
	var (
		reconcileStatefulSetOpts *statefulset.ReconcileOptions
	)

	if host.IsFirst() || host.IsLast() {
		reconcileStatefulSetOpts = reconcileStatefulSetOpts.SetDoNotWait()
	}

	if err := w.reconcileConfigMapHost(ctx, host); err != nil {
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 2. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	w.a.V(1).
		M(host).F().
		Info("Reconcile PVCs and check possible data loss for host: %s", host.GetName())
	if storage.ErrIsDataLoss(
		storage.NewStorageReconciler(
			w.task,
			w.c.namer,
			storage.NewStoragePVC(kube.NewPVC(w.c.Client)),
		).ReconcilePVCs(ctx, host, api.DesiredStatefulSet),
	) {
		// In case of data loss detection on existing volumes, we need to:
		// 1. recreate StatefulSet
		// 2. run tables migration again
		reconcileStatefulSetOpts = reconcileStatefulSetOpts.SetForceRecreate()
		w.a.V(1).
			M(host).F().
			Info("Data loss detected for host: %s. Will do force migrate", host.GetName())
	}

	if err := w.reconcileHostStatefulSet(ctx, host, reconcileStatefulSetOpts); err != nil {
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 3. Host: %s Err: %v", host.GetName(), err)
		return err
	}
	// Polish all new volumes that operator has to create
	_ = storage.NewStorageReconciler(
		w.task,
		w.c.namer,
		storage.NewStoragePVC(kube.NewPVC(w.c.Client)),
	).ReconcilePVCs(ctx, host, api.DesiredStatefulSet)

	_ = w.reconcileHostService(ctx, host)

	return nil
}

// reconcileHostBootstrap reconciles specified ClickHouse host
func (w *worker) reconcileHostBootstrap(ctx context.Context, host *api.Host) error {
	if err := w.includeHost(ctx, host); err != nil {
		metrics.HostReconcilesErrors(ctx, host.GetCR())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 4. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	return nil
}
