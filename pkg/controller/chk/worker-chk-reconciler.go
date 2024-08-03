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
	"sync"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (w *worker) reconcileCHK(ctx context.Context, old, new *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if new.HasAncestor() {
		log.V(2).M(new).F().Info("has ancestor, use it as a base for reconcile. CHK: %s", util.NamespaceNameString(new))
		old = new.GetAncestor()
	} else {
		log.V(2).M(new).F().Info("has NO ancestor, use empty CHK as a base for reconcile. CHK: %s", util.NamespaceNameString(new))
		old = nil
	}

	log.V(2).M(new).F().Info("Normalized OLD CHK: %s", util.NamespaceNameString(new))
	old = w.normalize(old)

	log.V(2).M(new).F().Info("Normalized NEW CHK %s", util.NamespaceNameString(new))
	new = w.normalize(new)
	new.SetAncestor(old)

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.newTask(new)

	if old.GetGeneration() != new.GetGeneration() {
		if err := w.reconcile(ctx, new); err != nil {
			// Something went wrong
		} else {
			// Reconcile successful
		}
	}

	return nil
}

//type reconcileFunc func(cluster *apiChk.ClickHouseKeeperInstallation) error

func (w *worker) reconcile(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	return chk.WalkTillError(
		ctx,
		w.reconcileCRAuxObjectsPreliminary,
		w.reconcileCluster,
		w.reconcileShardsAndHosts,
		w.reconcileCRAuxObjectsFinal,
	)
	//for _, f := range []reconcileFunc{
	//	w.reconcileConfigMap,
	//	w.reconcileStatefulSet,
	//	w.reconcileClientService,
	//	w.reconcileHeadlessService,
	//	w.reconcilePodDisruptionBudget,
	//} {
	//	if err := f(chk); err != nil {
	//		log.V(1).Error("Error during reconcile. f: %s err: %s", runtime.FunctionName(f), err)
	//		return err
	//	}
	//}
	return nil
}

// reconcileCHIAuxObjectsPreliminary reconciles CHI preliminary in order to ensure that ConfigMaps are in place
func (w *worker) reconcileCRAuxObjectsPreliminary(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chk).S().P()
	defer w.a.V(2).M(chk).E().P()

	if err := w.reconcileConfigMapCommon(ctx, chk); err != nil {
		w.a.F().Error("failed to reconcile config map common. err: %v", err)
	}
	return nil
}

// reconcileCluster reconciles ChkCluster, excluding nested shards
func (w *worker) reconcileCluster(ctx context.Context, cluster *apiChk.ChkCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	// Add ChkCluster's Service
	if service := w.task.Creator().CreateService(interfaces.ServiceCluster, cluster); service != nil {
		if err := w.reconcileService(ctx, cluster.Runtime.CHK, service); err == nil {
			w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
		} else {
			w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
		}
	}

	pdb := w.task.Creator().CreatePodDisruptionBudget(cluster)
	if err := w.reconcilePDB(ctx, cluster, pdb); err == nil {
		w.task.RegistryReconciled().RegisterPDB(pdb.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterPDB(pdb.GetObjectMeta())
	}

	return nil
}

// reconcileHostStatefulSet reconciles host's StatefulSet
func (w *worker) reconcileHostStatefulSet(ctx context.Context, host *api.Host, opts ...*statefulset.ReconcileStatefulSetOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).M(host).F().S().Info("reconcile StatefulSet start")
	defer log.V(1).M(host).F().E().Info("reconcile StatefulSet end")

	host.Runtime.CurStatefulSet, _ = w.c.kube.STS().Get(host)

	// We are in place, where we can  reconcile StatefulSet to desired configuration.
	w.a.V(1).M(host).F().Info("Reconcile host: %s. Reconcile StatefulSet", host.GetName())
	w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, false)
	err := w.stsReconciler.ReconcileStatefulSet(ctx, host, true, opts...)
	if err == nil {
		w.task.RegistryReconciled().RegisterStatefulSet(host.Runtime.DesiredStatefulSet.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterStatefulSet(host.Runtime.DesiredStatefulSet.GetObjectMeta())
		if err == common.ErrCRUDIgnore {
			// Pretend nothing happened in case of ignore
			err = nil
		}

		host.GetCR().EnsureStatus().HostFailed()
		w.a.WithEvent(host.GetCR(), common.EventActionReconcile, common.EventReasonReconcileFailed).
			WithStatusAction(host.GetCR()).
			WithStatusError(host.GetCR()).
			M(host).F().
			Error("FAILED to reconcile StatefulSet for host: %s", host.GetName())
	}

	return err
}

// getReconcileShardsWorkersNum calculates how many workers are allowed to be used for concurrent shard reconcile
func (w *worker) getReconcileShardsWorkersNum(shards []*apiChk.ChkShard, opts *common.ReconcileShardsAndHostsOptions) int {
	return 1
}

// reconcileShardsAndHosts reconciles shards and hosts of each shard
func (w *worker) reconcileShardsAndHosts(ctx context.Context, shards []*apiChk.ChkShard) error {
	// Sanity check - CHI has to have shard(s)
	if len(shards) == 0 {
		return nil
	}

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

func (w *worker) reconcileShardWithHosts(ctx context.Context, shard *apiChk.ChkShard) error {
	if err := w.reconcileShard(ctx, shard); err != nil {
		return err
	}
	for replicaIndex := range shard.Hosts {
		host := shard.Hosts[replicaIndex]
		if err := w.reconcileHost(ctx, host); err != nil {
			return err
		}
	}
	return nil
}

// reconcileShard reconciles specified shard, excluding nested replicas
func (w *worker) reconcileShard(ctx context.Context, shard *apiChk.ChkShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	// Add Shard's Service
	service := w.task.Creator().CreateService(interfaces.ServiceShard, shard)
	if service == nil {
		// This is not a problem, ServiceShard may be omitted
		return nil
	}
	err := w.reconcileService(ctx, shard.Runtime.CHK, service)
	if err == nil {
		w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
	}
	return err
}

// reconcileHost reconciles specified ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *api.Host) error {
	var (
		reconcileStatefulSetOpts *statefulset.ReconcileStatefulSetOptions
	)

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()

	// Create artifacts
	w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, false)

	w.a.V(1).
		M(host).F().
		Info("Reconcile PVCs and check possible data loss for host: %s", host.GetName())
	storage.NewStorageReconciler(
		w.task,
		w.c.namer,
		storage.NewStoragePVC(kube.NewPVC(w.c.Client)),
	).ReconcilePVCs(ctx, host, api.DesiredStatefulSet)

	if err := w.reconcileHostStatefulSet(ctx, host, reconcileStatefulSetOpts); err != nil {
		return err
	}
	// Polish all new volumes that operator has to create
	_ = storage.NewStorageReconciler(
		w.task,
		w.c.namer,
		storage.NewStoragePVC(kube.NewPVC(w.c.Client)),
	).ReconcilePVCs(ctx, host, api.DesiredStatefulSet)

	//_ = w.reconcileHostService(ctx, host)

	host.GetReconcileAttributes().UnsetAdd()

	return nil
}

// reconcileCHIAuxObjectsFinal reconciles CHI global objects
func (w *worker) reconcileCRAuxObjectsFinal(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chk).S().P()
	defer w.a.V(2).M(chk).E().P()

	// CHI ConfigMaps with update
	chk.GetRuntime().LockCommonConfig()
	err = w.reconcileConfigMapCommon(ctx, chk)
	chk.GetRuntime().UnlockCommonConfig()
	return err
}

// reconcileCHIConfigMapCommon reconciles all CHI's common ConfigMap
func (w *worker) reconcileConfigMapCommon(
	ctx context.Context,
	chk *apiChk.ClickHouseKeeperInstallation,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.task.Creator().CreateConfigMap(
		interfaces.ConfigMapConfig,
		chkConfig.NewConfigFilesGeneratorOptionsKeeper().SetSettings(chk.GetSpec().GetConfiguration().GetSettings()),
	)
	err := w.reconcileConfigMap(ctx, chk, configMapCommon)
	if err == nil {
		w.task.RegistryReconciled().RegisterConfigMap(configMapCommon.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterConfigMap(configMapCommon.GetObjectMeta())
	}
	return err
}
