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
	"errors"
	"math"
	"sync"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileCR runs reconcile cycle for a Custom Resource
func (w *worker) reconcileCR(ctx context.Context, old, new *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	common.LogOldAndNew("non-normalized yet (native)", old, new)

	switch {
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-1")
	case w.isGenerationTheSame(old, new):
		w.a.M(new).F().Info("isGenerationTheSame() - nothing to do here, exit")
		return nil
	}

	w.a.M(new).S().P()
	defer w.a.M(new).E().P()

	metrics.CHIInitZeroValues(ctx, new)
	metrics.CHIReconcilesStarted(ctx, new)
	startTime := time.Now()

	w.a.M(new).F().Info("Changing OLD to Normalized COMPLETED: %s", util.NamespaceNameString(new))

	if new.HasAncestor() {
		w.a.M(new).F().Info("has ancestor, use it as a base for reconcile. CR: %s", util.NamespaceNameString(new))
		old = new.GetAncestorT()
	} else {
		w.a.M(new).F().Info("has NO ancestor, use empty base for reconcile. CR: %s", util.NamespaceNameString(new))
		old = nil
	}

	w.a.M(new).F().Info("Normalized OLD: %s", util.NamespaceNameString(new))
	old = w.normalize(old)

	w.a.M(new).F().Info("Normalized NEW: %s", util.NamespaceNameString(new))
	new = w.normalize(new)

	new.SetAncestor(old)
	common.LogOldAndNew("normalized", old, new)

	actionPlan := action_plan.NewActionPlan(old, new)
	common.LogActionPlan(actionPlan)

	switch {
	case actionPlan.HasActionsToDo():
		w.a.M(new).F().Info("ActionPlan has actions - continue reconcile")
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-2")
	default:
		w.a.M(new).F().Info("ActionPlan has no actions and no need to install finalizer - nothing to do")
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.newTask(new, old)
	w.markReconcileStart(ctx, new, actionPlan)
	w.excludeStoppedCHIFromMonitoring(new)
	w.walkHosts(ctx, new, actionPlan)

	if err := w.reconcile(ctx, new); err != nil {
		// Something went wrong
		w.a.WithEvent(new, a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithError(new).
			M(new).F().
			Error("FAILED to reconcile CR %s, err: %v", util.NamespaceNameString(new), err)
		w.markReconcileCompletedUnsuccessfully(ctx, new, err)
		if errors.Is(err, common.ErrCRUDAbort) {
			metrics.CHIReconcilesAborted(ctx, new)
		}
	} else {
		// Reconcile successful
		// Post-process added items
		if util.IsContextDone(ctx) {
			log.V(2).Info("task is done")
			return nil
		}
		w.clean(ctx, new)
		w.dropReplicas(ctx, new, actionPlan)
		w.addCHIToMonitoring(new)
		w.waitForIPAddresses(ctx, new)
		w.finalizeReconcileAndMarkCompleted(ctx, new)

		metrics.CHIReconcilesCompleted(ctx, new)
		metrics.CHIReconcilesTimings(ctx, new, time.Now().Sub(startTime).Seconds())
	}

	return nil
}

// reconcile reconciles Custom Resource
func (w *worker) reconcile(ctx context.Context, cr *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	if counters := api.NewHostReconcileAttributesCounters().Count(cr); counters.AddOnly() {
		w.a.V(1).M(cr).Info("Enabling full fan-out mode. CR: %s", util.NamespaceNameString(cr))
		ctx = context.WithValue(ctx, common.ReconcileShardsAndHostsOptionsCtxKey, &common.ReconcileShardsAndHostsOptions{
			FullFanOut: true,
		})
	} else {
		w.a.V(1).M(cr).Info("Unable to use full fan-out mode. Counters: %s. CR: %s", counters, util.NamespaceNameString(cr))
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
func (w *worker) reconcileCRAuxObjectsPreliminary(ctx context.Context, cr *api.ClickHouseInstallation) error {
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
		prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceCR)
		if err := w.reconcileService(ctx, cr, service, prevService); err != nil {
			// Service not reconciled
			w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
			return err
		}
		w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
	}

	return nil
}

// reconcileCRAuxObjectsFinal reconciles CR global objects
func (w *worker) reconcileCRAuxObjectsFinal(ctx context.Context, cr *api.ClickHouseInstallation) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	// CR ConfigMaps with update
	cr.GetRuntime().LockCommonConfig()
	err = w.reconcileConfigMapCommon(ctx, cr)
	cr.GetRuntime().UnlockCommonConfig()

	// Wait for all hosts to be included into cluster
	cr.WalkHosts(func(host *api.Host) error {
		if host.ShouldIncludeIntoCluster() {
			_ = w.waitHostInCluster(ctx, host)
		}
		return nil
	})

	return err
}

// reconcileConfigMapCommon reconciles common ConfigMap
func (w *worker) reconcileConfigMapCommon(
	ctx context.Context,
	cr api.ICustomResource,
	options ...*config.FilesGeneratorOptions,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	var opts *config.FilesGeneratorOptions
	if len(options) > 0 {
		opts = options[0]
	}

	// ConfigMap common for all resources in CR
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.task.Creator().CreateConfigMap(interfaces.ConfigMapCommon, opts)
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
		w.a.WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithAction(host.GetCR()).
			WithError(host.GetCR()).
			M(host).F().
			Error("FAILED to reconcile StatefulSet for host: %s", host.GetName())
	}

	return err
}

func (w *worker) getHostSoftwareVersion(ctx context.Context, host *api.Host) string {
	version, _ := w.getHostClickHouseVersion(
		ctx,
		host,
		versionOptions{
			skipNew:             true,
			skipStoppedAncestor: true,
		},
	)
	return version
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
	prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceHost, host.GetAncestor())
	err := w.reconcileService(ctx, host.GetCR(), service, prevService)
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
func (w *worker) reconcileCluster(ctx context.Context, cluster *api.Cluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	// Add Cluster Service
	if service := w.task.Creator().CreateService(interfaces.ServiceCluster, cluster); service != nil {
		prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceCluster, cluster.GetAncestor())
		if err := w.reconcileService(ctx, cluster.GetRuntime().GetCR(), service, prevService); err == nil {
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

	reconcileZookeeperRootPath(cluster)
	return nil
}

func (w *worker) reconcileClusterSecret(ctx context.Context, cluster *api.Cluster) {
	// Add cluster's Auto Secret
	if cluster.Secret.Source() == api.ClusterSecretSourceAuto {
		if secret := w.task.Creator().CreateClusterSecret(w.c.namer.Name(interfaces.NameClusterAutoSecret, cluster)); secret != nil {
			if err := w.reconcileSecret(ctx, cluster.Runtime.CHI, secret); err == nil {
				w.task.RegistryReconciled().RegisterSecret(secret.GetObjectMeta())
			} else {
				w.task.RegistryFailed().RegisterSecret(secret.GetObjectMeta())
			}
		}
	}
}

// getReconcileShardsWorkersNum calculates how many workers are allowed to be used for concurrent shard reconcile
func (w *worker) getReconcileShardsWorkersNum(shards []*api.ChiShard, opts *common.ReconcileShardsAndHostsOptions) int {
	availableWorkers := float64(chop.Config().Reconcile.Runtime.ReconcileShardsThreadsNumber)
	maxConcurrencyPercent := float64(chop.Config().Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent)
	_100Percent := float64(100)
	shardsNum := float64(len(shards))

	if opts.FullFanOut {
		// For full fan-out scenarios use all available workers.
		// Always allow at least 1 worker.
		return int(math.Max(availableWorkers, 1))
	}

	// For non-full fan-out scenarios respect .Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent.
	// Always allow at least 1 worker.
	maxAllowedWorkers := math.Max(math.Round((maxConcurrencyPercent/_100Percent)*shardsNum), 1)
	return int(math.Min(availableWorkers, maxAllowedWorkers))
}

func (w *worker) reconcileShardsAndHostsFetchOpts(ctx context.Context) *common.ReconcileShardsAndHostsOptions {
	// Try to fetch options
	if opts, ok := ctx.Value(common.ReconcileShardsAndHostsOptionsCtxKey).(*common.ReconcileShardsAndHostsOptions); ok {
		w.a.V(1).Info("found ReconcileShardsAndHostsOptionsCtxKey")
		return opts
	} else {
		w.a.V(1).Info("not found ReconcileShardsAndHostsOptionsCtxKey, use empty opts")
		return &common.ReconcileShardsAndHostsOptions{}
	}
}

// reconcileShardsAndHosts reconciles shards and hosts of each shard
func (w *worker) reconcileShardsAndHosts(ctx context.Context, shards []*api.ChiShard) error {
	// Sanity check - has to have shard(s)
	if len(shards) == 0 {
		return nil
	}

	log.V(1).F().S().Info("reconcileShardsAndHosts start")
	defer log.V(1).F().E().Info("reconcileShardsAndHosts end")

	opts := w.reconcileShardsAndHostsFetchOpts(ctx)

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

		// Since shard with 0 index is already done, we'll proceed concurrently starting with the 1-st
		startShard = 1
	}

	// Process shards using specified concurrency level while maintaining specified max concurrency percentage.
	// Loop over shards.
	workersNum := w.getReconcileShardsWorkersNum(shards, opts)
	w.a.V(1).Info("Starting rest of shards on workers. Workers num: %d", workersNum)
	if err := w.runConcurrently(ctx, workersNum, startShard, shards[startShard:]); err != nil {
		w.a.V(1).Info("Finished with ERROR rest of shards on workers: %d, err: %v", workersNum, err)
		return err
	}
	w.a.V(1).Info("Finished successfully rest of shards on workers: %d", workersNum)
	return nil
}

func (w *worker) runConcurrently(ctx context.Context, workersNum int, startShardIndex int, shards []*api.ChiShard) error {
	if len(shards) == 0 {
		return nil
	}

	type shardReconcile struct {
		shard *api.ChiShard
		index int
	}

	ch := make(chan *shardReconcile)
	wg := sync.WaitGroup{}

	// Launch tasks feeder
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)
		for i, shard := range shards {
			ch <- &shardReconcile{
				shard,
				startShardIndex + i,
			}
		}
	}()

	// Launch workers
	var err error
	var errLock sync.Mutex
	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rq := range ch {
				w.a.V(1).Info("Starting shard index: %d on worker", rq.index)
				if e := w.reconcileShardWithHosts(ctx, rq.shard); e != nil {
					errLock.Lock()
					err = e
					errLock.Unlock()
				}
			}
		}()
	}

	w.a.V(1).Info("Starting to wait shards from index: %d on workers.", startShardIndex)
	wg.Wait()
	w.a.V(1).Info("Finished to wait shards from index: %d on workers.", startShardIndex)
	return err
}

func (w *worker) runConcurrentlyInBatches(ctx context.Context, workersNum int, start int, shards []*api.ChiShard) error {
	for startShardIndex := 0; startShardIndex < len(shards); startShardIndex += workersNum {
		endShardIndex := util.IncTopped(startShardIndex, workersNum, len(shards))
		concurrentlyProcessedShards := shards[startShardIndex:endShardIndex]
		w.a.V(1).Info("Starting shards from index: %d on workers. Shards indexes [%d:%d)", start+startShardIndex, start+startShardIndex, start+endShardIndex)

		// Processing error protected with mutex
		var err error
		var errLock sync.Mutex

		wg := sync.WaitGroup{}
		wg.Add(len(concurrentlyProcessedShards))
		// Launch shard concurrent processing
		for j := range concurrentlyProcessedShards {
			shard := concurrentlyProcessedShards[j]
			w.a.V(1).Info("Starting shard on worker. Shard index: %d", start+startShardIndex+j)
			go func() {
				defer wg.Done()
				w.a.V(1).Info("Starting shard on goroutine. Shard index: %d", start+startShardIndex+j)
				if e := w.reconcileShardWithHosts(ctx, shard); e != nil {
					errLock.Lock()
					err = e
					errLock.Unlock()
				}
				w.a.V(1).Info("Finished shard on goroutine. Shard index: %d", start+startShardIndex+j)
			}()
		}
		w.a.V(1).Info("Starting to wait shards from index: %d on workers. Shards indexes [%d:%d)", start+startShardIndex, start+startShardIndex, start+endShardIndex)
		wg.Wait()
		w.a.V(1).Info("Finished to wait shards from index: %d on workers. Shards indexes [%d:%d)", start+startShardIndex, start+startShardIndex, start+endShardIndex)
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
	// Add Shard's Service
	service := w.task.Creator().CreateService(interfaces.ServiceShard, shard)
	if service == nil {
		// This is not a problem, ServiceShard may be omitted
		return nil
	}
	prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceShard, shard.GetAncestor())
	err := w.reconcileService(ctx, shard.GetRuntime().GetCR(), service, prevService)
	if err == nil {
		w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
	}
	return err
}

// reconcileHost reconciles specified ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()

	metrics.HostReconcilesStarted(ctx, host.GetCR())
	startTime := time.Now()

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
		WithEvent(host.GetCR(), a.EventActionProgress, a.EventReasonProgressHostsCompleted).
		WithAction(host.GetCR()).
		M(host).F().
		Info("[now: %s] %s: %d of %d", now, a.EventReasonProgressHostsCompleted, hostsCompleted, hostsCount)

	_ = w.c.updateCRObjectStatus(ctx, host.GetCR(), types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	})

	metrics.HostReconcilesCompleted(ctx, host.GetCR())
	metrics.HostReconcilesTimings(ctx, host.GetCR(), time.Now().Sub(startTime).Seconds())

	return nil
}

// reconcileHostPrepare reconciles specified ClickHouse host
func (w *worker) reconcileHostPrepare(ctx context.Context, host *api.Host) error {
	// Check whether ClickHouse is running and accessible and what version is available

	// alz 18.12.2024: Host may be down or not accessible, so no reason to wait
	//	if version, err := w.getHostClickHouseVersion(ctx, host, versionOptions{skipNew: true, skipStoppedAncestor: true}); err == nil {
	//		w.a.V(1).
	//			WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileStarted).
	//			WithAction(host.GetCR()).
	//			M(host).F().
	//			Info("Reconcile Host start. Host: %s ClickHouse version running: %s", host.GetName(), version)
	//	} else {
	//		w.a.V(1).
	//			WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileStarted).
	//			WithAction(host.GetCR()).
	//			M(host).F().
	//			Warning("Reconcile Host start. Host: %s Failed to get ClickHouse version: %s", host.GetName(), version)
	//	}

	if w.excludeHost(ctx, host) {
		// Need to wait to complete queries only in case host is excluded from the cluster
		// In case host is not excluded from the cluster queries would continue to be started on the host
		// and there is no reason to wait for queries to complete. We may wait endlessly.
		_ = w.completeQueries(ctx, host)
	}

	return nil
}

// reconcileHostMain reconciles specified ClickHouse host
func (w *worker) reconcileHostMain(ctx context.Context, host *api.Host) error {
	var (
		stsReconcileOpts *statefulset.ReconcileOptions
		migrateTableOpts *migrateTableOptions
	)

	if err := w.reconcileConfigMapHost(ctx, host); err != nil {
		metrics.HostReconcilesErrors(ctx, host.GetCR())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 2. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	w.setHasData(host)

	w.a.V(1).
		M(host).F().
		Info("Reconcile PVCs and check possible data loss for host: %s", host.GetName())

	if storage.ErrIsDataLoss(w.reconcilePVCs(ctx, host)) {
		// In case of data loss detection on existing volumes, we need to:
		// 1. recreate StatefulSet
		// 2. run tables migration again
		stsReconcileOpts = stsReconcileOpts.SetForceRecreate()
		migrateTableOpts = &migrateTableOptions{
			forceMigrate: true,
			dropReplica:  true,
		}
		w.a.V(1).
			M(host).F().
			Info("Data loss detected for host: %s. Will do force migrate", host.GetName())
	}

	if err := w.reconcileHostStatefulSet(ctx, host, stsReconcileOpts); err != nil {
		metrics.HostReconcilesErrors(ctx, host.GetCR())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 3. Host: %s Err: %v", host.GetName(), err)
		return err
	}
	// Polish all new volumes that operator has to create
	_ = w.reconcilePVCs(ctx, host)

	_ = w.reconcileHostService(ctx, host)

	// Prepare for tables migration.
	// Sometimes service needs some time to start after creation|modification before being accessible for usage
	// Check whether ClickHouse is running and accessible and what version is available.
	if version, err := w.pollHostForClickHouseVersion(ctx, host); err == nil {
		w.a.V(1).
			M(host).F().
			Info("Check host for ClickHouse availability before migrating tables. Host: %s ClickHouse version running: %s", host.GetName(), version)
	} else {
		w.a.V(1).
			M(host).F().
			Warning("Check host for ClickHouse availability before migrating tables. Host: %s Failed to get ClickHouse version: %s", host.GetName(), version)
	}
	_ = w.migrateTables(ctx, host, migrateTableOpts)

	return nil
}

func (w *worker) reconcilePVCs(ctx context.Context, host *api.Host) storage.ErrorDataPersistence {
	return storage.NewStorageReconciler(
		w.task,
		w.c.namer,
		storage.NewStoragePVC(w.c.kube.Storage()),
	).ReconcilePVCs(ctx, host, api.DesiredStatefulSet)
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

	// Ensure host is running and accessible and what version is available.
	// Sometimes service needs some time to start after creation|modification before being accessible for usage
	if version, err := w.pollHostForClickHouseVersion(ctx, host); err == nil {
		w.a.V(1).
			WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Reconcile Host completed. Host: %s ClickHouse version running: %s", host.GetName(), version)
	} else {
		w.a.V(1).
			WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Warning("Reconcile Host completed. Host: %s Failed to get ClickHouse version: %s", host.GetName(), version)
	}

	return nil
}
