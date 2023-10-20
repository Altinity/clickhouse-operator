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
	"gopkg.in/d4l3k/messagediff.v1"
	"math"
	"sync"
	"time"

	coreV1 "k8s.io/api/core/v1"
	policyV1 "k8s.io/api/policy/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopModel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileCHI run reconcile cycle for a CHI
func (w *worker) reconcileCHI(ctx context.Context, old, new *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.logOldAndNew("non-normalized yet (native)", old, new)

	switch {
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-1")
	case w.isGenerationTheSame(old, new):
		w.a.M(new).F().Info("isGenerationTheSame() - nothing to do here, exit")
		return nil
	}

	w.a.M(new).S().P()
	defer w.a.M(new).E().P()

	metricsCHIReconcilesStarted(ctx)
	startTime := time.Now()

	w.a.M(new).F().Info("Changing OLD to Normalized COMPLETED: %s/%s", new.Namespace, new.Name)

	if new.HasAncestor() {
		w.a.M(new).F().Info("has ancestor, use it as a base for reconcile. CHI: %s/%s", new.Namespace, new.Name)
		old = new.GetAncestor()
	} else {
		w.a.M(new).F().Info("has NO ancestor, use empty CHI as a base for reconcile. CHI: %s/%s", new.Namespace, new.Name)
		old = nil
	}

	w.a.M(new).F().Info("Normalized OLD CHI: %s/%s", new.Namespace, new.Name)
	old = w.normalize(old)

	w.a.M(new).F().Info("Normalized NEW CHI: %s/%s", new.Namespace, new.Name)
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
	w.excludeStoppedCHIFromMonitoring(new)
	w.walkHosts(ctx, new, actionPlan)

	if err := w.reconcile(ctx, new); err != nil {
		w.a.WithEvent(new, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(new).
			M(new).F().
			Error("FAILED to reconcile CHI err: %v", err)
		w.markReconcileCompletedUnsuccessfully(ctx, new)
	} else {
		// Post-process added items
		if util.IsContextDone(ctx) {
			log.V(2).Info("task is done")
			return nil
		}
		w.a.V(1).
			WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
			WithStatusAction(new).
			M(new).F().
			Info("remove items scheduled for deletion")
		w.clean(ctx, new)
		w.dropReplicas(ctx, new, actionPlan)
		w.addCHIToMonitoring(new)
		w.waitForIPAddresses(ctx, new)
		w.finalizeReconcileAndMarkCompleted(ctx, new)

		metricsCHIReconcilesCompleted(ctx)
		metricsCHIReconcilesTimings(ctx, time.Now().Sub(startTime).Seconds())
	}

	return nil
}

// ReconcileShardsAndHostsOptionsCtxKeyType specifies type for ReconcileShardsAndHostsOptionsCtxKey
// More details here on why do we need special type
// https://stackoverflow.com/questions/40891345/fix-should-not-use-basic-type-string-as-key-in-context-withvalue-golint
type ReconcileShardsAndHostsOptionsCtxKeyType string

// ReconcileShardsAndHostsOptionsCtxKey specifies name of the key to be used for ReconcileShardsAndHostsOptions
const ReconcileShardsAndHostsOptionsCtxKey ReconcileShardsAndHostsOptionsCtxKeyType = "ReconcileShardsAndHostsOptions"

// reconcile reconciles ClickHouseInstallation
func (w *worker) reconcile(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	counters := chiV1.NewChiHostReconcileAttributesCounters()
	chi.WalkHosts(func(host *chiV1.ChiHost) error {
		counters.Add(host.GetReconcileAttributes())
		return nil
	})

	if counters.GetAdd() > 0 && counters.GetFound() == 0 && counters.GetModify() == 0 && counters.GetRemove() == 0 {
		w.a.V(1).M(chi).Info("Looks like we are just adding hosts to a new CHI. Enabling full fan-out mode. CHI: %s/%s", chi.Namespace, chi.Name)
		ctx = context.WithValue(ctx, ReconcileShardsAndHostsOptionsCtxKey, &ReconcileShardsAndHostsOptions{
			fullFanOut: true,
		})
	}

	return chi.WalkTillError(
		ctx,
		w.reconcileCHIAuxObjectsPreliminary,
		w.reconcileCluster,
		w.reconcileShardsAndHosts,
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

// getHostClickHouseVersion gets host ClickHouse version
func (w *worker) getHostClickHouseVersion(ctx context.Context, host *chiV1.ChiHost, skipNewHost bool) string {
	if skipNewHost && (host.GetReconcileAttributes().GetStatus() == chiV1.ObjectStatusNew) {
		return "not applicable"
	}

	version, err := w.ensureClusterSchemer(host).HostClickHouseVersion(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Warning("Failed to get ClickHouse version on host: %s", host.GetName())
		version = "failed to query"
		return version
	}

	w.a.V(1).M(host).F().Info("Get ClickHouse version on host: %s version: %s", host.GetName(), version)
	host.Version = chiV1.NewCHVersion(version)

	return version
}

type reconcileHostStatefulSetOptions struct {
	forceRecreate bool
}

func (o *reconcileHostStatefulSetOptions) ForceRecreate() bool {
	if o == nil {
		return false
	}
	return o.forceRecreate
}

type reconcileHostStatefulSetOptionsArr []*reconcileHostStatefulSetOptions

// NewReconcileHostStatefulSetOptionsArr creates new reconcileHostStatefulSetOptions array
func NewReconcileHostStatefulSetOptionsArr(opts ...*reconcileHostStatefulSetOptions) (res reconcileHostStatefulSetOptionsArr) {
	return append(res, opts...)
}

// First gets first option
func (a reconcileHostStatefulSetOptionsArr) First() *reconcileHostStatefulSetOptions {
	if len(a) > 0 {
		return a[0]
	}
	return nil
}

// reconcileHostStatefulSet reconciles host's StatefulSet
func (w *worker) reconcileHostStatefulSet(ctx context.Context, host *chiV1.ChiHost, opts ...*reconcileHostStatefulSetOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).M(host).F().S().Info("reconcile StatefulSet start")
	defer log.V(1).M(host).F().E().Info("reconcile StatefulSet end")

	version := w.getHostClickHouseVersion(ctx, host, true)
	host.CurStatefulSet, _ = w.c.getStatefulSet(host, false)

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
	err := w.reconcileStatefulSet(ctx, host, opts...)
	if err == nil {
		w.task.registryReconciled.RegisterStatefulSet(host.DesiredStatefulSet.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterStatefulSet(host.DesiredStatefulSet.ObjectMeta)
		if err == errCRUDIgnore {
			// Pretend nothing happened in case of ignore
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
		w.a.V(1).M(host).F().Info("DONE Reconcile service of the host %s.", host.GetName())
		w.task.registryReconciled.RegisterService(service.ObjectMeta)
	} else {
		w.a.V(1).M(host).F().Warning("FAILED Reconcile service of the host %s.", host.GetName())
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

// getReconcileShardsWorkersNum calculates how many workers are allowed to be used for concurrent shard reconcile
func (w *worker) getReconcileShardsWorkersNum(shards []*chiV1.ChiShard, opts *ReconcileShardsAndHostsOptions) int {
	availableWorkers := float64(chop.Config().Reconcile.Runtime.ReconcileShardsThreadsNumber)
	maxConcurrencyPercent := float64(chop.Config().Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent)
	_100Percent := float64(100)
	shardsNum := float64(len(shards))

	if opts.FullFanOut() {
		// For full fan-out scenarios use all available workers.
		// Always allow at least 1 worker.
		return int(math.Max(availableWorkers, 1))
	}

	// For non-full fan-out scenarios respect .Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent.
	// Always allow at least 1 worker.
	maxAllowedWorkers := math.Max(math.Round((maxConcurrencyPercent/_100Percent)*shardsNum), 1)
	return int(math.Min(availableWorkers, maxAllowedWorkers))
}

// ReconcileShardsAndHostsOptions is and options for reconciler
type ReconcileShardsAndHostsOptions struct {
	fullFanOut bool
}

// FullFanOut gets value
func (o *ReconcileShardsAndHostsOptions) FullFanOut() bool {
	if o == nil {
		return false
	}
	return o.fullFanOut
}

// reconcileShardsAndHosts reconciles shards and hosts of each shard
func (w *worker) reconcileShardsAndHosts(ctx context.Context, shards []*chiV1.ChiShard) error {
	// Sanity check - CHI has to have shard(s)
	if len(shards) == 0 {
		return nil
	}

	// Try to fetch options
	opts, ok := ctx.Value(ReconcileShardsAndHostsOptionsCtxKey).(*ReconcileShardsAndHostsOptions)
	if ok {
		w.a.V(1).Info("found ReconcileShardsAndHostsOptionsCtxKey")
	} else {
		w.a.V(1).Info("not found ReconcileShardsAndHostsOptionsCtxKey, use empty opts")
		opts = &ReconcileShardsAndHostsOptions{}
	}

	// Which shard to start concurrent processing with
	var startShard int
	if opts.FullFanOut() {
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

func (w *worker) reconcileShardWithHosts(ctx context.Context, shard *chiV1.ChiShard) error {
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

// reconcileHost reconciles specified ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *chiV1.ChiHost) error {
	var (
		reconcileHostStatefulSetOpts *reconcileHostStatefulSetOptions
		migrateTableOpts             *migrateTableOptions
	)

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()

	metricsHostReconcilesStarted(ctx)
	startTime := time.Now()

	if host.IsFirst() {
		w.reconcileCHIServicePreliminary(ctx, host.CHI)
		defer w.reconcileCHIServiceFinal(ctx, host.CHI)
	}

	version := w.getHostClickHouseVersion(ctx, host, true)
	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Reconcile Host start. Host: %s ClickHouse version running: %s", host.GetName(), version)

	// Create artifacts
	w.prepareHostStatefulSetWithStatus(ctx, host, false)

	if err := w.excludeHost(ctx, host); err != nil {
		metricsHostReconcilesErrors(ctx)
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 1. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	_ = w.completeQueries(ctx, host)

	if err := w.reconcileHostConfigMap(ctx, host); err != nil {
		metricsHostReconcilesErrors(ctx)
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 2. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	errPVC := w.reconcilePVCs(ctx, host)
	if errPVC == errLostPVCDeleted {
		reconcileHostStatefulSetOpts = &reconcileHostStatefulSetOptions{
			forceRecreate: true,
		}
		migrateTableOpts = &migrateTableOptions{
			forceMigrate: true,
			dropReplica:  true,
		}
	}

	if err := w.reconcileHostStatefulSet(ctx, host, reconcileHostStatefulSetOpts); err != nil {
		metricsHostReconcilesErrors(ctx)
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 3. Host: %s Err: %v", host.GetName(), err)
		return err
	}
	_ = w.reconcilePVCs(ctx, host)

	_ = w.reconcileHostService(ctx, host)

	host.GetReconcileAttributes().UnsetAdd()
	// Sometimes service needs some time to start after creation before being accessible for usage
	time.Sleep(30 * time.Second)
	_ = w.migrateTables(ctx, host, migrateTableOpts)

	if err := w.includeHost(ctx, host); err != nil {
		metricsHostReconcilesErrors(ctx)
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 4. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	version = w.getHostClickHouseVersion(ctx, host, false)
	w.a.V(1).
		WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Reconcile Host completed. Host: %s ClickHouse version running: %s", host.GetName(), version)

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

	metricsHostReconcilesCompleted(ctx)
	metricsHostReconcilesTimings(ctx, time.Now().Sub(startTime).Seconds())

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

// hasService checks whether specified service exists
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
func (w *worker) reconcileStatefulSet(ctx context.Context, host *chiV1.ChiHost, opts ...*reconcileHostStatefulSetOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	newStatefulSet := host.DesiredStatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))

	if host.GetReconcileAttributes().GetStatus() == chiV1.ObjectStatusSame {
		defer w.a.V(2).M(host).F().Info("no need to reconcile the same StatefulSet %s", util.NamespaceNameString(newStatefulSet.ObjectMeta))
		host.CHI.EnsureStatus().HostUnchanged()
		return nil
	}

	// Check whether this object already exists in k8s
	var err error
	host.CurStatefulSet, err = w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	// Report diff to trace
	if host.GetReconcileAttributes().GetStatus() == chiV1.ObjectStatusModified {
		w.a.V(1).M(host).F().Info("Modified StatefulSet %s", util.NamespaceNameString(newStatefulSet.ObjectMeta))
		if diff, equal := messagediff.DeepDiff(host.CurStatefulSet.Spec, host.DesiredStatefulSet.Spec); equal {
			w.a.V(1).M(host).Info("StatefulSet.Spec ARE EQUAL")
		} else {
			w.a.V(1).Info(
				"StatefulSet.Spec ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
				util.MessageDiffItemString("added .spec items", "", diff.Added),
				util.MessageDiffItemString("modified .spec items", "", diff.Modified),
				util.MessageDiffItemString("removed .spec items", "", diff.Removed),
			)
		}
		if diff, equal := messagediff.DeepDiff(host.CurStatefulSet.Labels, host.DesiredStatefulSet.Labels); equal {
			w.a.V(1).M(host).Info("StatefulSet.Labels ARE EQUAL")
		} else {
			if len(host.CurStatefulSet.Labels)+len(host.DesiredStatefulSet.Labels) > 0 {
				w.a.V(1).Info(
					"StatefulSet.Labels ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
					util.MessageDiffItemString("added .labels items", "", diff.Added),
					util.MessageDiffItemString("modified .labels items", "", diff.Modified),
					util.MessageDiffItemString("removed .labels items", "", diff.Removed),
				)
			}
		}
		if diff, equal := messagediff.DeepDiff(host.CurStatefulSet.Annotations, host.DesiredStatefulSet.Annotations); equal {
			w.a.V(1).M(host).Info("StatefulSet.Annotations ARE EQUAL")
		} else {
			if len(host.CurStatefulSet.Annotations)+len(host.DesiredStatefulSet.Annotations) > 0 {
				w.a.V(1).Info(
					"StatefulSet.Annotations ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
					util.MessageDiffItemString("added .annotations items", "", diff.Added),
					util.MessageDiffItemString("modified .annotations items", "", diff.Modified),
					util.MessageDiffItemString("removed .annotations items", "", diff.Removed),
				)
			}
		}
	}

	opt := NewReconcileHostStatefulSetOptionsArr(opts...).First()
	switch {
	case opt.ForceRecreate():
		// Force recreate prevails over all other requests
		w.recreateStatefulSet(ctx, host)
	case host.HasCurStatefulSet():
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

// Comment out PV
// reconcilePersistentVolumes reconciles all PVs of a host
//func (w *worker) reconcilePersistentVolumes(ctx context.Context, host *chiV1.ChiHost) {
//	if util.IsContextDone(ctx) {
//		return
//	}
//
//	w.c.walkPVs(host, func(pv *coreV1.PersistentVolume) {
//		pv = w.task.creator.PreparePersistentVolume(pv, host)
//		_, _ = w.c.updatePersistentVolume(ctx, pv)
//	})
//}

// reconcilePVCs reconciles all PVCs of a host
func (w *worker) reconcilePVCs(ctx context.Context, host *chiV1.ChiHost) (res ErrorPVC) {
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
		if e := w.reconcilePVCFromVolumeMount(ctx, host, volumeMount); e != nil {
			if res == nil {
				res = e
			}
		}
	})

	return
}

func (w *worker) reconcilePVCFromVolumeMount(ctx context.Context, host *chiV1.ChiHost, volumeMount *coreV1.VolumeMount) (res ErrorPVC) {
	// Which PVC are we going to reconcile
	pvc, volumeClaimTemplate, ok := w.fetchOrCreatePVC(ctx, host, volumeMount)
	if !ok {
		// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
		return
	}

	namespace := host.Address.Namespace

	w.a.V(2).M(host).S().Info("reconcile volumeMount (%s/%s/%s/%s)", namespace, host.GetName(), volumeMount.Name, pvc.Name)
	defer w.a.V(2).M(host).E().Info("reconcile volumeMount (%s/%s/%s/%s)", namespace, host.GetName(), volumeMount.Name, pvc.Name)

	if w.deleteLostPVC(ctx, pvc) {
		res = errLostPVCDeleted
		w.a.V(1).M(host).Info("deleted lost PVC (%s/%s/%s/%s)", namespace, host.GetName(), volumeMount.Name, pvc.Name)
		pvc, volumeClaimTemplate, ok = w.fetchOrCreatePVC(ctx, host, volumeMount)
		if !ok {
			// We are not expected to create this PVC, StatefulSet should do this
			return
		}
	}

	pvcReconciled, err := w.reconcilePVC(ctx, pvc, host, volumeClaimTemplate)
	if err != nil {
		w.a.M(host).F().Error("ERROR unable to reconcile PVC(%s/%s) err: %v", namespace, pvc.Name, err)
		w.task.registryFailed.RegisterPVC(pvc.ObjectMeta)
		return
	}

	w.task.registryReconciled.RegisterPVC(pvcReconciled.ObjectMeta)
	return
}

func (w *worker) fetchOrCreatePVC(
	ctx context.Context,
	host *chiV1.ChiHost,
	volumeMount *coreV1.VolumeMount,
) (*coreV1.PersistentVolumeClaim, *chiV1.ChiVolumeClaimTemplate, bool) {
	namespace := host.Address.Namespace
	pvcName, ok := chopModel.CreatePVCNameByVolumeMount(host, volumeMount)
	if !ok {
		// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
		return nil, nil, false
	}
	volumeClaimTemplate, ok := chopModel.GetVolumeClaimTemplate(host, volumeMount)
	if !ok {
		// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
		return nil, nil, false
	}

	pvc, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, newGetOptions())
	if err != nil {
		if apiErrors.IsNotFound(err) {
			// This is not an error per se, means PVC is not created (yet)?
			if w.task.creator.OperatorShouldCreatePVC(host, volumeClaimTemplate) {
				pvc = w.task.creator.CreatePVC(pvcName, host, &volumeClaimTemplate.Spec)
			} else {
				// PVC is not available and we are not expected to create PVC by ourselves
				return nil, nil, false
			}
		} else {
			// In case of any non-NotFound API error - unable to proceed
			w.a.M(host).F().Error("ERROR unable to get PVC(%s/%s) err: %v", namespace, pvcName, err)
			return nil, nil, false
		}
	}

	return pvc, volumeClaimTemplate, true
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
