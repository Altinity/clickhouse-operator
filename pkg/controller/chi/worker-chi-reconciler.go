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
	"fmt"
	"math"
	"sync"
	"time"

	"gopkg.in/d4l3k/messagediff.v1"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileCHI run reconcile cycle for a CHI
func (w *worker) reconcileCHI(ctx context.Context, old, new *api.ClickHouseInstallation) error {
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

	metricsCHIInitZeroValues(ctx, new)
	metricsCHIReconcilesStarted(ctx, new)
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

	actionPlan := model.NewActionPlan(old, new)
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
		// Something went wrong
		w.a.WithEvent(new, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(new).
			M(new).F().
			Error("FAILED to reconcile CHI err: %v", err)
		w.markReconcileCompletedUnsuccessfully(ctx, new, err)
		if errors.Is(err, errCRUDAbort) {
			metricsCHIReconcilesAborted(ctx, new)
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

		metricsCHIReconcilesCompleted(ctx, new)
		metricsCHIReconcilesTimings(ctx, new, time.Now().Sub(startTime).Seconds())
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
func (w *worker) reconcile(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	counters := api.NewChiHostReconcileAttributesCounters()
	chi.WalkHosts(func(host *api.ChiHost) error {
		counters.Add(host.GetReconcileAttributes())
		return nil
	})

	if counters.GetAdd() > 0 && counters.GetFound() == 0 && counters.GetModify() == 0 && counters.GetRemove() == 0 {
		w.a.V(1).M(chi).Info(
			"Looks like we are just adding hosts to a new CHI. Enabling full fan-out mode. CHI: %s/%s",
			chi.Namespace, chi.Name)
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
func (w *worker) reconcileCHIAuxObjectsPreliminary(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// CHI common ConfigMap without added hosts
	chi.EnsureRuntime().LockCommonConfig()
	if err := w.reconcileCHIConfigMapCommon(ctx, chi, w.options()); err != nil {
		w.a.F().Error("failed to reconcile config map common. err: %v", err)
	}
	chi.EnsureRuntime().UnlockCommonConfig()

	// 3. CHI users ConfigMap
	if err := w.reconcileCHIConfigMapUsers(ctx, chi); err != nil {
		w.a.F().Error("failed to reconcile config map users. err: %v", err)
	}

	return nil
}

// reconcileCHIServicePreliminary runs first stage of CHI reconcile process
func (w *worker) reconcileCHIServicePreliminary(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if chi.IsStopped() {
		// Stopped CHI must have no entry point
		_ = w.c.deleteServiceCHI(ctx, chi)
	}
	return nil
}

// reconcileCHIServiceFinal runs second stage of CHI reconcile process
func (w *worker) reconcileCHIServiceFinal(ctx context.Context, chi *api.ClickHouseInstallation) error {
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
func (w *worker) reconcileCHIAuxObjectsFinal(ctx context.Context, chi *api.ClickHouseInstallation) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// CHI ConfigMaps with update
	chi.EnsureRuntime().LockCommonConfig()
	err = w.reconcileCHIConfigMapCommon(ctx, chi, nil)
	chi.EnsureRuntime().UnlockCommonConfig()
	return err
}

// reconcileCHIConfigMapCommon reconciles all CHI's common ConfigMap
func (w *worker) reconcileCHIConfigMapCommon(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	options *model.ClickHouseConfigFilesGeneratorOptions,
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
func (w *worker) reconcileCHIConfigMapUsers(ctx context.Context, chi *api.ClickHouseInstallation) error {
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
func (w *worker) reconcileHostConfigMap(ctx context.Context, host *api.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// ConfigMap for a host
	configMap := w.task.creator.CreateConfigMapHost(host)
	err := w.reconcileConfigMap(ctx, host.GetCHI(), configMap)
	if err == nil {
		w.task.registryReconciled.RegisterConfigMap(configMap.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterConfigMap(configMap.ObjectMeta)
		return err
	}

	return nil
}

const unknownVersion = "failed to query"

type versionOptions struct {
	skipNew             bool
	skipStopped         bool
	skipStoppedAncestor bool
}

func (opts versionOptions) shouldSkip(host *api.ChiHost) (bool, string) {
	if opts.skipNew && (host.IsNewOne()) {
		return true, "host is a new one, version is not not applicable"
	}

	if opts.skipStopped && host.IsStopped() {
		return true, "host is stopped, version is not applicable"
	}

	if opts.skipStoppedAncestor && host.GetAncestor().IsStopped() {
		return true, "host ancestor is stopped, version is not applicable"
	}

	return false, ""
}

// getHostClickHouseVersion gets host ClickHouse version
func (w *worker) getHostClickHouseVersion(ctx context.Context, host *api.ChiHost, opts versionOptions) (string, error) {
	if skip, description := opts.shouldSkip(host); skip {
		return description, nil
	}

	version, err := w.ensureClusterSchemer(host).HostClickHouseVersion(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Warning("Failed to get ClickHouse version on host: %s", host.GetName())
		return unknownVersion, err
	}

	w.a.V(1).M(host).F().Info("Get ClickHouse version on host: %s version: %s", host.GetName(), version)
	host.Runtime.Version = swversion.NewSoftWareVersion(version)

	return version, nil
}

func (w *worker) pollHostForClickHouseVersion(ctx context.Context, host *api.ChiHost) (version string, err error) {
	err = w.c.pollHost(
		ctx,
		host,
		nil,
		func(_ctx context.Context, _host *api.ChiHost) bool {
			var e error
			version, e = w.getHostClickHouseVersion(_ctx, _host, versionOptions{skipStopped: true})
			if e == nil {
				return true
			}
			w.a.V(1).M(host).F().Warning("Host is NOT alive: %s ", host.GetName())
			return false
		},
	)
	return
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
func (w *worker) reconcileHostStatefulSet(ctx context.Context, host *api.ChiHost, opts ...*reconcileHostStatefulSetOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).M(host).F().S().Info("reconcile StatefulSet start")
	defer log.V(1).M(host).F().E().Info("reconcile StatefulSet end")

	version, _ := w.getHostClickHouseVersion(ctx, host, versionOptions{skipNew: true, skipStoppedAncestor: true})
	host.Runtime.CurStatefulSet, _ = w.c.getStatefulSet(host, false)

	w.a.V(1).M(host).F().Info("Reconcile host: %s. ClickHouse version: %s", host.GetName(), version)
	// In case we have to force-restart host
	// We'll do it via replicas: 0 in StatefulSet.
	if w.shouldForceRestartHost(host) {
		w.a.V(1).M(host).F().Info("Reconcile host: %s. Shutting host down due to force restart", host.GetName())
		w.prepareHostStatefulSetWithStatus(ctx, host, true)
		_ = w.reconcileStatefulSet(ctx, host, false)
		metricsHostReconcilesRestart(ctx, host.GetCHI())
		// At this moment StatefulSet has 0 replicas.
		// First stage of RollingUpdate completed.
	}

	// We are in place, where we can  reconcile StatefulSet to desired configuration.
	w.a.V(1).M(host).F().Info("Reconcile host: %s. Reconcile StatefulSet", host.GetName())
	w.prepareHostStatefulSetWithStatus(ctx, host, false)
	err := w.reconcileStatefulSet(ctx, host, true, opts...)
	if err == nil {
		w.task.registryReconciled.RegisterStatefulSet(host.Runtime.DesiredStatefulSet.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterStatefulSet(host.Runtime.DesiredStatefulSet.ObjectMeta)
		if err == errCRUDIgnore {
			// Pretend nothing happened in case of ignore
			err = nil
		}

		host.GetCHI().EnsureStatus().HostFailed()
		w.a.WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(host.GetCHI()).
			WithStatusError(host.GetCHI()).
			M(host).F().
			Error("FAILED to reconcile StatefulSet for host: %s", host.GetName())
	}

	return err
}

// reconcileHostService reconciles host's Service
func (w *worker) reconcileHostService(ctx context.Context, host *api.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}
	service := w.task.creator.CreateServiceHost(host)
	if service == nil {
		// This is not a problem, service may be omitted
		return nil
	}
	err := w.reconcileService(ctx, host.GetCHI(), service)
	if err == nil {
		w.a.V(1).M(host).F().Info("DONE Reconcile service of the host: %s", host.GetName())
		w.task.registryReconciled.RegisterService(service.ObjectMeta)
	} else {
		w.a.V(1).M(host).F().Warning("FAILED Reconcile service of the host: %s", host.GetName())
		w.task.registryFailed.RegisterService(service.ObjectMeta)
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

	// Add ChkCluster's Service
	if service := w.task.creator.CreateServiceCluster(cluster); service != nil {
		if err := w.reconcileService(ctx, cluster.Runtime.CHI, service); err == nil {
			w.task.registryReconciled.RegisterService(service.ObjectMeta)
		} else {
			w.task.registryFailed.RegisterService(service.ObjectMeta)
		}
	}

	// Add ChkCluster's Auto Secret
	if cluster.Secret.Source() == api.ClusterSecretSourceAuto {
		if secret := w.task.creator.CreateClusterSecret(model.CreateClusterAutoSecretName(cluster)); secret != nil {
			if err := w.reconcileSecret(ctx, cluster.Runtime.CHI, secret); err == nil {
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
func (w *worker) getReconcileShardsWorkersNum(shards []*api.ChiShard, opts *ReconcileShardsAndHostsOptions) int {
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
func (w *worker) reconcileShardsAndHosts(ctx context.Context, shards []*api.ChiShard) error {
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

func (w *worker) reconcileShardWithHosts(ctx context.Context, shard *api.ChiShard) error {
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
func (w *worker) reconcileShard(ctx context.Context, shard *api.ChiShard) error {
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
	err := w.reconcileService(ctx, shard.Runtime.CHI, service)
	if err == nil {
		w.task.registryReconciled.RegisterService(service.ObjectMeta)
	} else {
		w.task.registryFailed.RegisterService(service.ObjectMeta)
	}
	return err
}

// reconcileHost reconciles specified ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *api.ChiHost) error {
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

	metricsHostReconcilesStarted(ctx, host.GetCHI())
	startTime := time.Now()

	if host.IsFirst() {
		w.reconcileCHIServicePreliminary(ctx, host.GetCHI())
		defer w.reconcileCHIServiceFinal(ctx, host.GetCHI())
	}

	// Check whether ClickHouse is running and accessible and what version is available
	if version, err := w.getHostClickHouseVersion(ctx, host, versionOptions{skipNew: true, skipStoppedAncestor: true}); err == nil {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileStarted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Info("Reconcile Host start. Host: %s ClickHouse version running: %s", host.GetName(), version)
	} else {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileStarted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Warning("Reconcile Host start. Host: %s Failed to get ClickHouse version: %s", host.GetName(), version)
	}

	// Create artifacts
	w.prepareHostStatefulSetWithStatus(ctx, host, false)

	if err := w.excludeHost(ctx, host); err != nil {
		metricsHostReconcilesErrors(ctx, host.GetCHI())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 1. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	_ = w.completeQueries(ctx, host)

	if err := w.reconcileHostConfigMap(ctx, host); err != nil {
		metricsHostReconcilesErrors(ctx, host.GetCHI())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 2. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	w.a.V(1).
		M(host).F().
		Info("Reconcile PVCs and check possible data loss for host: %s", host.GetName())
	if errIsDataLoss(w.reconcilePVCs(ctx, host, api.DesiredStatefulSet)) {
		// In case of data loss detection on existing volumes, we need to:
		// 1. recreate StatefulSet
		// 2. run tables migration again
		reconcileHostStatefulSetOpts = &reconcileHostStatefulSetOptions{
			forceRecreate: true,
		}
		migrateTableOpts = &migrateTableOptions{
			forceMigrate: true,
			dropReplica:  true,
		}
		w.a.V(1).
			M(host).F().
			Info("Data loss detected for host: %s. Will do force migrate", host.GetName())
	}

	if err := w.reconcileHostStatefulSet(ctx, host, reconcileHostStatefulSetOpts); err != nil {
		metricsHostReconcilesErrors(ctx, host.GetCHI())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 3. Host: %s Err: %v", host.GetName(), err)
		return err
	}
	// Polish all new volumes that operator has to create
	_ = w.reconcilePVCs(ctx, host, api.DesiredStatefulSet)

	_ = w.reconcileHostService(ctx, host)

	host.GetReconcileAttributes().UnsetAdd()

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

	if err := w.includeHost(ctx, host); err != nil {
		metricsHostReconcilesErrors(ctx, host.GetCHI())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 4. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	// Ensure host is running and accessible and what version is available.
	// Sometimes service needs some time to start after creation|modification before being accessible for usage
	if version, err := w.pollHostForClickHouseVersion(ctx, host); err == nil {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileCompleted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Info("Reconcile Host completed. Host: %s ClickHouse version running: %s", host.GetName(), version)
	} else {
		w.a.V(1).
			WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileCompleted).
			WithStatusAction(host.GetCHI()).
			M(host).F().
			Warning("Reconcile Host completed. Host: %s Failed to get ClickHouse version: %s", host.GetName(), version)
	}

	now := time.Now()
	hostsCompleted := 0
	hostsCount := 0
	host.GetCHI().EnsureStatus().HostCompleted()
	if host.GetCHI() != nil && host.GetCHI().Status != nil {
		hostsCompleted = host.GetCHI().Status.GetHostsCompletedCount()
		hostsCount = host.GetCHI().Status.GetHostsCount()
	}
	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionProgress, eventReasonProgressHostsCompleted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("[now: %s] %s: %d of %d", now, eventReasonProgressHostsCompleted, hostsCompleted, hostsCount)

	_ = w.c.updateCHIObjectStatus(ctx, host.GetCHI(), UpdateCHIStatusOptions{
		CopyCHIStatusOptions: api.CopyCHIStatusOptions{
			MainFields: true,
		},
	})

	metricsHostReconcilesCompleted(ctx, host.GetCHI())
	metricsHostReconcilesTimings(ctx, host.GetCHI(), time.Now().Sub(startTime).Seconds())

	return nil
}

// reconcilePDB reconciles PodDisruptionBudget
func (w *worker) reconcilePDB(ctx context.Context, cluster *api.Cluster, pdb *policy.PodDisruptionBudget) error {
	cur, err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Get(ctx, pdb.Name, controller.NewGetOptions())
	switch {
	case err == nil:
		pdb.ResourceVersion = cur.ResourceVersion
		_, err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Update(ctx, pdb, controller.NewUpdateOptions())
		if err == nil {
			log.V(1).Info("PDB updated: %s/%s", pdb.Namespace, pdb.Name)
		} else {
			log.Error("FAILED to update PDB: %s/%s err: %v", pdb.Namespace, pdb.Name, err)
			return nil
		}
	case apiErrors.IsNotFound(err):
		_, err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Create(ctx, pdb, controller.NewCreateOptions())
		if err == nil {
			log.V(1).Info("PDB created: %s/%s", pdb.Namespace, pdb.Name)
		} else {
			log.Error("FAILED create PDB: %s/%s err: %v", pdb.Namespace, pdb.Name, err)
			return err
		}
	default:
		log.Error("FAILED get PDB: %s/%s err: %v", pdb.Namespace, pdb.Name, err)
		return err
	}

	return nil
}

// reconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) reconcileConfigMap(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	configMap *core.ConfigMap,
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
func (w *worker) hasService(ctx context.Context, chi *api.ClickHouseInstallation, service *core.Service) bool {
	// Check whether this object already exists
	curService, _ := w.c.getService(service)
	return curService != nil
}

// reconcileService reconciles core.Service
func (w *worker) reconcileService(ctx context.Context, chi *api.ClickHouseInstallation, service *core.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().Info(service.Name)
	defer w.a.V(2).M(chi).E().Info(service.Name)

	// Check whether this object already exists
	curService, err := w.c.getService(service)

	if curService != nil {
		// We have the Service - try to update it
		w.a.V(1).M(chi).F().Info("Service found: %s/%s. Will try to update", service.Namespace, service.Name)
		err = w.updateService(ctx, chi, curService, service)
	}

	if err != nil {
		if apiErrors.IsNotFound(err) {
			// The Service is either not found or not updated. Try to recreate it
			w.a.V(1).M(chi).F().Info("Service: %s/%s not found. err: %v", service.Namespace, service.Name, err)
		} else {
			// The Service is either not found or not updated. Try to recreate it
			w.a.WithEvent(chi, eventActionUpdate, eventReasonUpdateFailed).
				WithStatusAction(chi).
				WithStatusError(chi).
				M(chi).F().
				Error("Update Service: %s/%s failed with error: %v", service.Namespace, service.Name, err)
		}

		_ = w.c.deleteServiceIfExists(ctx, service.Namespace, service.Name)
		err = w.createService(ctx, chi, service)
	}

	if err == nil {
		w.a.V(1).M(chi).F().Info("Service reconcile successful: %s/%s", service.Namespace, service.Name)
	} else {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("FAILED to reconcile Service: %s/%s CHI: %s ", service.Namespace, service.Name, chi.Name)
	}

	return err
}

// reconcileSecret reconciles core.Secret
func (w *worker) reconcileSecret(ctx context.Context, chi *api.ClickHouseInstallation, secret *core.Secret) error {
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

func (w *worker) dumpStatefulSetDiff(host *api.ChiHost, cur, new *apps.StatefulSet) {
	if cur == nil {
		w.a.V(1).M(host).Info("Cur StatefulSet is not available, nothing to compare to")
		return
	}
	if new == nil {
		w.a.V(1).M(host).Info("New StatefulSet is not available, nothing to compare to")
		return
	}

	if diff, equal := messagediff.DeepDiff(cur.Spec, new.Spec); equal {
		w.a.V(1).M(host).Info("StatefulSet.Spec ARE EQUAL")
	} else {
		w.a.V(1).Info(
			"StatefulSet.Spec ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
			util.MessageDiffItemString("added .spec items", "none", "", diff.Added),
			util.MessageDiffItemString("modified .spec items", "none", "", diff.Modified),
			util.MessageDiffItemString("removed .spec items", "none", "", diff.Removed),
		)
	}
	if diff, equal := messagediff.DeepDiff(cur.Labels, new.Labels); equal {
		w.a.V(1).M(host).Info("StatefulSet.Labels ARE EQUAL")
	} else {
		if len(cur.Labels)+len(new.Labels) > 0 {
			w.a.V(1).Info(
				"StatefulSet.Labels ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
				util.MessageDiffItemString("added .labels items", "none", "", diff.Added),
				util.MessageDiffItemString("modified .labels items", "none", "", diff.Modified),
				util.MessageDiffItemString("removed .labels items", "none", "", diff.Removed),
			)
		}
	}
	if diff, equal := messagediff.DeepDiff(cur.Annotations, new.Annotations); equal {
		w.a.V(1).M(host).Info("StatefulSet.Annotations ARE EQUAL")
	} else {
		if len(cur.Annotations)+len(new.Annotations) > 0 {
			w.a.V(1).Info(
				"StatefulSet.Annotations ARE DIFFERENT:\nadded:\n%s\nmodified:\n%s\nremoved:\n%s",
				util.MessageDiffItemString("added .annotations items", "none", "", diff.Added),
				util.MessageDiffItemString("modified .annotations items", "none", "", diff.Modified),
				util.MessageDiffItemString("removed .annotations items", "none", "", diff.Removed),
			)
		}
	}
}

// reconcileStatefulSet reconciles StatefulSet of a host
func (w *worker) reconcileStatefulSet(
	ctx context.Context,
	host *api.ChiHost,
	register bool,
	opts ...*reconcileHostStatefulSetOptions,
) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	newStatefulSet := host.Runtime.DesiredStatefulSet

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))

	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusSame {
		w.a.V(2).M(host).F().Info("No need to reconcile THE SAME StatefulSet: %s", util.NamespaceNameString(newStatefulSet.ObjectMeta))
		if register {
			host.GetCHI().EnsureStatus().HostUnchanged()
			_ = w.c.updateCHIObjectStatus(ctx, host.GetCHI(), UpdateCHIStatusOptions{
				CopyCHIStatusOptions: api.CopyCHIStatusOptions{
					MainFields: true,
				},
			})
		}
		return nil
	}

	// Check whether this object already exists in k8s
	host.Runtime.CurStatefulSet, err = w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	// Report diff to trace
	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusModified {
		w.a.V(1).M(host).F().Info("Need to reconcile MODIFIED StatefulSet: %s", util.NamespaceNameString(newStatefulSet.ObjectMeta))
		w.dumpStatefulSetDiff(host, host.Runtime.CurStatefulSet, newStatefulSet)
	}

	opt := NewReconcileHostStatefulSetOptionsArr(opts...).First()
	switch {
	case opt.ForceRecreate():
		// Force recreate prevails over all other requests
		w.recreateStatefulSet(ctx, host, register)
	default:
		// We have (or had in the past) StatefulSet - try to update|recreate it
		err = w.updateStatefulSet(ctx, host, register)
	}

	if apiErrors.IsNotFound(err) {
		// StatefulSet not found - even during Update process - try to create it
		err = w.createStatefulSet(ctx, host, register)
	}

	// Host has to know current StatefulSet and Pod
	host.Runtime.CurStatefulSet, _ = w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	return err
}

// Comment out PV
// reconcilePersistentVolumes reconciles all PVs of a host
//func (w *worker) reconcilePersistentVolumes(ctx context.Context, host *api.ChiHost) {
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
func (w *worker) reconcilePVCs(ctx context.Context, host *api.ChiHost, which api.WhichStatefulSet) (res ErrorDataPersistence) {
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

func isLostPVC(pvc *core.PersistentVolumeClaim, isJustCreated bool, host *api.ChiHost) bool {
	if !model.HostHasTablesCreated(host) {
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

func (w *worker) reconcilePVCFromVolumeMount(
	ctx context.Context,
	host *api.ChiHost,
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
	if isLostPVC(pvc, isModelCreated, host) {
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
		w.task.registryReconciled.RegisterPVC(pvcReconciled.ObjectMeta)
	default:
		w.task.registryFailed.RegisterPVC(pvc.ObjectMeta)
		w.a.M(host).F().Error("Unable to reconcile PVC: %s/%s err: %v", pvc.Namespace, pvc.Name, err)
	}

	// It still may return data loss errors
	return res
}

func (w *worker) fetchPVC(
	ctx context.Context,
	host *api.ChiHost,
	volumeMount *core.VolumeMount,
) (
	pvc *core.PersistentVolumeClaim,
	vct *api.VolumeClaimTemplate,
	isModelCreated bool,
	err error,
) {
	namespace := host.Runtime.Address.Namespace

	// Try to find volumeClaimTemplate that is used to build this mounted volume
	// Volume mount can point not only to volume claim, but also to other entities, such as ConfigMap, for example.
	pvcName, ok := model.CreatePVCNameByVolumeMount(host, volumeMount)
	if !ok {
		// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
		return nil, nil, false, fmt.Errorf("unable to make PVC name from volume mount")
	}
	volumeClaimTemplate, ok := model.GetVolumeClaimTemplate(host, volumeMount)
	if !ok {
		// No this is not a reference to VolumeClaimTemplate, it may be reference to ConfigMap
		return nil, nil, false, fmt.Errorf("unable to find VolumeClaimTemplate from volume mount")
	}

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

	if creator.OperatorShouldCreatePVC(host, volumeClaimTemplate) {
		// Operator is in charge of PVCs
		// Create PVC model.
		pvc = w.task.creator.CreatePVC(pvcName, host, &volumeClaimTemplate.Spec)
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
	host *api.ChiHost,
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

	w.applyPVCResourcesRequests(pvc, template)
	pvc = w.task.creator.PreparePersistentVolumeClaim(pvc, host, template)
	return w.c.updatePersistentVolumeClaim(ctx, pvc)
}
