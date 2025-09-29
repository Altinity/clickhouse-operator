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
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileCR runs reconcile cycle for a Custom Resource
func (w *worker) reconcileCR(ctx context.Context, old, new *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR before start: %s ", new.GetName())
		return nil
	}

	common.LogOldAndNew("non-normalized yet (native)", old, new)

	switch {
	case w.isAfterFinalizerInstalled(old, new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-1")
	case w.isGenerationTheSame(old, new):
		log.V(2).M(new).F().Info("isGenerationTheSame() - nothing to do here, exit")
		return nil
	}

	w.a.M(new).S().P()
	defer w.a.M(new).E().P()

	metrics.CHIInitZeroValues(ctx, new)
	metrics.CHIReconcilesStarted(ctx, new)
	startTime := time.Now()

	new = w.buildCR(ctx, new)

	actionPlan := action_plan.NewActionPlan(new.GetAncestorT(), new)
	common.LogActionPlan(actionPlan)

	switch {
	case actionPlan.HasActionsToDo():
		w.a.M(new).F().Info("ActionPlan has actions - continue reconcile")
	case w.isAfterFinalizerInstalled(new.GetAncestorT(), new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-2")
	default:
		w.a.M(new).F().Info("ActionPlan has no actions - abort reconcile")
		return nil
	}

	w.markReconcileStart(ctx, new, actionPlan)
	w.excludeFromMonitoring(new)
	w.setHostStatusesPreliminary(ctx, new, actionPlan)

	if err := w.reconcile(ctx, new); err != nil {
		// Something went wrong
		w.a.WithEvent(new, a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithError(new).
			M(new).F().
			Error("FAILED to reconcile CR %s, err: %v", util.NamespaceNameString(new), err)
		err = common.ErrCRUDAbort
		w.markReconcileCompletedUnsuccessfully(ctx, new, err)
		if errors.Is(err, common.ErrCRUDAbort) {
			metrics.CHIReconcilesAborted(ctx, new)
		}
	} else {
		// Reconcile successful
		// Post-process added items
		if util.IsContextDone(ctx) {
			log.V(1).Info("Reconcile is aborted. CR post-process: %s ", new.GetName())
			return nil
		}

		w.clean(ctx, new)
		w.dropReplicas(ctx, new, actionPlan)
		w.addToMonitoring(new)
		w.waitForIPAddresses(ctx, new)
		w.finalizeReconcileAndMarkCompleted(ctx, new)

		metrics.CHIReconcilesCompleted(ctx, new)
		metrics.CHIReconcilesTimings(ctx, new, time.Since(startTime).Seconds())
	}

	return nil
}

func (w *worker) buildCR(ctx context.Context, _cr *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	cr := w.createTemplatedCR(_cr)
	w.newTask(cr, cr.GetAncestorT())
	w.findMinMaxVersions(ctx, cr)
	common.LogOldAndNew("norm stage 1:", cr.GetAncestorT(), cr)

	templates := w.buildTemplates(cr)
	ips := w.c.getPodsIPs(ctx, cr)
	w.a.V(1).M(cr).Info("IPs of the CR %s: len: %d %v", util.NamespacedName(cr), len(ips), ips)
	if len(ips) > 0 || len(templates) > 0 {
		// Rebuild CR with known list of templates and additional IPs
		opts := commonNormalizer.NewOptions[api.ClickHouseInstallation]()
		opts.DefaultUserAdditionalIPs = ips
		opts.Templates = templates
		cr = w.createTemplatedCR(_cr, opts)
		w.newTask(cr, cr.GetAncestorT())
		w.findMinMaxVersions(ctx, cr)
		common.LogOldAndNew("norm stage 2:", cr.GetAncestorT(), cr)
	}

	w.fillCurSTS(ctx, cr)
	w.logSWVersion(ctx, cr)

	return cr
}

func (w *worker) buildCRFromObj(ctx context.Context, obj meta.Object) (*api.ClickHouseInstallation, error) {
	_cr, err := w.c.GetCR(obj)
	if err != nil {
		w.a.M(obj).F().Error("UNABLE-1 to find obj by labels: %v err: %v", obj.GetLabels(), err)
		return nil, err
	}
	return w.buildCR(ctx, _cr), nil
}

func (w *worker) buildTemplates(chi *api.ClickHouseInstallation) (templates []*api.ClickHouseInstallation) {
	for _, spec := range model.GetConfigMatchSpecs(chi) {
		templates = append(templates, &api.ClickHouseInstallation{
			Spec: *spec,
		})
	}
	return templates
}

func (w *worker) findMinMaxVersions(ctx context.Context, cr *api.ClickHouseInstallation) {
	// Create artifacts
	cr.WalkHosts(func(host *api.Host) error {
		w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, host.IsStopped())
		version := w.getHostSoftwareVersion(ctx, host)
		host.Runtime.Version = version
		return nil
	})
	cr.FindMinMaxVersions()
}

func (w *worker) fillCurSTS(ctx context.Context, cr *api.ClickHouseInstallation) {
	cr.WalkHosts(func(host *api.Host) error {
		host.Runtime.CurStatefulSet, _ = w.c.kube.STS().Get(ctx, host)
		return nil
	})
}

func (w *worker) logSWVersion(ctx context.Context, cr *api.ClickHouseInstallation) {
	l := w.a.V(1).F()
	cr.WalkHosts(func(host *api.Host) error {
		l.M(host).Info("Host software version: %s %s", host.GetName(), host.Runtime.Version.Render())
		return nil
	})
	l.M(cr).Info("CR software versions [min, max]: %s %s", cr.GetMinVersion().Render(), cr.GetMaxVersion().Render())
}

// reconcile reconciles Custom Resource
func (w *worker) reconcile(ctx context.Context, cr *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR: %s ", cr.GetName())
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	if counters := cr.GetHostsAttributesCounters(); counters.HasOnly(types.ObjectStatusRequested) {
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
		w.reconcileCRAuxObjectsFinal,
	)
}

// reconcileCRAuxObjectsPreliminary reconciles CR preliminary in order to ensure that ConfigMaps are in place
func (w *worker) reconcileCRAuxObjectsPreliminary(ctx context.Context, cr *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR aux preliminary: %s ", cr.GetName())
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

	return w.reconcileCRAuxObjectsPreliminaryDomain(ctx, cr)
}

func (w *worker) reconcileCRAuxObjectsPreliminaryDomain(ctx context.Context, cr *api.ClickHouseInstallation) error {
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
	log.V(2).F().S().Info("second stage")
	defer log.V(2).F().E().Info("second stage")

	if cr.IsStopped() {
		// Stopped CHI must have no entry point
		return nil
	}

	// Create entry point for the whole CR
	for _, service := range w.task.Creator().CreateService(interfaces.ServiceCR) {
		if service != nil {
			prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceCR).First()
			if err := w.reconcileService(ctx, cr, service, prevService); err != nil {
				// Service not reconciled
				w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
				return err
			}
			w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
		}
	}

	return nil
}

// reconcileCRAuxObjectsFinal reconciles CR global objects
func (w *worker) reconcileCRAuxObjectsFinal(ctx context.Context, cr *api.ClickHouseInstallation) (err error) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR aux final: %s ", cr.GetName())
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	// CR ConfigMaps with update
	cr.GetRuntime().LockCommonConfig()
	err = w.reconcileConfigMapCommon(ctx, cr)
	cr.GetRuntime().UnlockCommonConfig()

	w.includeAllHostsIntoCluster(ctx, cr)
	return err
}

func (w *worker) includeAllHostsIntoCluster(ctx context.Context, cr *api.ClickHouseInstallation) {
	// Wait for all hosts to be included into cluster
	cr.WalkHosts(func(host *api.Host) error {
		if host.ShouldIncludeIntoCluster() {
			_ = w.waitHostIsInCluster(ctx, host)
		}
		return nil
	})
}

// reconcileConfigMapCommon reconciles common ConfigMap
func (w *worker) reconcileConfigMapCommon(
	ctx context.Context,
	cr api.ICustomResource,
	options ...*config.FilesGeneratorOptions,
) error {
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
	log.V(1).M(host).F().S().Info("reconcile StatefulSet start")
	defer log.V(1).M(host).F().E().Info("reconcile StatefulSet end")

	w.a.V(1).M(host).F().Info("Reconcile host STS: %s. App version: %s", host.GetName(), host.Runtime.Version.Render())

	// Start with force-restart host
	if w.shouldForceRestartHost(ctx, host) {
		w.a.V(1).M(host).F().Info("Reconcile host STS force restart: %s", host.GetName())
		_ = w.hostForceRestart(ctx, host, opts)
	}

	w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, host.IsStopped())
	opts = w.prepareStsReconcileOptsWaitSection(host, opts)

	// We are in place, where we can  reconcile StatefulSet to desired configuration.
	w.a.V(1).M(host).F().Info("Reconcile host STS: %s. Reconcile StatefulSet", host.GetName())
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

func (w *worker) hostForceRestart(ctx context.Context, host *api.Host, opts *statefulset.ReconcileOptions) error {
	w.a.V(1).M(host).F().Info("Reconcile host. Force restart: %s", host.GetName())

	if host.IsStopped() || (w.hostSoftwareRestart(ctx, host) != nil) {
		_ = w.hostScaleDown(ctx, host, opts)
	}

	metrics.HostReconcilesRestart(ctx, host.GetCR())
	return nil
}

func (w *worker) hostSoftwareRestart(ctx context.Context, host *api.Host) error {
	w.a.V(1).M(host).F().Info("Host software restart start. Host: %s", host.GetName())

	// Get restart counters - they'll be used to check restart success
	restartCounters, err := w.c.kube.Pod().(interfaces.IKubePodEx).GetRestartCounters(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software restart abort 1. Host: %s err: %v", host.GetName(), err)
		return err
	}

	// Issue SQL shutdown command - expect host to shutdown and pod be restarted by k8s
	err = w.ensureClusterSchemer(host).HostShutdown(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software restart abort 2. Host: %s err: %v", host.GetName(), err)
		return err
	}
	w.a.V(1).M(host).F().Info("Host software shutdown ok. Host: %s", host.GetName())

	// Wait for restart counters to change
	err = w.waitHostRestart(ctx, host, restartCounters)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software restart abort 3. Host: %s err: %v", host.GetName(), err)
		return err
	}
	w.a.V(1).M(host).F().Info("Host software restart ok. Host: %s", host.GetName())

	// Wait for pod to start
	err = w.waitHostIsStarted(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software restart abort 4. Host: %s is not started", host.GetName())
		return fmt.Errorf("host is not started")
	}
	w.a.V(1).M(host).F().Info("Host software pod is started. Host: %s ", host.GetName())

	// Ensure pod is running
	err = w.waitHostIsRunning(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software restart abort 5. Host: %s is not running", host.GetName())
		return fmt.Errorf("host is not running")
	}
	w.a.V(1).M(host).F().Info("Host software pod is running. Host: %s ", host.GetName())

	// Ensure pod is ready
	err = w.waitHostIsReady(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software restart abort 6. Host: %s is not ready", host.GetName())
		return fmt.Errorf("host is not ready")
	}
	w.a.V(1).M(host).F().Info("Host software pod is ready. Host: %s ", host.GetName())

	// At this stage we'd expect to have software up and able to respond
	err = w.isHostSoftwareAbleToRespond(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software restart abort 7. Host: %s err: %v", host.GetName(), err)
		return err
	}
	w.a.V(1).M(host).F().Info("Host software version ok. Host: %s ", host.GetName())

	// However, some containers within the pod may still have flapping problems and be in CrashLoopBackOff
	if w.isPodCrushed(ctx, host) {
		w.a.V(1).M(host).F().Info("Host software restart abort 8. Host: %s is crushed", host.GetName())
		return fmt.Errorf("host is crushed")
	}
	w.a.V(1).M(host).F().Info("Host software is not crushed. Host: %s ", host.GetName())

	// Check whole pod health
	if !w.isPodOK(ctx, host) {
		w.a.V(1).M(host).F().Info("Host software restart abort 9. Host: %s is not ok", host.GetName())
		return fmt.Errorf("host is not ok")
	}
	w.a.V(1).M(host).F().Info("Host software pod is ok. Host: %s ", host.GetName())

	w.a.V(1).M(host).F().Info("Host software restart success. Host: %s", host.GetName())

	return nil
}

func (w *worker) hostScaleDown(ctx context.Context, host *api.Host, opts *statefulset.ReconcileOptions) error {
	w.a.V(1).M(host).F().Info("Reconcile host. Host shutdown via scale down: %s", host.GetName())

	w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, true)
	err := w.stsReconciler.ReconcileStatefulSet(ctx, host, false, opts)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host shutdown abort 1. Host: %s err: %v", host.GetName(), err)
		return err
	}

	w.a.V(1).M(host).F().Info("Host shutdown success. Host: %s", host.GetName())
	return nil
}

// reconcileHostService reconciles host's Service
func (w *worker) reconcileHostService(ctx context.Context, host *api.Host) error {
	service := w.task.Creator().CreateService(interfaces.ServiceHost, host).First()
	if service == nil {
		// This is not a problem, service may be omitted
		return nil
	}
	prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceHost, host.GetAncestor()).First()
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

// reconcileCluster reconciles cluster, excluding nested shards
func (w *worker) reconcileCluster(ctx context.Context, cluster *api.Cluster) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. Cluster: %s ", cluster.GetName())
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	if err := w.reconcileClusterService(ctx, cluster); err != nil {
		return err
	}
	if err := w.reconcileClusterSecret(ctx, cluster); err != nil {
		return err
	}
	if err := w.reconcileClusterPodDisruptionBudget(ctx, cluster); err != nil {
		return err
	}
	if err := reconcileClusterZookeeperRootPath(cluster); err != nil {
		return err
	}
	if err := w.reconcileClusterShardsAndHosts(ctx, cluster); err != nil {
		return err
	}

	return nil
}

func (w *worker) reconcileClusterService(ctx context.Context, cluster *api.Cluster) error {
	// Add Cluster Service
	if service := w.task.Creator().CreateService(interfaces.ServiceCluster, cluster).First(); service != nil {
		prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceCluster, cluster.GetAncestor()).First()
		if err := w.reconcileService(ctx, cluster.GetRuntime().GetCR(), service, prevService); err == nil {
			w.task.RegistryReconciled().RegisterService(service.GetObjectMeta())
		} else {
			w.task.RegistryFailed().RegisterService(service.GetObjectMeta())
		}
	}
	return nil
}

func (w *worker) reconcileClusterSecret(ctx context.Context, cluster *api.Cluster) error {
	// Add cluster's Auto Secret
	if cluster.Secret.Source() == api.ClusterSecretSourceAuto {
		if secret := w.task.Creator().CreateClusterSecret(cluster); secret != nil {
			if err := w.reconcileSecret(ctx, cluster.Runtime.CHI, secret); err == nil {
				w.task.RegistryReconciled().RegisterSecret(secret.GetObjectMeta())
			} else {
				w.task.RegistryFailed().RegisterSecret(secret.GetObjectMeta())
			}
		}
	}
	return nil
}

func (w *worker) reconcileClusterPodDisruptionBudget(ctx context.Context, cluster *api.Cluster) error {
	if cluster.GetPDBManaged().IsFalse() {
		return nil
	}

	pdb := w.task.Creator().CreatePodDisruptionBudget(cluster)
	if err := w.reconcilePDB(ctx, cluster, pdb); err == nil {
		w.task.RegistryReconciled().RegisterPDB(pdb.GetObjectMeta())
	} else {
		w.task.RegistryFailed().RegisterPDB(pdb.GetObjectMeta())
	}
	return nil
}

// reconcileClusterShardsAndHosts reconciles shards and hosts of each shard
func (w *worker) reconcileClusterShardsAndHosts(ctx context.Context, cluster *api.Cluster) error {
	shards := cluster.Layout.Shards[:]

	// Sanity check - has to have shard(s)
	if len(shards) == 0 {
		return nil
	}

	log.V(1).F().S().Info("reconcileClusterShardsAndHosts start")
	defer log.V(1).F().E().Info("reconcileClusterShardsAndHosts end")

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
	workersNum := w.getReconcileShardsWorkersNum(cluster, opts)
	w.a.V(1).Info("Starting rest of shards on workers. Workers num: %d", workersNum)
	if err := w.runConcurrently(ctx, workersNum, startShard, shards[startShard:]); err != nil {
		w.a.V(1).Info("Finished with ERROR rest of shards on workers: %d, err: %v", workersNum, err)
		return err
	}
	w.a.V(1).Info("Finished successfully rest of shards on workers: %d", workersNum)
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
		log.V(1).Info("Reconcile is aborted. Shard: %s ", shard.GetName())
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	err := w.reconcileShardService(ctx, shard)

	return err
}

func (w *worker) reconcileShardService(ctx context.Context, shard api.IShard) error {
	// Add Shard's Service
	service := w.task.Creator().CreateService(interfaces.ServiceShard, shard).First()
	if service == nil {
		// This is not a problem, ServiceShard may be omitted
		return nil
	}
	prevService := w.task.CreatorPrev().CreateService(interfaces.ServiceShard, shard.GetAncestor()).First()
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
		log.V(1).Info("Reconcile is aborted. Host: %s ", host.GetName())
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()

	metrics.HostReconcilesStarted(ctx, host.GetCR())
	startTime := time.Now()

	if host.IsFirstInCR() {
		_ = w.reconcileCRServicePreliminary(ctx, host.GetCR())
		defer w.reconcileCRServiceFinal(ctx, host.GetCR())
	}

	w.a.V(1).M(host).F().Info("Reconcile host: %s. App version: %s", host.GetName(), host.Runtime.Version.Render())

	if err := w.reconcileHostPrepare(ctx, host); err != nil {
		return err
	}
	if err := w.reconcileHostMain(ctx, host); err != nil {
		return err
	}
	// Host is now added and functional

	if host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusRequested) {
		host.GetReconcileAttributes().SetStatus(types.ObjectStatusCreated)
	}
	if err := w.reconcileHostIncludeIntoAllActivities(ctx, host); err != nil {
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
	metrics.HostReconcilesTimings(ctx, host.GetCR(), time.Since(startTime).Seconds())

	return nil
}

// reconcileHostPrepare reconciles specified ClickHouse host
func (w *worker) reconcileHostPrepare(ctx context.Context, host *api.Host) error {
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

	// Reconcile ConfigMap
	if err := w.reconcileConfigMapHost(ctx, host); err != nil {
		metrics.HostReconcilesErrors(ctx, host.GetCR())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host Main - unable to reconcile ConfigMap. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	w.setHasData(host)

	w.a.V(1).M(host).F().Info("Reconcile PVCs and data loss for host: %s", host.GetName())

	// In case data loss or volumes missing detected we may need to specify additional reconcile options
	err := w.reconcileHostPVCs(ctx, host)
	switch {
	case storage.ErrIsDataLoss(err):
		stsReconcileOpts, migrateTableOpts = w.hostPVCsDataLossDetectedOptions(host)
		w.a.V(1).
			M(host).F().
			Info("Data loss detected for host: %s.", host.GetName())
	case storage.ErrIsVolumeMissed(err):
		// stsReconcileOpts, migrateTableOpts = w.hostPVCsDataVolumeMissedDetectedOptions(host)
		stsReconcileOpts, migrateTableOpts = w.hostPVCsDataLossDetectedOptions(host)
		w.a.V(1).
			M(host).F().
			Info("Data volume missed detected for host: %s.", host.GetName())
	}

	// Reconcile Service
	if err := w.reconcileHostService(ctx, host); err != nil {
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host Main - unable to reconcile Service. Host: %s Err: %v", host.GetName(), err)
	}

	// Reconcile StatefulSet
	if err := w.reconcileHostStatefulSet(ctx, host, stsReconcileOpts); err != nil {
		metrics.HostReconcilesErrors(ctx, host.GetCR())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host Main - unable to reconcile StatefulSet. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	// Polish all new volumes that the operator has to create
	if err := w.reconcileHostPVCs(ctx, host); err != nil {
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host Main - unable to reconcile PVC. Host: %s Err: %v", host.GetName(), err)
	}

	// Finalize main reconcile with domain activities
	if err := w.reconcileHostMainDomain(ctx, host, migrateTableOpts); err != nil {
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host Main - unable to reconcile domain reconcile. Host: %s Err: %v", host.GetName(), err)
	}

	return nil
}

func (w *worker) prepareStsReconcileOptsWaitSection(host *api.Host, opts *statefulset.ReconcileOptions) *statefulset.ReconcileOptions {
	if host.GetCluster().GetReconcile().Host.Wait.Probes.GetStartup().IsTrue() {
		opts = opts.SetWaitUntilStarted()
		w.a.V(1).
			M(host).F().
			Warning("Setting option SetWaitUntilStarted ")
	}
	if host.GetCluster().GetReconcile().Host.Wait.Probes.GetReadiness().IsTrue() {
		opts = opts.SetWaitUntilReady()
		w.a.V(1).
			M(host).F().
			Warning("Setting option SetWaitUntilReady")
	}
	return opts
}

func (w *worker) reconcileHostPVCs(ctx context.Context, host *api.Host) storage.ErrorDataPersistence {
	return storage.NewStorageReconciler(
		w.task,
		w.c.namer,
		storage.NewStoragePVC(w.c.kube.Storage()),
	).ReconcilePVCs(ctx, host, api.DesiredStatefulSet)
}

func (w *worker) reconcileHostMainDomain(ctx context.Context, host *api.Host, migrateTableOpts *migrateTableOptions) error {
	return w.reconcileHostTables(ctx, host, migrateTableOpts)
}

func (w *worker) reconcileHostTables(ctx context.Context, host *api.Host, opts *migrateTableOptions) error {
	if !w.shouldMigrateTables(host, opts) {
		w.a.V(1).
			M(host).F().
			Info(
				"No need to add tables on host %d to shard %d in cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	// Need to migrate tables

	// Prepare for tables migration.
	// Ensure host is running and accessible and what version is available.
	// Sometimes service needs some time to start after creation|modification before being accessible for usage
	// However, it is expected to have host up and running at this point
	version, err := w.pollHostForClickHouseVersion(ctx, host)
	if err != nil {
		w.a.V(1).
			M(host).F().
			Warning("Check host for ClickHouse availability before migrating tables. Host: %s Failed to get ClickHouse version. Err: %s", host.GetName(), err)
		return err
	}

	w.a.V(1).
		M(host).F().
		Info("Check host for ClickHouse availability before migrating tables. Host: %s ClickHouse version available: %s", host.GetName(), version)

	return w.migrateTables(ctx, host, opts)
}

// reconcileHostIncludeIntoAllActivities includes specified ClickHouse host into all activities
func (w *worker) reconcileHostIncludeIntoAllActivities(ctx context.Context, host *api.Host) error {
	if !w.shouldIncludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to include host into cluster. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	// Include host back into all activities - such as cluster, service, etc
	err := w.includeHost(ctx, host)
	if err != nil {
		metrics.HostReconcilesErrors(ctx, host.GetCR())
		w.a.V(1).
			M(host).F().
			Warning("Reconcile Host interrupted with an error 4. Host: %s Err: %v", host.GetName(), err)
		return err
	}

	l := w.a.V(1).
		WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileCompleted).
		WithAction(host.GetCR()).
		M(host).F()

	// In case host is unable to report its version we are done with inclusion
	switch {
	case host.IsStopped():
		l.Info("Reconcile Host completed. Host is stopped: %s", host.GetName())
		return nil
	case host.IsTroubleshoot():
		l.Info("Reconcile Host completed. Host is in troubleshoot mode: %s", host.GetName())
		return nil
	}

	// Report host software version
	version := w.getHostSoftwareVersion(ctx, host)
	l.Info("Reconcile Host completed. Host: %s ClickHouse version running: %s", host.GetName(), version.Render())

	return nil
}
