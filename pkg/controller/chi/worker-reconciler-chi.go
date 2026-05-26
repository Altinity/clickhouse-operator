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

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
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

	metrics.CRInitZeroValues(ctx, new)
	metrics.CRReconcilesStarted(ctx, new)
	startTime := time.Now()

	new = w.buildCR(ctx, new)

	// If buildCR's normalize already aborted the CR (e.g. FIPS strict rejected
	// plain-text ZooKeeper), persist the abort and skip reconcile — otherwise
	// markReconcileStart below would overwrite Status=Aborted with InProgress.
	// Recovery is via spec edit: informer UpdateFunc re-enqueues on next apply.
	if new.EnsureStatus().GetStatus() == api.StatusAborted {
		w.a.M(new).F().Info("Normalize marked CR Aborted — skipping reconcile (recovery via spec edit)")
		_ = w.c.updateCRObjectStatus(ctx, new, types.UpdateStatusOptions{
			CopyStatusOptions: types.CopyStatusOptions{
				CopyStatusFieldGroup: types.CopyStatusFieldGroup{
					FieldGroupMain: true,
				},
			},
		})
		metrics.CRReconcilesAborted(ctx, new)
		return nil
	}

	switch {
	case new.Spec.Suspend.Value():
		// if CR is suspended, should skip reconciliation
		w.a.M(new).F().Info("Suspended CR")
		if new.EnsureStatus().GetStatus() == api.StatusInProgress || new.EnsureRuntime().ActionPlan.HasActionsToDo() {
			// Either was mid-reconcile when suspended, or has pending changes suppressed by suspend — mark as Aborted
			new.EnsureStatus().ReconcileAbort()
			_ = w.c.updateCRObjectStatus(ctx, new, types.UpdateStatusOptions{
				CopyStatusOptions: types.CopyStatusOptions{
					CopyStatusFieldGroup: types.CopyStatusFieldGroup{
						FieldGroupMain: true,
					},
				},
			})
			w.a.V(1).
				WithEvent(new, a.EventActionReconcile, a.EventReasonReconcileFailed).
				WithAction(new).
				M(new).F().
				Warning("reconcile aborted due to suspend")
			metrics.CRReconcilesAborted(ctx, new)
		} else {
			metrics.CRReconcilesCompleted(ctx, new)
		}
		return nil
	case new.HasReconcileWork():
		w.a.M(new).F().Info("CR has reconcile work - continue reconcile")
	case w.isAfterFinalizerInstalled(new.GetAncestorT(), new):
		w.a.M(new).F().Info("isAfterFinalizerInstalled - continue reconcile-2")
	default:
		w.a.M(new).F().Info("No reconcile work - abort reconcile")
		metrics.CRReconcilesCompleted(ctx, new)
		return nil
	}

	w.markReconcileStart(ctx, new)
	w.prepareMonitoring(new)
	w.setHostStatusesPreliminary(ctx, new)

	if err := w.reconcile(ctx, new); err != nil {
		// Something went wrong
		w.a.WithEvent(new, a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithError(new).
			M(new).F().
			Error("FAILED to reconcile CR %s, err: %v", util.NamespaceNameString(new), err)
		err = common.ErrCRUDAbort
		w.markReconcileCompletedUnsuccessfully(ctx, new, err)
		if errors.Is(err, common.ErrCRUDAbort) {
			metrics.CRReconcilesAborted(ctx, new)
		}
	} else {
		// Reconcile successful
		// Post-process added items
		if util.IsContextDone(ctx) {
			log.V(1).Info("Reconcile is aborted. CR post-process: %s ", new.GetName())
			return nil
		}

		// Pre-delete hooks for hosts the action plan removed (scale-down). Pods are still
		// up at this point — clean()'s purge is what tears them down — so SQL hooks can
		// drain the dying hosts before they go away.
		w.runHostPreDeleteHooksOnRemovedHosts(ctx, new)

		w.clean(ctx, new)
		w.addToMonitoring(new)
		w.waitForIPAddresses(ctx, new)
		w.finalizeReconcileAndMarkCompleted(ctx, new)

		w.dropZKReplicas(ctx, new)

		metrics.CRReconcilesCompleted(ctx, new)
		metrics.CRReconcilesTimings(ctx, new, time.Since(startTime).Seconds())
	}

	return nil
}

func (w *worker) buildCR(ctx context.Context, _cr *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	// Resolve all keeper references before normalization — populates ZookeeperConfig.Nodes from KeeperRef
	if err := w.resolveAllKeeperReferences(ctx, _cr); err != nil {
		w.a.V(1).M(_cr).F().
			WithEvent(_cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
			Warning("Failed to resolve keeper references: %v", err)
	}

	cr := w.createTemplatedCR(_cr)
	w.newTask(cr, cr.GetAncestorT())
	// fillCurSTS must be called before findMinMaxVersions to ensure deterministic
	// StatefulSet fingerprint calculation. appendConfigLabels copies config version
	// labels from CurStatefulSet into the desired STS labels, which are part of the
	// fingerprint. Without CurStatefulSet populated, these labels are omitted, producing
	// a different fingerprint than the one computed later during reconcileHostStatefulSet
	// (where CurStatefulSet IS available). This hash mismatch can cause unnecessary STS
	// updates that cascade into STS deletion via the recreate-on-update-failure path.
	w.fillCurSTS(ctx, cr)
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
		// fillCurSTS again because createTemplatedCR creates new host objects
		w.fillCurSTS(ctx, cr)
		w.findMinMaxVersions(ctx, cr)
		common.LogOldAndNew("norm stage 2:", cr.GetAncestorT(), cr)
	}

	w.logSWVersion(ctx, cr)

	actionPlan := api.MakeActionPlan(cr.GetAncestorT(), cr)
	cr.EnsureRuntime().ActionPlan = actionPlan
	cr.EnsureStatus().SetActionPlan(actionPlan)
	w.a.V(1).M(cr).Info(actionPlan.Log("buildCR"))

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
	l.M(cr).Info("CR software versions min=%s max=%s", cr.GetMinVersion().Render(), cr.GetMaxVersion().Render())
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
		// Stopped CR must have no entry point
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

	w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, host.IsStopped())
	opts = w.prepareStsReconcileOptsWaitSection(host, opts)

	// Start with force-restart host.
	// If a pod-template rollout is already coming via the StatefulSet reconcile below, skip
	// the pre-rollout software restart — see hostRequiresStatefulSetRollout for why.
	if w.shouldForceRestartHost(ctx, host) {
		if hostRequiresStatefulSetRollout(host) {
			w.a.V(1).M(host).F().Info("Pod template changed - skipping software restart; StatefulSet rollout will restart the pod. Host: %s", host.GetName())
		} else {
			w.a.V(1).M(host).F().Info("Reconcile host STS force restart: %s", host.GetName())
			_ = w.hostForceRestart(ctx, host, opts)
			// hostForceRestart's fallback path (hostScaleDown) overwrites DesiredStatefulSet
			// with a shutdown spec via PrepareHostStatefulSetWithStatus(host, true). If we
			// don't reset it back to the normal desired spec, the main ReconcileStatefulSet
			// below would apply the shutdown spec and leave the pod at replicas=0 — breaking
			// test_010042 (expected Aborted when bad config crashes ClickHouse — actually
			// stayed Completed because the pod never came back up to crash).
			w.stsReconciler.PrepareHostStatefulSetWithStatus(ctx, host, host.IsStopped())
		}
	}

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

// hostRequiresStatefulSetRollout reports whether the current reconcile introduces a
// container env-var change that an in-place software restart CANNOT carry. The #1963
// scenario is adding a secret-backed env var alongside a config setting that reads it via
// from_env="..." — a software restart brings the SAME pod back with the OLD env (k8s hasn't
// rolled the template yet), so ClickHouse fails to resolve from_env and crashes. Deferring
// to the StatefulSet rollout instead lets k8s bring up a fresh pod with the new env.
//
// IMPORTANT: do NOT compare the whole PodTemplateSpec via Semantic.DeepEqual. The current
// StatefulSet fetched from k8s has apiserver-defaulted fields (dnsPolicy, restartPolicy,
// schedulerName, terminationMessagePath, securityContext{}, containerPort protocol,
// ConfigMap volume DefaultMode 420, etc.) that the in-memory desired StatefulSet leaves
// unset. A whole-template Semantic.DeepEqual would treat these as different on every
// reconcile — falsely skipping the software restart path for config-only changes (e.g.
// macro substitution in test_010059), which ClickHouse depends on to pick up new settings.
//
// We restrict the comparison to Container.Env, which is the ACTUAL signal #1963 describes
// and which is not subject to apiserver defaulting. Container image changes are already
// handled upstream by shouldForceRestartHost / isImageChangeRequested. Other template
// edits (command/args, volumes, probes) would either need their own case here or would
// go through the normal rolling-update path — not this pre-rollout-restart gate.
//
// NOTE: this check intentionally lives outside shouldForceRestartHost because that predicate
// is also consulted from the exclude-from-cluster path (worker-wait-exclude-include-restart.go)
// where pod-template-driven restarts still need the host excluded from the load balancer —
// hiding them from shouldForceRestartHost would suppress that exclusion.
//
// Conservative: if either StatefulSet is missing (shouldn't happen after PrepareHostStatefulSetWithStatus
// ran earlier in reconcileHostStatefulSet, but be defensive), returns true so we prefer the
// STS rollout path over a potentially-bad software restart.
//
// Package-private and pure so it can be unit-tested without constructing a worker/controller.
func hostRequiresStatefulSetRollout(host *api.Host) bool {
	if host == nil {
		return false
	}
	cur := host.Runtime.CurStatefulSet
	desired := host.Runtime.DesiredStatefulSet

	if cur == nil || desired == nil {
		return true
	}

	curContainers := cur.Spec.Template.Spec.Containers
	desiredContainers := desired.Spec.Template.Spec.Containers
	if len(curContainers) != len(desiredContainers) {
		return true
	}
	for i := range curContainers {
		if !apiequality.Semantic.DeepEqual(curContainers[i].Env, desiredContainers[i].Env) {
			return true
		}
	}
	return false
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
	if err := w.reconcileClusterZookeeperRootPath(cluster); err != nil {
		return err
	}

	// Cluster pre-hooks: failure aborts cluster reconcile
	if err := w.runClusterPreHooks(ctx, cluster); err != nil {
		return err
	}

	if err := w.reconcileClusterShardsAndHosts(ctx, cluster); err != nil {
		return err
	}

	// Cluster post-hooks: best-effort, logged but don't fail reconcile
	w.runClusterPostHooks(ctx, cluster)

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

	// Host pre-hooks: failure aborts this host's reconcile
	if err := w.runHostPreHooks(ctx, host); err != nil {
		return err
	}

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

	// Host post-hooks: best-effort, logged but don't fail reconcile
	w.runHostPostHooks(ctx, host)

	// Host reconcile completed successfully - add it to monitoring
	w.addHostToMonitoring(host)

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

	// In case data loss or volumes missing detected we may
	// 1. need to specify additional reconcile options
	// 2. abort the reconcile completely
	err := w.reconcileHostPVCs(ctx, host)
	onDataLoss := host.GetCluster().GetReconcile().StatefulSet.Recreate.OnDataLoss
	switch {
	case storage.ErrIsDataLoss(err):
		if onDataLoss == api.OnStatefulSetRecreateOnDataLossActionAbort {
			w.a.V(1).M(host).F().Warning("Data loss detected for host: %s. Aborting reconcile as configured (onDataLoss: abort)", host.GetName())
			return common.ErrCRUDAbort
		}
		stsReconcileOpts, migrateTableOpts = w.hostPVCsDataLossDetectedOptions(host)
		w.a.V(1).
			M(host).F().
			Info("Data loss detected for host: %s.", host.GetName())
	case storage.ErrIsVolumeMissed(err):
		if onDataLoss == api.OnStatefulSetRecreateOnDataLossActionAbort {
			w.a.V(1).M(host).F().Warning("Data volume missed for host: %s. Aborting reconcile as configured (onDataLoss: abort)", host.GetName())
			return common.ErrCRUDAbort
		}
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
		// Propagate abort signals (e.g. runtime FIPS image-policy violation) so
		// reconcile() routes them through markReconcileCompletedUnsuccessfully,
		// which persists StatusAborted via updateCRObjectStatus. Other errors
		// stay logged but non-fatal — preserves pre-existing tolerance.
		if errors.Is(err, common.ErrCRUDAbort) {
			return err
		}
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
	needMigrate := w.shouldMigrateTables(host, opts)
	needFIPSCheck := chop.Config().Security.GetImages().IsRequired()

	if !needMigrate && !needFIPSCheck {
		w.a.V(1).
			M(host).F().
			Info(
				"No need to add tables on host %d to shard %d in cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	// Fetch SELECT version() for migration (poll until ready) OR FIPS-only
	// confirmation (single call; fail-open on err so a dead host doesn't burn
	// the migrate poll timeout when migration is not needed).
	var version *swversion.SoftWareVersion
	if needMigrate {
		v, err := w.pollHostForClickHouseVersion(ctx, host)
		if err != nil {
			w.a.V(1).
				M(host).F().
				Warning("Check host for ClickHouse availability. Host: %s Failed to get ClickHouse version. Err: %s", host.GetName(), err)
			return err
		}
		version = v
		w.a.V(1).
			M(host).F().
			Info("Check host for ClickHouse availability. Host: %s ClickHouse version available: %s", host.GetName(), version)
	} else {
		version = w.getHostClickHouseVersion(ctx, host)
	}

	// Runtime FIPS image-policy confirmation. Returns common.ErrCRUDAbort
	// when the CR is aborted; propagating the error routes it through
	// reconcile()'s abort path which persists status correctly.
	if err := w.enforceFIPSImagePolicyRuntime(host, version); err != nil {
		return err
	}

	if !needMigrate {
		return nil
	}
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
