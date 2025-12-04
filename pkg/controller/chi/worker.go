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
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller/domain"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/queue"
)

// FinalizerName specifies name of the finalizer to be used with CHI
const FinalizerName = "finalizer.clickhouseinstallation.altinity.com"

// worker represents worker thread which runs reconcile tasks
type worker struct {
	c *Controller
	a a.Announcer

	//queue workqueue.RateLimitingInterface
	queue   queue.PriorityQueue
	schemer *schemer.ClusterSchemer

	normalizer    *normalizer.Normalizer
	task          *common.Task
	stsReconciler *statefulset.Reconciler

	start time.Time
}

// newWorker
func (c *Controller) newWorker(q queue.PriorityQueue, sys bool) *worker {
	start := time.Now()
	if !sys {
		start = start.Add(api.DefaultReconcileThreadsWarmup)
	}
	kind := "ClickHouseInstallation"
	generateName := "chop-chi-"
	component := componentName

	announcer := a.NewAnnouncer(
		a.NewEventEmitter(c.kube.Event(), kind, generateName, component),
		c.kube.CR(),
	)

	return &worker{
		c: c,
		a: announcer,

		queue:   q,
		schemer: nil,

		normalizer: normalizer.New(func(namespace, name string) (*core.Secret, error) {
			return c.kube.Secret().Get(context.TODO(), &core.Secret{
				ObjectMeta: meta.ObjectMeta{
					Namespace: namespace,
					Name:      name,
				},
			})
		}),
		start: start,
		task:  nil,
	}
}

func configGeneratorOptions(cr *api.ClickHouseInstallation) *config.GeneratorOptions {
	return &config.GeneratorOptions{
		Users:          cr.GetSpecT().GetConfiguration().GetUsers(),
		Profiles:       cr.GetSpecT().GetConfiguration().GetProfiles(),
		Quotas:         cr.GetSpecT().GetConfiguration().GetQuotas(),
		Settings:       cr.GetSpecT().GetConfiguration().GetSettings(),
		Files:          cr.GetSpecT().GetConfiguration().GetFiles(),
		DistributedDDL: cr.GetSpecT().GetDefaults().GetDistributedDDL(),
	}
}

func (w *worker) buildCreator(cr *api.ClickHouseInstallation) *commonCreator.Creator {
	if cr == nil {
		cr = &api.ClickHouseInstallation{}
	}
	return commonCreator.NewCreator(
		cr,
		managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeClickHouse, cr, configGeneratorOptions(cr)),
		managers.NewContainerManager(managers.ContainerManagerTypeClickHouse),
		managers.NewTagManager(managers.TagManagerTypeClickHouse, cr),
		managers.NewProbeManager(managers.ProbeManagerTypeClickHouse),
		managers.NewServiceManager(managers.ServiceManagerTypeClickHouse),
		managers.NewVolumeManager(managers.VolumeManagerTypeClickHouse),
		managers.NewConfigMapManager(managers.ConfigMapManagerTypeClickHouse),
		managers.NewNameManager(managers.NameManagerTypeClickHouse),
		managers.NewOwnerReferencesManager(managers.OwnerReferencesManagerTypeClickHouse),
		namer.New(),
		macro.New(),
		labeler.New(cr),
	)
}

func (w *worker) newTask(new, old *api.ClickHouseInstallation) {
	w.task = common.NewTask(w.buildCreator(new), w.buildCreator(old))
	w.stsReconciler = statefulset.NewReconciler(
		w.a,
		w.task,
		domain.NewHostObjectsPoller(domain.NewHostObjectPoller(w.c.kube.STS()), domain.NewHostObjectPoller(w.c.kube.Pod()), w.c.ctrlLabeler),
		w.c.namer,
		labeler.New(new),
		storage.NewStorageReconciler(w.task, w.c.namer, w.c.kube.Storage()),
		w.c.kube,
		w.c,
	)
}

// shouldForceRestartHost checks whether cluster requires hosts restart
func (w *worker) shouldForceRestartHost(ctx context.Context, host *api.Host) bool {
	switch {
	case host.HasAncestor() && host.GetAncestor().IsStopped():
		w.a.V(1).M(host).F().Info("Host ancestor is stopped, no restart applicable. Host: %s", host.GetName())
		return false

	case host.HasAncestor() && host.GetAncestor().IsTroubleshoot():
		w.a.V(1).M(host).F().Info("Host ancestor is in troubleshoot, no restart applicable. Host: %s", host.GetName())
		return false

	case host.IsStopped():
		w.a.V(1).M(host).F().Info("Host is stopped, no restart applicable. Host: %s", host.GetName())
		return false

	case host.IsTroubleshoot():
		w.a.V(1).M(host).F().Info("Host is in troubleshoot, no restart applicable. Host: %s", host.GetName())
		return false

	case host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusRequested):
		w.a.V(1).M(host).F().Info("Host is new, no restart applicable. Host: %s", host.GetName())
		return false

	case !host.HasAncestor():
		w.a.V(1).M(host).F().Info("Host has no ancestor, no restart applicable. Host: %s", host.GetName())
		return false

	case host.GetCR().IsRollingUpdate():
		w.a.V(1).M(host).F().Info("RollingUpdate requires force restart. Host: %s", host.GetName())
		return true

	case model.IsConfigurationChangeRequiresReboot(host):
		w.a.V(1).M(host).F().Info("Config change(s) require host restart. Host: %s", host.GetName())
		return true

	case host.Runtime.Version.IsUnknown() && w.isPodCrushed(ctx, host):
		w.a.V(1).M(host).F().Info("Host with unknown version and in CrashLoopBackOff should be restarted. It most likely is unable to start due to bad config. Host: %s", host.GetName())
		return true

	default:
		w.a.V(1).M(host).F().Info("Host force restart is not required. Host: %s", host.GetName())
		return false
	}
}

// ensureFinalizer
func (w *worker) ensureFinalizer(ctx context.Context, chi *api.ClickHouseInstallation) bool {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR ensure fin: %s ", chi.GetName())
		return false
	}

	// In case CHI is being deleted already, no need to meddle with finalizers
	if !chi.GetDeletionTimestamp().IsZero() {
		return false
	}

	// Finalizer can already be listed in CHI, do nothing in this case
	if util.InArray(FinalizerName, chi.GetFinalizers()) {
		w.a.V(2).M(chi).F().Info("finalizer already installed")
		return false
	}

	// No finalizer found - need to install it

	if err := w.c.installFinalizer(ctx, chi); err != nil {
		w.a.V(1).M(chi).F().Error("unable to install finalizer. err: %v", err)
		return false
	}

	w.a.V(3).M(chi).F().Info("finalizer installed")
	return true
}

// updateEndpoints updates endpoints
func (w *worker) updateEndpoints(ctx context.Context, m meta.Object) error {
	_ = w.finalizeCR(
		ctx,
		m,
		types.UpdateStatusOptions{
			TolerateAbsence: true,
			CopyStatusOptions: types.CopyStatusOptions{
				CopyStatusFieldGroup: types.CopyStatusFieldGroup{
					FieldGroupNormalized: true,
				},
			},
		},
		nil,
	)
	return nil
}

func (w *worker) finalizeCR(
	ctx context.Context,
	obj meta.Object,
	updateStatusOpts types.UpdateStatusOptions,
	f func(*api.ClickHouseInstallation),
) error {
	chi, err := w.buildCRFromObj(ctx, obj)
	if err != nil {
		log.V(1).Error("Unable to finalize CR: %s err: %v", util.NamespacedName(obj), err)
		return err
	}

	if f != nil {
		f(chi)
	}

	_ = w.reconcileConfigMapCommonUsers(ctx, chi)
	_ = w.c.updateCRObjectStatus(ctx, chi, updateStatusOpts)

	return nil
}

// updateCHI sync CHI which was already created earlier
func (w *worker) updateCHI(ctx context.Context, old, new *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Update CHI is aborted. CR: %s ", new.GetName())
		return nil
	}

	w.a.V(3).M(new).S().P()
	defer w.a.V(3).M(new).E().P()

	update := (old != nil) && (new != nil)

	if update && (old.GetResourceVersion() == new.GetResourceVersion()) {
		// No need to react
		w.a.V(3).M(new).F().Info("ResourceVersion did not change: %s", new.GetResourceVersion())
		return nil
	}

	w.a.V(1).M(new).S().P()
	defer w.a.V(1).M(new).E().P()

	if w.ensureFinalizer(context.Background(), new) {
		w.a.M(new).F().Info("finalizer installed, let's restart reconcile cycle. CHI: %s/%s", new.Namespace, new.Name)
		w.a.M(new).F().Info("---------------------------------------------------------------------")
		return nil
	} else {
		w.a.M(new).F().Info("finalizer in place, proceed to reconcile cycle. CHI: %s/%s", new.Namespace, new.Name)
	}

	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted")
		return nil
	}

	if new != nil {
		n, err := w.c.kube.CR().Get(ctx, new.GetNamespace(), new.GetName())
		if err != nil {
			return err
		}
		new = n.(*api.ClickHouseInstallation)
	}

	metrics.CHIRegister(ctx, new)

	if w.deleteCHI(ctx, old, new) {
		// CHI is being deleted
		metrics.CHIUnregister(ctx, new)
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted")
		return nil
	}

	if w.isCHIProcessedOnTheSameIP(new) {
		// First minute after restart do not reconcile already reconciled generations
		w.a.V(1).M(new).F().Info("Will not reconcile known generation after restart. Generation %d", new.Generation)
		return nil
	}

	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted.")
		return nil
	}

	// CHI is being reconciled
	return w.reconcileCR(ctx, old, new)
}

func (w *worker) markReconcileStart(ctx context.Context, cr *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", cr.GetName())
		return
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	cr.EnsureStatus().ReconcileStart(cr.EnsureRuntime().ActionPlan)
	_ = w.c.updateCRObjectStatus(ctx, cr, types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	})

	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileStarted).
		WithAction(cr).
		WithActions(cr).
		M(cr).F().
		Info("reconcile started, task id: %s", cr.GetSpecT().GetTaskID())
	w.a.V(2).M(cr).F().Info("action plan\n%s\n", cr.EnsureRuntime().ActionPlan.String())
}

func (w *worker) finalizeReconcileAndMarkCompleted(ctx context.Context, _cr *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", _cr.GetName())
		return
	}

	w.a.V(1).M(_cr).F().S().Info("finalize reconcile")

	// Update CHI object
	_ = w.finalizeCR(
		ctx,
		_cr,
		types.UpdateStatusOptions{
			CopyStatusOptions: types.CopyStatusOptions{
				CopyStatusFieldGroup: types.CopyStatusFieldGroup{
					FieldGroupWholeStatus: true,
				},
			},
		},
		func(c *api.ClickHouseInstallation) {
			c.SetAncestor(c.GetTarget())
			c.SetTarget(nil)
			c.EnsureStatus().ReconcileComplete()
		},
	)

	w.a.V(1).
		WithEvent(_cr, a.EventActionReconcile, a.EventReasonReconcileCompleted).
		WithAction(_cr).
		WithActions(_cr).
		M(_cr).F().
		Info("reconcile completed successfully, task id: %s", _cr.GetSpecT().GetTaskID())
}

func (w *worker) markReconcileCompletedUnsuccessfully(ctx context.Context, cr *api.ClickHouseInstallation, err error) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", cr.GetName())
		return
	}

	switch {
	case err == nil:
		cr.EnsureStatus().ReconcileComplete()
	case errors.Is(err, common.ErrCRUDAbort):
		cr.EnsureStatus().ReconcileAbort()
	}
	w.c.updateCRObjectStatus(ctx, cr, types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	})

	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
		WithAction(cr).
		WithActions(cr).
		M(cr).F().
		Warning("reconcile completed UNSUCCESSFULLY, task id: %s", cr.GetSpecT().GetTaskID())
}

func (w *worker) setHostStatusesPreliminary(ctx context.Context, cr *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", cr.GetName())
		return
	}

	existingObjects := w.c.discovery(ctx, cr)
	cr.EnsureRuntime().ActionPlan.WalkAdded(
		// Walk over added clusters
		func(cluster api.ICluster) {
			w.a.V(1).M(cr).Info("Walking over AP added clusters. Cluster: %s", cluster.GetName())

			cluster.WalkHosts(func(host *api.Host) error {
				w.a.V(1).M(cr).Info("Walking over hosts in added clusters. Cluster: %s Host: %s", cluster.GetName(), host.GetName())

				// Name of the StatefulSet for this host
				name := w.c.namer.Name(interfaces.NameStatefulSet, host)
				// Have we found this StatefulSet
				found := false

				existingObjects.WalkStatefulSet(func(meta meta.Object) {
					w.a.V(3).M(cr).Info("Walking over existing sts list. sts: %s", util.NamespacedName(meta))
					if name == meta.GetName() {
						// StatefulSet of this host already exist
						found = true
					}
				})

				if found {
					// StatefulSet of this host already exist, we can't name it a NEW one for sure
					// It looks like FOUND is the most correct approach
					w.a.V(1).M(cr).Info("Add host as FOUND via cluster. Host was found as sts. Host: %s", host.GetName())
					host.GetReconcileAttributes().SetStatus(types.ObjectStatusFound)
				} else {
					// StatefulSet of this host does not exist, looks like we can name it as a NEW one
					w.a.V(1).M(cr).Info("Add host as NEW via cluster. Host was not found as sts. Host: %s", host.GetName())
					host.GetReconcileAttributes().SetStatus(types.ObjectStatusRequested)
				}

				return nil
			})
		},
		// Walk over added shards
		func(shard api.IShard) {
			w.a.V(1).M(cr).Info("Walking over AP added shards. Shard: %s", shard.GetName())
			// Mark all hosts of the shard as newly added
			shard.WalkHosts(func(host *api.Host) error {
				w.a.V(1).M(cr).Info("Add host as NEW via shard. Shard: %s Host: %s", shard.GetName(), host.GetName())
				host.GetReconcileAttributes().SetStatus(types.ObjectStatusRequested)
				return nil
			})
		},
		// Walk over added hosts
		func(host *api.Host) {
			w.a.V(1).M(cr).Info("Walking over AP added hosts. Host: %s", host.GetName())
			w.a.V(1).M(cr).Info("Add host as NEW via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetStatus(types.ObjectStatusRequested)
		},
	)

	cr.EnsureRuntime().ActionPlan.WalkModified(
		func(cluster api.ICluster) {
			w.a.V(1).M(cr).Info("Walking over AP modified clusters. Cluster: %s", cluster.GetName())
		},
		func(shard api.IShard) {
			w.a.V(1).M(cr).Info("Walking over AP modified shards. Shard: %s", shard.GetName())
		},
		func(host *api.Host) {
			w.a.V(1).M(cr).Info("Walking over AP modified hosts. Host: %s", host.GetName())
			w.a.V(1).M(cr).Info("Add host as MODIFIED via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
		},
	)

	// Fill gaps in host statuses. Fill unfilled hosts
	cr.WalkHosts(func(host *api.Host) error {
		w.a.V(3).M(cr).Info("Walking over CR hosts. Host: %s", host.GetName())
		_, err := w.c.kube.STS().Get(ctx, host)
		switch {
		case host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusRequested):
			w.a.V(3).M(cr).Info("Walking over CR hosts. Host: is already listed as NEW. Status is clear. Host: %s", host.GetName())
			return nil
		case host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusModified):
			w.a.V(3).M(cr).Info("Walking over CR hosts. Host: is already listed as MODIFIED. Status is clear. Host: %s", host.GetName())
			return nil
		case host.HasAncestor():
			w.a.V(1).M(cr).Info("Add host as FOUND via host because host has an ancestor. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetStatus(types.ObjectStatusFound)
			return nil
		case err == nil:
			w.a.V(1).M(cr).Info("Add host as FOUND via host because has found sts. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetStatus(types.ObjectStatusFound)
			return nil
		default:
			w.a.V(1).M(cr).Info("Add host as New via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetStatus(types.ObjectStatusRequested)
			return nil
		}
	})

	w.logHosts(cr)
}

// Log hosts statuses
func (w *worker) logHosts(cr api.ICustomResource) {
	cr.WalkHosts(func(host *api.Host) error {
		w.a.M(host).Info("Host status: %s. Host: %s", host.GetReconcileAttributes().GetStatus(), host.Runtime.Address.CompactString())
		return nil
	})
}

func (w *worker) createTemplatedCR(_chi *api.ClickHouseInstallation, _opts ...*commonNormalizer.Options[api.ClickHouseInstallation]) *api.ClickHouseInstallation {
	l := w.a.V(1).M(_chi).F()

	if _chi.HasAncestor() {
		l.Info("CR has an ancestor, use it as a base for reconcile. CR: %s", util.NamespaceNameString(_chi))
	} else {
		l.Info("CR has NO ancestor, use empty base for reconcile. CR: %s", util.NamespaceNameString(_chi))
	}

	chi := w.createTemplated(_chi, _opts...)
	chi.SetAncestor(w.createTemplated(_chi.GetAncestorT()))

	return chi
}

func (w *worker) createTemplated(c *api.ClickHouseInstallation, _opts ...*commonNormalizer.Options[api.ClickHouseInstallation]) *api.ClickHouseInstallation {
	opts := commonNormalizer.NewOptions[api.ClickHouseInstallation]()
	if len(_opts) > 0 {
		opts = _opts[0]
	}
	chi, _ := w.normalizer.CreateTemplated(c, opts)
	return chi
}
