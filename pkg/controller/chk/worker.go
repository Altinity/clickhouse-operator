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
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller/domain"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	commonConfig "github.com/altinity/clickhouse-operator/pkg/model/common/config"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// worker represents worker thread which runs reconcile tasks
type worker struct {
	c *Controller
	a a.Announcer

	normalizer    *normalizer.Normalizer
	task          *common.Task
	stsReconciler *statefulset.Reconciler

	start time.Time
}

// newWorker
func (c *Controller) newWorker() *worker {
	start := time.Now()
	//kind := "ClickHouseKeeperInstallation"
	//generateName := "chop-chk-"
	//component := componentName

	announcer := a.NewAnnouncer(
		//common.NewEventEmitter(c.kube.Event(), kind, generateName, component),
		nil,
		c.kube.CR(),
	)

	return &worker{
		c: c,
		a: announcer,

		normalizer: normalizer.New(),
		start:      start,
		task:       nil,
	}
}

func configGeneratorOptions(cr *apiChk.ClickHouseKeeperInstallation) *config.GeneratorOptions {
	return &config.GeneratorOptions{
		Settings: cr.GetSpecT().GetConfiguration().GetSettings(),
		Files:    cr.GetSpecT().GetConfiguration().GetFiles(),
	}
}

func (w *worker) buildCreator(cr *apiChk.ClickHouseKeeperInstallation) *commonCreator.Creator {
	if cr == nil {
		cr = &apiChk.ClickHouseKeeperInstallation{}
	}
	return commonCreator.NewCreator(
		cr,
		managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeKeeper, cr, configGeneratorOptions(cr)),
		managers.NewContainerManager(managers.ContainerManagerTypeKeeper),
		managers.NewTagManager(managers.TagManagerTypeKeeper, cr),
		managers.NewProbeManager(managers.ProbeManagerTypeKeeper),
		managers.NewServiceManager(managers.ServiceManagerTypeKeeper),
		managers.NewVolumeManager(managers.VolumeManagerTypeKeeper),
		managers.NewConfigMapManager(managers.ConfigMapManagerTypeKeeper),
		managers.NewNameManager(managers.NameManagerTypeKeeper),
		managers.NewOwnerReferencesManager(managers.OwnerReferencesManagerTypeKeeper),
		namer.New(),
		macro.New(),
		labeler.New(cr),
	)
}

func (w *worker) newTask(new, old *apiChk.ClickHouseKeeperInstallation) {
	w.task = common.NewTask(w.buildCreator(new), w.buildCreator(old))
	w.stsReconciler = statefulset.NewReconciler(
		w.a,
		w.task,
		domain.NewHostPodPoller(domain.NewHostObjectPoller(w.c.kube.STS()), domain.NewHostObjectPoller(w.c.kube.Pod()), nil),
		w.c.namer,
		labeler.New(new),
		storage.NewStorageReconciler(w.task, w.c.namer, w.c.kube.Storage()),
		w.c.kube,
		statefulset.NewDefaultFallback(),
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

func (w *worker) markReconcileStart(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation, ap *action_plan.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", cr.GetName())
		return
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	cr.EnsureStatus().ReconcileStart(ap.GetRemovedHostsNum())
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
	w.a.V(2).M(cr).F().Info("action plan\n%s\n", ap.String())
}

func (w *worker) finalizeReconcileAndMarkCompleted(ctx context.Context, _cr *apiChk.ClickHouseKeeperInstallation) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", _cr.GetName())
		return
	}

	w.a.V(1).M(_cr).F().S().Info("finalize reconcile")

	// Update CHI object
	if chi, err := w.createCRFromObjectMeta(_cr, true, commonNormalizer.NewOptions[apiChk.ClickHouseKeeperInstallation]()); err == nil {
		w.a.V(1).M(chi).Info("updating endpoints for CR-2 %s", chi.Name)
		ips := w.c.getPodsIPs(ctx, chi)
		w.a.V(1).M(chi).Info("IPs of the CR-2 finalize reconcile %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
		opts := commonNormalizer.NewOptions[apiChk.ClickHouseKeeperInstallation]()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCRFromObjectMeta(_cr, true, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-2")
			chi.SetAncestor(chi.GetTarget())
			chi.SetTarget(nil)
			chi.EnsureStatus().ReconcileComplete()
			// TODO unify with update endpoints
			w.newTask(chi, chi.GetAncestorT())
			//w.reconcileConfigMapCommonUsers(ctx, chi)
			w.c.updateCRObjectStatus(ctx, chi, types.UpdateStatusOptions{
				CopyStatusOptions: types.CopyStatusOptions{
					CopyStatusFieldGroup: types.CopyStatusFieldGroup{
						FieldGroupWholeStatus: true,
					},
				},
			})
		} else {
			w.a.M(_cr).F().Error("internal unable to find CR by %v err: %v", _cr.GetLabels(), err)
		}
	} else {
		w.a.M(_cr).F().Error("external unable to find CR by %v err %v", _cr.GetLabels(), err)
	}

	w.a.V(1).
		WithEvent(_cr, a.EventActionReconcile, a.EventReasonReconcileCompleted).
		WithAction(_cr).
		WithActions(_cr).
		M(_cr).F().
		Info("reconcile completed successfully, task id: %s", _cr.GetSpecT().GetTaskID())
}

func (w *worker) markReconcileCompletedUnsuccessfully(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation, err error) {
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

func (w *worker) setHostStatusesPreliminary(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation, ap *action_plan.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. cr: %s ", cr.GetName())
		return
	}

	existingObjects := w.c.discovery(ctx, cr)
	ap.WalkAdded(
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

	ap.WalkModified(
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

func (w *worker) createTemplatedCR(_chk *apiChk.ClickHouseKeeperInstallation, _opts ...*commonNormalizer.Options[apiChk.ClickHouseKeeperInstallation]) *apiChk.ClickHouseKeeperInstallation {
	l := w.a.V(1).M(_chk).F()

	if _chk.HasAncestor() {
		l.Info("CR has an ancestor, use it as a base for reconcile. CR: %s", util.NamespaceNameString(_chk))
	} else {
		l.Info("CR has NO ancestor, use empty base for reconcile. CR: %s", util.NamespaceNameString(_chk))
	}

	chk := w.createTemplated(_chk, _opts...)
	chk.SetAncestor(w.createTemplated(_chk.GetAncestorT()))

	return chk
}

func (w *worker) createTemplated(c *apiChk.ClickHouseKeeperInstallation, _opts ...*commonNormalizer.Options[apiChk.ClickHouseKeeperInstallation]) *apiChk.ClickHouseKeeperInstallation {
	opts := commonNormalizer.NewOptions[apiChk.ClickHouseKeeperInstallation]()
	if len(_opts) > 0 {
		opts = _opts[0]
	}
	chk, _ := w.normalizer.CreateTemplated(c, opts)
	return chk
}

// getRaftGeneratorOptions build base set of RaftOptions
func (w *worker) getRaftGeneratorOptions() *commonConfig.HostSelector {
	// Raft specifies to exclude:
	// 1. all newly added hosts
	// 2. all explicitly excluded hosts
	return commonConfig.NewHostSelector().ExcludeReconcileAttributes(
		types.NewReconcileAttributes(),
		//SetAdd().
		//SetExclude(),
	)
}

// options build FilesGeneratorOptionsClickHouse
func (w *worker) options() *config.FilesGeneratorOptions {
	opts := w.getRaftGeneratorOptions()
	w.a.Info("RaftOptions: %s", opts)
	return config.NewFilesGeneratorOptions().SetRaftOptions(opts)
}

// createCRFromObjectMeta
func (w *worker) createCRFromObjectMeta(
	meta meta.Object,
	isCHI bool,
	options *commonNormalizer.Options[apiChk.ClickHouseKeeperInstallation],
) (*apiChk.ClickHouseKeeperInstallation, error) {
	w.a.V(3).M(meta).S().P()
	defer w.a.V(3).M(meta).E().P()

	chi, err := w.c.GetCHIByObjectMeta(meta, isCHI)
	if err != nil {
		return nil, err
	}

	chi, err = w.normalizer.CreateTemplated(chi, options)
	if err != nil {
		return nil, err
	}

	return chi, nil
}
