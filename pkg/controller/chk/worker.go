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
	commonMacro "github.com/altinity/clickhouse-operator/pkg/model/common/macro"
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
		commonMacro.New(macro.List),
		labeler.New(cr),
	)
}

func (w *worker) newTask(new, old *apiChk.ClickHouseKeeperInstallation) {
	w.task = common.NewTask(w.buildCreator(new), w.buildCreator(old))
	w.stsReconciler = statefulset.NewReconciler(
		w.a,
		w.task,
		//poller.NewHostStatefulSetPoller(poller.NewStatefulSetPoller(w.c.kube), w.c.kube, w.c.labeler),
		domain.NewHostStatefulSetPoller(domain.NewStatefulSetPoller(w.c.kube), w.c.kube, nil),
		w.c.namer,
		labeler.New(new),
		storage.NewStorageReconciler(w.task, w.c.namer, w.c.kube.Storage()),
		w.c.kube,
		statefulset.NewDefaultFallback(),
	)
}

// shouldForceRestartHost checks whether cluster requires hosts restart
func (w *worker) shouldForceRestartHost(host *api.Host) bool {
	// RollingUpdate purpose is to always shut the host down.
	// It is such an interesting policy.
	if host.GetCR().IsRollingUpdate() {
		w.a.V(1).M(host).F().Info("RollingUpdate requires force restart. Host: %s", host.GetName())
		return true
	}

	if host.GetReconcileAttributes().GetStatus() == api.ObjectStatusNew {
		w.a.V(1).M(host).F().Info("Host is new, no restart applicable. Host: %s", host.GetName())
		return false
	}

	if (host.GetReconcileAttributes().GetStatus() == api.ObjectStatusSame) && !host.HasAncestor() {
		w.a.V(1).M(host).F().Info("Host already exists, but has no ancestor, no restart applicable. Host: %s", host.GetName())
		return false
	}

	// For some configuration changes we have to force restart host
	if model.IsConfigurationChangeRequiresReboot(host) {
		w.a.V(1).M(host).F().Info("Config change(s) require host restart. Host: %s", host.GetName())
		return true
	}

	podIsCrushed := false
	// pod.Status.ContainerStatuses[0].State.Waiting.Reason
	if pod, err := w.c.kube.Pod().Get(host); err == nil {
		if len(pod.Status.ContainerStatuses) > 0 {
			if pod.Status.ContainerStatuses[0].State.Waiting != nil {
				if pod.Status.ContainerStatuses[0].State.Waiting.Reason == "CrashLoopBackOff" {
					podIsCrushed = true
				}
			}
		}
	}

	if host.Runtime.Version.IsUnknown() && podIsCrushed {
		w.a.V(1).M(host).F().Info("Host with unknown version and in CrashLoopBackOff should be restarted. It most likely is unable to start due to bad config. Host: %s", host.GetName())
		return true
	}

	w.a.V(1).M(host).F().Info("Host restart is not required. Host: %s", host.GetName())
	return false
}

// normalize
func (w *worker) normalize(c *apiChk.ClickHouseKeeperInstallation) *apiChk.ClickHouseKeeperInstallation {
	chk, err := normalizer.New().CreateTemplated(c, commonNormalizer.NewOptions())
	if err != nil {
		w.a.WithEvent(chk, a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithError(chk).
			M(chk).F().
			Error("FAILED to normalize CR 1: %v", err)
	}
	return chk
}

// areUsableOldAndNew checks whether there are old and new usable
func (w *worker) areUsableOldAndNew(old, new *apiChk.ClickHouseKeeperInstallation) bool {
	if old == nil {
		return false
	}
	if new == nil {
		return false
	}
	return true
}

// isGenerationTheSame checks whether old ans new CHI have the same generation
func (w *worker) isGenerationTheSame(old, new *apiChk.ClickHouseKeeperInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	return old.GetGeneration() == new.GetGeneration()
}

func (w *worker) markReconcileStart(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation, ap *action_plan.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
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

func (w *worker) finalizeReconcileAndMarkCompleted(ctx context.Context, _chk *apiChk.ClickHouseKeeperInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.a.V(1).M(_chk).F().S().Info("finalize reconcile")

	// Update CHI object
	if chi, err := w.createCRFromObjectMeta(_chk, true, commonNormalizer.NewOptions()); err == nil {
		w.a.V(1).M(chi).Info("updating endpoints for CR-2 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CR-2 finalize reconcile %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
		opts := commonNormalizer.NewOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCRFromObjectMeta(_chk, true, opts); err == nil {
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
			w.a.M(_chk).F().Error("internal unable to find CR by %v err: %v", _chk.GetLabels(), err)
		}
	} else {
		w.a.M(_chk).F().Error("external unable to find CR by %v err %v", _chk.GetLabels(), err)
	}

	w.a.V(1).
		WithEvent(_chk, a.EventActionReconcile, a.EventReasonReconcileCompleted).
		WithAction(_chk).
		WithActions(_chk).
		M(_chk).F().
		Info("reconcile completed successfully, task id: %s", _chk.GetSpecT().GetTaskID())
}

func (w *worker) markReconcileCompletedUnsuccessfully(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation, err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	switch {
	case err == nil:
		chk.EnsureStatus().ReconcileComplete()
	case errors.Is(err, common.ErrCRUDAbort):
		chk.EnsureStatus().ReconcileAbort()
	}
	w.c.updateCRObjectStatus(ctx, chk, types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	})

	w.a.V(1).
		WithEvent(chk, a.EventActionReconcile, a.EventReasonReconcileFailed).
		WithAction(chk).
		WithActions(chk).
		M(chk).F().
		Warning("reconcile completed UNSUCCESSFULLY, task id: %s", chk.GetSpecT().GetTaskID())
}

func (w *worker) walkHosts(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation, ap *action_plan.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	existingObjects := w.c.discovery(ctx, chk)
	ap.WalkAdded(
		// Walk over added clusters
		func(cluster api.ICluster) {
			w.a.V(1).M(chk).Info("Walking over AP added clusters. Cluster: %s", cluster.GetName())

			cluster.WalkHosts(func(host *api.Host) error {
				w.a.V(1).M(chk).Info("Walking over hosts in added clusters. Cluster: %s Host: %s", cluster.GetName(), host.GetName())

				// Name of the StatefulSet for this host
				name := w.c.namer.Name(interfaces.NameStatefulSet, host)
				// Have we found this StatefulSet
				found := false

				existingObjects.WalkStatefulSet(func(meta meta.Object) {
					w.a.V(3).M(chk).Info("Walking over existing sts list. sts: %s", util.NamespacedName(meta))
					if name == meta.GetName() {
						// StatefulSet of this host already exist
						found = true
					}
				})

				if found {
					// StatefulSet of this host already exist, we can't ADD it for sure
					// It looks like FOUND is the most correct approach
					w.a.V(1).M(chk).Info("Add host as FOUND via cluster. Host was found as sts. Host: %s", host.GetName())
					host.GetReconcileAttributes().SetFound()
				} else {
					// StatefulSet of this host does not exist, looks like we need to ADD it
					w.a.V(1).M(chk).Info("Add host as ADD via cluster. Host was not found as sts. Host: %s", host.GetName())
					host.GetReconcileAttributes().SetAdd()
				}

				return nil
			})
		},
		// Walk over added shards
		func(shard api.IShard) {
			w.a.V(1).M(chk).Info("Walking over AP added shards. Shard: %s", shard.GetName())
			// Mark all hosts of the shard as newly added
			shard.WalkHosts(func(host *api.Host) error {
				w.a.V(1).M(chk).Info("Add host as ADD via shard. Shard: %s Host: %s", shard.GetName(), host.GetName())
				host.GetReconcileAttributes().SetAdd()
				return nil
			})
		},
		// Walk over added hosts
		func(host *api.Host) {
			w.a.V(1).M(chk).Info("Walking over AP added hosts. Host: %s", host.GetName())
			w.a.V(1).M(chk).Info("Add host as ADD via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetAdd()
		},
	)

	ap.WalkModified(
		func(cluster api.ICluster) {
			w.a.V(1).M(chk).Info("Walking over AP modified clusters. Cluster: %s", cluster.GetName())
		},
		func(shard api.IShard) {
			w.a.V(1).M(chk).Info("Walking over AP modified shards. Shard: %s", shard.GetName())
		},
		func(host *api.Host) {
			w.a.V(1).M(chk).Info("Walking over AP modified hosts. Host: %s", host.GetName())
			w.a.V(1).M(chk).Info("Add host as MODIFIED via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetModify()
		},
	)

	chk.WalkHosts(func(host *api.Host) error {
		w.a.V(3).M(chk).Info("Walking over CR hosts. Host: %s", host.GetName())
		switch {
		case host.GetReconcileAttributes().IsAdd():
			w.a.V(3).M(chk).Info("Walking over CR hosts. Host: is already added Host: %s", host.GetName())
			return nil
		case host.GetReconcileAttributes().IsModify():
			w.a.V(3).M(chk).Info("Walking over CR hosts. Host: is already modified Host: %s", host.GetName())
			return nil
		default:
			w.a.V(3).M(chk).Info("Walking over CR hosts. Host: is not clear yet (not detected as added or modified) Host: %s", host.GetName())
			if host.HasAncestor() {
				w.a.V(1).M(chk).Info("Add host as FOUND via host. Host: %s", host.GetName())
				host.GetReconcileAttributes().SetFound()
			} else {
				w.a.V(1).M(chk).Info("Add host as ADD via host. Host: %s", host.GetName())
				host.GetReconcileAttributes().SetAdd()
			}
		}
		return nil
	})

	// Log hosts statuses
	chk.WalkHosts(func(host *api.Host) error {
		switch {
		case host.GetReconcileAttributes().IsAdd():
			w.a.M(host).Info("ADD host: %s", host.Runtime.Address.CompactString())
		case host.GetReconcileAttributes().IsModify():
			w.a.M(host).Info("MODIFY host: %s", host.Runtime.Address.CompactString())
		case host.GetReconcileAttributes().IsFound():
			w.a.M(host).Info("FOUND host: %s", host.Runtime.Address.CompactString())
		default:
			w.a.M(host).Info("UNKNOWN host: %s", host.Runtime.Address.CompactString())
		}
		return nil
	})
}

// getRaftGeneratorOptions build base set of RaftOptions
func (w *worker) getRaftGeneratorOptions() *commonConfig.HostSelector {
	// Raft specifies to exclude:
	// 1. all newly added hosts
	// 2. all explicitly excluded hosts
	return commonConfig.NewHostSelector().ExcludeReconcileAttributes(
		api.NewHostReconcileAttributes(),
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
func (w *worker) createCRFromObjectMeta(meta meta.Object, isCHI bool, options *commonNormalizer.Options) (*apiChk.ClickHouseKeeperInstallation, error) {
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
