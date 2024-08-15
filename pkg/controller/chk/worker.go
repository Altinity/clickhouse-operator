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
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	chkNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonMacro "github.com/altinity/clickhouse-operator/pkg/model/common/macro"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// worker represents worker thread which runs reconcile tasks
type worker struct {
	c             *Controller
	a             common.Announcer
	normalizer    *chkNormalizer.Normalizer
	task          *common.Task
	stsReconciler *statefulset.Reconciler

	start time.Time
}

const componentName = "clickhouse-operator"

// newWorker
func (c *Controller) newWorker() *worker {
	start := time.Now()
	//kind := "ClickHouseKeeperInstallation"
	//generateName := "chop-chk-"
	//component := componentName

	announcer := common.NewAnnouncer(
		//common.NewEventEmitter(c.kube.Event(), kind, generateName, component),
		nil,
		c.kube.CR(),
	)

	return &worker{
		c:          c,
		a:          announcer,
		normalizer: chkNormalizer.New(),
		start:      start,
		task:       nil,
	}
}

func (w *worker) newTask(chk *apiChk.ClickHouseKeeperInstallation) {
	w.task = common.NewTask(
		commonCreator.NewCreator(
			chk,
			managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeKeeper, chk, configGeneratorOptions(chk)),
			managers.NewContainerManager(managers.ContainerManagerTypeKeeper),
			managers.NewTagManager(managers.TagManagerTypeKeeper, chk),
			managers.NewProbeManager(managers.ProbeManagerTypeKeeper),
			managers.NewServiceManager(managers.ServiceManagerTypeKeeper),
			managers.NewVolumeManager(managers.VolumeManagerTypeKeeper),
			managers.NewConfigMapManager(managers.ConfigMapManagerTypeKeeper),
			managers.NewNameManager(managers.NameManagerTypeKeeper),
			managers.NewOwnerReferencesManager(managers.OwnerReferencesManagerTypeKeeper),
			namer.New(),
			commonMacro.New(macro.List),
			chkLabeler.New(chk),
		),
	)

	w.stsReconciler = statefulset.NewReconciler(
		w.a,
		w.task,
		//poller.NewHostStatefulSetPoller(poller.NewStatefulSetPoller(w.c.kube), w.c.kube, w.c.labeler),
		poller.NewHostStatefulSetPoller(poller.NewStatefulSetPoller(w.c.kube), w.c.kube, nil),
		w.c.namer,
		chkLabeler.New(chk),
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
func (w *worker) normalize(_chk *apiChk.ClickHouseKeeperInstallation) *apiChk.ClickHouseKeeperInstallation {
	chk, err := chkNormalizer.New().CreateTemplated(_chk, commonNormalizer.NewOptions())
	if err != nil {
		log.V(1).
			M(chk).F().
			Error("FAILED to normalize CHK: %v", err)
	}
	return chk
}

func configGeneratorOptions(chk *apiChk.ClickHouseKeeperInstallation) *chkConfig.GeneratorOptions {
	return &chkConfig.GeneratorOptions{}
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

	return old.Generation == new.Generation
}

func (w *worker) markReconcileStart(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation, ap *action_plan.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	chk.EnsureStatus().ReconcileStart(ap.GetRemovedHostsNum())
	_ = w.c.updateCRObjectStatus(ctx, chk, types.UpdateStatusOptions{
		CopyStatusOptions: types.CopyStatusOptions{
			MainFields: true,
		},
	})

	w.a.V(1).
		WithEvent(chk, common.EventActionReconcile, common.EventReasonReconcileStarted).
		WithStatusAction(chk).
		WithStatusActions(chk).
		M(chk).F().
		Info("reconcile started, task id: %s", chk.GetSpecT().GetTaskID())
	w.a.V(2).M(chk).F().Info("action plan\n%s\n", ap.String())
}

func (w *worker) finalizeReconcileAndMarkCompleted(ctx context.Context, _chk *apiChk.ClickHouseKeeperInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.a.V(1).M(_chk).F().S().Info("finalize reconcile")

	// Update CHI object
	if chi, err := w.createCHIFromObjectMeta(_chk, true, commonNormalizer.NewOptions()); err == nil {
		w.a.V(1).M(chi).Info("updating endpoints for CHI-2 %s", chi.Name)
		ips := w.c.getPodsIPs(chi)
		w.a.V(1).M(chi).Info("IPs of the CHI-2 finalize reconcile %s/%s: len: %d %v", chi.Namespace, chi.Name, len(ips), ips)
		opts := commonNormalizer.NewOptions()
		opts.DefaultUserAdditionalIPs = ips
		if chi, err := w.createCHIFromObjectMeta(_chk, true, opts); err == nil {
			w.a.V(1).M(chi).Info("Update users IPS-2")
			chi.SetAncestor(chi.GetTarget())
			chi.SetTarget(nil)
			chi.EnsureStatus().ReconcileComplete()
			// TODO unify with update endpoints
			w.newTask(chi)
			//w.reconcileConfigMapCommonUsers(ctx, chi)
			w.c.updateCRObjectStatus(ctx, chi, types.UpdateStatusOptions{
				CopyStatusOptions: types.CopyStatusOptions{
					WholeStatus: true,
				},
			})
		} else {
			w.a.M(_chk).F().Error("internal unable to find CHI by %v err: %v", _chk.GetLabels(), err)
		}
	} else {
		w.a.M(_chk).F().Error("external unable to find CHI by %v err %v", _chk.GetLabels(), err)
	}

	w.a.V(1).
		WithEvent(_chk, common.EventActionReconcile, common.EventReasonReconcileCompleted).
		WithStatusAction(_chk).
		WithStatusActions(_chk).
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
			MainFields: true,
		},
	})

	w.a.V(1).
		WithEvent(chk, common.EventActionReconcile, common.EventReasonReconcileFailed).
		WithStatusAction(chk).
		WithStatusActions(chk).
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
			cluster.WalkHosts(func(host *api.Host) error {

				// Name of the StatefulSet for this host
				name := w.c.namer.Name(interfaces.NameStatefulSet, host)
				// Have we found this StatefulSet
				found := false

				existingObjects.WalkStatefulSet(func(meta meta.Object) {
					if name == meta.GetName() {
						// StatefulSet of this host already exist
						found = true
					}
				})

				if found {
					// StatefulSet of this host already exist, we can't ADD it for sure
					// It looks like FOUND is the most correct approach
					host.GetReconcileAttributes().SetFound()
					w.a.V(1).M(chk).Info("Add host as FOUND via cluster. Host was found as sts. Host: %s", host.GetName())
				} else {
					// StatefulSet of this host does not exist, looks like we need to ADD it
					host.GetReconcileAttributes().SetAdd()
					w.a.V(1).M(chk).Info("Add host as ADD via cluster. Host was not found as sts. Host: %s", host.GetName())
				}

				return nil
			})
		},
		// Walk over added shards
		func(shard api.IShard) {
			// Mark all hosts of the shard as newly added
			shard.WalkHosts(func(host *api.Host) error {
				host.GetReconcileAttributes().SetAdd()
				w.a.V(1).M(chk).Info("Add host as ADD via shard. Host: %s", host.GetName())
				return nil
			})
		},
		// Walk over added hosts
		func(host *api.Host) {
			host.GetReconcileAttributes().SetAdd()
			w.a.V(1).M(chk).Info("Add host as ADD via host. Host: %s", host.GetName())
		},
	)

	ap.WalkModified(
		func(cluster api.ICluster) {
		},
		func(shard api.IShard) {
		},
		func(host *api.Host) {
			w.a.V(1).M(chk).Info("Add host as MODIFIED via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetModify()
		},
	)

	chk.WalkHosts(func(host *api.Host) error {
		switch {
		case host.GetReconcileAttributes().IsAdd():
			// Already added
			return nil
		case host.GetReconcileAttributes().IsModify():
			// Already modified
			return nil
		default:
			// Not clear yet
			w.a.V(1).M(chk).Info("Add host as FOUND via host. Host: %s", host.GetName())
			host.GetReconcileAttributes().SetFound()
		}
		return nil
	})

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

// createCHIFromObjectMeta
func (w *worker) createCHIFromObjectMeta(meta meta.Object, isCHI bool, options *commonNormalizer.Options) (*apiChk.ClickHouseKeeperInstallation, error) {
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
