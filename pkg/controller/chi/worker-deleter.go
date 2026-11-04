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
	"io"
	"net"
	"strings"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/util"
	goch "github.com/mailru/go-clickhouse/v2"
)

func (w *worker) clean(ctx context.Context, cr api.ICustomResource) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile clean is aborted. CR: %s ", cr.GetName())
		return
	}

	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileInProgress).
		WithAction(cr).
		M(cr).F().
		Info("remove items scheduled for deletion")

	// Remove deleted items
	w.a.V(1).M(cr).F().Info("List of objects which have failed to reconcile:\n%s", w.task.RegistryFailed())
	w.a.V(1).M(cr).F().Info("List of successfully reconciled objects:\n%s", w.task.RegistryReconciled())
	objs := w.c.discovery(ctx, cr)
	need := w.task.RegistryReconciled()
	w.a.V(1).M(cr).F().Info("List of existing objects:\n%s", objs)
	objs.Subtract(need)
	w.a.V(1).M(cr).F().Info("List of non-reconciled objects:\n%s", objs)
	if w.purge(ctx, cr, objs, w.task.RegistryFailed()) > 0 {
		util.WaitContextDoneOrTimeout(ctx, 1*time.Minute)
	}

	st := cr.(*api.ClickHouseInstallation).EnsureStatus()
	st.SyncHostTablesCreated()
	st.SyncHostsWithReplicaCaughtUp()
}

// dropZKReplicas cleans Zookeeper for replicas that are properly deleted - via Action Plan
func (w *worker) dropZKReplicas(ctx context.Context, cr *api.ClickHouseInstallation) error {
	// Iterate over Action Plan and drop all replicas that are properly removed as removed hosts
	w.a.V(1).M(cr).F().S().Info("drop replicas based on AP")
	var succeeded, failed, skipped int
	var firstErr error
	cr.EnsureRuntime().ActionPlan.WalkRemoved(
		func(cluster api.ICluster) {
		},
		func(shard api.IShard) {
		},
		func(host *api.Host) {
			dropped, err := w.dropZKReplica(ctx, cr, host, NewDropReplicaOptions().SetRegularDrop())
			if err != nil {
				failed++
				if firstErr == nil {
					firstErr = err
				}
				w.a.V(1).M(cr).F().Warning("Failed to drop ZK replica %s: %v", host.GetName(), err)
			} else if dropped {
				succeeded++
			} else {
				skipped++
			}
		},
	)
	if failed > 0 {
		w.a.WithEvent(cr, a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(cr).
			M(cr).F().
			Error("Failed to drop %d ZK replica(s). Succeeded: %d, skipped: %d. Error: %v", failed, succeeded, skipped, firstErr)
		w.a.V(1).M(cr).F().E().Error("drop replicas completed with errors. Succeeded: %d, failed: %d, skipped: %d", succeeded, failed, skipped)
		if isRetryableReplicaDropError(firstErr) {
			return fmt.Errorf("%w: failed to drop %d ZK replica(s): %w", ErrZKReplicaDropRetryable, failed, firstErr)
		}
		return fmt.Errorf("%w: failed to drop %d ZK replica(s): %w", ErrZKReplicaDropPermanent, failed, firstErr)
	}
	w.a.V(1).M(cr).F().E().Info("drop replicas completed. Succeeded: %d, failed: %d, skipped: %d", succeeded, failed, skipped)
	return nil
}

// purge
func (w *worker) purge(
	ctx context.Context,
	cr api.ICustomResource,
	reg *model.Registry,
	reconcileFailedObjs *model.Registry,
) (cnt int) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Purge is aborted. CR: %s ", cr.GetName())
		return cnt
	}

	reg.Walk(func(entityType model.EntityType, m meta.Object) {
		switch entityType {
		case model.StatefulSet:
			cnt += w.purgeStatefulSet(ctx, cr, reconcileFailedObjs, m)
		case model.PVC:
			w.purgePVC(ctx, cr, reconcileFailedObjs, m)
		case model.ConfigMap:
			w.purgeConfigMap(ctx, cr, reconcileFailedObjs, m)
		case model.Service:
			w.purgeService(ctx, cr, reconcileFailedObjs, m)
		case model.Secret:
			w.purgeSecret(ctx, cr, reconcileFailedObjs, m)
		case model.PDB:
			w.purgePDB(ctx, cr, reconcileFailedObjs, m)
		}
	})
	return cnt
}

func (w *worker) purgeStatefulSet(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) int {
	if shouldPurgeStatefulSet(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete StatefulSet: %s", util.NamespaceNameString(m))
		if err := w.c.kube.STS().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete StatefulSet: %s, err: %v", util.NamespaceNameString(m), err)
		}
		return 1
	}
	return 0
}

func (w *worker) purgePVC(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgePVC(cr, reconcileFailedObjs, m) {
		if chiLabeler.New(nil).GetReclaimPolicy(m) == api.PVCReclaimPolicyDelete {
			w.a.V(1).M(m).F().Info("Delete PVC: %s", util.NamespaceNameString(m))
			if err := w.c.kube.Storage().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
				w.a.V(1).M(m).F().Error("FAILED to delete PVC: %s, err: %v", util.NamespaceNameString(m), err)
			}
		}
	}
}

func (w *worker) purgeConfigMap(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgeConfigMap(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete ConfigMap: %s", util.NamespaceNameString(m))
		if err := w.c.kube.ConfigMap().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete ConfigMap: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func (w *worker) purgeService(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgeService(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete Service: %s", util.NamespaceNameString(m))
		if err := w.c.kube.Service().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete Service: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func (w *worker) purgeSecret(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgeSecret(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete Secret: %s", util.NamespaceNameString(m))
		if err := w.c.kube.Secret().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete Secret: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func (w *worker) purgePDB(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgePDB(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete PDB: %s", util.NamespaceNameString(m))
		if err := w.c.kube.PDB().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete PDB: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func shouldPurgeStatefulSet(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasStatefulSet(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetStatefulSet() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetStatefulSet() == api.ObjectsCleanupDelete
}

func shouldPurgePVC(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasPVC(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetPVC() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetPVC() == api.ObjectsCleanupDelete
}

func shouldPurgeConfigMap(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasConfigMap(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetConfigMap() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetConfigMap() == api.ObjectsCleanupDelete
}

func shouldPurgeService(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasService(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetService() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetService() == api.ObjectsCleanupDelete
}

func shouldPurgeSecret(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasSecret(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetSecret() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetSecret() == api.ObjectsCleanupDelete
}

func shouldPurgePDB(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	return true
}

// discoveryAndDeleteCR deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) discoveryAndDeleteCR(ctx context.Context, cr api.ICustomResource) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Discovery and delete is aborted. CR: %s ", cr.GetName())
		return nil
	}

	metrics.CRUnregister(ctx, cr)

	objs := w.c.discovery(ctx, cr)
	if objs.NumStatefulSet() > 0 {
		cr.WalkHosts(func(host *api.Host) error {
			_ = w.ensureClusterSchemer(host).HostSyncTables(ctx, host)
			return nil
		})
	}
	w.purge(ctx, cr, objs, nil)
	return nil
}

// deleteCRProtocol purges all child resources owned by the CR.
func (w *worker) deleteCRProtocol(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete CR protocol is aborted")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	var err error
	chi, err = w.normalizer.CreateTemplated(chi, normalizer.NewOptions[api.ClickHouseInstallation]())
	if err != nil {
		w.a.WithEvent(chi, a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(chi).
			M(chi).F().
			Error("Delete CHI failed - unable to normalize: %q", err)
		return err
	}

	// Announce delete procedure
	w.a.V(1).
		WithEvent(chi, a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(chi).
		M(chi).F().
		Info("Delete CHI started")

	chi.EnsureStatus().DeleteStart()
	if err := w.c.updateCRObjectStatus(ctx, chi, types.UpdateStatusOptions{
		TolerateAbsence: true,
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	}); err != nil {
		w.a.V(1).M(chi).F().Error("UNABLE to write normalized CHI. err: %q", err)
		return nil
	}

	// Start delete protocol

	// Exclude this CHI from monitoring
	w.c.deleteWatch(chi)

	// Delete Service
	_ = w.c.deleteServiceCR(ctx, chi)

	chi.WalkHosts(func(host *api.Host) error {
		_ = w.ensureClusterSchemer(host).HostSyncTables(ctx, host)
		return nil
	})

	// Delete all clusters
	var wg sync.WaitGroup
	chi.WalkClusters(func(cluster api.ICluster) error {
		return w.deleteCluster(ctx, chi, cluster.(*api.Cluster), &wg)
	})
	wg.Wait()

	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete is aborted")
		return nil
	}

	// Delete ConfigMap(s)
	_ = w.c.deleteConfigMapsCHI(ctx, chi)

	// Delete backup Secret
	_ = w.c.deleteSecretBackup(ctx, chi)

	w.a.V(1).
		WithEvent(chi, a.EventActionDelete, a.EventReasonDeleteCompleted).
		WithAction(chi).
		M(chi).F().
		Info("Delete CHI completed")

	return nil
}

// canDropReplica
func (w *worker) canDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.Host, opt *dropReplicaOptions) (can bool) {

	switch {
	case opt.RegularDrop():
		w.a.V(1).F().Info("Regular drop replica. hostToDrop: %s", hostToDrop.GetName())
		// Check whether drop replicas is allowed to the operator
		if cluster := hostToDrop.GetCluster(); cluster != nil {
			if dropReplicas := cluster.GetReconcile().Host.Drop.Replicas; dropReplicas != nil {
				if dropReplicas.OnDelete.IsFalse() {
					w.a.V(1).F().Info("Regular drop replicas are prohibited to drop. hostToDrop: %s", hostToDrop.GetName())
					return false
				}
			}
		}

		// In case host has volumes to retain - unable to drop replica
		if w.hasHostVolumesToRetain(ctx, hostToDrop) {
			w.a.V(1).F().Info("Retained volume(s) replicas are prohibited to drop. hostToDrop: %s", hostToDrop.GetName())
			return false
		}

		w.a.V(1).F().Info("Allowed to drop replica. hostToDrop: %s", hostToDrop.GetName())
		return true

	case opt.ForceDropUponStorageLoss():
		w.a.V(1).F().Info("Force drop replica upon storage loss. hostToDrop: %s", hostToDrop.GetName())
		// Check whether drop replicas on lost volume is allowed to the operator
		if cluster := hostToDrop.GetCluster(); cluster != nil {
			if dropReplicas := cluster.GetReconcile().Host.Drop.Replicas; dropReplicas != nil {
				if dropReplicas.OnLostVolume.IsFalse() {
					w.a.V(1).F().Info("Drop replicas on lost volume are prohibited. hostToDrop: %s", hostToDrop.GetName())
					return false
				}
				// Active replica may have restriction to be deleted
				if w.ensureClusterSchemer(hostToRunOn).IsHostActiveReplica(ctx, hostToRunOn, hostToDrop) {
					w.a.V(1).F().Info("Replica is an active one. Need ti check whether it can be dropped. hostToDrop: %s", hostToDrop.GetName())
					if dropReplicas.Active.IsFalse() {
						w.a.V(1).F().Info("Active replicas are prohibited to drop. hostToDrop: %s", hostToDrop.GetName())
						return false
					}
				}
			}
		}
		w.a.V(1).F().Info("Accepted force drop replica upon storage loss. hostToDrop: %s", hostToDrop.GetName())
		return true

	default:
		panic("unknown replica drop")
	}
}

// hasHostVolumesToRetain check whether host has PVCs to be retained
// Replica's state has to be kept in Zookeeper for retained volumes.
// ClickHouse expects to have state of the non-empty replica in-place when replica rejoins.
func (w *worker) hasHostVolumesToRetain(ctx context.Context, host *api.Host) (has bool) {
	if w == nil || w.c == nil || w.c.kube == nil || w.c.kube.Storage() == nil {
		w.a.V(1).F().Warning("Storage manager unavailable; assuming host %s has retained volumes to protect data", host.GetName())
		return true
	}
	// Check whether among all PVCs host has reclaim policy "retain" specified
	storage.NewStoragePVC(w.c.kube.Storage()).WalkDiscoveredPVCs(ctx, host, func(pvc *core.PersistentVolumeClaim) {
		if chiLabeler.New(nil).GetReclaimPolicy(pvc.GetObjectMeta()) == api.PVCReclaimPolicyRetain {
			w.a.V(1).F().Info("PVC: %s/%s blocks drop replica. Reclaim policy: %s", pvc.Namespace, pvc.Name, api.PVCReclaimPolicyRetain.String())
			has = true
		}
	})

	return has
}

type dropReplicaOptions struct {
	regularDrop              bool
	forceDropUponStorageLoss bool
}

func NewDropReplicaOptions() *dropReplicaOptions {
	return &dropReplicaOptions{}
}

func (o *dropReplicaOptions) SetRegularDrop() *dropReplicaOptions {
	if o == nil {
		return o
	}
	o.regularDrop = true

	return o
}

func (o *dropReplicaOptions) RegularDrop() bool {
	if o == nil {
		return false
	}

	return o.regularDrop
}

func (o *dropReplicaOptions) SetForceDropUponStorageLoss() *dropReplicaOptions {
	if o == nil {
		return o
	}
	o.forceDropUponStorageLoss = true

	return o
}

func (o *dropReplicaOptions) ForceDropUponStorageLoss() bool {
	if o == nil {
		return false
	}

	return o.forceDropUponStorageLoss
}

type dropReplicaOptionsArr []*dropReplicaOptions

// NewDropReplicaOptionsArr creates new dropReplicaOptions array
func NewDropReplicaOptionsArr(opts ...*dropReplicaOptions) (res dropReplicaOptionsArr) {
	return append(res, opts...)
}

// First gets first option
func (a dropReplicaOptionsArr) First() *dropReplicaOptions {
	if len(a) > 0 {
		return a[0]
	}
	return nil
}

// ErrZKReplicaDropFailed signals that SYSTEM DROP REPLICA failed after exhausting retries.
// ErrZKReplicaDropRetryable marks failures that should be retried with exponential backoff (e.g. Code 305).
// ErrZKReplicaDropPermanent marks non-retryable failures (e.g. authentication errors, syntax errors).
var (
	ErrZKReplicaDropFailed    = errors.New("zk replica drop failed")
	ErrZKReplicaDropRetryable = fmt.Errorf("%w: retryable", ErrZKReplicaDropFailed)
	ErrZKReplicaDropPermanent = fmt.Errorf("%w: permanent", ErrZKReplicaDropFailed)
)

const (
	// defaultRetryDelay specifies the initial backoff delay between retry attempts
	defaultRetryDelay = 5 * time.Second

	// maxRetryDelay caps the backoff delay between retry attempts
	maxRetryDelay = 15 * time.Second

	// defaultReplicaWaitTimeout is used when ZooKeeper session timeout is unconfigured
	defaultReplicaWaitTimeout = 120 * time.Second

	// replicaWaitGracePeriod gives ZooKeeper extra time after session expiration before giving up
	replicaWaitGracePeriod = 30 * time.Second
)

// isReplicaActiveError targets ClickHouse Code 305 ("replica active"), caused by lingering ZooKeeper sessions.
func isReplicaActiveError(err error) bool {
	if err == nil {
		return false
	}
	var chErr *goch.Error
	if errors.As(err, &chErr) && chErr != nil {
		return chErr.Code == 305
	}
	s := err.Error()
	if strings.Contains(s, "Code: 305") {
		return strings.Contains(s, "DB::Exception") ||
			strings.Contains(s, "because it's active") ||
			strings.Contains(s, "Replica is active") ||
			strings.Contains(s, "TABLE_WAS_NOT_DROPPED")
	}
	return strings.Contains(s, "Replica is active") || strings.Contains(s, "because it's active")
}

// isRetryableReplicaDropError determines whether a replica cleanup error is transient/retryable
// (e.g. Code 305 replica still active in Keeper, Keeper connection errors, network timeouts)
// or permanent (e.g. authentication failed, syntax errors, permission denied, context cancellation).
func isRetryableReplicaDropError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if isReplicaActiveError(err) {
		return true
	}
	var chErr *goch.Error
	if errors.As(err, &chErr) && chErr != nil {
		switch chErr.Code {
		case 305: // REPLICA_IS_ACTIVE
			return true
		case 225, // NO_ZOOKEEPER
			159, // TIMEOUT_EXCEEDED
			209, // SOCKET_TIMEOUT
			210: // NETWORK_ERROR
			return true
		case 999: // KEEPER_EXCEPTION
			// Code 999 wraps underlying ZooKeeper/Keeper exceptions.
			// Only transient coordination errors (connection loss, operation timeout, session expiration)
			// qualify for retries. Permanent Keeper errors (ZNOAUTH, ZAUTHFAILED, ZINVALIDACL, ZBADARGUMENTS)
			// must never be retried. Case-insensitive matching handles driver and server message formatting variations.
			msg := strings.ToLower(chErr.Message)
			if strings.Contains(msg, "not authenticated") ||
				strings.Contains(msg, "authentication failed") ||
				strings.Contains(msg, "invalid acl") ||
				strings.Contains(msg, "bad arguments") {
				return false
			}
			if strings.Contains(msg, "connection loss") ||
				strings.Contains(msg, "connection lost") ||
				strings.Contains(msg, "operation timeout") ||
				strings.Contains(msg, "operation timed out") ||
				strings.Contains(msg, "session expired") ||
				strings.Contains(msg, "session moved") ||
				strings.Contains(msg, "connection refused") ||
				strings.Contains(msg, "connection reset") {
				return true
			}
			return false
		case 192, // UNKNOWN_USER
			493, // ACCESS_DENIED
			515, // INCORRECT_ACCESS_ENTITY_DEFINITION
			516, // AUTHENTICATION_FAILED
			62,  // SYNTAX_ERROR
			60,  // UNKNOWN_TABLE
			81,  // UNKNOWN_DATABASE
			47,  // UNKNOWN_IDENTIFIER
			164: // READONLY
			return false
		default:
			return false
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return true
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	msg := err.Error()
	if strings.Contains(msg, "DB::Exception") {
		if strings.Contains(msg, "Code: 305") || strings.Contains(msg, "Code 305") ||
			strings.Contains(msg, "Code: 225") || strings.Contains(msg, "Code 225") ||
			strings.Contains(msg, "Code: 159") || strings.Contains(msg, "Code 159") ||
			strings.Contains(msg, "Code: 209") || strings.Contains(msg, "Code 209") ||
			strings.Contains(msg, "Code: 210") || strings.Contains(msg, "Code 210") {
			return true
		}
		if strings.Contains(msg, "Code: 999") || strings.Contains(msg, "Code 999") {
			lower := strings.ToLower(msg)
			if strings.Contains(lower, "not authenticated") ||
				strings.Contains(lower, "authentication failed") ||
				strings.Contains(lower, "invalid acl") ||
				strings.Contains(lower, "bad arguments") {
				return false
			}
			if strings.Contains(lower, "connection loss") ||
				strings.Contains(lower, "connection lost") ||
				strings.Contains(lower, "operation timeout") ||
				strings.Contains(lower, "operation timed out") ||
				strings.Contains(lower, "session expired") ||
				strings.Contains(lower, "session moved") ||
				strings.Contains(lower, "connection refused") ||
				strings.Contains(lower, "connection reset") {
				return true
			}
		}
	}
	return false
}

// getCoordinatorHost finds an operational surviving host in the same shard to execute SYSTEM DROP REPLICA queries against.
// In ClickHouse, SYSTEM DROP REPLICA resolves metadata against local replicated tables defined on the executing server.
// Cross-shard execution is invalid because hosts in other shards do not define the target shard's tables.
// Returns nil if no surviving coordinator exists in the shard (e.g. single-replica shard deletion or entire shard deletion).
func (w *worker) getCoordinatorHost(cr *api.ClickHouseInstallation, hostToDrop *api.Host) *api.Host {
	if hostToDrop == nil || !hostToDrop.HasCR() || cr == nil {
		return nil
	}

	var targetShard api.IShard
	if hostToDrop.Runtime.Address.ClusterName != "" && hostToDrop.Runtime.Address.ShardName != "" {
		targetShard = cr.FindShard(hostToDrop.Runtime.Address.ClusterName, hostToDrop.Runtime.Address.ShardName)
	} else if hostToDrop.GetShard() != nil && hostToDrop.GetShard().GetName() != "" && hostToDrop.GetCluster() != nil && hostToDrop.GetCluster().GetName() != "" {
		targetShard = cr.FindShard(hostToDrop.GetCluster().GetName(), hostToDrop.GetShard().GetName())
	} else {
		targetShard = cr.FindShard(hostToDrop.Runtime.Address.ClusterIndex, hostToDrop.Runtime.Address.ShardIndex)
	}
	if targetShard == nil || targetShard.IsZero() {
		return nil
	}

	var candidate *api.Host
	targetShard.WalkHosts(func(h *api.Host) error {
		if candidate == nil && h != nil && h.GetName() != hostToDrop.GetName() {
			candidate = h
		}
		return nil
	})
	return candidate
}

// dropZKReplica drops replica's info from Zookeeper
func (w *worker) dropZKReplica(ctx context.Context, cr *api.ClickHouseInstallation, hostToDrop *api.Host, opts *dropReplicaOptions) (dropped bool, err error) {
	if hostToDrop == nil {
		w.a.V(1).F().Error("FAILED to drop replica. Need host to drop.")
		return false, errors.New("hostToDrop is nil")
	}

	if !hostToDrop.HasCR() {
		w.a.V(1).F().Error("FAILED to drop replica. Host has no CR, hostToDrop: %s", hostToDrop.GetName())
		return false, fmt.Errorf("host %s has no CR", hostToDrop.GetName())
	}

	hostToRunOn := w.getCoordinatorHost(cr, hostToDrop)
	if hostToRunOn == nil {
		// Whole-shard or single-replica shard deletion: no surviving same-shard coordinator exists.
		// In ClickHouse, standard SYSTEM DROP REPLICA requires local table definitions on the executing host.
		// Because no same-shard host remains, this operation cannot execute via same-shard coordinator.
		// This is structurally impossible, so we log and emit a non-blocking postponement event rather than failing reconciliation.
		w.a.V(1).M(hostToDrop).F().Warning(
			"Cannot drop ZK replica for host %s: whole shard %d is removed, no surviving same-shard coordinator exists. Shard metadata must be purged from ZooKeeper via cluster-level / ZK-path cleanup.",
			hostToDrop.GetName(), hostToDrop.Runtime.Address.ShardIndex,
		)
		w.a.WithEvent(cr, a.EventActionDelete, a.EventReasonCleanupPostponed).
			M(hostToDrop).F().
			Warning("ZooKeeper metadata cleanup postponed for host %s (shard %d): no surviving same-shard coordinator",
				hostToDrop.GetName(), hostToDrop.Runtime.Address.ShardIndex)
		return false, nil
	}

	if !w.canDropReplica(ctx, hostToRunOn, hostToDrop, opts) {
		w.a.V(1).M(hostToDrop).F().Info("Skipping drop replica on host: %s (prohibited or volumes retained)", hostToDrop.GetName())
		return false, nil
	}

	err = w.retryDropReplica(ctx, hostToRunOn, hostToDrop)

	if err == nil {
		w.a.V(1).
			WithEvent(hostToRunOn.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
			WithAction(hostToRunOn.GetCR()).
			M(hostToRunOn).F().
			Info("Drop replica host: %s in cluster: %s", hostToDrop.GetName(), hostToDrop.Runtime.Address.ClusterName)
		return true, nil
	}

	w.a.WithEvent(hostToRunOn.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
		WithError(hostToRunOn.GetCR()).
		M(hostToRunOn).F().
		Error("FAILED to drop replica on host: %s with error: %v", hostToDrop.GetName(), err)
	return false, err
}

// retryDropReplica executes SYSTEM DROP REPLICA on hostToRunOn, retrying on ClickHouse Code 305
// ("replica is active") with backoff until the ZooKeeper/Keeper session expires or timeout is reached.
func (w *worker) retryDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.Host) error {
	if util.IsContextDone(ctx) {
		return ctx.Err()
	}

	schemer := w.clusterSchemer(hostToRunOn)
	if schemer == nil {
		return errors.New("schemer is nil")
	}

	timeout := defaultReplicaWaitTimeout
	if w != nil && w.dropReplicaTimeout > 0 {
		timeout = w.dropReplicaTimeout
	} else if hostToDrop.HasCR() {
		var zk *api.ZookeeperConfig
		if hostToDrop.GetCluster() != nil && hostToDrop.GetZookeeper() != nil {
			zk = hostToDrop.GetZookeeper()
		} else if chi, ok := hostToDrop.GetCR().(*api.ClickHouseInstallation); ok && chi != nil && chi.Spec.Configuration != nil {
			zk = chi.Spec.Configuration.Zookeeper
		}
		if zk != nil && zk.SessionTimeoutMs > 0 {
			// Protect against potential duration overflow
			if zk.SessionTimeoutMs < int(time.Hour/time.Millisecond) {
				timeout = time.Duration(zk.SessionTimeoutMs)*time.Millisecond + replicaWaitGracePeriod
			}
		}
	}

	baseDelay := defaultRetryDelay
	if w != nil && w.dropReplicaDelay > 0 {
		baseDelay = w.dropReplicaDelay
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		err     error
		attempt = 0
		delay   = baseDelay
	)

	for {
		attempt++
		if util.IsContextDone(timeoutCtx) {
			if timeoutCtx.Err() == context.Canceled {
				return context.Canceled
			}
			if err != nil {
				return fmt.Errorf("SYSTEM DROP REPLICA on %s for host %s (shard %d) timed out after %d attempts: %w",
					hostToRunOn.GetName(), hostToDrop.GetName(), hostToDrop.Runtime.Address.ShardIndex, attempt-1, err)
			}
			return timeoutCtx.Err()
		}

		err = schemer.HostDropReplica(timeoutCtx, hostToRunOn, hostToDrop)
		if err == nil {
			if attempt > 1 {
				w.a.V(1).M(hostToRunOn).F().Info("SYSTEM DROP REPLICA for host %s succeeded after %d attempts", hostToDrop.GetName(), attempt)
			}
			return nil
		}

		// Fail fast on non-305 errors (connection failure, authentication, syntax, etc.)
		if !isReplicaActiveError(err) {
			return fmt.Errorf("SYSTEM DROP REPLICA on %s for host %s (shard %d) failed with non-retryable error: %w",
				hostToRunOn.GetName(), hostToDrop.GetName(), hostToDrop.Runtime.Address.ShardIndex, err)
		}

		if util.IsContextDone(timeoutCtx) {
			if timeoutCtx.Err() == context.Canceled {
				return context.Canceled
			}
			return fmt.Errorf("SYSTEM DROP REPLICA on %s for host %s (shard %d) timed out after %d attempts: %w",
				hostToRunOn.GetName(), hostToDrop.GetName(), hostToDrop.Runtime.Address.ShardIndex, attempt, err)
		}

		w.a.V(1).M(hostToRunOn).F().Warning("Replica %s is still active in Keeper (attempt %d); retrying in %v: %v", hostToDrop.GetName(), attempt, delay, err)

		util.WaitContextDoneOrTimeout(timeoutCtx, delay)
		if util.IsContextDone(timeoutCtx) {
			if timeoutCtx.Err() == context.Canceled {
				return context.Canceled
			}
			return fmt.Errorf("SYSTEM DROP REPLICA on %s for host %s (shard %d) timed out after %d attempts: %w",
				hostToRunOn.GetName(), hostToDrop.GetName(), hostToDrop.Runtime.Address.ShardIndex, attempt, err)
		}

		delay += baseDelay
		if delay > maxRetryDelay {
			delay = maxRetryDelay
		}
	}
}

// deleteTables
func (w *worker) deleteTables(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete tables. Host: %s ", host.GetName())
		return nil
	}

	if !w.c.pvcDeleter.HostCanDeleteAllPVCs(host) {
		return nil
	}
	err := w.ensureClusterSchemer(host).HostDropTables(ctx, host)

	if err == nil {
		w.a.V(1).
			WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Deleted tables on host: %s replica: %d to shard: %d in cluster: %s",
				host.GetName(), host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
	} else {
		w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(host.GetCR()).
			M(host).F().
			Error("FAILED to delete tables on host: %s with error: %v", host.GetName(), err)
	}

	return err
}

// deleteHost deletes all kubernetes resources related to a host
// chi is the new CHI in which there will be no more this host
// Returns an error when the host's StatefulSet could not be read: the resources below it
// cannot be cleaned up safely without knowing whether the host still exists.
func (w *worker) deleteHost(ctx context.Context, chi *api.ClickHouseInstallation, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete host is aborted. Host: %s ", host.GetName())
		return nil
	}

	w.a.V(2).M(host).S().Info(host.Runtime.Address.HostName)
	defer w.a.V(2).M(host).E().Info(host.Runtime.Address.HostName)

	w.a.V(1).
		WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(host.GetCR()).
		M(host).F().
		Info("Delete host: %s/%s - started", host.Runtime.Address.ClusterName, host.GetName())

	sts, err := w.c.kube.STS().Get(ctx, host)
	if err != nil {
		// Never carry a StatefulSet we did not actually read. client-go's typed Get returns a
		// non-nil zero-valued object alongside the error, and the previous value may be stale.
		host.Runtime.CurStatefulSet = nil

		if apiErrors.IsNotFound(err) {
			w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
				WithAction(host.GetCR()).
				M(host).F().
				Info("Delete host: %s/%s - completed StatefulSet not found - already deleted",
					host.Runtime.Address.ClusterName, host.GetName())
			return nil
		}
		// Unable to tell whether the StatefulSet exists. Not retried here: the Get already runs
		// under GetWithRetry, and IsTransientAPIError excludes Forbidden, so an error arriving
		// here is either terminal or an outage that already outlived the whole back-off budget.
		//
		// Do not report deletion as completed - the cleanup below is skipped, and the host's
		// PVCs carry no owner reference (see model/common/creator/pvc.go), so nothing else
		// would reclaim them.
		//
		// No WithError: it would write CR status through an uncancellable retry loop, and the
		// CR is about to be finalized anyway. The event and the log line are what survive.
		w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
			M(host).F().
			Error("Delete host: %s/%s - unable to get StatefulSet, host deletion not performed. err: %v",
				host.Runtime.Address.ClusterName, host.GetName(), err)
		return err
	}
	host.Runtime.CurStatefulSet = sts

	// Pre-delete host hooks: run BEFORE we touch the host's k8s objects so the pod is
	// still up and SQL hooks can drain / de-register it. A pre-delete hook with
	// failurePolicy=Fail (default) aborts deletion — leaves the host in place for the
	// user to fix and re-reconcile (similar to admission-webhook failurePolicy=Fail).
	// failurePolicy=Ignore logs a warning and proceeds with deletion.
	// Unreachable hosts skip hooks with a warning rather than blocking deletion.
	//
	// Policy gating happens upstream in runHostHookActions, NOT here:
	//   - hook succeeds                        → returns nil
	//   - hook fails + failurePolicy=Ignore    → returns nil (warning logged inside)
	//   - hook fails + failurePolicy=Fail      → returns the error
	//   - host unreachable / no hooks defined  → returns nil
	// So a non-nil err here means the user explicitly opted into "abort deletion on
	// hook failure" via failurePolicy=Fail; just propagating it suffices.
	if err := w.runHostPreDeleteHooks(ctx, host); err != nil {
		w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(host.GetCR()).
			M(host).F().
			Error("Pre-delete hook failed for host %s/%s: %v", host.Runtime.Address.ClusterName, host.GetName(), err)
		return err
	}

	// Each host consists of
	// 1. User-level objects - tables on the host
	//    We need to delete tables on the host in order to clean Zookeeper data.
	//    If just delete tables, Zookeeper will still keep track of non-existent tables
	// 2. Kubernetes-level objects - such as StatefulSet, PVC(s), ConfigMap(s), Service(s)
	// Need to delete all these items

	_ = w.deleteTables(ctx, host)
	err = w.c.deleteHost(ctx, host)

	// When deleting the whole CHI (not particular host), CHI may already be unavailable, so update CHI tolerantly
	chi.EnsureStatus().HostDeleted()
	_ = w.c.updateCRObjectStatus(ctx, chi, types.UpdateStatusOptions{
		TolerateAbsence: true,
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	})

	if err == nil {
		w.a.V(1).
			WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Delete host: %s/%s - completed", host.Runtime.Address.ClusterName, host.GetName())
	} else {
		w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(host.GetCR()).
			M(host).F().
			Error("FAILED Delete host: %s/%s - completed", host.Runtime.Address.ClusterName, host.GetName())
	}

	return err
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
// chi is the new CHI in which there will be no more this shard
func (w *worker) deleteShard(ctx context.Context, chi *api.ClickHouseInstallation, shard *api.ChiShard, wg *sync.WaitGroup) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete shard is aborted. shard: %s ", shard.GetName())
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	w.a.V(1).
		WithEvent(shard.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(shard.Runtime.CHI).
		M(shard).F().
		Info("Delete shard: %s/%s - started", shard.Runtime.Address.Namespace, shard.Name)

	// Delete Shard Service
	_ = w.c.deleteServiceShard(ctx, shard)

	// Delete all replicas in parallel
	shard.WalkHosts(func(host *api.Host) error {
		wg.Add(1)
		go func(h *api.Host) {
			defer wg.Done()
			// TODO: propagate this error. deleteCHI uninstalls the finalizer regardless,
			// so a failed host delete still lets the CR go away and orphans its PVCs.
			// Needs an error sink shared across these goroutines (#2056).
			_ = w.deleteHost(ctx, chi, h)
		}(host)
		return nil
	})

	w.a.V(1).
		WithEvent(shard.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteCompleted).
		WithAction(shard.Runtime.CHI).
		M(shard).F().
		Info("Delete shard: %s/%s - completed", shard.Runtime.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
// chi is the new CHI in which there will be no more this cluster
func (w *worker) deleteCluster(ctx context.Context, chi *api.ClickHouseInstallation, cluster *api.Cluster, wg *sync.WaitGroup) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	w.a.V(1).
		WithEvent(cluster.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(cluster.Runtime.CHI).
		M(cluster).F().
		Info("Delete cluster: %s/%s - started", cluster.Runtime.Address.Namespace, cluster.Name)

	// Delete ChkCluster Service
	_ = w.c.deleteServiceCluster(ctx, cluster)

	// Delete ChkCluster's Auto Secret
	if cluster.Secret.Source() == api.ClusterSecretSourceAuto {
		// Delete ChkCluster Secret
		_ = w.c.deleteSecretCluster(ctx, cluster)
	}

	// Delete all shards
	cluster.WalkShards(func(index int, shard api.IShard) error {
		return w.deleteShard(ctx, chi, shard.(*api.ChiShard), wg)
	})

	w.a.V(1).
		WithEvent(cluster.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteCompleted).
		WithAction(cluster.Runtime.CHI).
		M(cluster).F().
		Info("Delete cluster: %s/%s - completed", cluster.Runtime.Address.Namespace, cluster.Name)

	return nil
}

// deleteCHI
func (w *worker) deleteCHI(ctx context.Context, old, new *api.ClickHouseInstallation) bool {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete CHI is aborted")
		return false
	}

	// Do we have pending request for CHI to be deleted?
	if new.GetDeletionTimestamp().IsZero() {
		// CHI is not being deleted and operator has not deleted anything.
		return false
	}

	w.a.V(3).M(new).S().P()
	defer w.a.V(3).M(new).E().P()

	// Ok, we have pending request for CHI to be deleted.
	// However, we need to decide, should CHI's child resources be deleted or not.
	// There is a curious situation, when CRD is deleted and k8s starts to delete all resources of the type,
	// described by CRD being deleted. This is may be unexpected and very painful situation,
	// so in this case we should agree to delete CHI itself, but has to keep all CHI's child resources.

	var purge bool
	crd, err := w.c.extClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, "clickhouseinstallations.clickhouse.altinity.com", controller.NewGetOptions())
	if err == nil {
		// CRD is in place
		if crd.GetObjectMeta().GetDeletionTimestamp().IsZero() {
			// CRD is not being deleted. It is standard request to delete a CR only.
			// Operator can delete all child resources.
			w.a.V(1).M(new).F().Info("CRD: %s/%s is not being deleted, operator will delete child resources", crd.Namespace, crd.Name)
			purge = true
		} else {
			// CRD is being deleted.
			// In most cases, users do not expect to delete all CRs with all their resources as along with CRD.
			// Operator should not delete child resources - especially storage, such as PVCs and PVs
			w.a.V(1).M(new).F().Info("CRD: %s/%s BEING DELETED, operator will NOT delete child resources", crd.Namespace, crd.Name)
			purge = false
		}
	} else {
		// CRD not found — proceed with cleanup
		w.a.V(1).M(new).F().Error("unable to get CRD, got error: %v ", err)
		w.a.V(1).M(new).F().Info("will delete CR with all resources: %s/%s", new.Namespace, new.Name)
		purge = true
	}

	if purge {
		cur, err := w.c.chopClient.ClickhouseV1().ClickHouseInstallations(new.Namespace).Get(ctx, new.Name, controller.NewGetOptions())
		if cur == nil {
			return false
		}
		if err != nil {
			return false
		}

		if !util.InArray(FinalizerName, new.GetFinalizers()) {
			// No finalizer found, unexpected behavior
			return false
		}

		_ = w.deleteCRProtocol(ctx, new)
	} else {
		new.GetRuntime().GetAttributes().SetSkipOwnerRef(true)
		_ = w.reconcileCR(ctx, old, new)
	}

	// We need to uninstall finalizer in order to allow k8s to delete CR resource
	w.a.V(2).M(new).F().Info("uninstall finalizer")
	if err := w.c.uninstallFinalizer(ctx, new); err != nil {
		w.a.V(1).M(new).F().Error("unable to uninstall finalizer. err: %v", err)
	}

	// CR delete completed
	return true
}

// removedHostFQDNs enumerates the FQDNs of the hosts this pass planned to remove.
//
// WalkRemoved dispatches on the diff entry's type, so a removed cluster or shard arrives as ONE
// entry rather than as its constituent hosts - both are expanded back into hosts here, the same
// way runHostPreDeleteHooksOnRemovedHosts does. nameFQDN is injected so the walk is reachable
// from a test without a live name manager.
func removedHostFQDNs(cr *api.ClickHouseInstallation, nameFQDN func(*api.Host) string) (fqdns []string) {
	if cr == nil {
		return nil
	}
	collect := func(host *api.Host) error {
		if host != nil {
			fqdns = append(fqdns, nameFQDN(host))
		}
		return nil
	}
	cr.EnsureRuntime().ActionPlan.WalkRemoved(
		func(cluster api.ICluster) { cluster.WalkHosts(collect) },
		func(shard api.IShard) { shard.WalkHosts(collect) },
		func(host *api.Host) { _ = collect(host) },
	)
	return fqdns
}

// announceCleanupPostponed surfaces the one consequence of a deferred pass that is otherwise
// invisible: clean() did not purge the removed hosts and dropZKReplicas did not drop their ZK
// paths, so they keep consuming storage and keep showing up in system.replicas.
//
// Gated on there actually being removed hosts. Events here are created directly against the API
// (chi/kube/event.go) with no client-go correlator to aggregate them, so an ungated call would
// mint a fresh Event object on every deferred pass, forever. Deliberately no WithError: the
// host-level branch already records WHY the pass deferred, and .status.errors holds only 10
// entries - filling it with per-pass cleanup notices would evict the useful one.
func (w *worker) announceCleanupPostponed(cr *api.ClickHouseInstallation) {
	fqdns := removedHostFQDNs(cr, func(host *api.Host) string {
		return w.c.namer.Name(interfaces.NameFQDN, host)
	})
	if len(fqdns) == 0 {
		return
	}
	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonCleanupPostponed).
		M(cr).F().
		Warning("Reconcile deferred - removed hosts are NOT cleaned up and remain on disk: %s",
			util.StringHead(strings.Join(fqdns, ","), 1024))
}
