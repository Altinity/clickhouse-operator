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

package chbackup

import (
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// WatchedNamespacePredicate passes events of objects in the namespaces the operator watches.
func WatchedNamespacePredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return chop.Config().IsNamespaceWatched(obj.GetNamespace())
	})
}

// RecoveryPredicate passes events of installations in watched namespaces whose spec names a backup to
// recover from and that have not been recovered yet. Of their updates it passes the ones that may
// start the recovery: a completed reconcile, which the recovery waits for and which follows every
// change of the spec, and a change of the annotation that records the recovery. The recovery
// restores are watched as owned objects: a change of their status records the recovery.
func RecoveryPredicate() predicate.Predicate {
	return predicate.And(WatchedNamespacePredicate(), predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return wantsRecovery(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return wantsRecovery(e.ObjectNew) &&
				(recoveryAnnotationsChanged(e.ObjectOld, e.ObjectNew) || reconcileCompleted(e.ObjectOld, e.ObjectNew))
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return wantsRecovery(e.Object)
		},
	})
}

// wantsRecovery reports whether the spec of an installation names a backup to recover from and the
// installation has not been recovered yet. It tells what may need a recovery: which clusters do the
// normalized installation tells. Whether its namespace is watched is checked before: by
// WatchedNamespacePredicate, and by skip in a reconcile.
func wantsRecovery(obj client.Object) bool {
	chi, ok := obj.(*api.ClickHouseInstallation)
	return ok && chi.RequestsBackupRecovery() && (chi.GetAnnotations()[api.AnnotationRecoveredFrom] == "")
}

// recoveryAnnotationsChanged reports whether an update changed an annotation that records recovery.
func recoveryAnnotationsChanged(old, cur client.Object) bool {
	for _, annotation := range api.BackupAnnotations {
		if old.GetAnnotations()[annotation] != cur.GetAnnotations()[annotation] {
			return true
		}
	}
	return false
}

// reconcileCompleted reports whether an update of an installation completed its reconcile.
func reconcileCompleted(old, cur client.Object) bool {
	oldCHI, ok := old.(*api.ClickHouseInstallation)
	if !ok {
		return false
	}
	curCHI, ok := cur.(*api.ClickHouseInstallation)
	if !ok {
		return false
	}
	return (oldCHI.Status.GetStatus() != api.StatusCompleted) && (curCHI.Status.GetStatus() == api.StatusCompleted)
}

// CHITopologyPredicate passes the installation events that may change the hosts a schedule
// targets or whether it runs: an installation appearing, disappearing, completing a reconcile,
// which follows every change of its spec, its recovery included, or changing the annotation that
// records its recovery.
func CHITopologyPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return reconcileCompleted(e.ObjectOld, e.ObjectNew) || recoveryAnnotationsChanged(e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

// TrimInstallation reduces an installation to what the backup controllers read from their cache:
// its metadata, for ownership, its reconcile status, for CHITopologyPredicate, and the recoveries
// its spec names, for RecoveryPredicate. They read the spec and the normalized state live, through
// the CR reader, so the cache does not keep a second full copy of every installation next to the
// CHI controller's own. kubectl's last-applied annotation, a copy of the whole spec, goes too.
//
// Used as a cache transform, it may be handed an already trimmed installation, or a tombstone,
// which it passes through as it is.
func TrimInstallation(obj any) (any, error) {
	chi, ok := obj.(*api.ClickHouseInstallation)
	if !ok {
		return obj, nil
	}
	trimmed := &api.ClickHouseInstallation{
		TypeMeta:   chi.TypeMeta,
		ObjectMeta: chi.ObjectMeta,
	}
	trimmed.ManagedFields = nil
	if _, applied := chi.GetAnnotations()[core.LastAppliedConfigAnnotation]; applied {
		trimmed.Annotations = make(map[string]string, len(chi.Annotations)-1)
		for key, value := range chi.Annotations {
			if key != core.LastAppliedConfigAnnotation {
				trimmed.Annotations[key] = value
			}
		}
	}
	trimmed.EnsureStatus().Status = chi.Status.GetStatus()
	trimRecoveries(chi, trimmed)
	return trimmed, nil
}

// trimRecoveries keeps of the spec of an installation the recoveries it names.
func trimRecoveries(chi, trimmed *api.ClickHouseInstallation) {
	if !chi.RequestsBackupRecovery() {
		return
	}
	if recovery := chi.GetSpecT().GetBackup().GetRecovery(); recovery != nil {
		trimmed.Spec.Backup = &api.ClusterBackup{Recovery: recovery.DeepCopy()}
	}
	var clusters []*api.Cluster
	for _, cluster := range chi.GetSpecT().GetConfiguration().GetClusters() {
		if recovery := cluster.GetBackup().GetRecovery(); recovery != nil {
			clusters = append(clusters, &api.Cluster{Name: cluster.GetName(), Backup: &api.ClusterBackup{Recovery: recovery.DeepCopy()}})
		}
	}
	if len(clusters) > 0 {
		trimmed.Spec.Configuration = &api.Configuration{Clusters: clusters}
	}
}
