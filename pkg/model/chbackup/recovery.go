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
	"slices"
	"strings"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

const (
	// recoveryRestoreNameSuffix ends the name of the restore that recovers a cluster of an
	// installation from the backup its spec.backup.recovery names.
	recoveryRestoreNameSuffix = "recovery"

	// recoveredFromSeparator separates the clusters in the value of api.AnnotationRecoveredFrom, and
	// recoveredFromClusterSeparator a cluster from the backup it was recovered from: neither occurs in
	// a cluster or a backup name.
	recoveredFromSeparator        = ","
	recoveredFromClusterSeparator = ":"
)

// RestoresRBAC reports whether a restore restores the access entities the backup holds. They belong
// to the whole installation, so a restore restores them only when it asks to and targets every
// cluster, or when it recovers a cluster of a new installation, which has none yet.
func RestoresRBAC(chr *api.ClickHouseRestore) bool {
	return chr.Spec.RBAC.IsTrue() && ((chr.TargetCluster() == "") || IsRecoveryRestore(chr))
}

// IsRecoveryRestore reports whether a restore is one the operator created to recover a cluster of its
// installation: the installation controls it.
func IsRecoveryRestore(chr *api.ClickHouseRestore) bool {
	owner := meta.GetControllerOf(chr)
	return (owner != nil) && (owner.Kind == api.ClickHouseInstallationCRDResourceKind) &&
		(owner.Name == chr.Spec.ClickHouseInstallation) && strings.HasPrefix(owner.APIVersion, clickhouse_altinity_com.APIGroupName+"/")
}

// ClusterRecovery is the recovery of one cluster of an installation.
type ClusterRecovery struct {
	// Cluster is the cluster to recover.
	Cluster string
	// BackupName is the backup to recover it from.
	BackupName string
	// SourceInstallation and SourceNamespace name the installation, and its namespace, that made the backup, ""
	// for the recovered installation's own.
	SourceInstallation string
	SourceNamespace    string
}

// Recoveries returns the recoveries a normalized installation asks for: one for every backup-enabled
// cluster that inherits a non-empty backup name from spec.backup.recovery, in spec order. A cluster
// without backup has no sidecar to restore through, and is left out.
func Recoveries(normalized *api.ClickHouseInstallation) []ClusterRecovery {
	return recoveriesOf(normalized, (*api.ClusterBackup).IsEnabled)
}

// RecoveriesWithoutStorage returns the recoveries a normalized installation asks for of the clusters that
// turn backup on without a storage, in spec order. Their backup is off, so nothing can restore them until the
// spec names a storage.
func RecoveriesWithoutStorage(normalized *api.ClickHouseInstallation) []ClusterRecovery {
	return recoveriesOf(normalized, (*api.ClusterBackup).LacksStorage)
}

// RecoveryClusters returns the clusters the recoveries recover, in their order.
func RecoveryClusters(recoveries []ClusterRecovery) []string {
	var clusters []string
	for i := range recoveries {
		clusters = append(clusters, recoveries[i].Cluster)
	}
	return clusters
}

// recoveriesOf returns the recoveries a normalized installation asks for of the clusters whose backup the filter
// takes, in spec order.
func recoveriesOf(normalized *api.ClickHouseInstallation, filter func(*api.ClusterBackup) bool) []ClusterRecovery {
	if normalized == nil {
		return nil
	}
	var recoveries []ClusterRecovery
	for _, cluster := range normalized.GetSpecT().GetConfiguration().GetClusters() {
		recovery := cluster.GetBackup().GetRecovery()
		backupName := recovery.GetBackupName().Value()
		if filter(cluster.GetBackup()) && (backupName != "") {
			recoveries = append(recoveries, ClusterRecovery{
				Cluster:            cluster.GetName(),
				BackupName:         backupName,
				SourceInstallation: recovery.GetSourceInstallation().Value(),
				SourceNamespace:    recovery.GetSourceNamespace().Value(),
			})
		}
	}
	return recoveries
}

// RecoveredFrom returns the value of api.AnnotationRecoveredFrom that records the recoveries: the
// backup name when every cluster was recovered from the same backup, else "<cluster>:<backup>" per
// cluster, sorted by cluster.
func RecoveredFrom(recoveries []ClusterRecovery) string {
	var names []string
	for _, recovery := range recoveries {
		names = append(names, recovery.BackupName)
	}
	if distinct := slices.Compact(slices.Sorted(slices.Values(names))); len(distinct) == 1 {
		return distinct[0]
	}
	var entries []string
	for _, recovery := range recoveries {
		entries = append(entries, recovery.Cluster+recoveredFromClusterSeparator+recovery.BackupName)
	}
	slices.Sort(entries)
	return strings.Join(entries, recoveredFromSeparator)
}

// RecoveredBackupNames returns the backups the value of api.AnnotationRecoveredFrom names, as
// RecoveredFrom writes it; a value set by hand is read the same way.
func RecoveredBackupNames(recoveredFrom string) []string {
	var names []string
	for _, entry := range strings.Split(recoveredFrom, recoveredFromSeparator) {
		_, name, found := strings.Cut(entry, recoveredFromClusterSeparator)
		if !found {
			name = entry
		}
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// RecoveryRestoreName returns the name of the ClickHouseRestore that recovers a cluster of an
// installation from the backup its spec.backup.recovery names.
func RecoveryRestoreName(chi *api.ClickHouseInstallation, cluster string) string {
	return boundedName(joinName(chi.GetName(), cluster), recoveryRestoreNameSuffix, validation.DNS1123SubdomainMaxLength)
}

// BuildRecoveryRestore builds the ClickHouseRestore that recovers a cluster of an installation. A
// recovery restores a new installation, users, roles and grants included.
func BuildRecoveryRestore(chi *api.ClickHouseInstallation, recovery ClusterRecovery) *api.ClickHouseRestore {
	restore := &api.ClickHouseRestore{
		ObjectMeta: meta.ObjectMeta{
			Name:      RecoveryRestoreName(chi, recovery.Cluster),
			Namespace: chi.GetNamespace(),
		},
		Spec: api.ClickHouseRestoreSpec{
			ClickHouseInstallation: chi.GetName(),
			Cluster:                recovery.Cluster,
			BackupName:             recovery.BackupName,
			SourceInstallation:     recovery.SourceInstallation,
			SourceNamespace:        recovery.SourceNamespace,
			RBAC:                   types.NewStringBool(true),
		},
	}
	return restore
}
