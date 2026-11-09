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

package v1

import (
	core "k8s.io/api/core/v1"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

const (
	// AnnotationRecoveredFrom is stamped by the operator once every recovery restore spec.backup.recovery
	// asks for has completed, naming the backup they restored: the backup name when every recovered
	// cluster restored the same one, else "<cluster>:<backup>" per recovered cluster, comma-separated and
	// sorted by cluster. Set by hand, with any value, it records a recovery made by hand. An installation
	// carrying it is never recovered again.
	AnnotationRecoveredFrom = clickhouse_altinity_com.APIGroupName + "/recovered-from"
)

// BackupStorageHint tells how to settle the backup of a cluster that turns it on without a storage,
// which leaves it off: name the Secret of the sidecar configuration, or turn backup off there, which a cluster may
// inherit unasked.
const BackupStorageHint = "name the Secret of the clickhouse-backup configuration in backup.configSecretRef, " +
	"or name the pod template of the sidecar in backup.podTemplate, or set backup.enabled to \"no\""

// BackupAnnotations lists the annotations that record operator-managed recovery. They describe the
// installation, not its pods, so they must never be copied into pod templates: a change there would
// restart every host.
var BackupAnnotations = []string{
	AnnotationRecoveredFrom,
}

// ClusterBackup configures the operator-managed clickhouse-backup sidecar, either for the whole
// installation (spec.backup) or for one cluster (spec.configuration.clusters[].backup). A cluster
// inherits every field it leaves empty from the installation level.
type ClusterBackup struct {
	// Enabled turns operator-managed backup on for the cluster: the operator injects the
	// clickhouse-backup sidecar into its pods and backup/restore resources may target it. It takes
	// effect only with ConfigSecretRef, or with PodTemplate, see IsEnabled.
	Enabled *types.StringBool `json:"enabled,omitempty"                yaml:"enabled,omitempty"`
	// PodTemplate names the pod template whose clickhouse-backup container the sidecar is built on: what the
	// container sets is kept, and only what it lacks is filled in. Only that container is taken, not the other
	// containers, volumes or pod settings of the template. Empty, the operator builds the sidecar on its own.
	PodTemplate *types.String `json:"podTemplate,omitempty"            yaml:"podTemplate,omitempty"`
	// ConfigSecretRef names a Secret in the installation's namespace whose key config.yml holds the clickhouse-backup
	// configuration of the sidecar, a YAML mapping of its sections, e.g. general.remote_storage, s3.bucket,
	// s3.access_key. The operator generates the configuration file of the sidecar from it, which follows a change of
	// the Secret without a restart. A cluster that leaves it out takes the installation's, an installation a
	// template's; an explicit name "" on a cluster leaves the cluster without one. Without one, the storage is
	// configured by the clickhouse-backup container of the pod template PodTemplate names.
	ConfigSecretRef *core.LocalObjectReference `json:"configSecretRef,omitempty" yaml:"configSecretRef,omitempty"`
	// Recovery names the backup a new installation restores the cluster from once it has
	// completed its first reconcile. It configures the operator, not the sidecar: it never changes a
	// pod. It is never taken from a ClickHouseInstallationTemplate.
	Recovery *BackupRecovery `json:"recovery,omitempty"               yaml:"recovery,omitempty"`
}

// BackupRecovery names the backup a cluster is recovered from.
type BackupRecovery struct {
	// BackupName is the backup to restore. An explicit "" on a cluster leaves it out of a
	// recovery the installation level asks for: nothing is restored into it.
	BackupName *types.String `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// SourceInstallation and SourceNamespace name the installation, and its namespace, that made the backup, when
	// not this one: the recovery reads the backup from the storage paths the {installation} and {namespace} macros
	// give there. Empty, each is that of this installation.
	SourceInstallation *types.String `json:"sourceInstallation,omitempty" yaml:"sourceInstallation,omitempty"`
	SourceNamespace    *types.String `json:"sourceNamespace,omitempty"    yaml:"sourceNamespace,omitempty"`
}

// IsBackupEnabled reports whether operator-managed backup is on for any cluster of the installation. It applies
// the clusters' inheritance from spec.backup itself, so it answers alike before and after normalization.
func (cr *ClickHouseInstallation) IsBackupEnabled() bool {
	if cr == nil {
		return false
	}
	for _, cluster := range cr.GetSpecT().GetConfiguration().GetClusters() {
		if cluster.GetBackup().DeepCopy().MergeFrom(cr.GetSpecT().GetBackup(), MergeTypeFillEmptyValues).IsEnabled() {
			return true
		}
	}
	return false
}

// RequestsBackupRecovery reports whether the spec names a backup to recover a cluster from, at the
// installation level or on a cluster of its own. It reads the spec as it is, so it tells what may
// need a recovery rather than which clusters do: those are the backup-enabled clusters of the
// normalized installation that inherit a non-empty backup name.
func (cr *ClickHouseInstallation) RequestsBackupRecovery() bool {
	if cr == nil {
		return false
	}
	if cr.GetSpecT().GetBackup().GetRecovery().GetBackupName().Value() != "" {
		return true
	}
	for _, cluster := range cr.GetSpecT().GetConfiguration().GetClusters() {
		if cluster.GetBackup().GetRecovery().GetBackupName().Value() != "" {
			return true
		}
	}
	return false
}

// DropBackupRecovery removes every recovery from the spec, of the installation level and of every
// cluster. A recovery is what one installation restores, so a template that applies to many must
// never contribute one.
func (spec *ChiSpec) DropBackupRecovery() {
	if spec == nil {
		return
	}
	if spec.Backup != nil {
		spec.Backup.Recovery = nil
	}
	for _, cluster := range spec.GetConfiguration().GetClusters() {
		if cluster.GetBackup() != nil {
			cluster.Backup.Recovery = nil
		}
	}
}

// IsEnabled reports whether operator-managed backup is on, nil-safe: the spec turns it on and names where the storage
// settings of its sidecar come from. Turned on without that, backup stays off, see LacksStorage.
func (b *ClusterBackup) IsEnabled() bool {
	return b.IsRequested() && b.namesStorageSettings()
}

// IsRequested reports whether the spec turns operator-managed backup on, whether or not it is on, nil-safe.
func (b *ClusterBackup) IsRequested() bool {
	if b == nil {
		return false
	}
	return b.Enabled.IsTrue()
}

// LacksStorage reports whether the spec turns operator-managed backup on without naming where the storage
// settings of its sidecar come from, which leaves backup off, nil-safe.
func (b *ClusterBackup) LacksStorage() bool {
	return b.IsRequested() && !b.namesStorageSettings()
}

// namesStorageSettings reports whether the backup names a source of the storage settings of its sidecar: the
// Secret of its configuration, or the pod template whose container the sidecar is built on. Without either, the sidecar
// runs the bundled image, which carries no configuration file, on the operator's environment alone, which sets
// no storage: clickhouse-backup takes none then, and refuses every upload and download.
func (b *ClusterBackup) namesStorageSettings() bool {
	return (b.GetConfigSecretName() != "") || (b.GetPodTemplate().Value() != "")
}

// GetPodTemplate returns the name of the pod template the sidecar is built on, nil-safe.
func (b *ClusterBackup) GetPodTemplate() *types.String {
	if b == nil {
		return nil
	}
	return b.PodTemplate
}

// GetConfigSecretName returns the name of the Secret of the sidecar configuration, "" when there is none, nil-safe.
func (b *ClusterBackup) GetConfigSecretName() string {
	if (b == nil) || (b.ConfigSecretRef == nil) {
		return ""
	}
	return b.ConfigSecretRef.Name
}

// GetRecovery returns the recovery of the cluster, nil-safe.
func (b *ClusterBackup) GetRecovery() *BackupRecovery {
	if b == nil {
		return nil
	}
	return b.Recovery
}

// GetBackupName returns the backup to recover from, nil-safe.
func (r *BackupRecovery) GetBackupName() *types.String {
	if r == nil {
		return nil
	}
	return r.BackupName
}

// GetSourceInstallation returns the installation that made the backup to recover from, nil-safe.
func (r *BackupRecovery) GetSourceInstallation() *types.String {
	if r == nil {
		return nil
	}
	return r.SourceInstallation
}

// GetSourceNamespace returns the namespace of the installation that made the backup to recover from, nil-safe.
func (r *BackupRecovery) GetSourceNamespace() *types.String {
	if r == nil {
		return nil
	}
	return r.SourceNamespace
}

// MergeFrom merges from the specified recovery, filling empty fields or overriding with non-empty
// ones according to _type. An explicit "" is a value: it is kept, and it overrides.
func (r *BackupRecovery) MergeFrom(from *BackupRecovery, _type MergeType) *BackupRecovery {
	if from == nil {
		return r
	}
	if r == nil {
		r = &BackupRecovery{}
	}
	r.BackupName = MergeScalar(r.BackupName, from.BackupName, _type)
	r.SourceInstallation = MergeScalar(r.SourceInstallation, from.SourceInstallation, _type)
	r.SourceNamespace = MergeScalar(r.SourceNamespace, from.SourceNamespace, _type)
	return r
}

// MergeFrom merges from the specified backup block, filling empty fields or overriding
// with non-empty ones according to _type.
func (b *ClusterBackup) MergeFrom(from *ClusterBackup, _type MergeType) *ClusterBackup {
	if from == nil {
		return b
	}
	if b == nil {
		b = &ClusterBackup{}
	}

	b.Enabled = MergeScalar(b.Enabled, from.Enabled, _type)
	b.PodTemplate = MergeScalar(b.PodTemplate, from.PodTemplate, _type)
	// The reference is copied: it may be that of a cached template or of the installation level, which other
	// clusters read
	b.ConfigSecretRef = MergePointer(b.ConfigSecretRef, from.ConfigSecretRef.DeepCopy(), _type)
	b.Recovery = b.Recovery.MergeFrom(from.Recovery, _type)

	return b
}
