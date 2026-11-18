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
	"fmt"
	"slices"
	"strconv"
	"strings"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// nameHashLength is the length of the hash that keeps shortened names unique.
	nameHashLength = 8

	// scheduleTimestampLayout formats the time a scheduled run was due into the "-YYYYMMDDHHMMSS"
	// suffix of its backup name, api.BackupNameScheduleSuffix.
	scheduleTimestampLayout = "20060102150405"
	// runNowTimestampLayout formats the time a backup that runs a schedule once is due into the "-YYYYMMDDTHHMMSS"
	// suffix of its backup name: of the length of a scheduled run's, so the name fits as one does, and never one of
	// those the schedule's retention prunes.
	runNowTimestampLayout = "20060102T150405"

	// recoveryRestoreNameSuffix ends the name of the restore that recovers a cluster of an
	// installation from the backup its spec.backup.recovery names.
	recoveryRestoreNameSuffix = "recovery"

	// recoveredFromSeparator separates the clusters in the value of api.AnnotationRecoveredFrom, and
	// recoveredFromClusterSeparator a cluster from the backup it was recovered from: neither occurs in
	// a cluster or a backup name.
	recoveredFromSeparator        = ","
	recoveredFromClusterSeparator = ":"
)

// boundedName joins base and suffix with a dash, shortening base and inserting a hash of it when
// the result would exceed maxLength. Equal inputs always yield equal names.
func boundedName(base, suffix string, maxLength int) string {
	name := joinName(base, suffix)
	if len(name) <= maxLength {
		return name
	}
	hash := util.HashIntoString([]byte(base))[:nameHashLength]
	return joinName(boundedHead(base, suffix, maxLength), hash, suffix)
}

// boundedHead returns what a name boundedName shortens keeps of base, ahead of the hash.
func boundedHead(base, suffix string, maxLength int) string {
	headLength := maxLength - len(joinName("", strings.Repeat("0", nameHashLength), suffix)) - 1
	return strings.TrimRight(util.StringHead(base, headLength), "-.")
}

func joinName(parts ...string) string {
	var nonEmpty []string
	for _, part := range parts {
		if part != "" {
			nonEmpty = append(nonEmpty, part)
		}
	}
	return strings.Join(nonEmpty, "-")
}

// ResolveBackupName returns the backup name of a one-off backup. A generated name is derived from the
// time the backup is due, not the time of the reconcile, so every reconcile resolves the same name. A
// backup that runs a schedule, which takes no explicit name, is named with the backup name prefix it
// recorded of the schedule: the prefix lets the recovery prefix guard tell where it came from, and the
// form of the time keeps it out of the series the schedule's retention prunes. Any other backup takes
// its explicit name, or one derived from the CR name.
func ResolveBackupName(chb *api.ClickHouseBackup) string {
	due := chb.DueTime().UTC()
	if prefix := chb.Status.Schedule.GetBackupNamePrefix(); (chb.Spec.ScheduleRef != nil) && (prefix != "") {
		return joinName(prefix, due.Format(runNowTimestampLayout))
	}
	if chb.Spec.BackupName != "" {
		return chb.Spec.BackupName
	}
	return boundedName(chb.GetName(), strconv.FormatInt(due.Unix(), 10), api.BackupNameMaxLength)
}

// BackupNaming tells how the backups of a cluster are named: by its naming template, backup.naming, with the values the
// template takes on the cluster. A zero BackupNaming is the default naming.
type BackupNaming struct {
	Template     string
	Cluster      string
	Installation string
	Namespace    string
	// FirstShard is the name of the first shard of the cluster, the value of its {shard} macro
	FirstShard string
}

// BackupNaming returns how the backups of the cluster of the topology are named.
func (t *Topology) BackupNaming() BackupNaming {
	if t.Naming() == "" {
		return BackupNaming{}
	}
	naming := BackupNaming{Template: t.Naming(), Cluster: t.cluster(), Installation: t.installation, Namespace: t.namespace}
	if (len(t.clusters) > 0) && (len(t.clusters[0].shards) > 0) {
		naming.FirstShard = t.clusters[0].shards[0].name
	}
	return naming
}

// Name returns the name a naming template gives the backup of the first shard of the cluster, named after tag, which
// tells the backup of every shard: the operator names those of the others alike.
func (n BackupNaming) Name(prefix, tag string) (string, error) {
	return api.RenderBackupName(n.Template, api.BackupNameValues{
		Shard: n.FirstShard, ShardNum: 1, Cluster: n.Cluster, Chi: n.Installation, Namespace: n.Namespace, Prefix: prefix, Tag: tag,
	})
}

// ResolveTemplateBackupName returns the backup name of a one-off backup of a cluster with a naming template, as
// ResolveBackupName does without one: the name the template gives the backup of the first shard, which tells the
// backup of every shard. A backup is named after the time it is due, in UTC, with a T before the hour, as it is made at
// once, or after the time its spec.backupName gives, YYYYMMDDHHMMSS or YYYYMMDDTHHMMSS, which is all a backup name of
// such a cluster may be; its {prefix} is the backup name prefix of the schedule it runs, or its own name. It fails for
// any other backupName, and a name the template cannot make, as of a prefix too long.
func ResolveTemplateBackupName(chb *api.ClickHouseBackup, naming BackupNaming) (string, error) {
	tag := api.BackupTag(chb.DueTime().UTC(), true)
	if chb.Spec.BackupName != "" {
		if !api.BackupTagRegexp.MatchString(chb.Spec.BackupName) {
			return "", fmt.Errorf("backupName %q is no time a backup is named after, YYYYMMDDHHMMSS or YYYYMMDDTHHMMSS, as "+
				"the naming template %q of cluster %s names the backup of every shard by it", chb.Spec.BackupName, naming.Template, naming.Cluster)
		}
		tag = chb.Spec.BackupName
	}
	prefix := boundedName(chb.GetName(), "", api.BackupNamePrefixMaxLength)
	if record := chb.Status.Schedule; (chb.Spec.ScheduleRef != nil) && (record != nil) && (record.BackupNamePrefix != "") {
		prefix = record.BackupNamePrefix
	}
	name, err := naming.Name(prefix, tag)
	if err != nil {
		return "", fmt.Errorf("the naming template %q of cluster %s names no backup of backupName %q: %w", naming.Template,
			naming.Cluster, chb.Spec.BackupName, err)
	}
	return name, nil
}

// BackupNamePrefix returns the prefix of the backups a schedule makes, which also scopes its
// retention. It defaults to the namespace, installation and schedule names: two installations may
// share a storage path, a restored one with the one its backup came from above all, which
// is often of the same name in another namespace, and a schedule of the same name on each must not
// prune the backups of the other.
func BackupNamePrefix(chbs *api.ClickHouseBackupSchedule) string {
	if chbs.Spec.BackupNamePrefix != "" {
		return chbs.Spec.BackupNamePrefix
	}
	return boundedName(joinName(chbs.GetNamespace(), chbs.Spec.ClickHouseInstallation, chbs.GetName()), "", api.BackupNamePrefixMaxLength)
}

// HasDefaultBackupNamePrefix reports whether a backup name carries the backup name prefix a schedule of
// the installation defaults to, whatever the schedule: it starts with the namespace and the installation
// name as BackupNamePrefix joins them, or, for a prefix BackupNamePrefix shortens within them, with as much
// of them as the shortened prefix keeps.
func HasDefaultBackupNamePrefix(namespace, chiName, backupName string) bool {
	installation := joinName(namespace, chiName)
	for _, root := range []string{installation, boundedHead(installation, "", api.BackupNamePrefixMaxLength)} {
		if strings.HasPrefix(backupName, root+"-") {
			return true
		}
	}
	return false
}

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

// ScheduledBackupName returns the name of the backup a run of a schedule of the prefix makes at the slot, in the time
// zone of the schedule, with a naming template the name of the backup of the first shard.
func ScheduledBackupName(prefix string, naming Naming, slot string) (string, error) {
	if naming.IsTemplate() {
		return naming.FirstName(slot, prefix)
	}
	return joinName(prefix, slot), nil
}
