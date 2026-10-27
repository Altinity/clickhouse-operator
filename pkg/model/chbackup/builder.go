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
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Labels on every Job and CronJob built here. They are deliberately distinct from the labels on
// installation-generated objects: those keys drive the CHI controller's own discovery and cache
// filters, which must never pick up a backup Job.
const (
	// LabelBackupCRKind names the kind of the backup custom resource that owns the object.
	LabelBackupCRKind = clickhouse_altinity_com.APIGroupName + "/backup-cr-kind"
	// LabelBackupCRName names the backup custom resource that owns the object.
	LabelBackupCRName = clickhouse_altinity_com.APIGroupName + "/backup-cr-name"
	// LabelBackupCHI names the installation the object operates on.
	LabelBackupCHI = clickhouse_altinity_com.APIGroupName + "/backup-chi"
	// LabelBackupCluster names the cluster of the installation the object operates on.
	LabelBackupCluster = clickhouse_altinity_com.APIGroupName + "/backup-cluster"
	// LabelBackupStorage names the backup storage of the cluster the object keeps backups in, or reads them from.
	LabelBackupStorage = clickhouse_altinity_com.APIGroupName + "/backup-storage"
)

// Annotations the backup controllers keep on the objects built here.
const (
	// AnnotationBackupObjectVersion carries the fingerprint of the CronJob spec the operator built
	// last, so a reconcile updates the CronJob only when that spec changes. Comparing specs directly
	// would never match, as the API server fills in defaults the builder leaves out.
	AnnotationBackupObjectVersion = clickhouse_altinity_com.APIGroupName + "/backup-object-version"
	// AnnotationScheduledJobObserved records the last phase of a scheduled backup Job that was
	// reported, so every scheduled run is counted and announced once.
	AnnotationScheduledJobObserved = clickhouse_altinity_com.APIGroupName + "/backup-job-observed"
	// AnnotationScheduledJobWithoutData marks a scheduled backup Job reported completed whose backup
	// holds no data, so that the run is recorded again as reported without asking its pod.
	AnnotationScheduledJobWithoutData = clickhouse_altinity_com.APIGroupName + "/backup-job-without-data"
	// AnnotationJobNotStarted marks a Job the operator stopped because no pod of it ever started, so
	// its failure is reported as such rather than as the deadline the stop works through.
	AnnotationJobNotStarted = clickhouse_altinity_com.APIGroupName + "/backup-job-not-started"
	// AnnotationJobStarted records when the operator first saw a pod of the Job started. The Job's
	// own pod counts only tell the present: a running pod that stops being ready would otherwise make
	// the Job look as if it never started.
	AnnotationJobStarted = clickhouse_altinity_com.APIGroupName + "/backup-job-started"
	// AnnotationCronJobSuspendedAt records when the operator suspended a CronJob. The CronJob controller
	// acts on its own cache, which may still show the CronJob active for a moment, so a restore starts
	// only once the suspension is old enough for that cache to have caught up.
	AnnotationCronJobSuspendedAt = clickhouse_altinity_com.APIGroupName + "/backup-suspended-at"
)

const (
	// jobContainerName is the name of the container of every backup, verify and restore Job.
	jobContainerName = "clickhouse-backup-job"

	// jobNameMaxLength keeps Job names usable as the job-name label value the Job controller
	// stamps on its pods.
	jobNameMaxLength = validation.LabelValueMaxLength
	// LegacyJobNameLabel names the Job on its pods on every Kubernetes version the operator supports:
	// batch.JobNameLabel does so only from 1.27 on. The Job script takes the name of a scheduled run
	// from it, so a slot names its backup alike on every version.
	LegacyJobNameLabel = "job-name"
	// cronJobNameMaxLength leaves room for the 11-character suffix the CronJob controller appends
	// to the names of the Jobs it creates.
	cronJobNameMaxLength = 52
	// nameHashLength is the length of the hash that keeps shortened names unique.
	nameHashLength = 8

	jobNameSuffixBackup  = "backup"
	jobNameSuffixVerify  = "verify"
	jobNameSuffixRestore = "restore"

	// hostListSeparator joins host lists in the Job environment.
	hostListSeparator = " "
	// partitionListSeparator joins partition ids in the Job environment.
	partitionListSeparator = ","

	// pollIntervalSeconds is how often the Job script polls a sidecar for the state of an action.
	pollIntervalSeconds = 5

	// scheduleTimestampLayout formats the time a scheduled run was due into the "-YYYYMMDD-HHMMSS"
	// suffix of its backup name, as the Job script does.
	scheduleTimestampLayout = "20060102-150405"
	// runNowTimestampLayout formats the time a backup that runs a schedule once is due into the "-YYYYMMDDTHHMMSS"
	// suffix of its backup name: of the length of a scheduled run's, so the name fits as one does, and never one of
	// those the schedule's retention prunes.
	runNowTimestampLayout = "20060102T150405"
	// scheduleSlotSeconds is the unit of the number the CronJob controller ends the names of its
	// Jobs with: the time the run was due, in minutes since the epoch.
	scheduleSlotSeconds = int64(time.Minute / time.Second)
	// scheduleSlotMaxDigits bounds that number, as the Job script does, so that it cannot overflow.
	scheduleSlotMaxDigits = 10

	// capabilityAll stands for every Linux capability.
	capabilityAll core.Capability = "ALL"

	// recoveryRestoreNameSuffix ends the name of the restore that recovers a cluster of an
	// installation from the backup its spec.backup.recovery names.
	recoveryRestoreNameSuffix = "recovery"

	// recoveredFromSeparator separates the clusters in the value of api.AnnotationRecoveredFrom, and
	// recoveredFromClusterSeparator a cluster from the backup it was recovered from: neither occurs in
	// a cluster or a backup name.
	recoveredFromSeparator        = ","
	recoveredFromClusterSeparator = ":"
)

// jobCommand runs the Job script. The script text is static: every value it acts on reaches it
// through the environment, so no user input is ever parsed as shell code.
var jobCommand = []string{"bash", "-c"}

// Resources of the Job container. The script holds whole sidecar responses in bash variables, and
// the restore preflight reads the table list of every target host, system tables included, which
// takes several hundred kilobytes per host and a few copies of it. Both limits are set, at most ten
// times their requests, so that a namespace whose ResourceQuota requires limits, or whose
// LimitRange caps the limit to request ratio, admits the Job without defaults of its own.
const (
	jobCPURequest    = "50m"
	jobCPULimit      = "500m"
	jobMemoryRequest = "64Mi"
	jobMemoryLimit   = "512Mi"
)

var jobResources = core.ResourceRequirements{
	Requests: core.ResourceList{
		core.ResourceCPU:    resource.MustParse(jobCPURequest),
		core.ResourceMemory: resource.MustParse(jobMemoryRequest),
	},
	Limits: core.ResourceList{
		core.ResourceCPU:    resource.MustParse(jobCPULimit),
		core.ResourceMemory: resource.MustParse(jobMemoryLimit),
	},
}

// Labels returns the labels of the Jobs and CronJobs built for a backup custom resource on a
// cluster of an installation, which keeps its backups in the named storage, or reads them from it.
func Labels(kind, crName, chiName, cluster, storage string) map[string]string {
	return map[string]string{
		LabelBackupCRKind:  kind,
		LabelBackupCRName:  LabelValue(crName),
		LabelBackupCHI:     LabelValue(chiName),
		LabelBackupCluster: LabelValue(cluster),
		LabelBackupStorage: storage,
	}
}

// LabelValue returns name as a valid label value. Object names may be longer than a label value
// allows, so long names are shortened with a hash that keeps them distinct.
func LabelValue(name string) string {
	return boundedName(name, "", validation.LabelValueMaxLength)
}

// JobSelector selects every Job and CronJob built here.
func JobSelector() k8sLabels.Selector {
	requirement, _ := k8sLabels.NewRequirement(LabelBackupCRKind, selection.Exists, nil)
	return k8sLabels.NewSelector().Add(*requirement)
}

// CHIJobsSelector selects the Jobs that operate on the named installation, optionally only those
// owned by a backup custom resource of the given kind.
func CHIJobsSelector(chiName, kind string) k8sLabels.Selector {
	set := k8sLabels.Set{LabelBackupCHI: LabelValue(chiName)}
	if kind != "" {
		set[LabelBackupCRKind] = kind
	}
	return k8sLabels.SelectorFromSet(set)
}

// ClusterJobsSelector selects the Jobs that operate on a cluster of the named installation,
// optionally only those owned by a backup custom resource of the given kind.
func ClusterJobsSelector(chiName, cluster, kind string) k8sLabels.Selector {
	set := k8sLabels.Set{LabelBackupCHI: LabelValue(chiName), LabelBackupCluster: LabelValue(cluster)}
	if kind != "" {
		set[LabelBackupCRKind] = kind
	}
	return k8sLabels.SelectorFromSet(set)
}

// ObjectCluster returns the cluster a Job or CronJob built here operates on, "" for any other object.
// Cluster names are shorter than a label value may be, so the label holds them as they are.
func ObjectCluster(obj meta.Object) string {
	return obj.GetLabels()[LabelBackupCluster]
}

// ObjectStorage returns the backup storage a Job or CronJob built here keeps backups in, or reads them from, ""
// for none or any other object. Storage names are label values, so the label holds them as they are.
func ObjectStorage(obj meta.Object) string {
	return obj.GetLabels()[LabelBackupStorage]
}

// OwnedJobsSelector selects the Jobs built for the named backup custom resource.
func OwnedJobsSelector(kind, crName string) k8sLabels.Selector {
	return k8sLabels.SelectorFromSet(k8sLabels.Set{
		LabelBackupCRKind: kind,
		LabelBackupCRName: LabelValue(crName),
	})
}

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

// BackupJobName returns the name of the Job that runs a one-off backup on a cluster.
func BackupJobName(chb *api.ClickHouseBackup, cluster string) string {
	return boundedName(joinName(chb.GetName(), cluster), jobNameSuffixBackup, jobNameMaxLength)
}

// VerifyJobName returns the name of the Job that verifies a one-off backup on a cluster.
func VerifyJobName(chb *api.ClickHouseBackup, cluster string) string {
	return boundedName(joinName(chb.GetName(), cluster), jobNameSuffixVerify, jobNameMaxLength)
}

// RestoreJobName returns the name of the Job that runs a restore on a cluster.
func RestoreJobName(chr *api.ClickHouseRestore, cluster string) string {
	return boundedName(joinName(chr.GetName(), cluster), jobNameSuffixRestore, jobNameMaxLength)
}

// CronJobName returns the name of the CronJob that runs a backup schedule on a cluster.
func CronJobName(chbs *api.ClickHouseBackupSchedule, cluster string) string {
	return boundedName(joinName(chbs.GetName(), cluster), jobNameSuffixBackup, cronJobNameMaxLength)
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

// JobBackupName returns the backup a Job works on, as the Job was built: a later edit of
// its custom resource does not change what the Job does. A scheduled run names its backup after
// the time it was due, see ScheduledBackupName.
func JobBackupName(job *batch.Job) string {
	if name := jobEnv(job, envBackupName); name != "" {
		return name
	}
	return ScheduledBackupName(job)
}

// ScheduledBackupName returns the name of the backup a scheduled run makes, as its Job script
// derives it: the CronJob controller names each Job after the minute its run was due, which the
// script reads from the Job name label of its pod, so every run of one slot makes the same
// backup. It returns "" for a run named otherwise, such as a Job created by hand from the
// CronJob, and for a Job the API server did not label with its name: the script then names the
// backup after the time it runs.
func ScheduledBackupName(job *batch.Job) string {
	prefix, cronJobName := jobEnv(job, envBackupNamePrefix), jobEnv(job, envCronJobName)
	if (prefix == "") || (cronJobName == "") || (podJobName(job.Spec.Template.GetLabels()) != job.GetName()) {
		return ""
	}
	slot, found := strings.CutPrefix(job.GetName(), cronJobName+"-")
	if !found || !scheduleSlotRegexp.MatchString(slot) {
		return ""
	}
	minutes, err := strconv.ParseInt(slot, 10, 64)
	if err != nil {
		return ""
	}
	timestamp := time.Unix(minutes*scheduleSlotSeconds, 0).UTC().Format(scheduleTimestampLayout)
	// A year past 9999 would not fit the layout, and the script falls back to the time it runs
	if len(timestamp) != len(scheduleTimestampLayout) {
		return ""
	}
	return joinName(prefix, timestamp)
}

// podJobName returns the name of the Job the pod labels tell, as the Job script reads it: from
// LegacyJobNameLabel alone, which the API server gives the pods of a Job on every version. A pod without it
// names its backup after the time it runs, whatever other label names its Job.
func podJobName(labels map[string]string) string {
	return labels[LegacyJobNameLabel]
}

// scheduleSlotRegexp matches the number a scheduled run's Job name ends with, as the Job script does.
var scheduleSlotRegexp = regexp.MustCompile(fmt.Sprintf(`^[0-9]{1,%d}$`, scheduleSlotMaxDigits))

// JobTables returns the table pattern a backup Job backs up, as the Job was built; "" stands for
// every table.
func JobTables(job *batch.Job) string {
	return jobEnv(job, envTables)
}

// JobPartitions returns the comma-separated partitions a backup Job backs up, as the Job was built;
// "" stands for every partition.
func JobPartitions(job *batch.Job) string {
	return jobEnv(job, envPartitions)
}

// JobKeepLast returns how many backups with data the retention of a scheduled backup Job keeps,
// as the Job was built; zero when it keeps all.
func JobKeepLast(job *batch.Job) int32 {
	keep, err := strconv.ParseInt(jobEnv(job, envKeepLast), 10, 32)
	if (err != nil) || (keep < 0) {
		return 0
	}
	return int32(keep)
}

// JobRetentionHosts returns the hosts a scheduled backup Job prunes on, as the Job was built.
func JobRetentionHosts(job *batch.Job) []string {
	return splitHosts(jobEnv(job, envRetentionHosts))
}

// JobHosts returns the hosts a Job drives, as the Job was built: those it backs up, verifies the
// backup on, or restores the schema on, which is every replica a restore touches.
func JobHosts(job *batch.Job) []string {
	for _, name := range []string{envBackupHosts, envVerifyHosts, envSchemaHosts} {
		if hosts := jobEnv(job, name); hosts != "" {
			return splitHosts(hosts)
		}
	}
	return nil
}

// CronJobRetentionHosts returns the hosts the runs of a CronJob prune on, as the CronJob was last
// built; none for no CronJob.
func CronJobRetentionHosts(cronJob *batch.CronJob) []string {
	if cronJob == nil {
		return nil
	}
	return splitHosts(podEnv(&cronJob.Spec.JobTemplate.Spec.Template.Spec, envRetentionHosts))
}

// jobEnv returns the plain value of a variable of the Job container, "" when it has none.
func jobEnv(job *batch.Job, name string) string {
	return podEnv(&job.Spec.Template.Spec, name)
}

// podEnv returns the plain value of a variable of the Job container of a pod, "" when it has none.
func podEnv(pod *core.PodSpec, name string) string {
	container, ok := k8s.PodSpecContainerGet(pod, jobContainerName)
	if !ok {
		return ""
	}
	for _, env := range container.Env {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

// BuildBackupJob builds the Job that runs a one-off backup on the cluster the topology targets.
// Every cluster makes the backup under the one name the backup resolved, which its status records,
// whatever the spec names by now.
func BuildBackupJob(chb *api.ClickHouseBackup, chi api.ICustomResource, topology *Topology) *batch.Job {
	cluster := topology.cluster()
	env := append(commonEnv(operationBackup, chi, cluster),
		envVar(envBackupName, cmp.Or(chb.Status.BackupName, ResolveBackupName(chb))),
		envVar(envDiffFromRemote, chb.Spec.DiffFromRemote),
	)
	options := chb.GetBackupOptions()
	env = append(env, backupOptionsEnv(options, topology)...)
	labels := Labels(api.ClickHouseBackupCRDResourceKind, chb.GetName(), chb.TargetInstallation(), cluster, topology.storage())
	return newJob(BackupJobName(chb, cluster), chb.GetNamespace(), labels, options.GetActiveDeadlineSeconds(), topology, env)
}

// BuildVerifyJob builds the Job that verifies a one-off backup is restorable on the cluster the
// topology targets: it downloads the backup and checks it without touching the installation's
// data. It verifies the backup that was made, which the status records, whatever the spec names by now.
func BuildVerifyJob(chb *api.ClickHouseBackup, chi api.ICustomResource, topology *Topology) *batch.Job {
	cluster := topology.cluster()
	backupName := cmp.Or(chb.Status.BackupName, ResolveBackupName(chb))
	env := append(commonEnv(operationVerify, chi, cluster),
		envVar(envBackupName, backupName),
		envVar(envVerifyHosts, joinHosts(topology.firstPerShard())),
	)
	labels := Labels(api.ClickHouseBackupCRDResourceKind, chb.GetName(), chb.TargetInstallation(), cluster, topology.storage())
	return newJob(VerifyJobName(chb, cluster), chb.GetNamespace(), labels, chb.GetBackupOptions().GetActiveDeadlineSeconds(), topology, env)
}

// BuildRestoreJob builds the Job that runs a restore on the cluster the topology targets. Every cluster
// restores the backup the restore resolved, which its status records, whatever the spec names by now.
// The schema is restored on every replica, each creating its tables from the backup's own definitions,
// which leaves Distributed tables pointing where they pointed; the data on the first replica of each
// shard, from which replication fills the others.
func BuildRestoreJob(chr *api.ClickHouseRestore, chi api.ICustomResource, topology *Topology) *batch.Job {
	cluster := topology.cluster()
	allReplicas := joinHosts(topology.allReplicas())
	env := append(commonEnv(operationRestore, chi, cluster),
		envVar(envBackupName, cmp.Or(chr.Status.BackupName, chr.Spec.BackupName)),
		envVar(envSchemaHosts, allReplicas),
		envVar(envDataHosts, joinHosts(topology.firstPerShard())),
		envVar(envGuardHosts, allReplicas),
		envVar(envSchemaOnly, strconv.FormatBool(chr.Spec.SchemaOnly.IsTrue())),
		envVar(envTables, chr.Spec.Tables),
		envVar(envOverwrite, strconv.FormatBool(chr.Spec.Overwrite.IsTrue())),
		envVar(envValidateTopology, strconv.FormatBool(chr.Spec.IsTopologyValidationEnabled())),
		envVar(envRestoreRBAC, strconv.FormatBool(RestoresRBAC(chr))),
		envVar(envRBACNotRestoredMessage, RBACNotRestoredMessage),
		envVar(envReplicated, strconv.FormatBool(topology.replicated())),
		envVar(envMaxTableSizeToDrop, strconv.FormatUint(topology.maxTableSizeToDrop(), 10)),
	)
	// A restore of a backup another installation made reads it from the paths of that installation
	if chr.Spec.SourceInstallation != "" {
		env = append(env, envVar(envSourceInstallation, chr.Spec.SourceInstallation))
	}
	if chr.Spec.SourceNamespace != "" {
		env = append(env, envVar(envSourceNamespace, chr.Spec.SourceNamespace))
	}
	// A restore checks every replica holds the backup of its shard under its own path, whatever the
	// replicas the backup was made on
	env = append(env, pathCheckEnv(topology, "")...)
	labels := Labels(api.ClickHouseRestoreCRDResourceKind, chr.GetName(), chr.Spec.ClickHouseInstallation, cluster, topology.storage())
	return newJob(RestoreJobName(chr, cluster), chr.GetNamespace(), labels, chr.Spec.GetActiveDeadlineSeconds(), topology, env)
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
	return &api.ClickHouseRestore{
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
}

// RetentionHosts returns the hosts the runs of a schedule prune on: every host the backup runs
// on. Replicas backed up individually usually keep separate storage paths, each pruned only from
// its own host, while on replicas sharing a path every prune after the first finds nothing left
// to do.
func RetentionHosts(chbs *api.ClickHouseBackupSchedule, topology *Topology) []string {
	return topology.hosts(chbs.Spec.GetReplicaSelection())
}

// BuildBackupCronJob builds the CronJob that runs a backup schedule on the cluster the topology
// targets. suspend overrides the schedule's own setting, so the operator can hold scheduled backups
// while a restore runs. The CronJobs of one schedule share its cron schedule and backup name prefix,
// so the runs of one slot make one backup name on every cluster.
func BuildBackupCronJob(chbs *api.ClickHouseBackupSchedule, chi api.ICustomResource, topology *Topology, suspend bool) *batch.CronJob {
	cluster := topology.cluster()
	options := &chbs.Spec.BackupOptions
	env := append(commonEnv(operationBackup, chi, cluster),
		envVar(envBackupNamePrefix, BackupNamePrefix(chbs)),
		envVar(envCronJobName, CronJobName(chbs, cluster)),
		k8s.NewEnvVarFromLabel(envJobName, LegacyJobNameLabel),
		envVar(envKeepLast, strconv.FormatInt(int64(chbs.Spec.Retention.GetKeepLast()), 10)),
		envVar(envRetentionHosts, joinHosts(RetentionHosts(chbs, topology))),
	)
	env = append(env, backupOptionsEnv(options, topology)...)
	labels := Labels(api.ClickHouseBackupScheduleCRDResourceKind, chbs.GetName(), chbs.Spec.ClickHouseInstallation, cluster,
		topology.storage())

	concurrencyPolicy := batch.ForbidConcurrent
	if chbs.Spec.ConcurrencyPolicy != "" {
		concurrencyPolicy = batch.ConcurrencyPolicy(chbs.Spec.ConcurrencyPolicy)
	}

	cronJob := &batch.CronJob{
		ObjectMeta: meta.ObjectMeta{
			Name:      CronJobName(chbs, cluster),
			Namespace: chbs.GetNamespace(),
			Labels:    labels,
		},
		Spec: batch.CronJobSpec{
			Schedule:                   chbs.Spec.Schedule,
			TimeZone:                   chbs.Spec.GetTimeZone(),
			Suspend:                    ptr.To(suspend),
			ConcurrencyPolicy:          concurrencyPolicy,
			StartingDeadlineSeconds:    chbs.Spec.StartingDeadlineSeconds,
			SuccessfulJobsHistoryLimit: chbs.Spec.SuccessfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     chbs.Spec.FailedJobsHistoryLimit,
			JobTemplate: batch.JobTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: labels},
				Spec:       newJobSpec(labels, options.GetActiveDeadlineSeconds(), topology, env),
			},
		},
	}
	cronJob.Annotations = map[string]string{
		AnnotationBackupObjectVersion: util.Fingerprint(cronJob.Spec),
	}
	return cronJob
}

// backupOptionsEnv returns the environment of the options one-off and scheduled backups share, the
// termination message that tells a backup without data, and what the check of the storage paths needs.
func backupOptionsEnv(options *api.BackupOptions, topology *Topology) []core.EnvVar {
	env := []core.EnvVar{
		envVar(envBackupHosts, joinHosts(topology.hosts(options.GetReplicaSelection()))),
		envVar(envSchemaOnly, strconv.FormatBool(options.SchemaOnly.IsTrue())),
		envVar(envTables, options.Tables),
		envVar(envPartitions, strings.Join(options.Partitions, partitionListSeparator)),
		envVar(envNoDataMessage, NoDataMessage),
	}
	return append(env, pathCheckEnv(topology, options.GetReplicaSelection())...)
}

// pathCheckEnv returns what a Job needs to check, before it changes anything, that the storage paths
// of the hosts it drives are their own, for the replica selection of a backup, none for a restore:
// every backup-enabled cluster of the installation by index, with its shards and every setting that
// tells where its sidecars keep their backups, taken from where the sidecars take it, see
// sidecarSettingSources. kubelet resolves the keys of the configuration file Secret of a cluster for the Job, and
// a key the Secret lacks stays unset, as the setting is in the file. The paths the configuration files default an
// unset path to are passed once. A cluster whose settings the Job cannot know is passed with why, and without
// settings.
func pathCheckEnv(topology *Topology, selection api.ReplicaSelection) []core.EnvVar {
	var indexes []string
	self := 0
	var clusterEnv []core.EnvVar
	for index, cluster := range topology.enabled {
		indexes = append(indexes, strconv.Itoa(index))
		if cluster.name == topology.cluster() {
			self = index
		}
		var shards []string
		for _, shard := range cluster.shards {
			shards = append(shards, shard.name)
		}
		clusterEnv = append(clusterEnv,
			envVar(pathClusterEnv(index, pathEnvCluster), cluster.name),
			envVar(pathClusterEnv(index, pathEnvShards), strings.Join(shards, hostListSeparator)),
		)
		if cluster.sidecar.unverifiable != "" {
			clusterEnv = append(clusterEnv, envVar(pathClusterEnv(index, pathEnvUnverifiable), cluster.sidecar.unverifiable))
			continue
		}
		for _, key := range PathCheckKeys {
			for _, source := range cluster.sidecar.sources[key] {
				source.Name = pathClusterEnv(index, key)
				clusterEnv = append(clusterEnv, source)
			}
		}
	}
	env := []core.EnvVar{
		envVar(envPathClusters, strings.Join(indexes, hostListSeparator)),
		envVar(envPathSelf, strconv.Itoa(self)),
		envVar(envPathInstallation, topology.installation),
		envVar(envPathNamespace, topology.namespace),
		envVar(envDefaultPath, chop.Config().Backup.StoragePath),
		envVar(envDefaultObjectDiskPath, chop.Config().Backup.StorageObjectDiskPath),
	}
	if selection != "" {
		env = append(env, envVar(envPathReplicaSelection, string(selection)))
	}
	return append(env, clusterEnv...)
}

// commonEnv returns the environment every Job script needs to reach the sidecars of the cluster it
// targets. The REST API credentials stay in the operator-generated Secret; the Job only references them.
func commonEnv(operation string, chi api.ICustomResource, cluster string) []core.EnvVar {
	secret := managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(interfaces.NameBackupAutoSecret, chi)
	return []core.EnvVar{
		envVar(envOperation, operation),
		envVar(envCluster, cluster),
		envVar(envAPIPort, strconv.Itoa(config.ClickHouseBackupAPIPortNumber)),
		k8s.NewEnvVarFromSecret(envAPIUsername, secret, config.ClickHouseBackupSecretKeyAPIUsername),
		k8s.NewEnvVarFromSecret(envAPIPassword, secret, config.ClickHouseBackupSecretKeyAPIPassword),
		envVar(envPollInterval, strconv.Itoa(pollIntervalSeconds)),
		envVar(envTerminationMessagePath, core.TerminationMessagePathDefault),
		envVar(envUnchangedExitCode, strconv.Itoa(UnchangedExitCode)),
	}
}

func envVar(name, value string) core.EnvVar {
	return core.EnvVar{Name: name, Value: value}
}

func joinHosts(hosts []string) string {
	return strings.Join(hosts, hostListSeparator)
}

// splitHosts returns the hosts of a list joinHosts made.
func splitHosts(hosts string) []string {
	if hosts == "" {
		return nil
	}
	return strings.Split(hosts, hostListSeparator)
}

func newJob(name, namespace string, labels map[string]string, deadline int64, topology *Topology, env []core.EnvVar) *batch.Job {
	return &batch.Job{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: newJobSpec(labels, deadline, topology, env),
	}
}

// newJobSpec builds a Job that runs the script once. A failed attempt is never retried: the
// script is not idempotent across a restore, and a backup failure needs a human look.
func newJobSpec(labels map[string]string, deadline int64, topology *Topology, env []core.EnvVar) batch.JobSpec {
	return batch.JobSpec{
		BackoffLimit:          ptr.To(int32(0)),
		ActiveDeadlineSeconds: ptr.To(deadline),
		Template: core.PodTemplateSpec{
			ObjectMeta: meta.ObjectMeta{Labels: labels},
			Spec:       newPodSpec(topology.pod(), env),
		},
	}
}

// newPodSpec builds the pod of a Job. It needs no Kubernetes API access, so the token of the
// inherited service account stays unmounted, and service links are off so no Service in the
// namespace can inject a variable that shadows the script's environment.
func newPodSpec(pod podSettings, env []core.EnvVar) core.PodSpec {
	return core.PodSpec{
		RestartPolicy:                core.RestartPolicyNever,
		ServiceAccountName:           pod.serviceAccountName,
		AutomountServiceAccountToken: ptr.To(false),
		EnableServiceLinks:           ptr.To(false),
		ImagePullSecrets:             slices.Clone(pod.imagePullSecrets),
		SecurityContext:              newPodSecurityContext(pod),
		Containers: []core.Container{
			{
				Name:            jobContainerName,
				Image:           pod.image,
				ImagePullPolicy: pod.imagePullPolicy,
				Command:         k8s.EscapeVariableReferencesAll(append(append([]string{}, jobCommand...), jobScript)),
				Env:             env,
				Resources:       *jobResources.DeepCopy(),
				// The script writes why it failed there, or that the backup it made holds no data, so
				// either outlives the Job's log; a container that dies without writing why reports the
				// tail of its log instead.
				TerminationMessagePath:   core.TerminationMessagePathDefault,
				TerminationMessagePolicy: core.TerminationMessageFallbackToLogsOnError,
				SecurityContext:          newContainerSecurityContext(pod.containerSecurityContext),
			},
		},
	}
}

// newPodSecurityContext returns the pod security context of a Job. The one of the hosts' pod
// template is what admission accepts in the namespace, an OpenShift SCC that assigns the user
// itself included, so it is kept. Sysctls go: the node has to allow them, and the Job does not
// follow the hosts to their nodes.
func newPodSecurityContext(pod podSettings) *core.PodSecurityContext {
	securityContext := &core.PodSecurityContext{}
	if pod.securityContext != nil {
		securityContext = pod.securityContext.DeepCopy()
	}
	securityContext.Sysctls = nil
	if (securityContext.RunAsNonRoot == nil) && (ptr.Deref(securityContext.RunAsUser, 0) > 0) {
		securityContext.RunAsNonRoot = ptr.To(true)
	}
	return securityContext
}

// newContainerSecurityContext returns the security context of the Job container: no privilege, and
// the identity and seccomp profile of the hosts' sidecar, which take precedence over the pod's as
// they do in the sidecar, so the Job runs as the sidecar does. Root and an explicit permission to run
// as root are not taken over: the Job needs no privilege. Nothing is invented: a platform that
// assigns users refuses a fixed one, and an SCC without seccomp profiles refuses any.
func newContainerSecurityContext(sidecar *core.SecurityContext) *core.SecurityContext {
	securityContext := &core.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities: &core.Capabilities{
			Drop: []core.Capability{capabilityAll},
		},
	}
	if sidecar == nil {
		return securityContext
	}
	if ptr.Deref(sidecar.RunAsUser, 0) > 0 {
		securityContext.RunAsUser = ptr.To(*sidecar.RunAsUser)
		securityContext.RunAsNonRoot = ptr.To(true)
	}
	if ptr.Deref(sidecar.RunAsGroup, 0) > 0 {
		securityContext.RunAsGroup = ptr.To(*sidecar.RunAsGroup)
	}
	if ptr.Deref(sidecar.RunAsNonRoot, false) {
		securityContext.RunAsNonRoot = ptr.To(true)
	}
	if sidecar.SeccompProfile != nil {
		securityContext.SeccompProfile = sidecar.SeccompProfile.DeepCopy()
	}
	return securityContext
}

// JobTerminationMessage returns what the Job container of a pod reported when it terminated: the
// error the Job script wrote, or the tail of its log for a container that failed without writing
// one. It returns "" for a container that has not terminated.
func JobTerminationMessage(pod *core.Pod) string {
	if terminated := jobContainerTermination(pod); terminated != nil {
		return strings.TrimSpace(terminated.Message)
	}
	return ""
}

// JobBackupHoldsNoData reports whether the Job container of a pod succeeded and made a backup that
// holds no data on any host, which the Job script tells by its termination message.
func JobBackupHoldsNoData(pod *core.Pod) bool {
	terminated := jobContainerTermination(pod)
	return (terminated != nil) && (terminated.ExitCode == 0) && (strings.TrimSpace(terminated.Message) == NoDataMessage)
}

// JobRBACNotRestored reports whether the Job container of a pod succeeded restoring a backup that holds
// access entities it did not restore, which the Job script tells by its termination message.
func JobRBACNotRestored(pod *core.Pod) bool {
	terminated := jobContainerTermination(pod)
	return (terminated != nil) && (terminated.ExitCode == 0) && (strings.TrimSpace(terminated.Message) == RBACNotRestoredMessage)
}

// JobFailedUnchanged reports whether the Job container of a pod failed before it changed anything,
// which the Job script tells by its exit code.
func JobFailedUnchanged(pod *core.Pod) bool {
	terminated := jobContainerTermination(pod)
	return (terminated != nil) && (terminated.ExitCode == UnchangedExitCode)
}

// jobContainerTermination returns how the Job container of a pod terminated, nil while it has not
// or when the pod has no status of it.
func jobContainerTermination(pod *core.Pod) *core.ContainerStateTerminated {
	return containerTermination(jobContainerStatus(pod))
}

// containerTermination returns how a container terminated, nil while it has not or without a status.
func containerTermination(status *core.ContainerStatus) *core.ContainerStateTerminated {
	if status == nil {
		return nil
	}
	for _, terminated := range []*core.ContainerStateTerminated{status.State.Terminated, status.LastTerminationState.Terminated} {
		if terminated != nil {
			return terminated
		}
	}
	return nil
}

// jobContainerStatus returns the status of the Job container of a pod, nil when the pod has none.
func jobContainerStatus(pod *core.Pod) *core.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == jobContainerName {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// JobContainerStarted reports whether the Job container of a pod runs or ran. A pod past Pending has
// started all of its containers; a pending one counts only by the Job container itself, as an
// injected sidecar may run while the Job container's image cannot be pulled.
func JobContainerStarted(pod *core.Pod) bool {
	switch pod.Status.Phase {
	case core.PodRunning, core.PodSucceeded, core.PodFailed:
		return true
	}
	status := jobContainerStatus(pod)
	return (status != nil) && ((status.State.Running != nil) || (containerTermination(status) != nil))
}
