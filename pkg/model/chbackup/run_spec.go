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
	"slices"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// RunSpec is what a run of a backup or a restore on one cluster works by: the operator builds it from
// the resource and the topology of the cluster as the run starts, and again, alike, as it resumes the run.
type RunSpec struct {
	Operation string
	// Cluster is the cluster the run works on
	Cluster string
	// ConfigSecret is the Secret of the clickhouse-backup configuration of the cluster
	ConfigSecret string
	// Naming tells how the backups of the cluster are named on its hosts
	Naming Naming
	// PathCheck is what the storage path check reads, nil for a run that checks no path
	PathCheck *PathCheck
	// Deadline bounds the run
	Deadline time.Duration
	// Endpoint returns how the operator reaches the ClickHouse of a host, from any namespace
	Endpoint func(host string) HostEndpoint
	// Shard returns the shard of a host, "" when unknown: a backup is made on a shard once any replica of it made it
	Shard func(host string) string
	// Timeouts bound how the run waits for its commands: those of the resource, then of the schedule a backup runs,
	// then of the operator configuration, field by field; a field none of them sets is the built-in default
	Timeouts *api.BackupTimeouts

	// BackupName is the backup the run makes or restores, with a template the name of the backup of any
	// shard; empty for a scheduled run, which names its backup after its slot
	BackupName string

	// Backup
	Prefix string
	// Slot is the time a scheduled run was due, the zero time for any other run
	Slot time.Time
	// Entries, TimeZone, KeepBackups and Previous are those of the schedule a backup runs, none for a one-off backup
	Entries     []SlotEntry
	TimeZone    string
	KeepBackups []api.BackupRetentionPin
	Previous    []api.RetiredScheduleRules
	// RunsSchedule tells a backup that runs a schedule: incremental and retention as its runs, but named apart
	RunsSchedule bool
	// RestoreInProgress skips retention
	RestoreInProgress bool
	// BackupHosts are the hosts the backup is made on, the preferred replica of every shard, and FallbackHosts the
	// host of every shard it is made on when the preferred one does not answer, none when they are the same
	BackupHosts      []string
	FallbackHosts    []string
	RetentionHosts   []string
	SchemaOnly       bool
	Tables           []string
	SkipTables       []string
	SkipTableEngines []string
	Partitions       []string
	// ConfigOverrides are those of a one-off backup
	ConfigOverrides []string
	DiffFromRemote  string

	// Restore
	SchemaHosts        []string
	DataHosts          []string
	Overwrite          bool
	ValidateTopology   bool
	RestoreRBAC        bool
	DatabaseMapping    string
	SourceInstallation string
	SourceNamespace    string
	// ReplicatedCopyToDetached has the replicas of a shard but the one that restores its data copy the data of the
	// backup into their detached directories first; without it they download the schema alone
	ReplicatedCopyToDetached bool
	// ResumeDownload has a rerun of the restore resume the downloads of its earlier runs; without it the rerun deletes
	// the local copies they downloaded and downloads the backup again
	ResumeDownload bool

	// PreviousActions are the commands the runs of the resource on the cluster inserted before spec.taskID reran it,
	// which the rerun takes for its own: a backup replaces the copies of its backup their creates left, a restore
	// resumes the downloads they made
	PreviousActions []api.BackupRunAction
}

// newRunSpec returns the spec of a run on the cluster of the topology.
func newRunSpec(operation string, topology *Topology, deadline int64) *RunSpec {
	return &RunSpec{
		Operation:    operation,
		Cluster:      topology.Cluster(),
		ConfigSecret: topology.ConfigSecret(),
		Naming:       topology.RunNaming(),
		Deadline:     time.Duration(deadline) * time.Second,
		Endpoint:     topology.Endpoint,
		Shard:        topology.ShardOf,
	}
}

// operatorTimeouts returns the timeouts of the operator configuration, nil without one.
func operatorTimeouts() *api.BackupTimeouts {
	if config := chop.Config(); config != nil {
		return &config.Backup.Timeouts
	}
	return nil
}

// RunTimeouts returns the timeouts of a run: those given, the first that sets a field winning, then those of the
// operator configuration.
func RunTimeouts(timeouts ...*api.BackupTimeouts) *api.BackupTimeouts {
	return (*api.BackupTimeouts)(nil).MergeFrom(append(timeouts, operatorTimeouts())...)
}

// PathCheckOverrides returns the lists of config overrides the run of a backup may be made with: those of a one-off
// backup, and those of every entry of the schedule it runs, see PathCheck.ConfigOverrides.
func (spec *RunSpec) PathCheckOverrides() [][]string {
	var overrides [][]string
	if len(spec.ConfigOverrides) > 0 {
		overrides = append(overrides, spec.ConfigOverrides)
	}
	for _, entry := range spec.Entries {
		if len(entry.ConfigOverrides) > 0 {
			overrides = append(overrides, entry.ConfigOverrides)
		}
	}
	return overrides
}

// backupHosts sets the hosts a backup with the options runs on.
func (spec *RunSpec) backupHosts(options *api.BackupOptions, topology *Topology) {
	spec.BackupHosts = topology.BackupHosts(options.GetReplicaSelection())
	// The preferred replica of a shard that does not answer leaves the backup to the first one, whose storage path
	// is the shard's as well
	fallback := topology.Hosts(options.GetReplicaSelection())
	if !slices.Equal(spec.BackupHosts, fallback) {
		spec.FallbackHosts = fallback
	}
	spec.SchemaOnly = options.SchemaOnly.IsTrue()
	spec.Tables = slices.Clone(options.Tables)
	spec.SkipTables = slices.Clone(options.SkipTables)
	spec.SkipTableEngines = slices.Clone(options.SkipTableEngines)
	spec.Partitions = slices.Clone(options.Partitions)
}

// BackupRunSpec returns the spec of the run of a one-off backup on the cluster of the topology.
func BackupRunSpec(chb *api.ClickHouseBackup, topology *Topology) *RunSpec {
	options := chb.GetBackupOptions()
	spec := newRunSpec(api.BackupRunOperationBackup, topology, options.GetActiveDeadlineSeconds())
	spec.BackupName = cmp.Or(chb.Status.BackupName, ResolveBackupName(chb))
	spec.DiffFromRemote = chb.Spec.DiffFromRemote
	spec.ConfigOverrides = api.ConfigOverrides(chb.Spec.ConfigOverrides)
	spec.backupHosts(options, topology)
	spec.Timeouts = RunTimeouts(chb.Spec.Timeouts, chb.Status.Schedule.GetTimeouts())
	// A backup that runs a schedule now runs as its runs do - incremental as the schedule says, retention after it -
	// but for its name, which keeps it out of the series of the scheduled runs
	if record := chb.Status.Schedule; (chb.Spec.ScheduleRef != nil) && (record != nil) && (len(record.Schedules) > 0) &&
		(record.BackupNamePrefix != "") {
		spec.RunsSchedule = true
		spec.Prefix = record.BackupNamePrefix
		spec.Entries = SlotEntries(record.Schedules, record.TimeZone)
		spec.TimeZone = record.TimeZone
		spec.KeepBackups = slices.Clone(record.KeepBackups)
		spec.Previous = record.PreviousSchedules
		spec.RetentionHosts = topology.Hosts(options.GetReplicaSelection())
	}
	return spec
}

// ScheduledRunSpec returns the spec of the run of a schedule at the slot on the cluster of the topology.
func ScheduledRunSpec(chbs *api.ClickHouseBackupSchedule, topology *Topology, slot time.Time) *RunSpec {
	options := &chbs.Spec.BackupOptions
	spec := newRunSpec(api.BackupRunOperationBackup, topology, options.GetActiveDeadlineSeconds())
	spec.Prefix = BackupNamePrefix(chbs)
	spec.Slot = slot
	spec.Timeouts = RunTimeouts(chbs.Spec.Timeouts)
	spec.Entries = SlotEntries(chbs.Spec.Schedules, chbs.Spec.TimeZone)
	spec.TimeZone = chbs.Spec.TimeZone
	spec.KeepBackups = slices.Clone(chbs.Spec.Retention.GetKeepBackups())
	spec.Previous = chbs.Status.Retention.GetPrevious()
	spec.backupHosts(options, topology)
	spec.RetentionHosts = topology.Hosts(options.GetReplicaSelection())
	return spec
}

// RestoreRunSpec returns the spec of the run of a restore on the cluster of the topology. The schema is restored on
// every replica, each creating its tables from the backup's own definitions, the data on the first replica of each
// shard, from which replication fills the others, from the copies of the data in their detached directories with
// replicatedCopyToDetached.
func RestoreRunSpec(chr *api.ClickHouseRestore, topology *Topology) *RunSpec {
	spec := newRunSpec(api.BackupRunOperationRestore, topology, chr.Spec.GetActiveDeadlineSeconds())
	spec.BackupName = cmp.Or(chr.Status.BackupName, chr.Spec.BackupName)
	spec.SchemaHosts = topology.AllReplicas()
	spec.DataHosts = topology.FirstPerShard()
	spec.SchemaOnly = chr.Spec.SchemaOnly.IsTrue()
	spec.Tables = slices.Clone(chr.Spec.Tables)
	spec.Partitions = slices.Clone(chr.Spec.Partitions)
	spec.SkipTables = slices.Clone(chr.Spec.SkipTables)
	spec.SkipTableEngines = slices.Clone(chr.Spec.SkipTableEngines)
	spec.DatabaseMapping = chr.Spec.GetDatabaseMapping()
	spec.Overwrite = chr.Spec.Overwrite.IsTrue()
	spec.ValidateTopology = chr.Spec.IsTopologyValidationEnabled()
	spec.RestoreRBAC = RestoresRBAC(chr)
	spec.ReplicatedCopyToDetached = chr.Spec.IsReplicatedCopyToDetached()
	spec.ResumeDownload = chr.Spec.IsResumeDownload()
	spec.SourceInstallation = chr.Spec.SourceInstallation
	spec.SourceNamespace = chr.Spec.SourceNamespace
	spec.Timeouts = RunTimeouts(chr.Spec.Timeouts)
	return spec
}

// Hosts returns the hosts a run drives, as its status records them: those it backs up, or restores the schema on,
// which is every replica a restore touches.
func (spec *RunSpec) Hosts() []string {
	if spec.Operation == api.BackupRunOperationBackup {
		return spec.BackupHosts
	}
	return spec.SchemaHosts
}
