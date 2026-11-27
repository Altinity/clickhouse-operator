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

// The operator builds it again on resume.
type RunSpec struct {
	Operation    api.BackupRunOperation
	Cluster      string
	ConfigSecret string
	Naming       Naming
	// PathCheck is nil when the run checks no path
	PathCheck *PathCheck
	Deadline  time.Duration
	Endpoint  func(host string) HostEndpoint
	Shard     func(host string) string
	// Timeouts are merged by field: resource, then schedule, then operator configuration, then default
	Timeouts *api.BackupTimeouts

	// With a template, BackupName is the name on any shard. It is empty for a scheduled run, named after its slot
	BackupName string

	Prefix      string
	Slot        time.Time
	Entries     []SlotEntry
	TimeZone    string
	KeepBackups []api.BackupRetentionPin
	Previous    []api.RetiredScheduleRules
	// RunsSchedule marks scheduleRef: schedule increments and retention, but its own name
	RunsSchedule bool
	// RestoreInProgress skips retention
	RestoreInProgress bool
	// FallbackHosts are empty when they equal BackupHosts
	BackupHosts      []string
	FallbackHosts    []string
	RetentionHosts   []string
	SchemaOnly       bool
	Tables           []string
	SkipTables       []string
	SkipTableEngines []string
	Partitions       []string
	ConfigOverrides  []string
	DiffFromRemote   string

	SchemaHosts        []string
	DataHosts          []string
	Overwrite          bool
	ValidateTopology   bool
	RestoreRBAC        bool
	DatabaseMapping    string
	SourceInstallation string
	SourceNamespace    string
	// Without ReplicatedCopyToDetached, the other replicas of a shard download only the schema
	ReplicatedCopyToDetached bool
	// Without ResumeDownload, a rerun deletes the local copies and downloads again
	ResumeDownload bool

	// A backup rerun replaces the backups of PreviousActions. A restore rerun resumes their downloads
	PreviousActions []api.BackupRunAction
}

func newRunSpec(operation api.BackupRunOperation, topology *Topology, deadline int64) *RunSpec {
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

func operatorTimeouts() *api.BackupTimeouts {
	if config := chop.Config(); config != nil {
		return &config.Backup.Timeouts
	}
	return nil
}

// The first value set wins.
func RunTimeouts(timeouts ...*api.BackupTimeouts) *api.BackupTimeouts {
	return (*api.BackupTimeouts)(nil).MergeFrom(append(timeouts, operatorTimeouts())...)
}

// CheckPaths sets the check that the run does not use the storage path of another cluster.
func (spec *RunSpec) CheckPaths(topology *Topology, selection api.ReplicaSelection, resolve SettingResolver) error {
	check, err := topology.PathCheck(spec.Operation, selection, resolve)
	if err != nil {
		return err
	}
	if spec.Operation == api.BackupRunOperationRestore {
		check.ValidateTopology = spec.ValidateTopology
	} else {
		check.ConfigOverrides = spec.PathCheckOverrides()
	}
	spec.PathCheck = check
	return nil
}

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

func (spec *RunSpec) backupHosts(options *api.BackupOptions, topology *Topology) {
	spec.BackupHosts = topology.BackupHosts(options.GetReplicaSelection())
	// The fallback replica has the same storage path as the preferred one
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

func BackupRunSpec(chb *api.ClickHouseBackup, topology *Topology) *RunSpec {
	options := chb.GetBackupOptions()
	spec := newRunSpec(api.BackupRunOperationBackup, topology, options.GetActiveDeadlineSeconds())
	spec.BackupName = cmp.Or(chb.Status.BackupName, ResolveBackupName(chb))
	spec.DiffFromRemote = chb.Spec.DiffFromRemote
	spec.ConfigOverrides = api.ConfigOverrides(chb.Spec.ConfigOverrides)
	spec.backupHosts(options, topology)
	spec.Timeouts = RunTimeouts(chb.Spec.Timeouts, chb.Status.Schedule.GetTimeouts())
	// With scheduleRef, use the schedule increments and retention, but a name outside the schedule series
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

// The data is restored on the first replica of each shard, and replication fills the others.
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
	spec.ConfigOverrides = api.ConfigOverrides(chr.Spec.ConfigOverrides)
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

func (spec *RunSpec) Hosts() []string {
	if spec.Operation == api.BackupRunOperationBackup {
		return spec.BackupHosts
	}
	return spec.SchemaHosts
}
