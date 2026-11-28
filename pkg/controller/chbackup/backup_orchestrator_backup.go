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
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type backupPlan struct {
	hosts        []string
	key          string
	namingPrefix string
	name         string
	commands     map[string]string
}

func (o *orchestrator) backup(ctx context.Context) error {
	if o.step() == "" {
		plan, err := o.planBackup(ctx)
		if err != nil {
			return err
		}
		hosts := make([]api.BackupRunHost, 0, len(plan.hosts))
		for _, host := range plan.hosts {
			hosts = append(hosts, api.BackupRunHost{Host: host, Backup: o.backupNames[host], Base: baseOf(plan.commands[host])})
		}
		o.update(func(status *api.BackupRunStatus) {
			status.Hosts = hosts
			status.Step = stepCreate
		})
		o.setPlan(func(p *api.BackupRunPlan) {
			p.BackupName, p.Key, p.NamingPrefix = plan.name, plan.key, plan.namingPrefix
		})
		for _, host := range plan.hosts {
			o.record(api.BackupRunAction{Step: stepCreate, Host: host, Command: plan.commands[host]})
		}
	}
	o.mu.Lock()
	hosts := slices.Clone(o.status.Hosts)
	o.mu.Unlock()
	var hostNames []string
	for _, host := range hosts {
		hostNames = append(hostNames, host.Host)
		o.backupNames[host.Host] = host.Backup
	}
	name := o.plan().BackupName
	o.outcome.backupName = name
	if !slices.Equal(hostNames, o.spec.BackupHosts) {
		o.outcome.hostsUsed = hostNames
	}

	if o.step() == stepCreate {
		failed, failures := o.runAll(ctx, stepCreate, hostNames, true, o.createCommand)
		var made, unmade []string
		for _, host := range hostNames {
			if o.succeeded(stepCreate, host) {
				made = append(made, host)
			} else {
				unmade = append(unmade, host)
			}
		}
		var lost []string
		for _, host := range unmade {
			shard := o.shardOf(host)
			if !slices.ContainsFunc(made, func(other string) bool { return o.shardOf(other) == shard }) && !slices.Contains(lost, shard) {
				lost = append(lost, shard)
			}
		}
		if (len(unmade) > 0) && (ctx.Err() == nil) {
			o.dropFailedBackup(context.WithoutCancel(ctx), unmade)
		}
		if (len(lost) > 0) || (ctx.Err() != nil) {
			if len(failed) == 0 {
				failed = unmade
			}
			return failf("backup %s failed on %s: %s", name, strings.Join(failed, ", "), strings.Join(failures, "; "))
		}
		if len(unmade) > 0 {
			o.warnf("backup %s failed on %s, whose shards other replicas backed up: %s", name, strings.Join(unmade, ", "),
				strings.Join(failures, "; "))
			o.eventf(core.EventTypeWarning, "OrchestratorReplicaFailed", "backup %s failed on %s, whose shards other replicas backed up",
				name, strings.Join(unmade, ", "))
		}
		o.logf("backup %s completed on %d shard(s)", name, len(hostNames)-len(unmade))
		o.setStep(stepReport)
	}
	var made []string
	for _, host := range hostNames {
		if o.succeeded(stepCreate, host) {
			made = append(made, host)
		}
	}
	if (len(made) > 0) && (len(made) < len(hostNames)) {
		hostNames = made
		o.outcome.hostsUsed = made
	}
	if !o.spec.SchemaOnly {
		o.reportData(ctx, hostNames, name)
	}
	o.eventf(core.EventTypeNormal, "OrchestratorBackupCompleted", "backup %s completed on %d host(s)", name, len(hostNames))
	o.setStep(stepRetention)
	o.retention(ctx)
	return nil
}

func (o *orchestrator) createCommand(host string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, action := range o.status.Actions {
		if (action.Step == stepCreate) && (action.Host == host) {
			return action.Command
		}
	}
	return ""
}

func baseOf(command string) string {
	for _, field := range strings.Fields(command) {
		if base, found := strings.CutPrefix(field, "--diff-from-remote="); found {
			return base
		}
	}
	return ""
}

func (o *orchestrator) planBackup(ctx context.Context) (*backupPlan, error) {
	spec := o.spec
	naming := spec.Naming
	hosts := o.fallBackHosts(ctx)
	slotTag := ""
	if !spec.Slot.IsZero() {
		slotTag = model.SlotTag(spec.Slot, spec.TimeZone)
	}
	plan := &backupPlan{hosts: hosts, commands: map[string]string{}}
	// A ClickHouseBackup always has a name. A scheduled run has none: its name is the prefix and the scheduled time.
	switch {
	case spec.BackupName != "":
		key, prefix, err := naming.Key("backup name", spec.BackupName, spec.Prefix)
		if err != nil {
			return nil, failf("%v", err)
		}
		plan.key, plan.namingPrefix = key, prefix
	case naming.IsTemplate():
		plan.key, plan.namingPrefix = slotTag, spec.Prefix
	default:
		plan.key = spec.Prefix + "-" + slotTag
	}
	for _, host := range hosts {
		name, err := naming.HostName(host, plan.key, plan.namingPrefix)
		if err != nil {
			return nil, failf("%v", err)
		}
		o.backupNames[host] = name
	}
	plan.name = plan.key
	if naming.IsTemplate() {
		name, err := naming.FirstName(plan.key, plan.namingPrefix)
		if err != nil {
			return nil, failf("%v", err)
		}
		plan.name = name
		var named []string
		for _, host := range hosts {
			named = append(named, host+"="+o.backupNames[host])
		}
		o.logf("backup %s: named by the naming template '%s', %s", plan.name, naming.Template, strings.Join(named, " "))
	}

	bases := map[string]string{}
	if spec.DiffFromRemote != "" {
		key, prefix, err := naming.Key("diffFromRemote", spec.DiffFromRemote, spec.Prefix)
		if err != nil {
			return nil, failf("%v", err)
		}
		for _, host := range hosts {
			if bases[host], err = naming.HostName(host, key, prefix); err != nil {
				return nil, failf("%v", err)
			}
		}
	}
	incremental, keep := false, 0
	overrides := spec.ConfigOverrides
	switch {
	case (len(spec.Entries) > 0) && spec.RunsSchedule:
		if winner := model.IncrementalEntry(spec.Entries); winner < 0 {
			o.logf("backup %s: runs the schedule now, which has no incremental entry: the backup is full", plan.name)
		} else if spec.DiffFromRemote == "" {
			o.logf("backup %s: runs the schedule now, incremental as its %s entry", plan.name, spec.Entries[winner].Period)
			incremental, keep = true, spec.Entries[winner].Keep
		}
	case len(spec.Entries) > 0:
		winner, matched := model.WinnerEntry(spec.Entries, slotTag, naming.Tolerance())
		if winner < 0 {
			o.logf("backup %s: the scheduled time %s belongs to no schedule entry, the backup is full", plan.name, slotTag)
		} else {
			lighter := ""
			if len(matched) > 1 {
				lighter = " and lighter ones"
			}
			o.logf("backup %s: the scheduled time %s belongs to the %s entry%s", plan.name, slotTag, spec.Entries[winner].Period, lighter)
			if spec.Entries[winner].Incremental && (spec.DiffFromRemote == "") {
				incremental, keep = true, spec.Entries[winner].Keep
			}
			overrides = spec.Entries[winner].ConfigOverrides
		}
	}
	flags := configOverrides(overrides)
	if flags != "" {
		o.logf("backup %s: made with the config overrides%s", plan.name, flags)
		o.eventf(core.EventTypeNormal, "OrchestratorConfigOverrides", "backup %s is made with the config overrides%s", plan.name, flags)
	}
	if err := o.checkPaths(); err != nil {
		return nil, err
	}
	if err := o.ready(ctx, hosts); err != nil {
		return nil, err
	}

	o.eventf(core.EventTypeNormal, "OrchestratorBackupStarted", "backup %s on %d host(s)", plan.name, len(hosts))
	o.logf("backup %s: preflight on %d host(s)", plan.name, len(hosts))
	required := map[string]map[string]string{}
	left := &leftCopies{remote: map[string]bool{}, local: map[string]bool{}}
	for _, host := range hosts {
		rows, err := o.list(ctx, host, "remote")
		if err != nil {
			return nil, failf("preflight failed on %s: %v", host, err)
		}
		earlier := o.leftByEarlierRun(host, o.backupNames[host])
		if findBackup(rows, o.backupNames[host]) != nil {
			if !earlier {
				return nil, failf("backup %s already exists in the storage of %s", o.backupNames[host], host)
			}
			left.remote[host] = true
		}
		if naming.IsTemplate() {
			for _, row := range rows {
				if parsed, ok := naming.Parse(row.Name, plan.namingPrefix); ok && (parsed.Tag == plan.key) && !(earlier && (row.Name == o.backupNames[host])) {
					return nil, failf("backup %s, of the same time as %s, already exists in the storage of %s", row.Name, plan.name, host)
				}
			}
		}
		if earlier {
			present, err := o.listed(ctx, host, "local", o.backupNames[host])
			if err != nil {
				return nil, failf("preflight failed on %s: %v", host, err)
			}
			left.local[host] = present
		}
		if (spec.DiffFromRemote != "") && (findBackup(rows, bases[host]) == nil) {
			return nil, failf("base backup %s is missing in the storage of %s", bases[host], host)
		}
		if incremental {
			required[host] = map[string]string{}
			for _, row := range rows {
				if row.broken() {
					continue
				}
				if key, ok := naming.SeriesKey(host, row.Name, spec.Prefix); ok && (key != plan.key) {
					required[host][key] = row.Required
				}
			}
		}
	}
	if incremental {
		base, chain := model.IncrementalBase(naming, spec.Prefix, required, hosts, keep)
		if base != "" {
			for _, host := range hosts {
				name, err := naming.HostName(host, base, plan.namingPrefix)
				if err != nil {
					return nil, failf("%v", err)
				}
				bases[host] = name
			}
			o.logf("backup %s: incremental from %s, which ends a chain of %d incremental backups", plan.name, bases[hosts[0]], chain)
			o.eventf(core.EventTypeNormal, "OrchestratorIncrementalBase", "%s is incremental from %s", plan.name, bases[hosts[0]])
		} else {
			o.logf("backup %s: full, as the chain of incremental backups is %d long, of %d at most, or there is no base every "+
				"host holds", plan.name, chain, keep)
			o.eventf(core.EventTypeNormal, "OrchestratorIncrementalFull", "%s is a full backup", plan.name)
		}
	}
	for _, host := range hosts {
		command := commandCreateRemote + flags
		if spec.SchemaOnly {
			command += " --schema"
		}
		if len(spec.Tables) > 0 {
			command += " --tables=" + strings.Join(spec.Tables, ",")
		}
		if len(spec.Partitions) > 0 {
			command += " --partitions=" + strings.Join(spec.Partitions, ",")
		}
		for _, flag := range skipEnv(spec.SkipTables, spec.SkipTableEngines) {
			command += " " + flag
		}
		if bases[host] != "" {
			command += " --diff-from-remote=" + bases[host]
		}
		plan.commands[host] = command + " " + o.backupNames[host]
	}
	if err := o.replaceLeft(ctx, hosts, left); err != nil {
		return nil, err
	}
	return plan, nil
}

type leftCopies struct {
	remote, local map[string]bool
}

func (o *orchestrator) leftByEarlierRun(host, name string) bool {
	return slices.ContainsFunc(o.spec.PreviousActions, func(action api.BackupRunAction) bool {
		fields := strings.Fields(action.Command)
		return (action.Step == stepCreate) && (action.Host == host) && (action.TimeFrom != "") && (len(fields) > 1) &&
			(fields[len(fields)-1] == name)
	})
}

// clickhouse-backup refuses a name it already lists, and cannot resume a backup whose local state was deleted.
func (o *orchestrator) replaceLeft(ctx context.Context, hosts []string, left *leftCopies) error {
	deletedRemote := map[string]bool{}
	for _, host := range hosts {
		name := o.backupNames[host]
		if left.local[host] {
			if err := o.deleteBackup(ctx, stepReplaceLocal, host, "local", name); err != nil {
				return failf("%s: cannot delete the local copy of %s an earlier run of the backup left: %v", host, name, err)
			}
			o.eventf(core.EventTypeNormal, "OrchestratorRerunReplaced", "%s: the local copy of %s an earlier run of the backup left deleted", host, name)
		}
		shard := o.shardOf(host) + "|" + name
		if !left.remote[host] || deletedRemote[shard] {
			continue
		}
		if err := o.deleteBackup(ctx, stepReplaceRemote, host, "remote", name); err != nil {
			return failf("%s: cannot delete the remote copy of %s an earlier run of the backup left: %v", host, name, err)
		}
		deletedRemote[shard] = true
		o.eventf(core.EventTypeNormal, "OrchestratorRerunReplaced", "%s: the remote copy of %s an earlier run of the backup left deleted", host, name)
	}
	return nil
}

// dropFailedBackup deletes nothing remote. A running upload can still complete the backup,
// and the object disk data of a failed replica can belong to a replica that made the backup.
func (o *orchestrator) dropFailedBackup(ctx context.Context, hosts []string) {
	o.bestEffort = true
	defer func() { o.bestEffort = false }()
	var names []string
	for _, host := range hosts {
		name := o.backupNames[host]
		names = append(names, host+"="+name)
		present, err := o.listed(ctx, host, "local", name)
		switch {
		case err != nil:
			o.eventf(core.EventTypeWarning, "OrchestratorFailedBackupLeft", "%s: the local copy of the failed backup %s, if any, is "+
				"left: %v; delete it by hand with %s", host, name, err, deleteByHand(name))
			continue
		case !present:
			o.logf("%s: no local copy of the failed backup %s to delete", host, name)
			continue
		}
		if err := o.cleanup(ctx, stepDropFailed, host, name); err != nil {
			o.eventf(core.EventTypeWarning, "OrchestratorFailedBackupLeft", "%s: the local copy of the failed backup %s is left: %v; "+
				"delete it by hand with %s", host, name, err, deleteByHand(name))
			continue
		}
		o.eventf(core.EventTypeNormal, "OrchestratorFailedBackupDeleted", "%s: the local copy of the failed backup %s deleted", host, name)
	}
	o.logf("what the failed backup left in the remote storage of %s is left to retention, which deletes an incomplete backup "+
		"once a complete one is newer", strings.Join(names, " "))
	o.eventf(core.EventTypeNormal, "OrchestratorFailedBackupRemoteLeft", "what the failed backup left in the remote storage of %s, "+
		"listed as broken, is left to the retention of its schedule, which deletes an incomplete backup once a complete one is "+
		"newer; delete it by hand otherwise", strings.Join(names, " "))
}

func (o *orchestrator) listed(ctx context.Context, host, location, name string) (bool, error) {
	rows, err := o.list(ctx, host, location)
	return findBackup(rows, name) != nil, err
}

// listAll lists the backups of the hosts. A backup is complete when each shard that must hold it lists it on a replica.
// With timeOf nil, every shard must hold it. Otherwise only a shard that lists a backup as old or older must: a shard
// added later has no copy of the backups made before it, and a restore of them leaves it empty.
func (o *orchestrator) listAll(ctx context.Context, hosts []string, keyOf func(host, name string) (string, bool),
	timeOf func(key string) string) (model.BackupSets, []string) {
	sets := model.NewBackupSets()
	var unlisted, shards []string
	shardListed := map[string]map[string]bool{}
	// oldest is the time of the oldest backup each shard lists
	oldest := map[string]string{}
	for _, host := range hosts {
		rows, err := o.list(ctx, host, "remote")
		if err != nil {
			unlisted = append(unlisted, host)
			continue
		}
		shard := o.shardOf(host)
		listed := map[string]bool{}
		for _, row := range rows {
			key, ok := keyOf(host, row.Name)
			if !ok {
				continue
			}
			if timeOf != nil {
				t := timeOf(key)
				if first, found := oldest[shard]; !found || (t < first) {
					oldest[shard] = t
				}
			}
			sets.All[key] = true
			recordStored(sets, key, host, row, keyOf)
			if row.sized() {
				sets.Sized[key] = true
			}
			if row.broken() {
				sets.Broken[key] = true
				continue
			}
			listed[key] = true
			if row.holdsData() {
				sets.WithData[key] = true
			}
			if !row.sized() {
				sets.Empty[key] = true
			}
		}
		if shardListed[shard] == nil {
			shardListed[shard] = map[string]bool{}
			shards = append(shards, shard)
		}
		for key := range listed {
			shardListed[shard][key] = true
		}
	}
	for _, shard := range shards {
		for key := range shardListed[shard] {
			sets.Complete[key] = true
		}
	}
	for key := range sets.Complete {
		for _, shard := range shards {
			mustHold := timeOf == nil
			if t, found := oldest[shard]; found {
				mustHold = t <= timeOf(key)
			}
			if mustHold && !shardListed[shard][key] {
				delete(sets.Complete, key)
				break
			}
		}
	}
	return sets, unlisted
}

func recordStored(sets model.BackupSets, key, host string, row backupRow, keyOf func(host, name string) (string, bool)) {
	detail := sets.Stored[key]
	if detail == nil {
		detail = &model.StoredDetail{Base: row.Required}
		if row.Required != "" {
			if baseKey, ok := keyOf(host, row.Required); ok {
				detail.BaseKey = baseKey
			}
		}
		sets.Stored[key] = detail
	}
	detail.Size += row.Size
	if !row.Created.IsZero() && (detail.Created.IsZero() || row.Created.Before(detail.Created)) {
		detail.Created = row.Created
	}
}

// storedBackups describes the backups of the keys as the listing tells, newest first. nameOf names a key.
func storedBackups(sets model.BackupSets, keys []string, nameOf func(key string) string) []api.StoredBackup {
	backups := []api.StoredBackup{}
	for _, key := range keys {
		backup := api.StoredBackup{Name: nameOf(key), State: api.StoredBackupIncomplete}
		switch {
		case sets.Broken[key]:
			backup.State = api.StoredBackupBroken
		case sets.Complete[key]:
			backup.State = api.StoredBackupComplete
		}
		if detail := sets.Stored[key]; detail != nil {
			if !detail.Created.IsZero() {
				backup.Created = &meta.Time{Time: detail.Created}
			}
			backup.Size = int64(min(detail.Size, math.MaxInt64))
			backup.Base = detail.Base
			if detail.BaseKey != "" {
				backup.Base = nameOf(detail.BaseKey)
			}
		}
		backups = append(backups, backup)
	}
	api.SortStoredBackups(backups)
	return backups
}

func (o *orchestrator) reportData(ctx context.Context, hosts []string, name string) {
	o.bestEffort = true
	defer func() { o.bestEffort = false }()
	key := o.plan().Key
	sets, unlisted := o.listAll(ctx, hosts, func(host, backup string) (string, bool) {
		return key, backup == o.backupNames[host]
	}, nil)
	if len(unlisted) > 0 {
		o.logf("whether %s holds data is unknown: %s cannot be listed", name, strings.Join(unlisted, " "))
		return
	}
	var keys []string
	if sets.All[key] {
		keys = append(keys, key)
	}
	o.outcome.stored = storedBackups(sets, keys, func(string) string { return name })
	if !sets.Complete[key] || sets.WithData[key] {
		return
	}
	o.warnf("%s holds no data", name)
	o.eventf(core.EventTypeWarning, "OrchestratorNoData", "%s holds no data", name)
	o.outcome.noData = noDataCause(o.spec)
}

func noDataCause(spec *model.RunSpec) string {
	switch {
	case len(spec.Tables) > 0:
		return fmt.Sprintf("no table matches %q, or none of those that do holds data", strings.Join(spec.Tables, ","))
	case len(spec.Partitions) > 0:
		return fmt.Sprintf("no table holds data in the partitions %s", strings.Join(spec.Partitions, ","))
	}
	return "the cluster has no table that holds data"
}

// writing returns all keys if it cannot read the commands of a host.
func (o *orchestrator) writing(ctx context.Context, sets model.BackupSets, made string) map[string]bool {
	var incomplete []string
	for _, key := range util.MapGetSortedKeys(sets.All) {
		if !sets.Complete[key] && (sets.Sized[key] || sets.Broken[key]) && (key != made) {
			incomplete = append(incomplete, key)
		}
	}
	writing := map[string]bool{}
	if len(incomplete) == 0 {
		return writing
	}
	spec := o.spec
	for _, host := range spec.RetentionHosts {
		var rows []actionRow
		err := o.retry(ctx, host, "read the commands in progress", func() (err error) {
			rows, err = o.sc.Running(ctx, host)
			return err
		})
		if err != nil {
			o.warnf("%s: retention leaves every incomplete backup, as system.backup_actions cannot tell whether a create_remote "+
				"or an upload still writes one: %v", host, err)
			for _, key := range incomplete {
				writing[key] = true
			}
			return writing
		}
		for _, key := range incomplete {
			name, err := spec.Naming.HostName(host, key, o.plan().NamingPrefix)
			if err != nil {
				continue
			}
			if i := slices.IndexFunc(rows, func(row actionRow) bool { return row.running() && writes(row.Command, name) }); i >= 0 {
				o.logf("%s: %s is incomplete, and a command in progress writes it: %s", host, name, rows[i].Command)
				writing[key] = true
			}
		}
	}
	return writing
}

func writes(command, name string) bool {
	verb, _, _ := strings.Cut(strings.TrimSpace(command), " ")
	return ((verb == strings.Fields(commandCreateRemote)[0]) || (verb == commandUpload)) && namesBackup(command, name)
}

func (o *orchestrator) retention(ctx context.Context) {
	spec := o.spec
	switch {
	case len(spec.Entries) == 0:
		return
	case spec.RestoreInProgress:
		o.warnf("retention skipped: a restore of the cluster is in progress")
		return
	case spec.Prefix == "":
		o.warnf("retention skipped: the schedule has no backup name prefix")
		return
	case len(spec.RetentionHosts) == 0:
		o.warnf("retention skipped: no host")
		return
	}
	o.logf("retention: keeping the newest %s-* backup, the newest backups of the scheduled times of every schedule entry it keeps, "+
		"and the backups made at once no older than the oldest of those", spec.Prefix)
	o.bestEffort = true
	defer func() { o.bestEffort = false }()
	naming := spec.Naming
	sets, unlisted := o.listAll(ctx, spec.RetentionHosts, func(host, name string) (string, bool) {
		return naming.SeriesKey(host, name, spec.Prefix)
	}, func(key string) string {
		t, _ := naming.KeyTime(key, spec.Prefix)
		return t
	})
	if len(unlisted) > 0 {
		o.warnf("retention skipped: %s cannot be listed, backups are left as they are", strings.Join(unlisted, " "))
		o.eventf(core.EventTypeWarning, "OrchestratorRetentionSkipped", "%s", o.lastWarning)
		return
	}
	retention := model.Retention{
		Naming:   naming,
		Prefix:   spec.Prefix,
		Entries:  spec.Entries,
		Made:     o.plan().Key,
		Pinned:   model.PinnedKeys(naming, spec.Prefix, spec.KeepBackups),
		Previous: spec.Previous,
		Now:      o.clock.Now(),
	}
	retention.Writing = o.writing(ctx, sets, retention.Made)
	plan := retention.Plan(util.MapGetSortedKeys(sets.All), sets)
	nameOf := func(key string) string {
		if naming.IsTemplate() {
			if rendered, err := naming.FirstName(key, o.plan().NamingPrefix); err == nil {
				return rendered
			}
		}
		return key
	}
	if len(plan.Held) > 0 {
		var names []string
		var held []api.HeldBackup
		for _, b := range plan.Held {
			name := nameOf(b.Key)
			expires := ""
			if !b.Expires.IsZero() {
				expires = b.Expires.UTC().Format(time.RFC3339)
				names = append(names, name+" (until "+expires+")")
			} else {
				names = append(names, name)
			}
			held = append(held, api.HeldBackup{Name: name, Expires: expires})
		}
		o.outcome.held = held
		o.warnf("retention holds %d backup(s) the schedule kept before a change of it, which it would delete now, each until it "+
			"expires as the schedule before would have deleted it: %s; keep those to keep longer in spec.retention.keepBackups, "+
			"or release the change at once with spec.retention.releaseHeld", len(names), heldReport(names))
		o.eventf(core.EventTypeWarning, "OrchestratorRetentionHeld", "%s", o.lastWarning)
	}
	if len(plan.Expired) > 0 {
		var names []string
		for _, key := range plan.Expired {
			names = append(names, nameOf(key))
		}
		o.logf("retention deletes %d backup(s) whose hold for a change of the schedule or pin of spec.retention.keepBackups "+
			"has expired: %s", len(names), heldReport(names))
		o.eventf(core.EventTypeNormal, "OrchestratorRetentionHeldExpired", "the hold for a change of the schedule or the pin of "+
			"spec.retention.keepBackups of %d backup(s) has expired, retention deletes them: %s", len(names), heldReport(names))
	}
	if len(plan.Writing) > 0 {
		var names []string
		for _, key := range plan.Writing {
			names = append(names, nameOf(key))
		}
		o.logf("retention leaves %d incomplete backup(s) a create_remote or an upload may still write: %s", len(names),
			heldReport(names))
		o.eventf(core.EventTypeNormal, "OrchestratorRetentionIncompleteKept", "retention leaves %d incomplete backup(s) a "+
			"create_remote or an upload still writes, or of a host whose system.backup_actions cannot be read: %s", len(names),
			heldReport(names))
	}
	obsolete := plan.Obsolete
	// A backup stays in the list unless retention deleted it on every host that lists it
	undeleted := map[string]bool{}
	for _, host := range spec.RetentionHosts {
		rows, err := o.list(ctx, host, "remote")
		if err != nil {
			o.warnf("%s: retention failed, backups are left as they are", host)
			for _, key := range obsolete {
				undeleted[key] = true
			}
			continue
		}
		for _, key := range obsolete {
			name, err := naming.HostName(host, key, o.plan().NamingPrefix)
			if (err != nil) || (findBackup(rows, name) == nil) {
				continue
			}
			if err := o.deleteBackup(ctx, stepRetention, host, "remote", name); err != nil {
				undeleted[key] = true
				o.eventf(core.EventTypeWarning, "OrchestratorRetentionFailed", "%s: retention failed to delete %s: %v", host, name, err)
				continue
			}
			o.logf("%s: retention deleted %s", host, name)
			o.eventf(core.EventTypeNormal, "OrchestratorRetentionDeleted", "%s: retention deleted %s", host, name)
		}
	}
	var kept []string
	for _, key := range util.MapGetSortedKeys(sets.All) {
		if !slices.Contains(obsolete, key) || undeleted[key] {
			kept = append(kept, key)
		}
	}
	o.outcome.series = storedBackups(sets, kept, nameOf)
}
