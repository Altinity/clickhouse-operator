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
	"regexp"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

var sourcePathRegexp = regexp.MustCompile(`^[A-Za-z0-9_./{}-]+$`)

type restoreHosts struct {
	schema, data, all []string
}

func (o *orchestrator) restoreHosts() restoreHosts {
	spec := o.spec
	hosts := restoreHosts{schema: spec.SchemaHosts, data: spec.DataHosts}
	if len(spec.Partitions) > 0 {
		hosts.schema = nil
	}
	if spec.SchemaOnly {
		hosts.data = nil
	}
	hosts.all = util.NonEmpty(util.SliceDistinct(slices.Concat(spec.SchemaHosts, spec.DataHosts)))
	return hosts
}

func (o *orchestrator) restore(ctx context.Context) error {
	spec := o.spec
	hosts := o.restoreHosts()
	key, prefix, err := spec.Naming.Key("backup name", spec.BackupName, "")
	if err != nil {
		return failf("%v", err)
	}
	for _, host := range hosts.all {
		if o.backupNames[host], err = spec.Naming.HostName(host, key, prefix); err != nil {
			return failf("%v", err)
		}
	}
	if o.step() == "" {
		if err := o.planRestore(ctx, hosts); err != nil {
			if o.source.env != "" {
				o.dropSourceCopies(context.WithoutCancel(ctx), false)
			}
			return err
		}
	}
	plan := o.plan()
	o.source.env, o.source.path = plan.SourceEnv, plan.SourceShown
	name := spec.BackupName
	hadLocal := plan.HadLocal

	if o.step() == stepDownload {
		var downloads []string
		for _, host := range hosts.all {
			if !slices.Contains(hadLocal, host) {
				downloads = append(downloads, host)
			}
		}
		if err := o.dropSchemaCopies(ctx, downloads); err != nil {
			return err
		}
		if len(downloads) > 0 {
			failed, failures := o.runAll(ctx, stepDownload, downloads, false, o.commandOf(stepDownload))
			if len(failed) > 0 {
				o.keepDownloads()
				return failf("download of %s failed on %s: %s", name, strings.Join(failed, ", "), strings.Join(failures, "; "))
			}
		}
		o.setStep(stepSchema)
	}
	if o.step() == stepSchema {
		for _, host := range hosts.schema {
			if err := o.runOne(ctx, stepSchema, host, o.restoreCommand(stepSchema, host), true); err != nil {
				o.keepDownloads()
				return failf("schema restore of %s failed on %s: %v", name, host, err)
			}
		}
		o.setStep(stepCopy)
	}
	if o.step() == stepCopy {
		if o.copiesToDetached() && (len(hosts.data) > 0) {
			if len(o.plan().DataHosts) == 0 {
				o.chooseDataHosts(ctx)
			}
			if copies := o.plan().CopyHosts; len(copies) > 0 {
				failed, failures := o.runAll(ctx, stepCopy, copies, true, o.commandOf(stepCopy))
				if len(failed) > 0 {
					o.keepDownloads()
					return failf("copy of the data of %s to detached failed on %s: %s", name, strings.Join(failed, ", "),
						strings.Join(failures, "; "))
				}
			}
		}
		o.setStep(stepData)
	}
	if o.step() == stepData {
		if len(hosts.data) > 0 {
			failed, failures := o.runData(ctx, hosts.data)
			if len(failed) > 0 {
				o.keepDownloads()
				return failf("data restore of %s failed on %s: %s", name, strings.Join(failed, ", "), strings.Join(failures, "; "))
			}
		}
		o.setStep(stepCleanup)
	}
	o.bestEffort = true
	for _, host := range hosts.all {
		backup := o.backupNames[host]
		if present, err := o.listed(ctx, host, "local", backup); (err == nil) && present {
			if err := o.deleteBackup(ctx, stepCleanup, host, "local", backup); err != nil {
				o.warnf("%s: the downloaded copy of %s is left on local disk", host, backup)
			}
		}
	}
	o.bestEffort = false
	o.logf("restore %s completed", name)
	o.eventf(core.EventTypeNormal, "OrchestratorRestoreCompleted", "restore %s completed", name)
	o.reportRBAC()
	return nil
}

func (o *orchestrator) copiesToDetached() bool {
	return o.plan().CopyToDetached
}

func (o *orchestrator) restoreShards() [][]string {
	spec := o.spec
	var shards [][]string
	for _, host := range spec.SchemaHosts {
		switch {
		case slices.Contains(spec.DataHosts, host):
			shards = append(shards, []string{host})
		case len(shards) > 0:
			shards[len(shards)-1] = append(shards[len(shards)-1], host)
		}
	}
	for _, host := range spec.DataHosts {
		if !slices.Contains(spec.SchemaHosts, host) {
			shards = append(shards, []string{host})
		}
	}
	return shards
}

func (o *orchestrator) downloadsData(host string, copies bool) bool {
	if o.spec.SchemaOnly {
		return false
	}
	if !copies {
		return slices.Contains(o.spec.DataHosts, host)
	}
	return slices.ContainsFunc(o.restoreShards(), func(shard []string) bool { return slices.Contains(shard, host) })
}

func (o *orchestrator) downloadedSchema(host, name string) bool {
	actions := slices.Clone(o.spec.PreviousActions)
	o.mu.Lock()
	actions = append(actions, o.status.Actions...)
	o.mu.Unlock()
	found := false
	for _, action := range actions {
		fields := strings.Fields(action.Command)
		if (action.Step != stepDownload) || (action.Host != host) || (len(fields) < 2) || (fields[0] != commandDownload) ||
			(fields[len(fields)-1] != name) {
			continue
		}
		if !slices.Contains(fields, "--schema") {
			return false
		}
		found = true
	}
	return found
}

// The download would treat a schema-only copy as complete, so it is deleted first.
func (o *orchestrator) dropSchemaCopies(ctx context.Context, downloads []string) error {
	for _, host := range o.plan().SchemaCopies {
		name := o.backupNames[host]
		if !slices.Contains(downloads, host) || (o.recorded(stepDownload, host, o.restoreCommand(stepDownload, host)) != nil) {
			continue
		}
		present, err := o.listed(ctx, host, "local", name)
		if err != nil {
			return failf("cannot list the local backups of %s: %v", host, err)
		}
		if !present {
			continue
		}
		o.logf("%s: holds a local copy of the schema of %s alone, which it deletes to download the data too", host, name)
		if err := o.deleteBackup(ctx, stepReplaceLocal, host, "local", name); err != nil {
			return failf("%s: cannot delete the local copy of the schema of %s an earlier run downloaded, to download the data "+
				"too: %v", host, name, err)
		}
	}
	return nil
}

// A first replica that does not answer gets the data by replication later.
func (o *orchestrator) chooseDataHosts(ctx context.Context) {
	var data, copies []string
	for _, shard := range o.restoreShards() {
		chosen := 0
		for ; chosen < len(shard)-1; chosen++ {
			err := o.sc.Ready(ctx, shard[chosen])
			if err == nil {
				break
			}
			o.logf("%s: does not answer (%v), restoring the data of its shard on %s instead", shard[chosen], err, shard[chosen+1])
		}
		if chosen > 0 {
			o.eventf(core.EventTypeNormal, "OrchestratorReplicaFallback", "%s does not answer, the data of its shard is restored "+
				"on %s, and %s takes it by replication once it answers", strings.Join(shard[:chosen], ", "), shard[chosen],
				strings.Join(shard[:chosen], ", "))
		}
		data = append(data, shard[chosen])
		copies = append(copies, shard[chosen+1:]...)
	}
	o.setPlan(func(plan *api.BackupRunPlan) { plan.DataHosts, plan.CopyHosts = data, copies })
}

func (o *orchestrator) commandOf(step api.BackupRunStep) func(host string) string {
	return func(host string) string { return o.restoreCommand(step, host) }
}

func (o *orchestrator) runData(ctx context.Context, hosts []string) ([]string, []string) {
	if !o.copiesToDetached() || (len(o.plan().DataHosts) == 0) {
		return o.runAll(ctx, stepData, hosts, true, o.commandOf(stepData))
	}
	data := slices.Clone(o.plan().DataHosts)
	var actions []*api.BackupRunAction
	var failed, failures []string
submit:
	for i := range data {
		for {
			host := data[i]
			action, err := o.submit(ctx, stepData, host, o.restoreCommand(stepData, host), true)
			if err == nil {
				actions = append(actions, action)
				break
			}
			next := o.dataFallback(host)
			if (next == "") || (ctx.Err() != nil) {
				failed, failures = append(failed, host), append(failures, err.Error())
				o.eventf(core.EventTypeWarning, "OrchestratorHostFailed", "%s: %s failed: %v", host, stepData, err)
				break submit
			}
			o.logf("%s: the restore of the data cannot be submitted (%v), restoring the data of its shard on %s instead", host, err, next)
			o.eventf(core.EventTypeNormal, "OrchestratorReplicaFallback", "%s does not take the restore of the data, the data of its "+
				"shard is restored on %s, over its copy of it to detached", host, next)
			data[i] = next
			dataHosts := slices.Clone(data)
			o.setPlan(func(plan *api.BackupRunPlan) { plan.DataHosts = dataHosts })
		}
	}
	return o.waitAll(ctx, stepData, data, actions, failed, failures)
}

func (o *orchestrator) dataFallback(host string) string {
	copies := o.plan().CopyHosts
	for _, shard := range o.restoreShards() {
		at := slices.Index(shard, host)
		if at < 0 {
			continue
		}
		for _, next := range shard[at+1:] {
			if slices.Contains(copies, next) && o.succeeded(stepCopy, next) {
				return next
			}
		}
	}
	return ""
}

func (o *orchestrator) reportRBAC() {
	switch {
	case o.spec.RestoreRBAC:
	case !o.plan().Listed:
		o.logf("whether %s holds access entities is unknown: its copies in the storage were not listed", o.spec.BackupName)
	case o.plan().HoldsRBAC:
		o.warnf("%s holds access entities - users, roles, grants - which this restore did not restore", o.spec.BackupName)
		o.eventf(core.EventTypeWarning, "OrchestratorRBACNotRestored", "%s", o.lastWarning)
		o.outcome.rbacNotRestored = true
	}
}

func (o *orchestrator) restoreCommand(step api.BackupRunStep, host string) string {
	spec := o.spec
	var flags []string
	switch step {
	case stepDownload:
		flags = []string{commandDownload, "--hardlink-exists-files"}
		if !o.downloadsData(host, o.copiesToDetached()) {
			flags = []string{commandDownload, "--schema"}
		}
	case stepSchema:
		flags = []string{commandRestore, "--schema=1"}
		// --rm drops the whole backup databases, so use it only when all tables are restored.
		if spec.Overwrite && (len(spec.Tables) == 0) {
			flags = append(flags, "--rm")
		}
		flags = append(flags, "--ignore-dependencies")
		if spec.RestoreRBAC {
			flags = append(flags, "--rbac")
		}
	case stepData:
		flags = []string{commandRestore, "--data=1", "--ignore-dependencies"}
	case stepCopy:
		flags = []string{commandRestore, "--data=1", "--ignore-dependencies", "--replicated-copy-to-detached"}
	}
	if len(spec.Tables) > 0 {
		flags = append(flags, "--tables="+strings.Join(spec.Tables, ","))
	}
	if len(spec.Partitions) > 0 {
		flags = append(flags, "--partitions="+strings.Join(spec.Partitions, ","))
	}
	if env := o.plan().ActionEnv; env != "" {
		flags = append(flags, env)
	}
	if (spec.DatabaseMapping != "") && (step != stepDownload) {
		flags = append(flags, "--restore-database-mapping="+spec.DatabaseMapping)
	}
	if (step == stepSchema) && (len(spec.SchemaHosts) > 0) && (host == spec.SchemaHosts[0]) {
		flags = append(flags, "--env=RESTORE_SCHEMA_ON_CLUSTER={cluster}")
	}
	return strings.Join(flags, " ") + " " + o.backupNames[host]
}

func (o *orchestrator) restoreEnv() string {
	spec := o.spec
	var flags []string
	// The operator settings come last, so they win
	if overrides := configOverrides(spec.ConfigOverrides); overrides != "" {
		flags = append(flags, strings.TrimSpace(overrides))
	}
	if o.source.env != "" {
		flags = append(flags, o.source.env)
	}
	flags = append(flags, skipEnv(spec.SkipTables, spec.SkipTableEngines)...)
	if len(spec.Partitions) > 0 {
		flags = append(flags, "--env=CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0")
	}
	return strings.Join(flags, " ")
}

func (o *orchestrator) planRestore(ctx context.Context, hosts restoreHosts) error {
	spec := o.spec
	name := spec.BackupName
	if err := o.checkPaths(); err != nil {
		return err
	}
	if err := o.sourcePaths(); err != nil {
		return err
	}
	if err := o.ready(ctx, hosts.all); err != nil {
		return err
	}
	o.eventf(core.EventTypeNormal, "OrchestratorRestoreStarted", "restore %s on %d host(s)", name, len(hosts.all))
	o.logf("restore %s: preflight on %d host(s)", name, len(hosts.all))
	var replaced []string
	if !spec.ResumeDownload {
		var err error
		if replaced, err = o.replaceEarlierDownloads(ctx, hosts.all); err != nil {
			return err
		}
	}
	if o.source.env != "" {
		for _, host := range hosts.all {
			if err := o.refuseLocalCopy(ctx, host); err != nil {
				return err
			}
			if !o.source.ownStoragePath {
				if err := o.refuseOwnBackup(ctx, host); err != nil {
					return err
				}
			}
		}
	}
	holdsRBAC := false
	if spec.ValidateTopology {
		hint := o.otherInstallationHint()
		for _, host := range hosts.all {
			backup := o.backupNames[host]
			var row *backupRow
			where := "the storage of " + host
			if o.source.env != "" {
				where = fmt.Sprintf("the storage path '%s' of %s", o.source.path, host)
				// A schema copy would overwrite the local copy and be deleted with it.
				resumed := o.source.resumesDownload[host]
				if !resumed {
					if err := o.sourceCopy(ctx, host); err != nil {
						return err
					}
				}
				rows, err := o.list(ctx, host, "local")
				if err != nil {
					return failf("preflight failed on %s: %v", host, err)
				}
				if row = findBackup(rows, backup); row == nil {
					return failf("preflight failed: backup %s is missing in %s: its download left no local copy", backup, where)
				}
				if resumed && row.broken() {
					o.logf("%s: the incomplete local copy of %s an earlier run of this restore downloaded, after its preflight found "+
						"the backup in %s, is checked by its download, which resumes it", host, backup, where)
					continue
				}
				if (row.Required != "") && o.source.ownRemote[host][row.Required] {
					return failf("preflight failed: backup %s is incremental from %s, and %s holds a backup %s at its own storage path "+
						"too, which clickhouse-backup would take for the one at the storage path '%s', as it tells the backups of every "+
						"path apart by name alone: delete that one first", backup, row.Required, host, row.Required, o.source.path)
				}
			} else {
				rows, err := o.list(ctx, host, "remote")
				if err != nil {
					return failf("preflight failed on %s: %v", host, err)
				}
				if row = findBackup(rows, backup); row == nil {
					if strings.Contains(o.storagePaths.Path, "{replica}") {
						return failf("preflight failed: backup %s is missing in the storage of %s: the storage path '%s' holds {replica}, "+
							"which gives every replica a path of its own, and the backup was made on the first replica of each shard only%s",
							backup, host, o.storagePaths.Path, hint)
					}
					return failf("preflight failed: backup %s is missing in the storage of %s%s", backup, host, hint)
				}
			}
			if row.broken() {
				return failf("preflight failed: backup %s is %s in %s", backup, row.Desc, where)
			}
			if row.RBACSize != 0 {
				holdsRBAC = true
			}
		}
	}
	if len(spec.Partitions) > 0 {
		o.logf("restore %s: restores the partitions %s into the tables as they are, without the schema, as the Altinity usual "+
			"backup workflow does", name, strings.Join(spec.Partitions, ","))
	}
	copies := spec.ReplicatedCopyToDetached
	var hadLocal, resumed, schemaCopies []string
	for _, host := range hosts.all {
		if o.downloadsData(host, copies) && o.downloadedSchema(host, o.backupNames[host]) {
			schemaCopies = append(schemaCopies, host)
		}
	}
	if o.source.env == "" {
		for _, host := range hosts.all {
			if slices.Contains(schemaCopies, host) {
				continue
			}
			rows, err := o.list(ctx, host, "local")
			if err != nil {
				continue
			}
			row := findBackup(rows, o.backupNames[host])
			switch {
			case row == nil:
			case row.broken():
				o.logf("%s: holds an incomplete local copy of %s, whose download the restore resumes", host, o.backupNames[host])
				resumed = append(resumed, host)
			default:
				hadLocal = append(hadLocal, host)
			}
		}
	} else {
		if err := o.dropSourceCopies(ctx, true); err != nil {
			return err
		}
		for _, host := range hosts.all {
			if o.source.resumesDownload[host] && !slices.Contains(schemaCopies, host) {
				resumed = append(resumed, host)
			}
		}
	}
	if flags := configOverrides(o.spec.ConfigOverrides); flags != "" {
		o.logf("restore of %s: made with the config overrides%s", o.spec.BackupName, flags)
		o.eventf(core.EventTypeNormal, "OrchestratorConfigOverrides", "restore of %s is made with the config overrides%s", o.spec.BackupName, flags)
	}
	plan := api.BackupRunPlan{
		ActionEnv:      o.restoreEnv(),
		SourceEnv:      o.source.env,
		SourceShown:    o.source.path,
		HadLocal:       hadLocal,
		Resumed:        resumed,
		Replaced:       replaced,
		SchemaCopies:   schemaCopies,
		CopyToDetached: copies,
		HoldsRBAC:      holdsRBAC,
		Listed:         spec.ValidateTopology,
	}
	o.setPlan(func(p *api.BackupRunPlan) { *p = plan })
	hostsOf := slices.Clone(hosts.all)
	o.update(func(status *api.BackupRunStatus) {
		status.Hosts = nil
		for _, host := range hostsOf {
			status.Hosts = append(status.Hosts, api.BackupRunHost{Host: host, Backup: o.backupNames[host]})
		}
		status.Step = stepDownload
	})
	return nil
}

func (o *orchestrator) otherInstallationHint() string {
	var macros, what, fields []string
	if strings.Contains(o.storagePaths.Path, "{installation}") {
		macros, what, fields = append(macros, "{installation}"), append(what, "name"), append(fields, "sourceInstallation")
	}
	if strings.Contains(o.storagePaths.Path, "{namespace}") {
		macros, what, fields = append(macros, "{namespace}"), append(what, "namespace"), append(fields, "sourceNamespace")
	}
	if len(macros) == 0 {
		return ""
	}
	return fmt.Sprintf("; the storage path '%s' holds %s, which keeps apart the backups of installations of another %s: a backup "+
		"made by one of them is found by naming that installation in the restore (%s)", o.storagePaths.Path, strings.Join(macros, " and "),
		strings.Join(what, " or "), strings.Join(fields, ", "))
}

func sourceTemplate(template, installation, namespace string) string {
	return regexp.MustCompile(`\{([^{}]*)\}`).ReplaceAllStringFunc(template, func(macro string) string {
		switch macro {
		case "{installation}":
			if installation != "" {
				return installation
			}
		case "{namespace}":
			if namespace != "" {
				return namespace
			}
		}
		return macro
	})
}

func (o *orchestrator) sourcePaths() error {
	spec := o.spec
	check := spec.PathCheck
	ownInstallation, ownNamespace := "", ""
	if check != nil {
		ownInstallation, ownNamespace = check.Installation, check.Namespace
	}
	installation, namespace := spec.SourceInstallation, spec.SourceNamespace
	if installation == ownInstallation {
		installation = ""
	}
	if namespace == ownNamespace {
		namespace = ""
	}
	name := spec.BackupName
	if (installation == "") && (namespace == "") {
		if (spec.SourceInstallation != "") || (spec.SourceNamespace != "") {
			o.logf("WARNING: restore %s names its own installation as the one it restores the backup of, which it reads at its own "+
				"storage paths", name)
		}
		return nil
	}
	self := o.storagePaths
	if self.StoragePrefix == "" {
		return failf("a restore from another installation needs the storage paths of the sidecars, which it reads the backup at with "+
			"the {installation} and {namespace} of that installation, and the storage path check cannot tell them: %s", self.Unknown)
	}
	sourcePath := sourceTemplate(self.Path, installation, namespace)
	sourceObject := sourceTemplate(self.ObjectDiskPath, installation, namespace)
	if (sourcePath == self.Path) && (sourceObject == self.ObjectDiskPath) {
		o.logf("WARNING: the storage paths of cluster %s hold no {installation} or {namespace} that tells the installation restore %s "+
			"names apart from this one, so it reads them as they are: '%s'", self.ClusterName, name, self.Path)
		return nil
	}
	if sourcePath == self.Path {
		o.source.ownStoragePath = true
	} else if (sourceObject != "") && (sourceObject == self.ObjectDiskPath) {
		return failf("the object disk path '%s' of cluster %s holds no {installation} or {namespace} that tells the installation "+
			"restore %s names apart from this one, while the storage path does: deleting the copy the restore downloads would delete "+
			"the object disk data of the backup; give the object disk path the macros of the storage path", self.ObjectDiskPath,
			self.ClusterName, name)
	}
	for _, value := range []string{sourcePath, sourceObject} {
		if (value != "") && !sourcePathRegexp.MatchString(value) {
			return failf("the storage path '%s' of the installation restore %s reads the backup of cannot be set in a command of the "+
				"restore: only a path of letters, digits and _ . / { } - can", value, name)
		}
	}
	o.source.env = fmt.Sprintf("--env=%s_PATH=%s", self.StoragePrefix, sourcePath)
	if sourceObject != "" {
		o.source.env += fmt.Sprintf(" --env=%s_OBJECT_DISK_PATH=%s", self.StoragePrefix, sourceObject)
	}
	if (installation != "") && strings.Contains(sourcePath, "{replica}") {
		o.source.missingHint = "; the storage path holds {replica}, the host name, which holds the name of the installation, so the hosts " +
			"of installations of different names have different paths"
	}
	o.source.path = sourcePath
	if expanded, unknown := model.ExpandMacros(sourcePath, check.Installation, check.Namespace, self.ClusterName, "{shard}"); unknown == "" {
		o.source.path = expanded
	}
	o.logf("restore %s: reading the backup at the storage path '%s' of the installation it names", name, o.source.path)
	return nil
}

// clickhouse-backup would restore a local copy instead of the other installation's backup.
func (o *orchestrator) refuseLocalCopy(ctx context.Context, host string) error {
	if o.source.localChecked[host] {
		return nil
	}
	name := o.backupNames[host]
	present, err := o.listed(ctx, host, "local", name)
	if err != nil {
		return failf("preflight failed on %s: %v", host, err)
	}
	if present && o.downloaded(host, name) {
		o.logf("%s: holds the local copy of %s this restore downloaded, whose download it resumes", host, name)
		if o.downloadedEarlier(host, name, false) {
			if o.source.resumesDownload == nil {
				o.source.resumesDownload = map[string]bool{}
			}
			o.source.resumesDownload[host] = true
		}
	} else if present {
		return failf("preflight failed: %s holds a local copy of %s, which a restore from the storage paths of another installation "+
			"would restore in place of the backup there; delete it (clickhouse-backup delete local %s) and restore again", host, name, name)
	}
	if o.source.localChecked == nil {
		o.source.localChecked = map[string]bool{}
	}
	o.source.localChecked[host] = true
	return nil
}

func (o *orchestrator) downloaded(host, name string) bool {
	download := func(action api.BackupRunAction) bool {
		fields := strings.Fields(action.Command)
		return (action.Host == host) && (len(fields) > 1) && (fields[0] == commandDownload) && (fields[len(fields)-1] == name)
	}
	if slices.ContainsFunc(o.spec.PreviousActions, download) {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.ContainsFunc(o.status.Actions, download)
}

func (o *orchestrator) downloadedEarlier(host, name string, preflight bool) bool {
	return slices.ContainsFunc(o.spec.PreviousActions, func(action api.BackupRunAction) bool {
		fields := strings.Fields(action.Command)
		return ((action.Step == stepDownload) || (preflight && (action.Step == stepSource))) && (action.Host == host) &&
			(action.TimeFrom != "") && (len(fields) > 1) && (fields[0] == commandDownload) && (fields[len(fields)-1] == name)
	})
}

func (o *orchestrator) replaceEarlierDownloads(ctx context.Context, hosts []string) ([]string, error) {
	var replaced []string
	for _, host := range hosts {
		name := o.backupNames[host]
		if !o.downloadedEarlier(host, name, true) {
			continue
		}
		present, err := o.listed(ctx, host, "local", name)
		if err != nil {
			return nil, failf("preflight failed on %s: %v", host, err)
		}
		if !present {
			continue
		}
		o.logf("%s: deletes the local copy of %s an earlier run of the restore downloaded, to download the backup again", host, name)
		if err := o.deleteBackup(ctx, stepReplaceLocal, host, "local", name); err != nil {
			return nil, failf("%s: cannot delete the local copy of %s an earlier run of the restore downloaded, to download the backup "+
				"again: %v", host, name, err)
		}
		o.eventf(core.EventTypeNormal, "OrchestratorRerunReplaced", "%s: the local copy of %s an earlier run of the restore "+
			"downloaded deleted, the download starts over", host, name)
		replaced = append(replaced, host)
	}
	return replaced, nil
}

func (o *orchestrator) downloadNote(step api.BackupRunStep, host string) string {
	switch {
	case step != stepDownload:
	case slices.Contains(o.plan().Resumed, host):
		return ", resuming the local copy the host held"
	case slices.Contains(o.plan().Replaced, host):
		return ", downloaded again in place of the local copy an earlier run of the restore downloaded"
	}
	return ""
}

func (o *orchestrator) refuseOwnBackup(ctx context.Context, host string) error {
	rows, err := o.list(ctx, host, "remote")
	if err != nil {
		return failf("preflight failed on %s: %v", host, err)
	}
	if o.source.ownRemote == nil {
		o.source.ownRemote = map[string]map[string]bool{}
	}
	o.source.ownRemote[host] = map[string]bool{}
	for _, row := range rows {
		o.source.ownRemote[host][row.Name] = true
	}
	name := o.backupNames[host]
	if o.source.ownRemote[host][name] {
		return failf("preflight failed: %s holds a backup %s at its own storage path too, which clickhouse-backup would take for the "+
			"one at the storage path '%s', as it tells the backups of every path apart by name alone: restore a backup of another name, "+
			"or delete that one first", host, name, o.source.path)
	}
	return nil
}

func (o *orchestrator) sourceCopy(ctx context.Context, host string) error {
	if slices.Contains(o.source.schemaCopies, host) {
		return nil
	}
	if err := o.refuseLocalCopy(ctx, host); err != nil {
		return err
	}
	o.source.schemaCopies = append(o.source.schemaCopies, host)
	name := o.backupNames[host]
	if err := o.runOne(ctx, stepSource, host, "download --schema --rbac "+o.source.env+" "+name, false); err != nil {
		return failf("preflight failed: backup %s is missing in the storage path '%s' of %s, or cannot be read there: %v%s", name,
			o.source.path, host, err, o.source.missingHint)
	}
	return nil
}

// The restore would treat a schema copy as its own, so strict deletes them before it.
func (o *orchestrator) dropSourceCopies(ctx context.Context, strict bool) error {
	hosts := slices.Clone(o.source.schemaCopies)
	if !strict {
		o.source.schemaCopies = nil
		o.bestEffort = true
		defer func() { o.bestEffort = false }()
	}
	for _, host := range hosts {
		name := o.backupNames[host]
		if !strict {
			if present, err := o.listed(ctx, host, "local", name); (err == nil) && !present {
				continue
			}
		}
		if strict {
			if err := o.deleteBackup(ctx, stepSource+"-drop", host, "local", name); err != nil {
				return failf("%s: cannot delete the copy of %s the preflight downloaded, which the restore would take for its own: %v",
					host, name, err)
			}
		} else if err := o.cleanup(ctx, stepSource+"-drop", host, name); err != nil {
			o.logf("WARNING: %s: the copy of the schema of %s the preflight downloaded is left on local disk (%v), which refuses another "+
				"restore of %s from another installation until it is deleted with %s", host, name, err, name, deleteByHand(name))
			o.eventf(core.EventTypeWarning, "OrchestratorSourceCopyLeft", "%s: the copy of the schema of %s the preflight downloaded is "+
				"left: %v; delete it by hand with %s", host, name, err, deleteByHand(name))
		}
		if strict {
			o.source.schemaCopies = o.source.schemaCopies[1:]
		}
	}
	return nil
}

func (o *orchestrator) keepDownloads() {
	var kept []string
	o.mu.Lock()
	for _, action := range o.status.Actions {
		if (action.Step == stepDownload) && (action.TimeFrom != "") && !slices.Contains(kept, action.Host) {
			kept = append(kept, action.Host)
		}
	}
	o.mu.Unlock()
	if len(kept) == 0 {
		return
	}
	var copies []string
	for _, host := range kept {
		copies = append(copies, host+"="+o.backupNames[host])
	}
	o.logf("the restore failed and keeps the local copies it downloaded: %s", strings.Join(copies, " "))
	o.eventf(core.EventTypeWarning, "OrchestratorRestoreDownloadsKept", "the failed restore keeps the local copies of the backup it "+
		"downloaded, to look into and for a restore of the backup again to resume their download, on %s; delete each by hand on "+
		"its host with %s", strings.Join(copies, " "), deleteByHand("<backup>"))
}
