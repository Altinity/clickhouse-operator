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
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// A command's row is its first row that started at or after the ClickHouse time read just before the insert.
const (
	pollInterval       = 5 * time.Second
	busyBackoffMax     = 5 * time.Minute
	errorResponsesMax  = 5
	existenceAttempts  = 3
	actionTimeout      = 12 * time.Hour
	unreachableTimeout = 10 * time.Minute
	// Long, because a synchronous delete answers only when it is done.
	httpReceiveTimeout = 4 * time.Hour
	// A restart can lose a command's record. A row of an unrecorded command that started
	// up to this long before the run start still belongs to the run.
	resumeAdoptWindow    = 5 * time.Minute
	clockSkew            = 30 * time.Second
	heldReportMax        = 24
	clickHouseTimeLayout = "2006-01-02 15:04:05"
)

const (
	commandCreateRemote = "create_remote --rbac"
	commandDeleteLocal  = "delete local"
	commandDeleteRemote = "delete remote"
	commandDownload     = "download"
	commandUpload       = "upload"
	commandRestore      = "restore"
	defaultSkipTables   = "system.*,INFORMATION_SCHEMA.*,information_schema.*,_temporary_and_external_tables.*"
)

const (
	stepCreate        api.BackupRunStep = "create"
	stepReport        api.BackupRunStep = "report"
	stepDownload      api.BackupRunStep = "download"
	stepSchema        api.BackupRunStep = "schema"
	stepCopy          api.BackupRunStep = "copyToDetached"
	stepData          api.BackupRunStep = "data"
	stepCleanup       api.BackupRunStep = "cleanup"
	stepRetention     api.BackupRunStep = "retention"
	stepDropFailed    api.BackupRunStep = "dropFailed"
	stepSource        api.BackupRunStep = "source"
	stepReplaceLocal  api.BackupRunStep = "replaceLocal"
	stepReplaceRemote api.BackupRunStep = "replaceRemote"
)

type runOutcome struct {
	failed bool
	// unchanged marks a run that failed before any changing command could reach a sidecar.
	unchanged       bool
	deadline        bool
	message         string
	backupName      string
	noData          string
	hostsUsed       []string
	held            []api.HeldBackup
	rbacNotRestored bool
	// stored is the backup the run made, as listed after it; nil when not listed.
	stored []api.StoredBackup
	// series are the backups of the schedule that retention kept, as listed by it; nil when retention did not list.
	series []api.StoredBackup
}

type runFailure struct {
	message string
}

func (f *runFailure) Error() string {
	return f.message
}

func failf(format string, args ...any) error {
	return &runFailure{message: fmt.Sprintf(format, args...)}
}

type orchestrator struct {
	spec *model.RunSpec
	sc   sidecars
	// mu guards status, which the controller reads while the run goes on.
	mu        *sync.Mutex
	status    *api.BackupRunStatus
	event     func(eventType, reason, message string)
	changed   func()
	logPrefix string
	clock     clock.WithTicker
	poll      time.Duration
	startedAt time.Time
	// resumed is true when the run goes on after an operator restart.
	resumed bool
	// elapsed is the run time before that restart. It counts against the deadline.
	elapsed time.Duration

	outcome runOutcome
	// lastWarning is the last message of warnf, for the event that follows it.
	lastWarning string
	// bestEffort makes a failed command a warning, and the run goes on.
	bestEffort bool
	// backupNames maps a host to the name of its backup.
	backupNames map[string]string
	// storagePaths is the result of the storage path check.
	storagePaths model.PathCheckResult
	// source is set when a restore reads the backup of another installation.
	source restoreSource
}

// restoreSource is the storage path of another installation that a restore reads the backup from.
type restoreSource struct {
	// env holds the --env flags that point clickhouse-backup to that storage path.
	env string
	// path is that storage path, for messages.
	path string
	// ownStoragePath is true when only the object disk path differs from the path of this installation.
	ownStoragePath bool
	// missingHint is added to the error when the backup is missing.
	missingHint string
	// schemaCopies are the hosts where the preflight downloaded the schema. They are deleted before the restore.
	schemaCopies []string
	// localChecked are the hosts checked to hold no local copy that clickhouse-backup would restore instead.
	localChecked map[string]bool
	// resumesDownload are the hosts that hold a local copy an earlier run of this restore downloaded.
	resumesDownload map[string]bool
	// ownRemote maps a host to the backups at its own storage path. A backup of the same name hides the source one.
	ownRemote map[string]map[string]bool
}

func (o *orchestrator) logf(format string, args ...any) {
	log.V(1).Info("%s: %s", o.logPrefix, fmt.Sprintf(format, args...))
}

func (o *orchestrator) warnf(format string, args ...any) {
	o.lastWarning = fmt.Sprintf(format, args...)
	log.V(1).Warning("%s: %s", o.logPrefix, o.lastWarning)
}

func (o *orchestrator) eventf(eventType, reason, format string, args ...any) {
	if o.event != nil {
		o.event(eventType, reason, fmt.Sprintf(format, args...))
	}
}

func (o *orchestrator) update(change func(status *api.BackupRunStatus)) {
	o.mu.Lock()
	change(o.status)
	o.mu.Unlock()
	if o.changed != nil {
		o.changed()
	}
}

// plan returns a copy of the plan. Its slices are shared, so callers must not change them.
func (o *orchestrator) plan() api.BackupRunPlan {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.status.Plan == nil {
		return api.BackupRunPlan{}
	}
	return *o.status.Plan
}

func (o *orchestrator) setPlan(change func(plan *api.BackupRunPlan)) {
	o.update(func(status *api.BackupRunStatus) {
		if status.Plan == nil {
			status.Plan = &api.BackupRunPlan{}
		}
		change(status.Plan)
	})
}

func (o *orchestrator) step() api.BackupRunStep {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.status.Step
}

func (o *orchestrator) setStep(step api.BackupRunStep) {
	o.update(func(status *api.BackupRunStatus) { status.Step = step })
}

func (o *orchestrator) sleep(ctx context.Context) bool {
	return o.sleepFor(ctx, o.poll)
}

func (o *orchestrator) sleepFor(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-o.clock.After(d):
		return true
	}
}

func (o *orchestrator) actionTimeout() time.Duration {
	return cmp.Or(o.spec.Timeouts.GetActionTimeout(), actionTimeout)
}

func (o *orchestrator) unreachableTimeout() time.Duration {
	return cmp.Or(o.spec.Timeouts.GetUnreachableTimeout(), unreachableTimeout)
}

func (o *orchestrator) httpReceiveTimeout() time.Duration {
	return cmp.Or(o.spec.Timeouts.GetHTTPReceiveTimeout(), httpReceiveTimeout)
}

func (o *orchestrator) errorResponsesMax() int {
	return cmp.Or(o.spec.Timeouts.GetErrorResponsesMax(), errorResponsesMax)
}

func busyBackoff(poll time.Duration, tries int) time.Duration {
	d := poll
	for i := 1; (i < tries) && (d < busyBackoffMax); i++ {
		d *= 2
	}
	return min(d, max(busyBackoffMax, poll))
}

// retry waits for a silent host, because its pod can restart.
func (o *orchestrator) retry(ctx context.Context, host, what string, f func() error) error {
	since := o.clock.Now()
	announced := false
	for {
		err := f()
		if (err == nil) || errors.Is(err, errSidecarBusy) {
			return err
		}
		if (ctx.Err() != nil) || o.bestEffort || (o.clock.Since(since) >= o.unreachableTimeout()) {
			return fmt.Errorf("%s: cannot %s: %w", host, what, err)
		}
		if !announced {
			announced = true
			o.logf("%s: cannot %s, retrying for %s: %s", host, what, o.unreachableTimeout(), describeQueryError(err))
		}
		if !o.sleep(ctx) {
			return ctx.Err()
		}
	}
}

func (o *orchestrator) recorded(step api.BackupRunStep, host, command string) *api.BackupRunAction {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := range o.status.Actions {
		action := &o.status.Actions[i]
		if (action.Step == step) && (action.Host == host) && (action.Command == command) {
			copied := *action
			return &copied
		}
	}
	return nil
}

func (o *orchestrator) record(action api.BackupRunAction) {
	o.update(func(status *api.BackupRunStatus) {
		status.Actions = slices.DeleteFunc(status.Actions, func(recorded api.BackupRunAction) bool {
			return (recorded.Step == action.Step) && (recorded.Host == action.Host)
		})
		status.Actions = append(status.Actions, action)
	})
}

func (o *orchestrator) adoptable(row *actionRow) bool {
	return row.running() || (row.Finish >= o.startedAt.Add(-clockSkew).UTC().Format(clickHouseTimeLayout))
}

func (o *orchestrator) adoptFrom() string {
	return o.startedAt.Add(-resumeAdoptWindow).UTC().Format(clickHouseTimeLayout)
}

func (o *orchestrator) submit(ctx context.Context, step api.BackupRunStep, host, command string, mutates bool) (*api.BackupRunAction, error) {
	o.lastWarning = ""
	// An action recorded without a time was planned but never inserted.
	if action := o.recorded(step, host, command); (action != nil) && (action.TimeFrom != "") {
		return action, nil
	}
	var row *actionRow
	if o.resumed {
		if err := o.retry(ctx, host, "read the actions of the sidecar", func() (err error) {
			row, err = o.sc.Action(ctx, host, command, o.adoptFrom())
			return err
		}); err != nil {
			return nil, err
		}
	}
	if (row != nil) && o.adoptable(row) {
		action := api.BackupRunAction{Step: step, Host: host, Command: command, TimeFrom: row.Start, Status: row.Status, Error: row.Error}
		o.update(func(status *api.BackupRunStatus) {
			status.Mutated = status.Mutated || mutates
		})
		o.record(action)
		o.logf("%s: %s already started at %s", host, command, row.Start)
		return &action, nil
	}
	announced := false
	tries := 0
	for {
		var busy bool
		if err := o.retry(ctx, host, "read the actions of the sidecar", func() (err error) {
			busy, err = o.sc.Busy(ctx, host)
			return err
		}); err != nil {
			return nil, err
		}
		if busy {
			tries++
			if !announced {
				announced = true
				o.logf("%s: another operation is in progress, waiting", host)
			}
			if !o.sleepFor(ctx, busyBackoff(o.poll, tries)) {
				return nil, ctx.Err()
			}
			continue
		}
		var timeFrom string
		if err := o.retry(ctx, host, "read the time of ClickHouse", func() (err error) {
			timeFrom, err = o.sc.Now(ctx, host)
			return err
		}); err != nil {
			return nil, err
		}
		action := api.BackupRunAction{Step: step, Host: host, Command: command, TimeFrom: timeFrom}
		o.update(func(status *api.BackupRunStatus) {
			status.Mutated = status.Mutated || mutates
		})
		o.record(action)
		o.logf("%s: %s", host, command)
		o.eventf(core.EventTypeNormal, "OrchestratorCommand", "%s: %s", host, command)
		err := o.sc.Push(ctx, host, command, o.httpReceiveTimeout())
		if errors.Is(err, errSidecarBusy) {
			tries++
			o.logf("%s: sidecar busy, queueing %s", host, command)
			if !o.sleepFor(ctx, busyBackoff(o.poll, tries)) {
				return nil, ctx.Err()
			}
			continue
		}
		// A synchronous command whose insert failed, for example on a timeout, can still run on the sidecar.
		attempts := existenceAttempts
		if (err != nil) && synchronous(command) {
			attempts = max(o.errorResponsesMax(), existenceAttempts)
			o.logf("%s: the insert of %s failed (%s), looking for its row in system.backup_actions", host, command,
				describeQueryError(err))
		}
		var failingSince time.Time
		for attempt := 0; attempt < attempts; {
			found, findErr := o.sc.Action(ctx, host, command, timeFrom)
			switch {
			case ctx.Err() != nil:
				return nil, ctx.Err()
			case findErr != nil:
				if o.bestEffort {
					attempt++
					break
				}
				if failingSince.IsZero() {
					failingSince = o.clock.Now()
					o.logf("%s: cannot read the row of %s, retrying for %s: %s", host, command, o.unreachableTimeout(),
						describeQueryError(findErr))
				}
				if o.clock.Since(failingSince) >= o.unreachableTimeout() {
					o.warnf("%s: %s: cannot read its row in system.backup_actions for %s: %s", host, command,
						o.unreachableTimeout(), describeQueryError(findErr))
					return nil, errors.New(o.lastWarning)
				}
			case found != nil:
				if err != nil {
					o.logf("%s: %s reached the sidecar despite the failed insert, its status: %s", host, command, found.Status)
				}
				return &action, nil
			default:
				failingSince = time.Time{}
				attempt++
			}
			if !o.sleep(ctx) {
				return nil, ctx.Err()
			}
		}
		if err != nil {
			o.warnf("%s: %s failed: %v", host, command, err)
		} else {
			o.warnf("%s: %s: no response from the server", host, command)
		}
		return nil, errors.New(o.lastWarning)
	}
}

func synchronous(command string) bool {
	return strings.HasPrefix(command, commandDeleteLocal+" ") || strings.HasPrefix(command, commandDeleteRemote+" ")
}

func (o *orchestrator) lost(ctx context.Context, action *api.BackupRunAction, name string) (bool, bool, error) {
	if rows, err := o.sc.Actions(ctx, action.Host, action.TimeFrom); err == nil {
		for _, row := range rows {
			if strings.HasSuffix(row.Command, " "+name) && row.running() {
				return true, false, nil
			}
		}
	}
	where := ""
	switch {
	case strings.HasPrefix(action.Command, "create_remote"):
		where = "remote"
	case strings.HasPrefix(action.Command, commandDownload):
		where = "local"
	default:
		return false, false, fmt.Errorf("%s: the sidecar restarted during %s, which it does not resume: the %s is partially applied",
			action.Host, action.Command, o.spec.Operation)
	}
	if usable, err := o.usable(ctx, action.Host, where, name); (err == nil) && usable {
		o.logf("%s: %s completed before the sidecar restarted", action.Host, action.Command)
		return false, true, nil
	}
	return false, false, fmt.Errorf("%s: the sidecar restarted during %s and resumed nothing, the outcome is unknown", action.Host,
		action.Command)
}

func (o *orchestrator) wait(ctx context.Context, actions []*api.BackupRunAction, names map[string]string) map[string]error {
	failures := map[string]error{}
	empty := map[string]int{}
	failing := map[string]time.Time{}
	failure := map[string]string{}
	pending := slices.Clone(actions)
	since := o.clock.Now()
	for len(pending) > 0 {
		var running []*api.BackupRunAction
		for _, action := range pending {
			if action.Status == actionSuccess {
				continue
			}
			row, err := o.sc.Action(ctx, action.Host, action.Command, action.TimeFrom)
			switch {
			case ctx.Err() != nil:
				failures[action.Host] = ctx.Err()
				continue
			case err != nil:
				// A failed poll says nothing about the command, so poll again until the unreachable timeout.
				description := describeQueryError(err)
				first, failed := failing[action.Host]
				if !failed {
					first = o.clock.Now()
					failing[action.Host] = first
				}
				if description != failure[action.Host] {
					failure[action.Host] = description
					o.logf("%s: cannot read the row of %s, retrying for %s from the first error: %s", action.Host,
						action.Command, o.unreachableTimeout(), description)
				}
				if o.clock.Since(first) >= o.unreachableTimeout() {
					failures[action.Host] = fmt.Errorf("%s: %s: cannot read its row in system.backup_actions for %s: %s",
						action.Host, action.Command, o.unreachableTimeout(), description)
					continue
				}
				running = append(running, action)
				continue
			}
			if _, failed := failing[action.Host]; failed {
				o.logf("%s: system.backup_actions answers again", action.Host)
				delete(failing, action.Host)
				delete(failure, action.Host)
			}
			if row == nil {
				empty[action.Host]++
				o.logf("%s: no row of %s started at %s or later in system.backup_actions, %d of %d polls in a row",
					action.Host, action.Command, action.TimeFrom, empty[action.Host], o.errorResponsesMax())
				if empty[action.Host] <= o.errorResponsesMax() {
					running = append(running, action)
					continue
				}
				resumed, done, lostErr := o.lost(ctx, action, names[action.Host])
				switch {
				case resumed:
					empty[action.Host] = 0
					running = append(running, action)
				case done:
					action.Status = actionSuccess
					o.record(*action)
				default:
					failures[action.Host] = lostErr
				}
				continue
			}
			empty[action.Host] = 0
			if row.running() {
				if action.Status != actionInProgress {
					action.Status = actionInProgress
					o.record(*action)
				}
				running = append(running, action)
				continue
			}
			action.Status, action.Error = row.Status, row.Error
			o.record(*action)
			o.eventf(core.EventTypeNormal, "OrchestratorCommandFinished", "%s: %s ended with status %s", action.Host, action.Command,
				row.Status)
			if row.Status != actionSuccess {
				failures[action.Host] = fmt.Errorf("%s: %s ended with status '%s': %s", action.Host, action.Command, row.Status, row.Error)
			}
		}
		pending = running
		if len(pending) == 0 {
			break
		}
		if o.clock.Since(since) >= o.actionTimeout() {
			for _, action := range pending {
				failures[action.Host] = fmt.Errorf("%s: %s: Action Timeout, not finished in %s", action.Host, action.Command,
					o.actionTimeout())
			}
			break
		}
		if !o.sleep(ctx) {
			for _, action := range pending {
				failures[action.Host] = ctx.Err()
			}
			break
		}
	}
	return failures
}

func (o *orchestrator) runAll(ctx context.Context, step api.BackupRunStep, hosts []string, mutates bool,
	command func(host string) string) ([]string, []string) {
	var actions []*api.BackupRunAction
	var failed, failures []string
	for _, host := range hosts {
		action, err := o.submit(ctx, step, host, command(host), mutates)
		if err == nil {
			actions = append(actions, action)
			continue
		}
		failed, failures = append(failed, host), append(failures, err.Error())
		o.eventf(core.EventTypeWarning, "OrchestratorHostFailed", "%s: %s failed: %v", host, step, err)
		break
	}
	return o.waitAll(ctx, step, hosts, actions, failed, failures)
}

func (o *orchestrator) waitAll(ctx context.Context, step api.BackupRunStep, hosts []string, actions []*api.BackupRunAction,
	failed, failures []string) ([]string, []string) {
	results := o.wait(ctx, actions, o.backupNames)
	for _, host := range hosts {
		if err := results[host]; err != nil {
			failed, failures = append(failed, host), append(failures, err.Error())
			o.eventf(core.EventTypeWarning, "OrchestratorHostFailed", "%s: %s failed: %v", host, step, err)
		} else if slices.ContainsFunc(actions, func(action *api.BackupRunAction) bool { return action.Host == host }) {
			o.eventf(core.EventTypeNormal, "OrchestratorHostStepSucceeded", "%s: %s %s succeeded%s", host, step, o.backupNames[host],
				o.downloadNote(step, host))
		}
	}
	return failed, failures
}

func (o *orchestrator) runOne(ctx context.Context, step api.BackupRunStep, host, command string, mutates bool) error {
	failed, failures := o.runAll(ctx, step, []string{host}, mutates, func(string) string { return command })
	if len(failed) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func (o *orchestrator) list(ctx context.Context, host, location string) ([]backupRow, error) {
	var rows []backupRow
	err := o.retry(ctx, host, "list the "+location+" backups", func() (err error) {
		rows, err = o.sc.List(ctx, host, location)
		return err
	})
	if err != nil {
		o.warnf("%v", err)
	}
	return rows, err
}

func findBackup(rows []backupRow, name string) *backupRow {
	for i := range rows {
		if rows[i].Name == name {
			return &rows[i]
		}
	}
	return nil
}

func (o *orchestrator) usable(ctx context.Context, host, location, name string) (bool, error) {
	rows, err := o.list(ctx, host, location)
	if err != nil {
		return false, err
	}
	row := findBackup(rows, name)
	return (row != nil) && !row.broken(), nil
}

func (o *orchestrator) deleteBackup(ctx context.Context, step api.BackupRunStep, host, location, name string) error {
	command := commandDeleteLocal + " " + name
	if location == "remote" {
		command = commandDeleteRemote + " " + name
	}
	return o.runOne(ctx, step, host, command, location == "remote")
}

// cleanup is bounded, so the run always ends, even if ctx is never cancelled.
func (o *orchestrator) cleanup(ctx context.Context, step api.BackupRunStep, host, name string) error {
	return o.cleanupCommand(ctx, step, host, commandDeleteLocal+" "+name)
}

func (o *orchestrator) cleanupCommand(ctx context.Context, step api.BackupRunStep, host, command string) error {
	busy, err := o.sc.Busy(ctx, host)
	switch {
	case err != nil:
		return fmt.Errorf("cannot read the actions of the sidecar: %s", describeQueryError(err))
	case busy:
		return errors.New("the sidecar runs another command")
	}
	timeFrom, err := o.sc.Now(ctx, host)
	if err != nil {
		return fmt.Errorf("cannot read the time of ClickHouse: %s", describeQueryError(err))
	}
	action := api.BackupRunAction{Step: step, Host: host, Command: command, TimeFrom: timeFrom}
	o.record(action)
	o.logf("%s: %s", host, command)
	o.eventf(core.EventTypeNormal, "OrchestratorCommand", "%s: %s", host, command)
	pushErr := o.sc.Push(ctx, host, command, o.httpReceiveTimeout())
	if errors.Is(pushErr, errSidecarBusy) {
		return errors.New("the sidecar runs another command")
	}
	polls := max(o.errorResponsesMax(), 1)
	for attempt := 0; attempt < polls; attempt++ {
		if (attempt > 0) && !o.sleep(ctx) {
			return ctx.Err()
		}
		row, err := o.sc.Action(ctx, host, command, timeFrom)
		if (err != nil) || (row == nil) || row.running() {
			continue
		}
		action.Status, action.Error = row.Status, row.Error
		o.record(action)
		if row.Status != actionSuccess {
			return fmt.Errorf("%s ended with status '%s': %s", command, row.Status, row.Error)
		}
		return nil
	}
	if pushErr != nil {
		return fmt.Errorf("%s: %s", command, describeQueryError(pushErr))
	}
	return fmt.Errorf("%s: no finished row of it in system.backup_actions after %d polls", command, polls)
}

func deleteByHand(name string) string {
	return fmt.Sprintf("INSERT INTO system.backup_actions(command) VALUES ('%s %s')", commandDeleteLocal, name)
}

func (o *orchestrator) run(ctx context.Context) runOutcome {
	if o.spec.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, max(o.spec.Deadline-o.elapsed, 0))
		defer cancel()
	}
	o.backupNames = map[string]string{}
	var err error
	switch o.spec.Operation {
	case api.BackupRunOperationBackup:
		err = o.backup(ctx)
	case api.BackupRunOperationRestore:
		err = o.restore(ctx)
	case api.BackupRunOperationDelete:
		err = o.deleteUnfinished(ctx)
	default:
		err = failf("unknown operation %q", o.spec.Operation)
	}
	if err != nil {
		if o.source.env != "" && len(o.source.schemaCopies) > 0 {
			o.dropSourceCopies(context.WithoutCancel(ctx), false)
		}
		o.outcome.failed = true
		o.outcome.message = err.Error()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			o.outcome.deadline = true
			o.outcome.message = fmt.Sprintf("the %s ran past its activeDeadlineSeconds, %s", o.spec.Operation, o.spec.Deadline)
		}
		o.mu.Lock()
		o.outcome.unchanged = !o.status.Mutated
		o.mu.Unlock()
		o.eventf(core.EventTypeWarning, "Orchestrator"+operationTitle(o.spec.Operation)+"Failed", "%s", o.outcome.message)
	}
	return o.outcome
}

func (o *orchestrator) shardOf(host string) string {
	if o.spec.Shard != nil {
		if shard := o.spec.Shard(host); shard != "" {
			return shard
		}
	}
	return host
}

func (o *orchestrator) succeeded(step api.BackupRunStep, host string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, action := range o.status.Actions {
		if (action.Step == step) && (action.Host == host) && (action.Status == actionSuccess) {
			return true
		}
	}
	return false
}

func operationTitle(operation api.BackupRunOperation) string {
	switch operation {
	case api.BackupRunOperationBackup:
		return "Backup"
	case api.BackupRunOperationDelete:
		return "Deletion"
	}
	return "Restore"
}

func (o *orchestrator) fallBackHosts(ctx context.Context) []string {
	hosts := slices.Clone(o.spec.BackupHosts)
	if len(o.spec.FallbackHosts) != len(hosts) {
		return hosts
	}
	for i, host := range hosts {
		fallback := o.spec.FallbackHosts[i]
		if host == fallback {
			continue
		}
		if err := o.sc.Ready(ctx, host); err != nil {
			o.logf("%s: the preferred replica does not answer (%v), backing up %s instead", host, err, fallback)
			o.eventf(core.EventTypeNormal, "OrchestratorReplicaFallback", "%s does not answer, the backup of its shard runs on %s",
				host, fallback)
			hosts[i] = fallback
		}
	}
	return hosts
}

func (o *orchestrator) ready(ctx context.Context, hosts []string) error {
	for _, host := range hosts {
		if err := o.retry(ctx, host, "drive the sidecar", func() error { return o.sc.Ready(ctx, host) }); err != nil {
			return failf("preflight failed: %v", err)
		}
	}
	return nil
}

func (o *orchestrator) checkPaths() error {
	if o.spec.PathCheck == nil {
		return nil
	}
	result, err := o.spec.PathCheck.Check()
	o.storagePaths = result
	for _, advice := range result.Advice {
		o.logf("WARNING: %s", advice)
		o.eventf(core.EventTypeWarning, "OrchestratorStoragePathAdvice", "%s", advice)
	}
	if err != nil {
		return failf("%v", err)
	}
	if result.Log != "" {
		o.logf("%s", result.Log)
	}
	return nil
}

func configOverrides(overrides []string) string {
	var flags strings.Builder
	for _, item := range overrides {
		flags.WriteString(" " + api.BackupConfigOverrideFlag + item)
	}
	return flags.String()
}

// skipEnv adds the default system tables, because a custom list would replace them.
func skipEnv(skipTables, skipEngines []string) []string {
	var flags []string
	if len(skipTables) > 0 {
		flags = append(flags, "--env=CLICKHOUSE_SKIP_TABLES="+defaultSkipTables+","+strings.Join(skipTables, ","))
	}
	if len(skipEngines) > 0 {
		flags = append(flags, "--env=CLICKHOUSE_SKIP_TABLE_ENGINES="+strings.Join(skipEngines, ","))
	}
	return flags
}

func heldReport(names []string) string {
	shown := names
	more := ""
	if len(names) > heldReportMax {
		shown, more = names[:heldReportMax], fmt.Sprintf(" +%d", len(names)-heldReportMax)
	}
	return strings.Join(shown, " ") + more
}
