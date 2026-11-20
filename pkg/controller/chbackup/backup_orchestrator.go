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

// The command framework of the orchestrator, as the Altinity usual backup workflow drives clickhouse-backup: a command
// is inserted into system.backup_actions of a host, and its row is the first row of the command that started at the
// time of the ClickHouse of the host just before, or later; the row is polled until it finishes.
const (
	// pollInterval is how often a row of a command is read
	pollInterval = 5 * time.Second
	// busyBackoffMax caps the wait between two tries of a command a busy sidecar refused, which doubles from the poll
	// interval on
	busyBackoffMax = 5 * time.Minute
	// errorResponsesMax is how many times in a row a command may have no row before it counts as lost, unless the
	// run's timeouts set errorResponsesMax
	errorResponsesMax = 5
	// existenceAttempts is how many times the row of a command just inserted is looked for
	existenceAttempts = 3
	// actionTimeout bounds a command, unless the run's timeouts set actionTimeout
	actionTimeout = 12 * time.Hour
	// unreachableTimeout bounds how long a host may not answer before the run fails, unless the run's timeouts set
	// unreachableTimeout
	unreachableTimeout = 10 * time.Minute
	// httpReceiveTimeout is the http_receive_timeout of an insert into system.backup_actions, unless the run's
	// timeouts set httpReceiveTimeout: a synchronous delete answers once it is done
	httpReceiveTimeout = 4 * time.Hour
	// resumeAdoptWindow is how long before the recorded start of a run a row of a command the run did not record is
	// still taken as the run's own, after a restart that lost the record: the clocks of the operator and of
	// ClickHouse differ
	resumeAdoptWindow = 5 * time.Minute
	// clockSkew is how far the clocks of the operator and of ClickHouse may differ for a row that finished before the
	// recorded start of a run to be still taken as the run's own
	clockSkew = 30 * time.Second
	// heldReportMax bounds the held backups an Event and the status tell
	heldReportMax = 24
	// clickHouseTimeLayout is the layout of a time of ClickHouse as toString formats it
	clickHouseTimeLayout = "2006-01-02 15:04:05"
)

// Commands of clickhouse-backup.
const (
	commandCreateRemote = "create_remote --rbac"
	commandDeleteLocal  = "delete local"
	commandDeleteRemote = "delete remote"
	commandDownload     = "download"
	commandUpload       = "upload"
	commandRestore      = "restore"
	// defaultSkipTables are the tables clickhouse-backup leaves out by default, which a restore that leaves out more
	// keeps leaving out
	defaultSkipTables = "system.*,INFORMATION_SCHEMA.*,information_schema.*,_temporary_and_external_tables.*"
)

// Steps of a run. stepCopy copies the data of the backup into the detached directories of the replicas of a shard but
// the one that restores its data, before that one does.
const (
	stepCreate     = "create"
	stepReport     = "report"
	stepDownload   = "download"
	stepSchema     = "schema"
	stepCopy       = "copyToDetached"
	stepData       = "data"
	stepCleanup    = "cleanup"
	stepRetention  = "retention"
	stepDropFailed = "dropFailed"
	stepSource     = "source"
	// stepReplaceLocal and stepReplaceRemote delete the copies of a backup an earlier run of it left, before spec.taskID
	// reran it
	stepReplaceLocal  = "replaceLocal"
	stepReplaceRemote = "replaceRemote"
)

// Keys of the plan of a run.
const (
	planBackupName   = "backupName"
	planKey          = "key"
	planNamingPrefix = "namingPrefix"
	planSourceEnv    = "sourceEnv"
	planActionEnv    = "actionEnv"
	planHadLocal     = "hadLocal"
	planResumed      = "resumed"
	planHoldsRBAC    = "holdsRBAC"
	planListed       = "listed"
	planHostsUsed    = "hostsUsed"
	planSourceShown  = "sourceShown"
	// planReplaced are the hosts whose local copy of the backup an earlier run of the restore downloaded the rerun
	// deleted, without spec.resumeDownload, to download the backup again
	planReplaced = "replaced"
	// planCopyToDetached tells a restore whose replicas copy the data to detached, planDataHosts and planCopyHosts are
	// the hosts that restore the data of their shard and those that copy it, as the restore chose them as it came to
	// copy it, and planSchemaCopies the hosts whose local copy of the backup an earlier run downloaded the schema of
	// alone, which they download again whole
	planCopyToDetached = "copyToDetached"
	planDataHosts      = "dataHosts"
	planCopyHosts      = "copyHosts"
	planSchemaCopies   = "schemaCopies"
)

// runOutcome is how a run went.
type runOutcome struct {
	failed bool
	// unchanged tells a run that failed before any command that changes something may have reached a sidecar
	unchanged bool
	// deadline tells a run stopped at its deadline
	deadline bool
	message  string
	// backupName is the backup the run made, with a template the backup of the first shard
	backupName string
	// noData tells why the backup the run made holds no data, "" when it holds data
	noData string
	// hostsUsed are the hosts the backup was made on when they are not those the run was to make it on
	hostsUsed []string
	// held are the backups retention held for a change of the schedule that the schedule as it is would delete, with
	// when each hold ends
	held []api.HeldBackup
	// rbacNotRestored tells a restore that left the access entities of the backup unrestored
	rbacNotRestored bool
}

// runFailure fails a run with its message.
type runFailure struct {
	message string
}

func (f *runFailure) Error() string {
	return f.message
}

func failf(format string, args ...any) error {
	return &runFailure{message: fmt.Sprintf(format, args...)}
}

// orchestrator runs one operation on one cluster: it drives the clickhouse-backup sidecars of the hosts of the cluster
// through SQL, and records what it needs to resume in the status of the run.
type orchestrator struct {
	spec *model.RunSpec
	sc   sidecars
	// mu guards status, which the controller reads while the run goes on
	mu     *sync.Mutex
	status *api.BackupRunStatus
	// event posts an Event on the resource the run works for
	event func(eventType, reason, message string)
	// changed tells the controller the status of the run changed
	changed func()
	// logPrefix names the run in log lines
	logPrefix string
	clock     clock.WithTicker
	poll      time.Duration
	// startedAt is when the run started, which a row of a command the run did not record must not be older than
	startedAt time.Time
	// resumed tells a run resumed from the status of an earlier one, which may have inserted commands it did not record
	resumed bool
	// elapsed is how long the run ran before it started or resumed here, which its deadline counts
	elapsed time.Duration

	lastWarning string
	bestEffort  bool
	outcome     runOutcome
	// restore
	sourceEnv     string
	sourceShown   string
	sourceOwnPath bool
	sourceHint    string
	sourceCopies  []string
	noLocalCopy   map[string]bool
	ownBackups    map[string]map[string]bool
	pathResult    model.PathCheckResult
	backupNames   map[string]string
	// resumable are the hosts of a restore from another installation whose local copy of the backup an earlier run of
	// the restore downloaded, whose download the rerun resumes
	resumable map[string]bool
}

func (o *orchestrator) logf(format string, args ...any) {
	log.V(1).Info("%s: %s", o.logPrefix, fmt.Sprintf(format, args...))
}

// warnf logs a warning, which a failure that follows tells as its cause.
func (o *orchestrator) warnf(format string, args ...any) {
	o.lastWarning = fmt.Sprintf(format, args...)
	log.V(1).Warning("%s: %s", o.logPrefix, o.lastWarning)
}

func (o *orchestrator) eventf(eventType, reason, format string, args ...any) {
	if o.event != nil {
		o.event(eventType, reason, fmt.Sprintf(format, args...))
	}
}

// withWarning appends the last warning to a message.
func (o *orchestrator) withWarning(message string) string {
	if o.lastWarning != "" {
		return message + ": " + o.lastWarning
	}
	return message
}

// update changes the status of the run under its lock and tells the controller.
func (o *orchestrator) update(change func(status *api.BackupRunStatus)) {
	o.mu.Lock()
	change(o.status)
	o.mu.Unlock()
	if o.changed != nil {
		o.changed()
	}
}

func (o *orchestrator) plan(key string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.status.Plan[key]
}

func (o *orchestrator) setPlan(values map[string]string) {
	o.update(func(status *api.BackupRunStatus) {
		if status.Plan == nil {
			status.Plan = map[string]string{}
		}
		for key, value := range values {
			if value == "" {
				delete(status.Plan, key)
				continue
			}
			status.Plan[key] = value
		}
	})
}

func (o *orchestrator) step() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.status.Step
}

func (o *orchestrator) setStep(step string) {
	o.update(func(status *api.BackupRunStatus) { status.Step = step })
}

// sleep waits for the poll interval, false when the run is to stop.
func (o *orchestrator) sleep(ctx context.Context) bool {
	return o.sleepFor(ctx, o.poll)
}

// sleepFor waits for the duration, false when the run is to stop. The run waits in a goroutine of its own, never in a
// worker of a controller: the controllers requeue the resource and read the run when it changes.
func (o *orchestrator) sleepFor(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-o.clock.After(d):
		return true
	}
}

// actionTimeout returns how long the run waits for one command on one host.
func (o *orchestrator) actionTimeout() time.Duration {
	return cmp.Or(o.spec.Timeouts.GetActionTimeout(), actionTimeout)
}

// unreachableTimeout returns how long a host that does not answer is retried before it fails.
func (o *orchestrator) unreachableTimeout() time.Duration {
	return cmp.Or(o.spec.Timeouts.GetUnreachableTimeout(), unreachableTimeout)
}

// httpReceiveTimeout returns the http_receive_timeout of an insert into system.backup_actions.
func (o *orchestrator) httpReceiveTimeout() time.Duration {
	return cmp.Or(o.spec.Timeouts.GetHTTPReceiveTimeout(), httpReceiveTimeout)
}

// errorResponsesMax returns how many polls in a row may find no row of a command before it counts as lost.
func (o *orchestrator) errorResponsesMax() int {
	return cmp.Or(o.spec.Timeouts.GetErrorResponsesMax(), errorResponsesMax)
}

// busyBackoff returns how long to wait before the try that follows the given number of tries a busy sidecar refused:
// the poll interval, doubled with every try, at most busyBackoffMax.
func busyBackoff(poll time.Duration, tries int) time.Duration {
	d := poll
	for i := 1; (i < tries) && (d < busyBackoffMax); i++ {
		d *= 2
	}
	return min(d, max(busyBackoffMax, poll))
}

// retry runs f until it succeeds, or fails for longer than the unreachable timeout: a host that does not answer is
// waited for, as its pod may restart.
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

// recorded returns the recorded action of the command of the step on the host, nil for none.
func (o *orchestrator) recorded(step, host, command string) *api.BackupRunAction {
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

// record records an action of the run, replacing the one of the same step and host.
func (o *orchestrator) record(action api.BackupRunAction) {
	o.update(func(status *api.BackupRunStatus) {
		status.Actions = slices.DeleteFunc(status.Actions, func(recorded api.BackupRunAction) bool {
			return (recorded.Step == action.Step) && (recorded.Host == action.Host)
		})
		status.Actions = append(status.Actions, action)
	})
}

// adoptable tells whether the row of a command the run did not record may be the run's own: one still running, or
// that finished after the run started. An identical command of an earlier operation, which finished before, is not.
func (o *orchestrator) adoptable(row *actionRow) bool {
	return row.running() || (row.Finish >= o.startedAt.Add(-clockSkew).UTC().Format(clickHouseTimeLayout))
}

// adoptFrom returns the time a row of a command the run did not record is the run's own from, in the time of the
// ClickHouse of the host: the run may have inserted it before a restart of the operator lost the record.
func (o *orchestrator) adoptFrom() string {
	return o.startedAt.Add(-resumeAdoptWindow).UTC().Format(clickHouseTimeLayout)
}

// submit inserts the command into system.backup_actions of the host once the sidecar runs no other one, and returns
// the action, or the one recorded or found of a run that resumes. mutates tells a command that changes something.
func (o *orchestrator) submit(ctx context.Context, step, host, command string, mutates bool) (*api.BackupRunAction, error) {
	o.lastWarning = ""
	// An action recorded without its time was planned, never inserted
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
		// The command the run inserted before a restart lost its record changed something as much as one it inserts now
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
		// The row tells whether the command reached the sidecar, whatever the insert answered. A synchronous command
		// whose insert failed, as one cut by a timeout, may still run or have finished on the sidecar: its row is
		// looked for as long as that of a command already submitted, and settles it as wait does.
		attempts := existenceAttempts
		if (err != nil) && synchronous(command) {
			attempts = max(o.errorResponsesMax(), existenceAttempts)
			o.logf("%s: the insert of %s failed (%s), looking for its row in system.backup_actions", host, command,
				describeQueryError(err))
		}
		// A read without the row counts as an attempt, a read that fails as one only for a run at its best effort: it
		// is read again until the unreachable timeout from the first error in a row
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

// synchronous reports whether the sidecar answers the insert of the command only once the command is done: a delete.
func synchronous(command string) bool {
	return strings.HasPrefix(command, commandDeleteLocal+" ") || strings.HasPrefix(command, commandDeleteRemote+" ")
}

// lost settles a command whose row is gone, as after a restart of the sidecar: an upload or a download the sidecar
// resumes is waited for, a backup found complete counts as made.
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

// wait polls the actions until none runs, and returns why each one that failed failed, by host.
func (o *orchestrator) wait(ctx context.Context, actions []*api.BackupRunAction, names map[string]string) map[string]error {
	failures := map[string]error{}
	// empty counts the polls in a row that found no row of the command of a host; failing is when the polls of a
	// host started to fail in a row, failure the last error they told
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
				// A host whose ClickHouse does not answer, or whose sidecar answers the URL engine with an error or
				// with what is no JSON, is polled again until the unreachable timeout from the first error in a row:
				// the poll tells nothing of the command
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

// runAll runs a command on every host at once, the command of each host as command tells it, and returns the hosts it
// failed on and why. A host whose command cannot be submitted stops the submission of the rest.
func (o *orchestrator) runAll(ctx context.Context, step string, hosts []string, mutates bool,
	command func(host string) (string, error)) ([]string, []string) {
	var actions []*api.BackupRunAction
	var failed, failures []string
	for _, host := range hosts {
		cmd, err := command(host)
		if err == nil {
			var action *api.BackupRunAction
			if action, err = o.submit(ctx, step, host, cmd, mutates); err == nil {
				actions = append(actions, action)
				continue
			}
		}
		failed, failures = append(failed, host), append(failures, err.Error())
		o.eventf(core.EventTypeWarning, "OrchestratorHostFailed", "%s: %s failed: %v", host, step, err)
		break
	}
	return o.waitAll(ctx, step, hosts, actions, failed, failures)
}

// waitAll waits for the actions of the step submitted on the hosts, and returns the hosts it failed on and why, after
// the failed and failures given.
func (o *orchestrator) waitAll(ctx context.Context, step string, hosts []string, actions []*api.BackupRunAction,
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

// runOne runs a command on a host and waits for it.
func (o *orchestrator) runOne(ctx context.Context, step, host, command string, mutates bool) error {
	failed, failures := o.runAll(ctx, step, []string{host}, mutates, func(string) (string, error) { return command, nil })
	if len(failed) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

// list returns the backups the sidecar of the host lists at the location.
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

// usable reports whether the host lists the backup at the location, not broken.
func (o *orchestrator) usable(ctx context.Context, host, location, name string) (bool, error) {
	rows, err := o.list(ctx, host, location)
	if err != nil {
		return false, err
	}
	row := findBackup(rows, name)
	return (row != nil) && !row.broken(), nil
}

// deleteBackup deletes the backup of the host at the location.
func (o *orchestrator) deleteBackup(ctx context.Context, step, host, location, name string) error {
	command := commandDeleteLocal + " " + name
	if location == "remote" {
		command = commandDeleteRemote + " " + name
	}
	return o.runOne(ctx, step, host, command, location == "remote")
}

// cleanup deletes the local copy of the backup on the host after a run that failed, at once and bounded, so that the
// run always ends, even under a context that is never cancelled: it tries once and never waits for a busy sidecar, the
// insert is bounded by the HTTP receive timeout, and its row is read errorResponsesMax times at most. It returns why the
// copy is left, nil once it is deleted.
func (o *orchestrator) cleanup(ctx context.Context, step, host, name string) error {
	return o.cleanupCommand(ctx, step, host, commandDeleteLocal+" "+name)
}

// cleanupCommand runs a delete on the host as cleanup does: once, bounded, never waiting for a busy sidecar. It returns
// why the command did not succeed, nil once it did.
func (o *orchestrator) cleanupCommand(ctx context.Context, step, host, command string) error {
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

// deleteByHand tells how to delete the local copy of a backup on its host by hand.
func deleteByHand(name string) string {
	return fmt.Sprintf("INSERT INTO system.backup_actions(command) VALUES ('%s %s')", commandDeleteLocal, name)
}

// run runs the operation and returns how it went.
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
	case operationDelete:
		err = o.deleteUnfinished(ctx)
	default:
		err = failf("unknown operation %q", o.spec.Operation)
	}
	if err != nil {
		if o.sourceEnv != "" && len(o.sourceCopies) > 0 {
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

// shardOf returns the shard of the host, the host itself when the spec does not tell: every host its own shard.
func (o *orchestrator) shardOf(host string) string {
	if o.spec.Shard != nil {
		if shard := o.spec.Shard(host); shard != "" {
			return shard
		}
	}
	return host
}

// succeeded reports whether the command of the step succeeded on the host, as the run recorded it.
func (o *orchestrator) succeeded(step, host string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, action := range o.status.Actions {
		if (action.Step == step) && (action.Host == host) && (action.Status == actionSuccess) {
			return true
		}
	}
	return false
}

func operationTitle(operation string) string {
	switch operation {
	case api.BackupRunOperationBackup:
		return "Backup"
	case operationDelete:
		return "Deletion"
	}
	return "Restore"
}

// fallBackHosts returns the hosts the backup runs on: a preferred replica that does not answer falls back to the host
// of its shard the fallback hosts name.
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

// ready checks that every host can be driven.
func (o *orchestrator) ready(ctx context.Context, hosts []string) error {
	for _, host := range hosts {
		if err := o.retry(ctx, host, "drive the sidecar", func() error { return o.sc.Ready(ctx, host) }); err != nil {
			return failf("preflight failed: %v", err)
		}
	}
	return nil
}

// checkPaths runs the storage path check, which refuses a storage path the cluster shares before anything changes.
func (o *orchestrator) checkPaths() error {
	if o.spec.PathCheck == nil {
		return nil
	}
	result, err := o.spec.PathCheck.Check()
	o.pathResult = result
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

// configOverrides returns the --env flags of the config overrides.
func configOverrides(overrides []string) string {
	var flags strings.Builder
	for _, item := range overrides {
		flags.WriteString(" " + api.BackupConfigOverrideFlag + item)
	}
	return flags.String()
}

// splitHosts returns the hosts of a space-separated list.
func splitHosts(value string) []string {
	return strings.Fields(value)
}

// unique returns the items in order, each once, empty ones left out.
func unique(items ...[]string) []string {
	var out []string
	for _, list := range items {
		for _, item := range list {
			if (item != "") && !slices.Contains(out, item) {
				out = append(out, item)
			}
		}
	}
	return out
}

// heldReport returns the names of held backups an Event and the status tell.
func heldReport(names []string) string {
	shown := names
	more := ""
	if len(names) > heldReportMax {
		shown, more = names[:heldReportMax], fmt.Sprintf(" +%d", len(names)-heldReportMax)
	}
	return strings.Join(shown, " ") + more
}

// parseClickHouseTime parses a time of the ClickHouse of a host, as clickHouseTimeLayout tells it.
func parseClickHouseTime(value string) (time.Time, bool) {
	t, err := time.Parse(clickHouseTimeLayout, value)
	return t, err == nil
}
