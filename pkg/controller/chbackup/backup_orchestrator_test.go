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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// fakeHost is a host the fake sidecars drive: its rows of system.backup_actions, and its backups.
type fakeHost struct {
	actions []actionRow
	remote  []backupRow
	local   []backupRow
	// notReady is why the host cannot be driven, nil when it can
	notReady error
	// busy refuses that many commands as the sidecar runs another operation
	busy int
	// actionErrors fails that many reads of the rows of a command, as a host that does not answer, with actionError
	actionErrors int
	actionError  error
	// emptyReads finds no row of a command that many times, as a sidecar that lost it
	emptyReads int
	// hang keeps a command whose text holds it in progress, the sidecar busy, as an upload that outlives the run
	hang string
	// runningError fails every read of the commands in progress
	runningError error
}

// runs reports whether the sidecar of the host runs a command.
func (host *fakeHost) runs() bool {
	return slices.ContainsFunc(host.actions, func(row actionRow) bool { return row.running() })
}

// fakeSidecars runs the commands of the tests on fake hosts at once, as clickhouse-backup would: create_remote makes a
// remote backup, download a local one, delete deletes one, restore changes nothing the tests read.
type fakeSidecars struct {
	mu    sync.Mutex
	hosts map[string]*fakeHost
	// seconds is the clock of ClickHouse, which every query moves on
	seconds int
	// pushed are the commands inserted, "host: command"
	pushed []string
	// fail fails a command whose text holds the key on the host, "host" or "" for every host, with the error
	fail map[string]string
	// any makes a host of every name asked
	any bool
	// pushErr is what the insert of a command whose text holds the key answers once the command ran, as a timeout
	pushErr map[string]error
	// receiveTimeouts are the HTTP receive timeouts of the inserts, in order
	receiveTimeouts []time.Duration
	// refuse refuses, as a busy sidecar does, every command whose text holds one of them
	refuse []string
	// leave makes a create_remote or a download that fails leave its backup, as a failed upload leaves the local
	// backup and a broken remote one, and a failed download a broken local copy
	leave bool
	// slow keeps a command whose text holds the key, "host|text", in progress for that many reads of its row
	slow map[string]int
	// lose fails the insert of a command whose text holds the key, "host|text", with the error, the command never
	// reaching the sidecar
	lose map[string]error
	// remaining are the reads of its row a slow command stays in progress for, by "host|command"
	remaining map[string]int
}

func newFakeSidecars(hosts ...string) *fakeSidecars {
	f := &fakeSidecars{hosts: map[string]*fakeHost{}, fail: map[string]string{}, pushErr: map[string]error{}}
	for _, host := range hosts {
		f.hosts[host] = &fakeHost{}
	}
	return f
}

func (f *fakeSidecars) now() string {
	f.seconds++
	return time.Date(2026, time.November, 12, 0, 0, 0, 0, time.UTC).Add(time.Duration(f.seconds) * time.Second).Format(clickHouseTimeLayout)
}

func (f *fakeSidecars) host(name string) (*fakeHost, error) {
	host, found := f.hosts[name]
	switch {
	case !found && f.any:
		host = &fakeHost{}
		f.hosts[name] = host
	case !found:
		return nil, fmt.Errorf("%s: no such host", name)
	}
	return host, nil
}

func (f *fakeSidecars) Now(_ context.Context, host string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.host(host); err != nil {
		return "", err
	}
	return f.now(), nil
}

func (f *fakeSidecars) Push(_ context.Context, name, command string, receiveTimeout time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	host, err := f.host(name)
	if err != nil {
		return err
	}
	// A kill is taken while the sidecar runs a command: it cancels the command it names
	if strings.HasPrefix(command, commandKill+" ") {
		f.pushed = append(f.pushed, name+": "+command)
		target, err := strconv.Unquote(strings.TrimPrefix(command, commandKill+" "))
		if err != nil {
			return err
		}
		for i := range host.actions {
			if row := &host.actions[i]; row.running() && (row.Command == target) {
				row.Status, row.Error, row.Finish = "cancel", "canceled from API /backup/actions", f.now()
			}
		}
		start := f.now()
		host.actions = append(host.actions, actionRow{Command: command, Start: start, Finish: f.now(), Status: actionSuccess})
		return nil
	}
	if host.busy > 0 {
		host.busy--
		return fmt.Errorf("%w: another operation is currently running", errSidecarBusy)
	}
	if host.runs() || slices.ContainsFunc(f.refuse, func(text string) bool { return strings.Contains(command, text) }) {
		return fmt.Errorf("%w: another operation is currently running", errSidecarBusy)
	}
	for key, err := range f.lose {
		if target, text, _ := strings.Cut(key, "|"); (target == name) && strings.Contains(command, text) {
			return err
		}
	}
	f.pushed = append(f.pushed, name+": "+command)
	f.receiveTimeouts = append(f.receiveTimeouts, receiveTimeout)
	row := actionRow{Command: command, Start: f.now(), Status: actionSuccess}
	for key, reads := range f.slow {
		if target, text, _ := strings.Cut(key, "|"); (target == name) && strings.Contains(command, text) {
			if f.remaining == nil {
				f.remaining = map[string]int{}
			}
			f.remaining[name+"|"+command] = reads
			row.Status = actionInProgress
			host.actions = append(host.actions, row)
			return nil
		}
	}
	fields := strings.Fields(command)
	backup := fields[len(fields)-1]
	if (host.hang != "") && strings.Contains(command, host.hang) {
		row.Status = actionInProgress
		if strings.HasPrefix(command, "create_remote") {
			host.local = append(host.local, backupRow{Name: backup, Location: "local"})
		}
		host.actions = append(host.actions, row)
		return nil
	}
	for key, message := range f.fail {
		target, text, _ := strings.Cut(key, "|")
		if ((target == "") || (target == name)) && strings.Contains(command, text) {
			row.Status, row.Error = "error", message
		}
	}
	row.Finish = f.now()
	if (row.Status != actionSuccess) && f.leave {
		switch {
		case strings.HasPrefix(command, "create_remote"):
			host.local = append(host.local, backupRow{Name: backup, Location: "local"})
			host.remote = append(host.remote, backupRow{Name: backup, Location: "remote", Desc: "broken (can't stat metadata.json)"})
		case strings.HasPrefix(command, "download"):
			host.local = append(host.local, backupRow{Name: backup, Location: "local", Desc: "broken (can't stat metadata.json)"})
		}
	}
	if row.Status == actionSuccess {
		switch {
		case strings.HasPrefix(command, "create_remote"):
			required := baseOf(command)
			host.remote = append(host.remote, backupRow{Name: backup, Location: "remote", Required: required, DataSize: 100, MetadataSize: 10,
				Size: 110})
		case strings.HasPrefix(command, "download"):
			host.local = append(host.local, backupRow{Name: backup, Location: "local", DataSize: 100, MetadataSize: 10})
		case strings.HasPrefix(command, "delete local"):
			host.local = slices.DeleteFunc(host.local, func(row backupRow) bool { return row.Name == backup })
		case strings.HasPrefix(command, "delete remote"):
			host.remote = slices.DeleteFunc(host.remote, func(row backupRow) bool { return row.Name == backup })
		}
	}
	host.actions = append(host.actions, row)
	for key, err := range f.pushErr {
		if strings.Contains(command, key) {
			return err
		}
	}
	return nil
}

func (f *fakeSidecars) Action(_ context.Context, name, command, timeFrom string) (*actionRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	host, err := f.host(name)
	if err != nil {
		return nil, err
	}
	if host.actionErrors > 0 {
		host.actionErrors--
		return nil, host.actionError
	}
	if host.emptyReads > 0 {
		host.emptyReads--
		return nil, nil
	}
	for i, row := range host.actions {
		if (row.Command == command) && (row.Start >= timeFrom) {
			if key := name + "|" + command; row.running() && (f.remaining[key] > 0) {
				if f.remaining[key]--; f.remaining[key] == 0 {
					host.actions[i].Status, host.actions[i].Finish = actionSuccess, f.now()
					row = host.actions[i]
				}
			}
			return &row, nil
		}
	}
	return nil, nil
}

func (f *fakeSidecars) Actions(_ context.Context, name, timeFrom string) ([]actionRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	host, err := f.host(name)
	if err != nil {
		return nil, err
	}
	var rows []actionRow
	for _, row := range host.actions {
		if row.Start >= timeFrom {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (f *fakeSidecars) Busy(_ context.Context, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	host, err := f.host(name)
	if err != nil {
		return false, err
	}
	return host.runs(), nil
}

func (f *fakeSidecars) Running(_ context.Context, name string) ([]actionRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	host, err := f.host(name)
	if err != nil {
		return nil, err
	}
	if host.runningError != nil {
		return nil, host.runningError
	}
	var rows []actionRow
	for _, row := range host.actions {
		if row.Status == actionInProgress {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (f *fakeSidecars) List(_ context.Context, name, location string) ([]backupRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	host, err := f.host(name)
	if err != nil {
		return nil, err
	}
	if location == "local" {
		return slices.Clone(host.local), nil
	}
	return slices.Clone(host.remote), nil
}

func (f *fakeSidecars) Ready(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	host, err := f.host(name)
	if err != nil {
		return err
	}
	return host.notReady
}

// commands returns the commands inserted, "host: command".
func (f *fakeSidecars) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pushed)
}

// event is an Event a run posted.
type postedEvent struct {
	eventType, reason, message string
}

// runOf runs the spec on the fake sidecars from the status given, nil for a run that starts, and returns how it went,
// its status and its Events.
func runOf(t *testing.T, sc *fakeSidecars, spec *model.RunSpec, status *api.BackupRunStatus) (runOutcome, *api.BackupRunStatus, []postedEvent) {
	t.Helper()
	resumed := status != nil
	if status == nil {
		status = &api.BackupRunStatus{Operation: spec.Operation}
	}
	var events []postedEvent
	o := &orchestrator{
		spec:   spec,
		sc:     sc,
		mu:     &sync.Mutex{},
		status: status,
		event: func(eventType, reason, message string) {
			events = append(events, postedEvent{eventType, reason, message})
		},
		log:       log.V(1).M(t.Name()),
		clock:     clock.RealClock{},
		poll:      time.Millisecond,
		startedAt: time.Date(2026, time.November, 12, 0, 0, 0, 0, time.UTC),
		resumed:   resumed,
	}
	outcome := o.run(context.Background())
	return outcome, status, events
}

func reasonsOf(events []postedEvent) []string {
	var reasons []string
	for _, e := range events {
		reasons = append(reasons, e.reason)
	}
	return reasons
}

func backupSpec(name string, hosts ...string) *model.RunSpec {
	return &model.RunSpec{Operation: api.BackupRunOperationBackup, Cluster: "main", BackupName: name, BackupHosts: hosts,
		Naming: model.Naming{}}
}

// A backup is made by create_remote --rbac of its name on every host at once, inserted into system.backup_actions of
// each, and completes once the row of each tells success.
func TestOrchestratorBacksUp(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	outcome, status, events := runOf(t, sc, backupSpec("b1", "h0", "h1"), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0: create_remote --rbac b1", "h1: create_remote --rbac b1"}, sc.commands())
	require.Equal(t, "b1", outcome.backupName)
	require.Empty(t, outcome.noData)
	require.True(t, status.Mutated)
	require.Equal(t, stepRetention, status.Step)
	require.Equal(t, []api.BackupRunHost{{Host: "h0", Backup: "b1"}, {Host: "h1", Backup: "b1"}}, status.Hosts)
	for _, action := range status.Actions {
		require.Equal(t, actionSuccess, action.Status, action.Host)
		require.NotEmpty(t, action.TimeFrom)
	}
	require.Contains(t, reasonsOf(events), "OrchestratorBackupStarted")
	require.Contains(t, reasonsOf(events), "OrchestratorBackupCompleted")
}

// A backup the storage of a host holds already is refused before anything changes.
func TestOrchestratorRefusesABackupThatExists(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h1"].remote = []backupRow{{Name: "b1"}}
	outcome, _, _ := runOf(t, sc, backupSpec("b1", "h0", "h1"), nil)
	require.True(t, outcome.failed)
	require.True(t, outcome.unchanged)
	require.Equal(t, "backup b1 already exists in the storage of h1", outcome.message)
	require.Empty(t, sc.commands())
}

// A backup made with config overrides takes an --env flag of each; an incremental one from a base takes it by
// --diff-from-remote.
func TestOrchestratorBacksUpWithOverridesAndABase(t *testing.T) {
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{{Name: "base", DataSize: 1, MetadataSize: 1}}
	spec := backupSpec("b1", "h0")
	spec.ConfigOverrides = []string{"S3_STORAGE_CLASS=GLACIER"}
	spec.DiffFromRemote = "base"
	spec.Tables = []string{"db.*", "other.t?"}
	spec.SkipTables = []string{"db.tmp_*", "logs.*"}
	spec.SkipTableEngines = []string{"Kafka", "RabbitMQ"}
	spec.Partitions = []string{"2026", "2027"}
	outcome, status, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0: create_remote --rbac --env=S3_STORAGE_CLASS=GLACIER --tables=db.*,other.t? --partitions=2026,2027 " +
		"--env=CLICKHOUSE_SKIP_TABLES=" + defaultSkipTables + ",db.tmp_*,logs.* --env=CLICKHOUSE_SKIP_TABLE_ENGINES=Kafka,RabbitMQ " +
		"--diff-from-remote=base b1"}, sc.commands())
	require.Equal(t, "base", status.Hosts[0].Base)

	missing := newFakeSidecars("h0")
	outcome, _, _ = runOf(t, missing, spec, nil)
	require.True(t, outcome.failed)
	require.Equal(t, "base backup base is missing in the storage of h0", outcome.message)
}

// A failed backup deletes what it left, local and remote, on the hosts it failed on.
func TestOrchestratorDropsAFailedBackup(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.fail["h1|create_remote"] = "upload failed"
	outcome, _, events := runOf(t, sc, backupSpec("b1", "h0", "h1"), nil)
	require.True(t, outcome.failed)
	require.False(t, outcome.unchanged)
	require.Contains(t, outcome.message, "backup b1 failed on h1: h1: create_remote --rbac b1 ended with status 'error': upload failed")
	require.Equal(t, []string{"h0: create_remote --rbac b1", "h1: create_remote --rbac b1"}, sc.commands(),
		"a backup that failed left nothing to delete")
	require.Contains(t, reasonsOf(events), "OrchestratorHostFailed")
	require.Contains(t, reasonsOf(events), "OrchestratorBackupFailed")
}

// A shard is backed up once any of its replicas made the backup, AllReplicas included: a replica that failed only
// loses its own copy, which the run deletes, and the backup fails only on a shard no replica of which made it.
func TestOrchestratorBacksUpAShardOnAnyReplica(t *testing.T) {
	shardOf := func(host string) string { return strings.SplitN(host, "-", 2)[0] }
	sc := newFakeSidecars("s0-r0", "s0-r1", "s1-r0", "s1-r1")
	sc.fail["s0-r0|create_remote"] = "upload failed"
	spec := backupSpec("b1", "s0-r0", "s0-r1", "s1-r0", "s1-r1")
	spec.Shard = shardOf
	outcome, _, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"s0-r1", "s1-r0", "s1-r1"}, outcome.hostsUsed)
	require.Contains(t, reasonsOf(events), "OrchestratorReplicaFailed")
	require.NotContains(t, reasonsOf(events), "OrchestratorBackupFailed")

	sc = newFakeSidecars("s0-r0", "s0-r1", "s1-r0", "s1-r1")
	sc.fail["s0-r0|create_remote"] = "upload failed"
	sc.fail["s0-r1|create_remote"] = "upload failed"
	outcome, _, events = runOf(t, sc, spec, nil)
	require.True(t, outcome.failed)
	require.Contains(t, outcome.message, "backup b1 failed on s0-r0, s0-r1")
	require.Contains(t, reasonsOf(events), "OrchestratorBackupFailed")
}

// A backup counts as complete for retention once every shard lists it complete on any of its replicas.
func TestOrchestratorRetentionTakesABackupCompleteOnAnyReplicaOfEveryShard(t *testing.T) {
	sc := newFakeSidecars("s0-r0", "s0-r1")
	sc.hosts["s0-r0"].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
	o := &orchestrator{spec: &model.RunSpec{Shard: func(host string) string { return "s0" }}, sc: sc, mu: &sync.Mutex{},
		status: &api.BackupRunStatus{}, event: func(string, string, string) {}, clock: clock.RealClock{}, poll: time.Millisecond}
	sets, unlisted := o.listAll(context.Background(), []string{"s0-r0", "s0-r1"}, func(_, name string) (string, bool) { return name, true }, nil)
	require.Empty(t, unlisted)
	require.True(t, sets.Complete["b1"], "the shard lists it on one replica")

	o.spec.Shard = nil
	sets, _ = o.listAll(context.Background(), []string{"s0-r0", "s0-r1"}, func(_, name string) (string, bool) { return name, true }, nil)
	require.False(t, sets.Complete["b1"], "every host its own shard")
}

// A shard must hold only the backups made since its oldest listed backup: a shard added later leaves the older ones
// complete, a backup it misses after that is incomplete, and a shard that lists none holds no backup back.
func TestOrchestratorRetentionTakesBackupsBeforeAShardWasAddedComplete(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	rows := func(names ...string) []backupRow {
		var listed []backupRow
		for _, name := range names {
			listed = append(listed, backupRow{Name: name, DataSize: 1, MetadataSize: 1})
		}
		return listed
	}
	o := &orchestrator{spec: &model.RunSpec{}, sc: sc, mu: &sync.Mutex{}, status: &api.BackupRunStatus{},
		event: func(string, string, string) {}, clock: clock.RealClock{}, poll: time.Millisecond}
	complete := func(timeOf func(string) string) []string {
		sets, unlisted := o.listAll(context.Background(), []string{"h0", "h1"},
			func(_, name string) (string, bool) { return name, true }, timeOf)
		require.Empty(t, unlisted)
		return util.MapGetSortedKeys(sets.Complete)
	}
	byName := func(key string) string { return key }

	sc.hosts["h0"].remote = rows("d-20261101020000", "d-20261102020000", "d-20261103020000", "d-20261104020000")
	sc.hosts["h1"].remote = rows("d-20261103020000", "d-20261104020000")
	require.Equal(t, []string{"d-20261101020000", "d-20261102020000", "d-20261103020000", "d-20261104020000"}, complete(byName))
	require.Equal(t, []string{"d-20261103020000", "d-20261104020000"}, complete(nil), "every shard must hold each")

	sc.hosts["h1"].remote = rows("d-20261102020000", "d-20261104020000")
	require.Equal(t, []string{"d-20261101020000", "d-20261102020000", "d-20261104020000"}, complete(byName),
		"the backup the shard missed after it was added")

	sc.hosts["h1"].remote = nil
	require.Len(t, complete(byName), 4, "a shard that lists no backup")
}

// retentionOn runs the retention of a daily schedule that keeps one backup, of prefix d, after the backup of the key
// made, on the hosts, and returns its Events.
func retentionOn(t *testing.T, sc *fakeSidecars, made string, pins []api.BackupRetentionPin, hosts ...string) []postedEvent {
	t.Helper()
	var events []postedEvent
	spec := &model.RunSpec{Operation: api.BackupRunOperationBackup, Cluster: "main", Prefix: "d", RetentionHosts: hosts,
		Entries: []model.SlotEntry{{Period: api.BackupPeriodDaily, Hour: 2, Day: -1, Keep: 1}}, KeepBackups: pins}
	o := &orchestrator{spec: spec, sc: sc, mu: &sync.Mutex{}, status: &api.BackupRunStatus{Plan: &api.BackupRunPlan{Key: made}},
		event: func(eventType, reason, message string) {
			events = append(events, postedEvent{eventType, reason, message})
		},
		log: log.V(1).M(t.Name()), clock: clock.RealClock{}, poll: time.Millisecond, backupNames: map[string]string{}}
	o.retention(context.Background())
	return events
}

// An incomplete backup older than the newest complete one is deleted once no host of its shard tells a create_remote
// or an upload of it in progress; a host whose system.backup_actions cannot be read leaves every incomplete backup,
// and a pin leaves it as well.
func TestOrchestratorRetentionOfIncompleteBackups(t *testing.T) {
	const broken = "d-20261102020000"
	setUp := func() *fakeSidecars {
		sc := newFakeSidecars("h0", "h1")
		for _, host := range []string{"h0", "h1"} {
			sc.hosts[host].remote = []backupRow{
				{Name: "d-20261101020000", DataSize: 1, MetadataSize: 1},
				{Name: broken, Desc: "broken (can't stat metadata.json)"},
				{Name: "d-20261103020000", DataSize: 1, MetadataSize: 1},
			}
		}
		return sc
	}
	deleted := func(sc *fakeSidecars) []string {
		var names []string
		for _, command := range sc.commands() {
			if _, name, found := strings.Cut(command, commandDeleteRemote+" "); found && strings.HasPrefix(command, "h0: ") {
				names = append(names, name)
			}
		}
		return names
	}

	sc := setUp()
	retentionOn(t, sc, "d-20261103020000", nil, "h0", "h1")
	require.Equal(t, []string{broken, "d-20261101020000"}, deleted(sc), "no command writes it")

	sc = setUp()
	sc.hosts["h0"].actions = []actionRow{{Command: "create_remote --rbac " + broken, Start: sc.now(), Finish: sc.now(), Status: "error"}}
	retentionOn(t, sc, "d-20261103020000", nil, "h0", "h1")
	require.Equal(t, []string{broken, "d-20261101020000"}, deleted(sc), "a create_remote that failed writes nothing")

	sc = setUp()
	sc.hosts["h1"].runningError = errors.New("connection refused")
	events := retentionOn(t, sc, "d-20261103020000", nil, "h0", "h1")
	require.Equal(t, []string{"d-20261101020000"}, deleted(sc), "system.backup_actions of h1 cannot be read")
	require.Contains(t, reasonsOf(events), "OrchestratorRetentionIncompleteKept")

	sc = setUp()
	retentionOn(t, sc, "d-20261103020000", []api.BackupRetentionPin{{Name: broken}}, "h0", "h1")
	require.Equal(t, []string{"d-20261101020000"}, deleted(sc), "pinned")

	// A sidecar that runs a command refuses any other, the deletes of retention included: which backups a command in
	// progress writes is told apart
	written := func(sc *fakeSidecars) []string {
		o := &orchestrator{spec: &model.RunSpec{RetentionHosts: []string{"h0", "h1"}}, sc: sc, mu: &sync.Mutex{},
			status: &api.BackupRunStatus{}, event: func(string, string, string) {}, clock: clock.RealClock{}, poll: time.Millisecond,
			bestEffort: true}
		sets, _ := o.listAll(context.Background(), []string{"h0", "h1"}, func(_, name string) (string, bool) { return name, true }, nil)
		return util.MapGetSortedKeys(o.writing(context.Background(), sets, "d-20261103020000"))
	}
	sc = setUp()
	require.Empty(t, written(sc))
	sc.hosts["h1"].actions = []actionRow{{Command: "upload --diff-from-remote=d-20261101020000 " + broken, Start: sc.now(),
		Status: actionInProgress}}
	require.Equal(t, []string{broken}, written(sc), "an upload in progress writes it")
	sc = setUp()
	sc.hosts["h0"].actions = []actionRow{{Command: "create_remote --rbac " + broken, Start: sc.now(), Status: actionInProgress}}
	require.Equal(t, []string{broken}, written(sc), "a create_remote in progress writes it")
	sc = setUp()
	sc.hosts["h0"].actions = []actionRow{{Command: "create_remote --rbac d-20261101020000", Start: sc.now(), Status: actionInProgress}}
	require.Empty(t, written(sc), "a command in progress of another backup")
	sc = setUp()
	sc.hosts["h0"].runningError = errors.New("connection refused")
	require.Equal(t, []string{broken}, written(sc), "system.backup_actions of h0 cannot be read")
}

// A pin whose until is past no longer keeps its backup, which retention deletes and tells.
func TestOrchestratorRetentionPinExpires(t *testing.T) {
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{
		{Name: "d-20261101020000", DataSize: 1, MetadataSize: 1}, {Name: "d-20261103020000", DataSize: 1, MetadataSize: 1},
	}
	events := retentionOn(t, sc, "d-20261103020000", []api.BackupRetentionPin{{Name: "d-20261101020000", Until: "2099-01-01T00:00:00Z"}}, "h0")
	require.Empty(t, sc.commands())
	require.NotContains(t, reasonsOf(events), "OrchestratorRetentionHeldExpired")
	events = retentionOn(t, sc, "d-20261103020000", []api.BackupRetentionPin{{Name: "d-20261101020000", Until: "2026-11-02T00:00:00Z"}}, "h0")
	require.Equal(t, []string{"h0: delete remote d-20261101020000"}, sc.commands())
	require.Contains(t, reasonsOf(events), "OrchestratorRetentionHeldExpired")
}

// The tries of a command a busy sidecar refused back off exponentially from the poll interval, capped at 5 minutes.
func TestBusyBackoff(t *testing.T) {
	var got []time.Duration
	for tries := 1; tries <= 9; tries++ {
		got = append(got, busyBackoff(pollInterval, tries))
	}
	require.Equal(t, []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second,
		160 * time.Second, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute}, got)
}

// A sidecar that runs another operation refuses the command, which is inserted again once it is free.
func TestOrchestratorQueuesOnABusySidecar(t *testing.T) {
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].busy = 2
	outcome, _, _ := runOf(t, sc, backupSpec("b1", "h0"), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0: create_remote --rbac b1"}, sc.commands())
}

// A preferred replica that does not answer leaves the backup of its shard to the first replica.
func TestOrchestratorFallsBackFromThePreferredReplica(t *testing.T) {
	sc := newFakeSidecars("h0-1", "h0-0", "h1-1", "h1-0")
	sc.hosts["h1-1"].notReady = errors.New("connection refused")
	spec := backupSpec("b1", "h0-1", "h1-1")
	spec.FallbackHosts = []string{"h0-0", "h1-0"}
	outcome, _, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0-1", "h1-0"}, outcome.hostsUsed)
	require.Equal(t, []string{"h0-1: create_remote --rbac b1", "h1-0: create_remote --rbac b1"}, sc.commands())
	require.Contains(t, reasonsOf(events), "OrchestratorReplicaFallback")
}

// A run resumed after a restart of the operator inserts no command again whose row it recorded: it polls the row.
func TestOrchestratorResumesACommandItRecorded(t *testing.T) {
	sc := newFakeSidecars("h0")
	timeFrom := sc.now()
	sc.hosts["h0"].actions = []actionRow{{Command: "create_remote --rbac b1", Start: sc.now(), Finish: sc.now(), Status: actionSuccess}}
	sc.hosts["h0"].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
	status := &api.BackupRunStatus{
		Operation: api.BackupRunOperationBackup, Step: stepCreate, Mutated: true,
		Hosts:   []api.BackupRunHost{{Host: "h0", Backup: "b1"}},
		Actions: []api.BackupRunAction{{Step: stepCreate, Host: "h0", Command: "create_remote --rbac b1", TimeFrom: timeFrom}},
		Plan:    &api.BackupRunPlan{BackupName: "b1", Key: "b1"},
	}
	outcome, _, _ := runOf(t, sc, backupSpec("b1", "h0"), status)
	require.False(t, outcome.failed, outcome.message)
	require.Empty(t, sc.commands(), "the command ran before the restart")

	// A command planned but never inserted is inserted; one inserted whose record a restart lost is found by its row
	lost := newFakeSidecars("h0")
	lost.hosts["h0"].actions = []actionRow{{Command: "create_remote --rbac b1", Start: lost.now(), Finish: lost.now(), Status: actionSuccess}}
	status.Actions[0].TimeFrom = ""
	outcome, _, _ = runOf(t, lost, backupSpec("b1", "h0"), status)
	require.False(t, outcome.failed, outcome.message)
	require.Empty(t, lost.commands(), "the row of the command started after the run did")
}

// A command a resumed run adopts, which it inserted before a restart lost its record, marks the run as one that changed
// something, as a command it inserts does: the run's cleanup after a failure, and its deletion, count on it.
func TestOrchestratorMarksAnAdoptedCommandMutated(t *testing.T) {
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].actions = []actionRow{{Command: "create_remote --rbac b1", Start: sc.now(), Finish: sc.now(), Status: actionSuccess}}
	status := &api.BackupRunStatus{Operation: api.BackupRunOperationBackup}
	outcome, status, _ := runOf(t, sc, backupSpec("b1", "h0"), status)
	require.False(t, outcome.failed, outcome.message)
	require.Empty(t, sc.commands(), "the command is adopted, not inserted again")
	require.True(t, status.Mutated)
}

// An identical command of an earlier operation is no command of the run: a new run inserts its own, and so does a run
// resumed after a restart for a row that finished before it started.
func TestOrchestratorAdoptsNoCommandOfAnEarlierOperation(t *testing.T) {
	earlier := actionRow{Command: "create_remote --rbac b1", Start: "2026-11-11 23:58:00", Finish: "2026-11-11 23:58:30",
		Status: actionSuccess}
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].actions = []actionRow{{Command: "create_remote --rbac b1", Start: sc.now(), Finish: sc.now(), Status: actionSuccess}}
	outcome, _, _ := runOf(t, sc, backupSpec("b1", "h0"), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0: create_remote --rbac b1"}, sc.commands(), "a new run adopts no row")

	resumed := newFakeSidecars("h0")
	resumed.hosts["h0"].actions = []actionRow{earlier}
	status := &api.BackupRunStatus{Operation: api.BackupRunOperationBackup}
	outcome, _, _ = runOf(t, resumed, backupSpec("b1", "h0"), status)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0: create_remote --rbac b1"}, resumed.commands(), "the row finished before the run started")
}

// A scheduled run names its backup after its scheduled time, and prunes the series of the schedule after it: every entry keeps
// its newest keep backups.
func TestOrchestratorPrunesAfterAScheduledRun(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	for _, host := range []string{"h0", "h1"} {
		for _, day := range []string{"09", "10", "11"} {
			sc.hosts[host].remote = append(sc.hosts[host].remote, backupRow{Name: "nightly-202611" + day + "030000", DataSize: 1, MetadataSize: 1})
		}
		sc.hosts[host].remote = append(sc.hosts[host].remote, backupRow{Name: "other-20261101030000", DataSize: 1, MetadataSize: 1})
	}
	spec := &model.RunSpec{
		Operation: api.BackupRunOperationBackup, Cluster: "main", Prefix: "nightly",
		Slot:           time.Date(2026, time.November, 12, 3, 0, 0, 0, time.UTC),
		Entries:        model.SlotEntries([]api.BackupScheduleEntry{{Period: api.BackupPeriodDaily, Keep: 2}}, "03:00", ""),
		BackupHosts:    []string{"h0", "h1"},
		RetentionHosts: []string{"h0", "h1"},
	}
	outcome, _, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, "nightly-20261112030000", outcome.backupName)
	require.Equal(t, []string{
		"h0: create_remote --rbac nightly-20261112030000", "h1: create_remote --rbac nightly-20261112030000",
		"h0: delete remote nightly-20261110030000", "h0: delete remote nightly-20261109030000",
		"h1: delete remote nightly-20261110030000", "h1: delete remote nightly-20261109030000",
	}, sc.commands())
	require.Contains(t, reasonsOf(events), "OrchestratorRetentionDeleted")

	// A restore of the cluster in progress skips retention
	spec.Slot = spec.Slot.Add(24 * time.Hour)
	spec.RestoreInProgress = true
	before := len(sc.commands())
	outcome, _, _ = runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Len(t, sc.commands(), before+2)
}

// The run of a scheduled time that does not exist the day the clocks go forward, 02:30 in Berlin on 2026-03-29, which the zone
// moves to 03:30, which the backup is named after, is the run of the entry whose scheduled time it is: it makes the backup as that
// entry says, with its config overrides and as incremental, and retention keeps it, as it keeps the backup of any scheduled time.
func TestOrchestratorRunsTheSlotOfASpringForwardGap(t *testing.T) {
	sc := newFakeSidecars("h0")
	for _, name := range []string{"nightly-20260327023000", "nightly-20260328023000"} {
		sc.hosts["h0"].remote = append(sc.hosts["h0"].remote, backupRow{Name: name, DataSize: 1, MetadataSize: 1})
	}
	spec := &model.RunSpec{
		Operation: api.BackupRunOperationBackup, Cluster: "main", Prefix: "nightly", TimeZone: "Europe/Berlin",
		Slot: time.Date(2026, time.March, 29, 1, 30, 0, 0, time.UTC),
		Entries: model.SlotEntries([]api.BackupScheduleEntry{
			{Period: api.BackupPeriodDaily, Keep: 2},
			{Period: api.BackupPeriodWeekly, Day: ptr.To(int32(0)), Keep: 1,
				ConfigOverrides: []string{"S3_STORAGE_CLASS=GLACIER"}},
		}, "02:30", "Europe/Berlin"),
		BackupHosts:    []string{"h0"},
		RetentionHosts: []string{"h0"},
	}
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, "nightly-20260329033000", outcome.backupName)
	require.Equal(t, []string{"h0: create_remote --rbac --env=S3_STORAGE_CLASS=GLACIER nightly-20260329033000"}, sc.commands()[:1],
		"the scheduled time is the weekly entry's, whose overrides the backup is made with")

	// The next day's run keeps the backup of the gap, which is the Sunday's, and prunes the one before it
	spec.Slot = time.Date(2026, time.March, 30, 0, 30, 0, 0, time.UTC)
	outcome, _, _ = runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.ElementsMatch(t, []string{"h0: delete remote nightly-20260328023000", "h0: delete remote nightly-20260327023000"},
		commandsHolding(sc, "delete remote"), "the backup of the gap is kept")
}

// An incremental entry makes the backup incremental from the newest backup of the series every host holds, until the
// chain is as long as the entry keeps.
func TestOrchestratorMakesIncrementalBackups(t *testing.T) {
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{
		{Name: "nightly-20261110030000", DataSize: 1, MetadataSize: 1},
		{Name: "nightly-20261111030000", Required: "nightly-20261110030000", DataSize: 1, MetadataSize: 1},
	}
	spec := &model.RunSpec{
		Operation: api.BackupRunOperationBackup, Cluster: "main", Prefix: "nightly",
		Slot: time.Date(2026, time.November, 12, 3, 0, 0, 0, time.UTC),
		Entries: model.SlotEntries([]api.BackupScheduleEntry{{Period: api.BackupPeriodDaily, Keep: 3,
			Incremental: types.NewStringBool(true)}}, "03:00", ""),
		BackupHosts:    []string{"h0"},
		RetentionHosts: []string{"h0"},
	}
	outcome, _, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, "h0: create_remote --rbac --diff-from-remote=nightly-20261111030000 nightly-20261112030000", sc.commands()[0])
	require.Contains(t, reasonsOf(events), "OrchestratorIncrementalBase")
}

// With a naming template every shard backs up under a name of its own, which a template of the Altinity usual backup
// workflow makes of its shard and the scheduled time.
func TestOrchestratorNamesTheBackupOfEveryShard(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	spec := &model.RunSpec{
		Operation: api.BackupRunOperationBackup, Cluster: "main", Prefix: "nightly",
		Slot:        time.Date(2026, time.November, 12, 3, 0, 0, 0, time.UTC),
		Naming:      model.Naming{Template: api.BackupNamingShardTime, Cluster: "main", Shards: []string{"0", "1"}, ShardOf: map[string]int{"h0": 0, "h1": 1}},
		Entries:     model.SlotEntries([]api.BackupScheduleEntry{{Period: api.BackupPeriodDaily, Keep: 2}}, "03:00", ""),
		BackupHosts: []string{"h0", "h1"},
	}
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, "shard1-20261112030000", outcome.backupName)
	require.Equal(t, []string{"h0: create_remote --rbac shard1-20261112030000", "h1: create_remote --rbac shard2-20261112030000"}, sc.commands())
}

func restoreSpec(hosts ...string) *model.RunSpec {
	return &model.RunSpec{Operation: api.BackupRunOperationRestore, Cluster: "main", BackupName: "b1", SchemaHosts: hosts,
		DataHosts: hosts[:1], ValidateTopology: true, ReplicatedCopyToDetached: true, ResumeDownload: true}
}

// A restore downloads the backup on every host, restores the schema on every replica - on the first replica of the
// first shard ON CLUSTER - copies the data to detached on the other replicas of a shard and restores it on its first
// one, then drops the downloaded copies. Without
// overwrite it drops nothing: clickhouse-backup refuses tables that exist.
func TestOrchestratorRestores(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	for _, host := range []string{"h0", "h1"} {
		sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
	}
	outcome, status, events := runOf(t, sc, restoreSpec("h0", "h1"), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h0: download --hardlink-exists-files b1", "h1: download --hardlink-exists-files b1",
		"h0: restore --schema=1 --ignore-dependencies --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1",
		"h1: restore --schema=1 --ignore-dependencies b1",
		"h1: restore --data=1 --ignore-dependencies --replicated-copy-to-detached b1",
		"h0: restore --data=1 --ignore-dependencies b1",
		"h0: delete local b1", "h1: delete local b1",
	}, sc.commands())
	require.Equal(t, stepCleanup, status.Step)
	require.Contains(t, reasonsOf(events), "OrchestratorRestoreCompleted")
	require.False(t, outcome.rbacNotRestored)

	missing := newFakeSidecars("h0")
	outcome, _, _ = runOf(t, missing, restoreSpec("h0"), nil)
	require.True(t, outcome.failed)
	require.True(t, outcome.unchanged)
	require.Equal(t, "preflight failed: backup b1 is missing in the storage of h0", outcome.message)
	require.Empty(t, missing.commands())
}

// Every restore command ignores dependencies. An overwrite of every table recreates the databases of the backup
// whole (--rm); one of some tables never drops a database, as clickhouse-backup drops the tables of the backup that
// exist anyway. The filters of the restore go on every step, the database mapping on the restore steps.
func TestOrchestratorOverwrites(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	for _, host := range []string{"h0", "h1"} {
		sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1, RBACSize: 5}}
	}
	spec := restoreSpec("h0", "h1")
	spec.Overwrite = true
	spec.Tables = []string{"db.*", "other.t?"}
	spec.SkipTables = []string{"db.tmp_*", "logs.*"}
	spec.DatabaseMapping = "db:db2"
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	skip := "--env=CLICKHOUSE_SKIP_TABLES=" + defaultSkipTables + ",db.tmp_*,logs.*"
	require.Equal(t, []string{
		"h0: download --hardlink-exists-files --tables=db.*,other.t? " + skip + " b1",
		"h1: download --hardlink-exists-files --tables=db.*,other.t? " + skip + " b1",
		"h0: restore --schema=1 --ignore-dependencies --tables=db.*,other.t? " + skip + " --restore-database-mapping=db:db2 --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1",
		"h1: restore --schema=1 --ignore-dependencies --tables=db.*,other.t? " + skip + " --restore-database-mapping=db:db2 b1",
		"h1: restore --data=1 --ignore-dependencies --replicated-copy-to-detached --tables=db.*,other.t? " + skip + " --restore-database-mapping=db:db2 b1",
		"h0: restore --data=1 --ignore-dependencies --tables=db.*,other.t? " + skip + " --restore-database-mapping=db:db2 b1",
		"h0: delete local b1", "h1: delete local b1",
	}, sc.commands())
	require.True(t, outcome.rbacNotRestored, "the backup holds access entities the restore left out")

	sc = newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
	spec = restoreSpec("h0")
	spec.Overwrite = true
	outcome, _, _ = runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Contains(t, sc.commands(), "h0: restore --schema=1 --rm --ignore-dependencies --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1",
		"an overwrite of every table recreates the databases of the backup")
	require.Contains(t, sc.commands(), "h0: restore --data=1 --ignore-dependencies b1")

	sc = newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1, RBACSize: 5}}
	spec = restoreSpec("h0")
	spec.RestoreRBAC = true
	outcome, _, _ = runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Contains(t, sc.commands(), "h0: restore --schema=1 --ignore-dependencies --rbac --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1")
	require.False(t, outcome.rbacNotRestored)
}

// A schema-only restore downloads only the schema, on every host.
func TestOrchestratorRestoresSchemaOnly(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	for _, host := range []string{"h0", "h1"} {
		sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
	}
	spec := restoreSpec("h0", "h1")
	spec.SchemaOnly = true
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h0: download --schema b1", "h1: download --schema b1",
		"h0: restore --schema=1 --ignore-dependencies --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1",
		"h1: restore --schema=1 --ignore-dependencies b1",
		"h0: delete local b1", "h1: delete local b1",
	}, sc.commands())
}

// A restore of partitions downloads and attaches them into the tables as they are, without the schema step.
// The config overrides come before the settings of the operator, which win.
func TestOrchestratorRestoresPartitions(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	for _, host := range []string{"h0", "h1"} {
		sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
	}
	spec := restoreSpec("h0", "h1")
	spec.Partitions = []string{"2026"}
	spec.SkipTableEngines = []string{"Kafka"}
	spec.DatabaseMapping = "db:db2"
	spec.ConfigOverrides = []string{"DOWNLOAD_CONCURRENCY=8"}
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	env := "--env=DOWNLOAD_CONCURRENCY=8 --env=CLICKHOUSE_SKIP_TABLE_ENGINES=Kafka --env=CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0"
	require.Equal(t, []string{
		"h0: download --hardlink-exists-files --partitions=2026 " + env + " b1",
		"h1: download --hardlink-exists-files --partitions=2026 " + env + " b1",
		"h1: restore --data=1 --ignore-dependencies --replicated-copy-to-detached --partitions=2026 " + env + " --restore-database-mapping=db:db2 b1",
		"h0: restore --data=1 --ignore-dependencies --partitions=2026 " + env + " --restore-database-mapping=db:db2 b1",
		"h0: delete local b1", "h1: delete local b1",
	}, sc.commands())
}

// A run past its deadline stops, and tells it.
func TestOrchestratorStopsAtItsDeadline(t *testing.T) {
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].busy = 1 << 30
	spec := backupSpec("b1", "h0")
	spec.Deadline = time.Nanosecond
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.True(t, outcome.failed)
	require.True(t, outcome.deadline)
	require.Equal(t, api.BackupReasonDeadlineExceeded, outcomeReason(outcome, api.BackupRunOperationBackup))
}

// A restore of a backup named by a template that holds {prefix} names the backup of every shard with the prefix the
// name of the backup tells.
func TestOrchestratorRestoresABackupNamedWithAPrefix(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h0"].remote = []backupRow{{Name: "nightly-shard1-20261112030000", DataSize: 1, MetadataSize: 1}}
	sc.hosts["h1"].remote = []backupRow{{Name: "nightly-shard2-20261112030000", DataSize: 1, MetadataSize: 1}}
	spec := &model.RunSpec{Operation: api.BackupRunOperationRestore, Cluster: "main", BackupName: "nightly-shard1-20261112030000",
		SchemaHosts: []string{"h0", "h1"}, DataHosts: []string{"h0", "h1"},
		Naming: model.Naming{Template: "{prefix}-shard{shardNum}-{YYYYMMDDHHIISS}", Cluster: "main", Shards: []string{"0", "1"}, ShardOf: map[string]int{"h0": 0, "h1": 1}}}
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h0: download --hardlink-exists-files nightly-shard1-20261112030000",
		"h1: download --hardlink-exists-files nightly-shard2-20261112030000",
	}, sc.commands()[:2])
	require.Contains(t, sc.commands(), "h1: restore --data=1 --ignore-dependencies nightly-shard2-20261112030000")
}

// A scheduled run lists the backups of the series that retention keeps: a backup deleted on every host is left out, one
// whose delete failed on a host stays. Sizes add up over the shards, the earliest creation time counts, and the base
// of an incremental backup is named.
func TestOrchestratorListsTheSeriesRetentionKeeps(t *testing.T) {
	day := func(d int) time.Time { return time.Date(2026, time.November, d, 3, 0, 5, 0, time.UTC) }
	sc := newFakeSidecars("h0", "h1")
	for i, host := range []string{"h0", "h1"} {
		sc.hosts[host].remote = []backupRow{
			{Name: "nightly-20261109030000", DataSize: 1, MetadataSize: 1, Size: 10, Created: day(9)},
			{Name: "nightly-20261110030000", DataSize: 1, MetadataSize: 1, Size: 10, Created: day(10)},
			{Name: "nightly-20261111030000", Required: "nightly-20261110030000", DataSize: 1, MetadataSize: 1, Size: 5,
				Created: day(11).Add(time.Duration(i) * time.Second)},
			{Name: "other-20261101030000", DataSize: 1, MetadataSize: 1},
		}
	}
	sc.fail["h1|delete remote nightly-20261109030000"] = "access denied"
	spec := &model.RunSpec{
		Operation: api.BackupRunOperationBackup, Cluster: "main", Prefix: "nightly",
		Slot:           time.Date(2026, time.November, 12, 3, 0, 0, 0, time.UTC),
		Entries:        model.SlotEntries([]api.BackupScheduleEntry{{Period: api.BackupPeriodDaily, Keep: 2}}, "03:00", ""),
		BackupHosts:    []string{"h0", "h1"},
		RetentionHosts: []string{"h0", "h1"},
	}
	outcome, _, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	var names []string
	for _, backup := range outcome.series {
		names = append(names, backup.Name)
	}
	// The new backup lists no creation time in the fake, so it sorts last
	require.Equal(t, []string{"nightly-20261111030000", "nightly-20261109030000", "nightly-20261112030000"}, names,
		"nightly-20261110030000 is deleted everywhere, nightly-20261109030000 not on h1, other- is no backup of the series")
	require.Equal(t, api.StoredBackup{Name: "nightly-20261111030000", Created: &meta.Time{Time: day(11)}, Size: 10,
		State: api.StoredBackupComplete, Base: "nightly-20261110030000"}, outcome.series[0])
	require.Equal(t, int64(220), outcome.series[2].Size)

	// A one-off backup lists itself only
	sc = newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{{Name: "other", DataSize: 1, MetadataSize: 1, Size: 3}}
	outcome, _, _ = runOf(t, sc, backupSpec("b1", "h0"), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []api.StoredBackup{{Name: "b1", Size: 110, State: api.StoredBackupComplete}}, outcome.stored)
	require.Nil(t, outcome.series, "no retention")
}
