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
	"sync"
	"testing"
	"time"

	chdriver "github.com/mailru/go-clickhouse/v2"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// testOrchestrator returns an orchestrator of the spec on the fake sidecars, polling every millisecond.
func testOrchestrator(t *testing.T, sc *fakeSidecars, spec *model.RunSpec) *orchestrator {
	t.Helper()
	return &orchestrator{spec: spec, sc: sc, mu: &sync.Mutex{}, status: &api.BackupRunStatus{Operation: spec.Operation},
		event: func(string, string, string) {}, logPrefix: t.Name(), clock: clock.RealClock{}, poll: time.Millisecond,
		startedAt: time.Date(2026, time.November, 12, 0, 0, 0, 0, time.UTC)}
}

// withTimeouts returns the spec with the timeouts.
func withTimeouts(spec *model.RunSpec, timeouts api.BackupTimeouts) *model.RunSpec {
	spec.Timeouts = &timeouts
	return spec
}

// The timeouts of the run are the orchestrator's, the built-in defaults standing for those it leaves unset.
func TestOrchestratorTimeoutsDefault(t *testing.T) {
	o := testOrchestrator(t, newFakeSidecars(), backupSpec("b1"))
	require.Equal(t, 12*time.Hour, o.actionTimeout())
	require.Equal(t, 10*time.Minute, o.unreachableTimeout())
	require.Equal(t, 4*time.Hour, o.httpReceiveTimeout())
	require.Equal(t, 5, o.errorResponsesMax())

	o = testOrchestrator(t, newFakeSidecars(), withTimeouts(backupSpec("b1"), api.BackupTimeouts{
		ActionTimeout: dur(1 * time.Minute), UnreachableTimeout: dur(30 * time.Second),
		ErrorResponsesMax: ptr.To(int32(2)), HTTPReceiveTimeout: dur(15 * time.Minute),
	}))
	require.Equal(t, time.Minute, o.actionTimeout())
	require.Equal(t, 30*time.Second, o.unreachableTimeout())
	require.Equal(t, 15*time.Minute, o.httpReceiveTimeout())
	require.Equal(t, 2, o.errorResponsesMax())
}

// Every insert carries the HTTP receive timeout of the run, the built-in default without one.
func TestOrchestratorPushesWithTheHTTPReceiveTimeout(t *testing.T) {
	sc := newFakeSidecars("h0")
	outcome, _, _ := runOf(t, sc, backupSpec("b1", "h0"), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []time.Duration{4 * time.Hour}, sc.receiveTimeouts)

	sc = newFakeSidecars("h0")
	outcome, _, _ = runOf(t, sc, withTimeouts(backupSpec("b1", "h0"), api.BackupTimeouts{HTTPReceiveTimeout: dur(10 * time.Minute)}), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []time.Duration{10 * time.Minute}, sc.receiveTimeouts)
}

// pushed inserts a command into the rows of the host as a sidecar that ran it, and returns its action.
func pushed(t *testing.T, sc *fakeSidecars, host, command string) *api.BackupRunAction {
	t.Helper()
	timeFrom, err := sc.Now(context.Background(), host)
	require.NoError(t, err)
	require.NoError(t, sc.Push(context.Background(), host, command, time.Hour))
	return &api.BackupRunAction{Step: stepData, Host: host, Command: command, TimeFrom: timeFrom}
}

// A poll that fails is no poll without a row: errors are retried until the unreachable timeout, however many, and
// do not take the command as lost.
func TestOrchestratorWaitRetriesErrorsApartFromEmptyResponses(t *testing.T) {
	sc := newFakeSidecars("h0")
	o := testOrchestrator(t, sc, withTimeouts(restoreSpec("h0"), api.BackupTimeouts{
		ErrorResponsesMax: ptr.To(int32(1)), UnreachableTimeout: dur(1 * time.Hour),
	}))
	action := pushed(t, sc, "h0", "restore --data=1 b1")
	sc.hosts["h0"].actionErrors = 10
	sc.hosts["h0"].actionError = errors.New("dial tcp 10.0.0.1:8123: connect: connection refused")
	failures := o.wait(context.Background(), []*api.BackupRunAction{action}, nil)
	require.Empty(t, failures, "ten errors in a row with errorResponsesMax 1")
	require.Equal(t, actionSuccess, action.Status)
}

// Polls without a row count against errorResponsesMax: within it the command is waited for, past it the command is
// lost, which a restore does not resume.
func TestOrchestratorWaitCountsEmptyResponses(t *testing.T) {
	sc := newFakeSidecars("h0")
	o := testOrchestrator(t, sc, withTimeouts(restoreSpec("h0"), api.BackupTimeouts{ErrorResponsesMax: ptr.To(int32(3))}))
	action := pushed(t, sc, "h0", "restore --data=1 b1")
	sc.hosts["h0"].emptyReads = 3
	require.Empty(t, o.wait(context.Background(), []*api.BackupRunAction{action}, nil))

	sc = newFakeSidecars("h0")
	o = testOrchestrator(t, sc, withTimeouts(restoreSpec("h0"), api.BackupTimeouts{ErrorResponsesMax: ptr.To(int32(3))}))
	action = pushed(t, sc, "h0", "restore --data=1 b1")
	sc.hosts["h0"].emptyReads = 4
	failures := o.wait(context.Background(), []*api.BackupRunAction{action}, nil)
	require.ErrorContains(t, failures["h0"], "the sidecar restarted during restore --data=1 b1, which it does not resume")
}

// A host whose polls fail for longer than the unreachable timeout fails, with what ClickHouse told: its error code
// and the HTTP status code the URL engine got from the sidecar.
func TestOrchestratorWaitFailsAnUnreachableHost(t *testing.T) {
	sc := newFakeSidecars("h0")
	o := testOrchestrator(t, sc, withTimeouts(restoreSpec("h0"), api.BackupTimeouts{UnreachableTimeout: dur(1 * time.Second)}))
	o.poll = 50 * time.Millisecond
	action := pushed(t, sc, "h0", "restore --data=1 b1")
	sc.hosts["h0"].actionErrors = 1000
	sc.hosts["h0"].actionError = fmt.Errorf("query: %w", &chdriver.Error{Code: 86,
		Message: "Received error from remote server http://127.0.0.1:7171/backup/actions. HTTP status code: 502 Bad Gateway"})
	failures := o.wait(context.Background(), []*api.BackupRunAction{action}, nil)
	require.ErrorContains(t, failures["h0"], "cannot read its row in system.backup_actions for 1s: ClickHouse error code 86, "+
		"HTTP status code 502 from the sidecar")
}

// The insert of a delete answers once the delete is done: an insert that fails, as one cut by a timeout, is settled by
// the row of the delete, looked for as long as errorResponsesMax polls, and fails only without one.
func TestOrchestratorSettlesADeleteWhoseInsertFailed(t *testing.T) {
	timeout := errors.New("doRequest: transport failed to send a request to ClickHouse: context deadline exceeded")
	sc := newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{{Name: "b1"}}
	sc.pushErr["delete remote"] = timeout
	sc.hosts["h0"].emptyReads = 4
	o := testOrchestrator(t, sc, withTimeouts(backupSpec("b1", "h0"), api.BackupTimeouts{ErrorResponsesMax: ptr.To(int32(5))}))
	require.NoError(t, o.deleteBackup(context.Background(), stepCleanup, "h0", "remote", "b1"),
		"the row tells the delete succeeded after four polls without one")
	require.Empty(t, sc.hosts["h0"].remote)

	sc = newFakeSidecars("h0")
	sc.hosts["h0"].remote = []backupRow{{Name: "b1"}}
	sc.pushErr["delete remote"] = timeout
	sc.fail["|delete remote"] = "remote storage unavailable"
	o = testOrchestrator(t, sc, backupSpec("b1", "h0"))
	err := o.deleteBackup(context.Background(), stepCleanup, "h0", "remote", "b1")
	require.ErrorContains(t, err, "delete remote b1 ended with status 'error': remote storage unavailable",
		"settled by the status of its row")

	sc = newFakeSidecars("h0")
	sc.pushErr["delete remote"] = timeout
	sc.hosts["h0"].emptyReads = 3
	o = testOrchestrator(t, sc, withTimeouts(backupSpec("b1", "h0"), api.BackupTimeouts{ErrorResponsesMax: ptr.To(int32(3))}))
	err = o.deleteBackup(context.Background(), stepCleanup, "h0", "remote", "b1")
	require.ErrorContains(t, err, "h0: delete remote b1 failed: "+timeout.Error(), "no row within errorResponsesMax polls")

	// Reads of the row that fail are no reads without it: retried until the unreachable timeout
	sc = newFakeSidecars("h0")
	sc.pushErr["delete local"] = timeout
	sc.hosts["h0"].actionErrors = 10
	sc.hosts["h0"].actionError = errors.New("connection refused")
	o = testOrchestrator(t, sc, withTimeouts(backupSpec("b1", "h0"), api.BackupTimeouts{ErrorResponsesMax: ptr.To(int32(1))}))
	require.NoError(t, o.deleteBackup(context.Background(), stepCleanup, "h0", "local", "b1"))

	// An asynchronous command whose insert failed is looked for as before, existenceAttempts times
	sc = newFakeSidecars("h0")
	sc.pushErr["create_remote"] = timeout
	sc.hosts["h0"].emptyReads = 4
	o = testOrchestrator(t, sc, backupSpec("b1", "h0"))
	_, err = o.submit(context.Background(), stepCreate, "h0", "create_remote --rbac b1", true)
	require.ErrorContains(t, err, "h0: create_remote --rbac b1 failed: "+timeout.Error())
}

// describeQueryError tells the error code of ClickHouse, the HTTP status code of the sidecar, a response that is no
// JSON, and a ClickHouse that does not answer.
func TestDescribeQueryError(t *testing.T) {
	require.Equal(t, "ClickHouse error code 86, HTTP status code 500 from the sidecar: code: 86, message: Received error from "+
		"remote server /backup/actions. HTTP status code: 500 Internal Server Error, body: oops",
		describeQueryError(errors.New("code: 86, message: Received error from remote server /backup/actions. "+
			"HTTP status code: 500 Internal Server Error, body: oops")))
	require.Contains(t, describeQueryError(&chdriver.Error{Code: 117, Message: "Cannot parse input: expected '{' before: 'oops'"}),
		"ClickHouse error code 117, the response of the sidecar is not valid JSON")
	require.Contains(t, describeQueryError(errors.New("dial tcp 10.0.0.1:8123: connect: connection refused")),
		"ClickHouse does not answer")
	require.Equal(t, "something else", describeQueryError(errors.New("something else")))
}

// dur returns a duration of a spec.
func dur(d time.Duration) *meta.Duration {
	return &meta.Duration{Duration: d}
}
