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
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// A termination message holds up to 4096 bytes: an error the Job script wrote, which says what
// failed first, or the tail of the log for a container that wrote none, which says it last. Both
// ends are kept.
func TestTruncateMessage(t *testing.T) {
	require.Equal(t, "short", truncateMessage("short"))
	for _, filler := range []string{"ж", "ab", "€"} {
		long := "refusing to restore over existing tables (" + strings.Repeat(filler, terminationMessageMaxLength) + "); the error"
		truncated := truncateMessage(long)
		require.LessOrEqual(t, len(truncated), terminationMessageMaxLength+len(truncationMarker))
		require.True(t, strings.HasPrefix(truncated, "refusing to restore over existing tables ("), truncated)
		require.Contains(t, truncated, truncationMarker)
		require.True(t, strings.HasSuffix(truncated, "); the error"), truncated)
		require.True(t, utf8.ValidString(truncated), "a message is never cut inside a character")
	}
}

func lockCount(locks *keyedLocks) int {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	return len(locks.locks)
}

// One key has one holder at a time, other keys are independent, and a key is forgotten once
// nobody holds or waits for it: installations come and go, the map must not grow with them.
func TestKeyedLocks(t *testing.T) {
	locks := newKeyedLocks()
	unlockA := locks.lock("ns/a")
	unlockB := locks.lock("ns/b")

	acquired, done := make(chan struct{}), make(chan struct{})
	go func() {
		unlock := locks.lock("ns/a")
		close(acquired)
		unlock()
		close(done)
	}()
	select {
	case <-acquired:
		require.Fail(t, "a key was locked twice")
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 2, lockCount(locks))

	unlockA()
	<-done
	require.Equal(t, 1, lockCount(locks), "a key is dropped with its last holder")
	unlockB()
	require.Zero(t, lockCount(locks))
}

func TestKeyedLocksUnderContention(t *testing.T) {
	locks := newKeyedLocks()
	// Each counter is guarded by nothing but the lock of its key, which the race detector checks.
	holders := map[string]*int{"ns/a": new(int), "ns/b": new(int)}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		key := []string{"ns/a", "ns/b"}[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				unlock := locks.lock(key)
				*holders[key]++
				if *holders[key] != 1 {
					t.Errorf("%d holders of %s", *holders[key], key)
				}
				*holders[key]--
				unlock()
			}
		}()
	}
	wg.Wait()
	require.Zero(t, lockCount(locks))
}

// A schedule resumes the CronJobs of several clusters under all of their locks while restores of
// those clusters take one each: taking them in one order, each once, never leaves two callers waiting
// for each other.
func TestLockOperationsNeverDeadlocks(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, clusters := range [][]string{{"hot", "cold"}, {"cold", "hot"}, {"cold", "cold", "hot"}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					lockOperations(testNamespace, testCHI, clusters...)()
				}
			}()
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("callers locking the same clusters in another order wait for each other")
	}
	require.Zero(t, lockCount(operationLocks))
}

// refuseJobs fails the creation of every Job with err, as the API server does for a Job created
// meanwhile, which the cache has not seen yet, or for a Job it refuses.
func refuseJobs(err error) *interceptor.Funcs {
	return &interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, isJob := obj.(*batch.Job); isJob {
				return err
			}
			return c.Create(ctx, obj, opts...)
		},
	}
}

// otherOperation returns the running Job of another operation on the main cluster of the installation.
func otherOperation(t *testing.T) *batch.Job {
	return otherOperationOn(t, "main")
}

// otherOperationOn returns the running Job of another operation on a cluster of the installation.
func otherOperationOn(t *testing.T, cluster string) *batch.Job {
	return jobOwnedBy(t, "other-restore-"+cluster, newRestoreCR("other"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "other", testCHI, cluster, testStorage))
}

// The creation of the Job of an operation on a cluster ends the same way for every operation: the Job
// runs, the operation waits for its gate or for another one on the cluster, a Job of that name created
// meanwhile is left to the next reconcile, or the error is returned. Only a created Job is run, and
// only an operation that waits records why. An operation on another cluster drives other sidecars and
// does not conflict.
func TestStartJob(t *testing.T) {
	refused := apiErrors.NewForbidden(batch.Resource("jobs"), "b1-main-backup", errors.New("quota exceeded"))
	blocked := &pending{reason: api.BackupReasonWaitingForSchedules, message: "held", requeue: time.Minute}
	for name, tc := range map[string]struct {
		funcs  *interceptor.Funcs
		objs   []client.Object
		gate   *pending
		result ctrl.Result
		err    error
		waits  []string
		runs   int
	}{
		"created": {result: ctrl.Result{RequeueAfter: runningRequeueInterval}, runs: 1},
		"another operation runs": {objs: []client.Object{otherOperation(t)}, result: ctrl.Result{RequeueAfter: pendingRequeueInterval},
			waits: []string{api.BackupReasonOperationInProgress, operationInProgressMessage(testCHI, "main")}},
		"another operation runs on another cluster": {objs: []client.Object{otherOperationOn(t, "second")},
			result: ctrl.Result{RequeueAfter: runningRequeueInterval}, runs: 1},
		"the gate holds it": {gate: blocked, result: ctrl.Result{RequeueAfter: time.Minute}, waits: []string{blocked.reason, blocked.message}},
		"created meanwhile": {funcs: refuseJobs(apiErrors.NewAlreadyExists(batch.Resource("jobs"), "b1-main-backup")), result: ctrl.Result{Requeue: true}},
		"refused":           {funcs: refuseJobs(refused), err: refused},
	} {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, tc.funcs, tc.objs...)
			r := newReconciler(c, c, c.Scheme(), newFakeCR(), api.ClickHouseBackupCRDResourceKind)
			owner := newBackupCR("b1")
			job := &batch.Job{}
			job.Name = "b1-main-backup"
			job.Namespace = testNamespace
			job.Labels = model.Labels(api.ClickHouseBackupCRDResourceKind, owner.GetName(), testCHI, "main", testStorage)
			gate := &startGate{check: func(context.Context) (*pending, error) { return tc.gate, nil }}

			var waits []string
			runs := 0
			result, err := r.startJob(context.Background(), owner, testCHI, "main", job, gate, func(reason, message string) {
				waits = append(waits, reason, message)
			}, func() {
				runs++
			})
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.result, result)
			require.Equal(t, tc.waits, waits)
			require.Equal(t, tc.runs, runs)
			require.Equal(t, len(tc.objs)+tc.runs, jobCount(t, c))
		})
	}
}

// A backup and its verification start their Jobs alike: each waits for another operation on the
// installation in a condition of its own, and leaves a Job of its name created meanwhile to the
// next reconcile, recording nothing about the Job.
func TestBackupStartsItsJobsAlike(t *testing.T) {
	verifying := func() *api.ClickHouseBackup {
		chb := newBackupCR("b1")
		chb.Spec.Verify = types.NewStringBool(true)
		chb.Status.Phase = api.OperationPhaseRunning
		chb.Status.Clusters = []api.OperationClusterStatus{{
			Name: "main", Phase: api.OperationPhaseCompleted, JobName: "b1-main-backup", Reason: api.BackupReasonCompleted, Message: "Job b1-main-backup completed",
		}}
		chb.Status.BackupName = "b1-backup-name"
		newBackupOperation(chb).setCondition(api.BackupConditionReady, meta.ConditionTrue, api.BackupReasonCompleted, "Job b1-main-backup completed")
		return chb
	}
	for name, tc := range map[string]struct {
		chb       *api.ClickHouseBackup
		condition string
		status    meta.ConditionStatus
		phase     api.OperationPhase
	}{
		"backup": {chb: newBackupCR("b1"), condition: api.BackupConditionJobCreated, status: meta.ConditionFalse, phase: api.OperationPhasePending},
		"verify": {chb: verifying(), condition: api.BackupConditionVerified, status: meta.ConditionUnknown, phase: api.OperationPhaseRunning},
	} {
		t.Run(name+" waits for another operation", func(t *testing.T) {
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, tc.chb.DeepCopy(), otherOperation(t))

			require.Equal(t, ctrl.Result{RequeueAfter: pendingRequeueInterval}, f.reconcile("b1"))
			got := f.backup("b1")
			require.Equal(t, tc.phase, got.Status.Phase)
			waiting := condition(t, got.Status.Conditions, tc.condition)
			require.Equal(t, tc.status, waiting.Status)
			require.Equal(t, api.BackupReasonOperationInProgress, waiting.Reason)
			require.Equal(t, operationInProgressMessage(testCHI, "main"), waiting.Message)
			require.Equal(t, 1, jobCount(t, f.client))
		})
		t.Run(name+" leaves a Job created meanwhile to the next reconcile", func(t *testing.T) {
			c := newClient(t, refuseJobs(apiErrors.NewAlreadyExists(batch.Resource("jobs"), "b1")), tc.chb.DeepCopy())
			controller := NewBackupController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))

			result, err := controller.Reconcile(context.Background(), request("b1"))
			require.NoError(t, err)
			require.Equal(t, ctrl.Result{Requeue: true}, result)
			got := &api.ClickHouseBackup{}
			require.NoError(t, c.Get(context.Background(), key("b1"), got))
			require.Equal(t, tc.phase, got.Status.Phase)
			require.Nil(t, apiMeta.FindStatusCondition(got.Status.Conditions, tc.condition))
			require.Zero(t, eventCount(t, c, api.BackupReasonJobCreated))
			require.Zero(t, eventCount(t, c, api.BackupReasonVerifying))
		})
	}
}

// An absence from the cache is confirmed against the API server, which may know an object the
// cache has not caught up with yet. A cache that fails otherwise is not second-guessed.
func TestGetConfirmed(t *testing.T) {
	job := jobOwnedBy(t, "b1-main-backup", newBackupCR("b1"), nil)
	live := newClient(t, nil, job)
	lagging := newClient(t, nil)
	broken := errors.New("cache is broken")
	failing := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return broken
		},
	})
	for name, tc := range map[string]struct {
		cache  client.Client
		reader client.Reader
		err    func(error) bool
	}{
		"in the cache":              {cache: live, reader: lagging},
		"only on the API server":    {cache: lagging, reader: live},
		"nowhere":                   {cache: lagging, reader: lagging, err: apiErrors.IsNotFound},
		"in a cache that is broken": {cache: failing, reader: live, err: func(err error) bool { return errors.Is(err, broken) }},
	} {
		t.Run(name, func(t *testing.T) {
			r := newReconciler(tc.cache, tc.reader, live.Scheme(), newFakeCR(), api.ClickHouseBackupCRDResourceKind)
			got := &batch.Job{}
			err := r.getConfirmed(context.Background(), key("b1-main-backup"), got)
			if tc.err != nil {
				require.True(t, tc.err(err), "unexpected error %v", err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, job.GetName(), got.GetName())
		})
	}
}
