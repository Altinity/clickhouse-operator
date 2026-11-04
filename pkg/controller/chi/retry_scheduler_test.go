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

package chi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/queue"
)

func TestRetryScheduler_SchedulesDelayedRetry(t *testing.T) {
	var enqueued []queue.PriorityQueueItem
	var mu sync.Mutex

	enqueueFunc := func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		enqueued = append(enqueued, item)
	}

	scheduler := newReconcileRetryScheduler(enqueueFunc, nil)
	scheduler.SetDelays(10*time.Millisecond, 50*time.Millisecond)
	defer scheduler.Close()

	cr := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	cr.Namespace = "test-ns"
	cr.Name = "test-chi"

	scheduler.Schedule(cr)

	// Immediately, nothing should be enqueued yet
	mu.Lock()
	assert.Len(t, enqueued, 0, "Item must not be enqueued immediately")
	mu.Unlock()

	// Wait for the timer to fire
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(enqueued) == 1
	}, 1*time.Second, 5*time.Millisecond, "Scheduled retry must fire after delay")

	mu.Lock()
	defer mu.Unlock()
	cmd, ok := enqueued[0].(*cmd_queue.ReconcileCHI)
	require.True(t, ok)
	assert.Equal(t, cmd_queue.ReconcileAdd, cmd.Cmd)
	assert.Equal(t, "test-ns", cmd.New.Namespace)
	assert.Equal(t, "test-chi", cmd.New.Name)
}

func TestRetryScheduler_NonConcurrentExecution(t *testing.T) {
	var queueDoneCalled bool
	var retryFiredAfterDone bool
	var mu sync.Mutex

	enqueueFunc := func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		if queueDoneCalled {
			retryFiredAfterDone = true
		}
	}

	scheduler := newReconcileRetryScheduler(enqueueFunc, nil)
	scheduler.SetDelays(15*time.Millisecond, 50*time.Millisecond)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	cr := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	cr.Namespace = "test-ns"
	cr.Name = "test-chi"
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileUpdate, nil, cr)

	// 1. Process fails with retryable drop error
	dropErr := fmt.Errorf("%w: timeout waiting for keeper session", ErrZKReplicaDropRetryable)

	// 2. Queue.Done is called, simulating worker completing queue processing
	mu.Lock()
	queueDoneCalled = true
	mu.Unlock()

	// 3. handlePostReconcileRetry schedules retry
	w.handlePostReconcileRetry(item, dropErr)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return retryFiredAfterDone
	}, 1*time.Second, 5*time.Millisecond)
}

func TestRetryScheduler_ReachesNormalReconciliationPath(t *testing.T) {
	var enqueuedCmd *cmd_queue.ReconcileCHI
	var mu sync.Mutex

	scheduler := newReconcileRetryScheduler(func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		if cmd, ok := item.(*cmd_queue.ReconcileCHI); ok {
			enqueuedCmd = cmd
		}
	}, nil)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	cr := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	cr.Namespace = "prod"
	cr.Name = "analytics"
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, cr)

	dropErr := fmt.Errorf("%w: replica drop failed", ErrZKReplicaDropRetryable)
	w.handlePostReconcileRetry(item, dropErr)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return enqueuedCmd != nil
	}, 1*time.Second, 2*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, cmd_queue.ReconcileAdd, enqueuedCmd.Cmd)
	assert.Equal(t, "prod", enqueuedCmd.New.Namespace)
	assert.Equal(t, "analytics", enqueuedCmd.New.Name)
}

func TestRetryScheduler_RediscoversRemovedReplica(t *testing.T) {
	oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	oldCR.Namespace = "test-ns"
	oldCR.Name = "test-chi"

	newCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	newCR.Namespace = "test-ns"
	newCR.Name = "test-chi"
	newCR.SetAncestor(oldCR)

	var retriedCR *api.ClickHouseInstallation
	var mu sync.Mutex

	scheduler := newReconcileRetryScheduler(func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		if cmd, ok := item.(*cmd_queue.ReconcileCHI); ok {
			retriedCR = cmd.New
		}
	}, nil)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	dropErr := fmt.Errorf("%w: keeper timeout", ErrZKReplicaDropRetryable)
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, newCR)
	w.handlePostReconcileRetry(item, dropErr)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return retriedCR != nil
	}, 1*time.Second, 2*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, retriedCR.GetAncestorT(), "Ancestor must be preserved")
	actionPlan := api.MakeActionPlan(retriedCR.GetAncestorT(), retriedCR)
	var removedCount int
	actionPlan.WalkRemoved(func(api.ICluster) {}, func(api.IShard) {}, func(h *api.Host) {
		removedCount++
		assert.Equal(t, "h1", h.GetName())
	})
	assert.Equal(t, 1, removedCount, "Retried reconcile must still see removed replica h1")
}

func TestRetryScheduler_SuccessfulRetryResetsBackoff(t *testing.T) {
	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {}, nil)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	cr := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	cr.Namespace = "ns"
	cr.Name = "chi"
	key := "ns/chi"

	// Simulate failure
	scheduler.Schedule(cr)

	scheduler.mu.Lock()
	assert.Contains(t, scheduler.retries, key)
	scheduler.mu.Unlock()

	// Pass with err == nil (reconcile succeeded)
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, cr)
	w.handlePostReconcileRetry(item, nil)

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	assert.NotContains(t, scheduler.retries, key, "Success must delete entry and reset backoff")
}

func TestRetryScheduler_SuccessfulInitialCleanupDoesNotScheduleRetry(t *testing.T) {
	var enqueuedCount int
	var mu sync.Mutex

	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		enqueuedCount++
	}, nil)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	cr := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	cr.Namespace = "ns"
	cr.Name = "chi"

	// Success (err == nil)
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, cr)
	w.handlePostReconcileRetry(item, nil)

	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, enqueuedCount, "Successful reconcile must never schedule a retry")
}

func TestRetryScheduler_ReplicaReaddedBeforeScheduledRetry(t *testing.T) {
	var enqueuedCR *api.ClickHouseInstallation
	var mu sync.Mutex

	scheduler := newReconcileRetryScheduler(func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		if cmd, ok := item.(*cmd_queue.ReconcileCHI); ok {
			enqueuedCR = cmd.New
		}
	}, nil)
	scheduler.SetDelays(15*time.Millisecond, 50*time.Millisecond)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	oldCR.Namespace = "ns"
	oldCR.Name = "chi"

	downscaledCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	downscaledCR.Namespace = "ns"
	downscaledCR.Name = "chi"
	downscaledCR.SetAncestor(oldCR)

	// Fail drop
	dropErr := fmt.Errorf("%w: timeout", ErrZKReplicaDropRetryable)
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, downscaledCR)
	w.handlePostReconcileRetry(item, dropErr)

	// User re-adds replica h1 before the timer fires
	readdedCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	readdedCR.Namespace = "ns"
	readdedCR.Name = "chi"
	readdedCR.SetAncestor(oldCR)

	// User update starts: cancels pending retry timer
	_ = w.updateCHI(context.Background(), downscaledCR, readdedCR)

	scheduler.mu.Lock()
	entry := scheduler.retries["ns/chi"]
	scheduler.mu.Unlock()
	require.NotNil(t, entry)
	assert.Nil(t, entry.timer, "Starting new reconcile must stop pending retry timer")
	assert.True(t, entry.token.cancelled.Load(), "Starting new reconcile must mark token cancelled")

	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Nil(t, enqueuedCR, "Cancelled retry must not have fired")
}

func TestRetryScheduler_MultipleFailuresRespectBackoff(t *testing.T) {
	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {}, nil)
	scheduler.SetDelays(10*time.Millisecond, 80*time.Millisecond)
	defer scheduler.Close()

	cr := &api.ClickHouseInstallation{}
	cr.Namespace = "ns"
	cr.Name = "chi"
	key := "ns/chi"

	// Failure 1: 10ms
	scheduler.Schedule(cr)
	assert.Equal(t, 1, scheduler.retries[key].failures)
	assert.Equal(t, 10*time.Millisecond, scheduler.retries[key].currentDelay)

	// Failure 2: 20ms
	scheduler.Schedule(cr)
	assert.Equal(t, 2, scheduler.retries[key].failures)
	assert.Equal(t, 20*time.Millisecond, scheduler.retries[key].currentDelay)

	// Failure 3: 40ms
	scheduler.Schedule(cr)
	assert.Equal(t, 3, scheduler.retries[key].failures)
	assert.Equal(t, 40*time.Millisecond, scheduler.retries[key].currentDelay)

	// Failure 4: 80ms (capped)
	scheduler.Schedule(cr)
	assert.Equal(t, 4, scheduler.retries[key].failures)
	assert.Equal(t, 80*time.Millisecond, scheduler.retries[key].currentDelay)

	// Failure 5: 80ms (still capped at maxDelay)
	scheduler.Schedule(cr)
	assert.Equal(t, 5, scheduler.retries[key].failures)
	assert.Equal(t, 80*time.Millisecond, scheduler.retries[key].currentDelay)
}

func TestRetryScheduler_CloseCancelsTimersNoLeak(t *testing.T) {
	var enqueuedCount int
	var mu sync.Mutex

	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		enqueuedCount++
	}, nil)
	scheduler.SetDelays(20*time.Millisecond, 50*time.Millisecond)

	cr := &api.ClickHouseInstallation{}
	cr.Namespace = "ns"
	cr.Name = "chi"

	scheduler.Schedule(cr)

	// Close immediately (simulates operator shutdown)
	scheduler.Close()

	time.Sleep(40 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, enqueuedCount, "Closed scheduler must cancel all timers and not fire callbacks")
}

func TestRetryScheduler_DoesNotScheduleOnContextCancellation(t *testing.T) {
	var enqueuedCount int
	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {
		enqueuedCount++
	}, nil)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	cr := &api.ClickHouseInstallation{}
	cr.Namespace = "ns"
	cr.Name = "chi"
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, cr)

	// Error caused by context cancellation
	cancelErr := fmt.Errorf("%w: %w", ErrZKReplicaDropRetryable, context.Canceled)
	w.handlePostReconcileRetry(item, cancelErr)

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 0, enqueuedCount, "Context cancellation must not schedule retries")
}

func TestRetryScheduler_DoesNotScheduleOnPermanentError(t *testing.T) {
	var enqueuedCount int
	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {
		enqueuedCount++
	}, nil)
	defer scheduler.Close()

	ctrl := &Controller{retryScheduler: scheduler}
	w := &worker{c: ctrl}

	cr := &api.ClickHouseInstallation{}
	cr.Namespace = "ns"
	cr.Name = "chi"
	item := cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, cr)

	// Permanent error (e.g. auth failed, syntax error)
	permErr := fmt.Errorf("%w: authentication failed", ErrZKReplicaDropPermanent)
	w.handlePostReconcileRetry(item, permErr)

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 0, enqueuedCount, "Permanent error must not schedule retries")
}

func TestRetryScheduler_Race_CancelWithCallbackExecution(t *testing.T) {
	// Tests race condition where Cancel() is called right as the callback is executing.
	var enqueuedCount int
	var mu sync.Mutex

	gate := make(chan struct{})
	getterCalled := make(chan struct{})

	getCR := func(ctx context.Context, ns, name string) (*api.ClickHouseInstallation, error) {
		close(getterCalled)
		<-gate // block callback inside getter
		return &api.ClickHouseInstallation{
			ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name},
		}, nil
	}

	scheduler := newReconcileRetryScheduler(func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		enqueuedCount++
	}, getCR)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"},
	}
	scheduler.Schedule(cr)

	// Wait until callback enters getCR
	<-getterCalled

	// Now cancel while callback is paused inside getCR
	scheduler.Cancel("ns/chi")

	// Release getter
	close(gate)

	// Closing the scheduler drains the in-flight callback deterministically via s.wg.Wait()
	scheduler.Close()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, enqueuedCount, "Cancelled callback must detect token cancellation and not enqueue")
}

func TestRetryScheduler_Race_ReplaceTimerWithCallbackExecution(t *testing.T) {
	// Tests race where a new Schedule replaces an existing entry while callback 1 is in-flight.
	var enqueuedGen []int64
	var mu sync.Mutex

	gate := make(chan struct{})
	firstGetterEntered := make(chan struct{})
	var callbackCount atomic.Int32

	getCR := func(ctx context.Context, ns, name string) (*api.ClickHouseInstallation, error) {
		count := callbackCount.Add(1)
		if count == 1 {
			close(firstGetterEntered)
			<-gate // pause first callback
		}
		return &api.ClickHouseInstallation{
			ObjectMeta: meta.ObjectMeta{
				Namespace:  ns,
				Name:       name,
				Generation: int64(count),
			},
		}, nil
	}

	scheduler := newReconcileRetryScheduler(func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		if cmd, ok := item.(*cmd_queue.ReconcileCHI); ok && cmd.New != nil {
			enqueuedGen = append(enqueuedGen, cmd.New.Generation)
		}
	}, getCR)
	scheduler.SetDelays(5*time.Millisecond, 50*time.Millisecond)
	defer scheduler.Close()

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"},
	}
	scheduler.Schedule(cr)

	// Wait for callback 1 to enter getter
	<-firstGetterEntered

	// Schedule again (supersedes callback 1)
	scheduler.Schedule(cr)

	// Release first callback
	close(gate)

	// Wait for both to settle
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(enqueuedGen) == 1
	}, 1*time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// Only callback 2 should have successfully enqueued
	assert.Equal(t, []int64{2}, enqueuedGen, "Superceded callback 1 must not enqueue; only callback 2 enqueues")
}

func TestRetryScheduler_Race_ShutdownWithCallbackExecution(t *testing.T) {
	// Tests that Close() aborts in-flight CR fetches via context cancellation,
	// waits for in-flight callbacks, and guarantees no enqueueing occurs.
	var enqueuedCount atomic.Int32
	getterEntered := make(chan struct{})

	getCR := func(ctx context.Context, ns, name string) (*api.ClickHouseInstallation, error) {
		close(getterEntered)
		// Block until context is cancelled by Close()
		<-ctx.Done()
		return nil, ctx.Err()
	}

	scheduler := newReconcileRetryScheduler(func(item queue.PriorityQueueItem) {
		enqueuedCount.Add(1)
	}, getCR)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"},
	}
	scheduler.Schedule(cr)

	<-getterEntered

	// Close() cancels scheduler context and waits for callback to exit
	scheduler.Close()

	assert.Equal(t, int32(0), enqueuedCount.Load(), "Callback must abort on Close() and never enqueue")
}

func TestRetryScheduler_Linearization_CloseWaitsForInFlightEnqueue(t *testing.T) {
	// Proves that if a callback has already passed the linearization claim point and is executing
	// s.enqueue(), Close() synchronously waits on s.wg.Wait() until enqueue finishes.
	enqueueStarted := make(chan struct{})
	enqueueDone := make(chan struct{})
	var enqueueFinished atomic.Bool

	enqueueFunc := func(item queue.PriorityQueueItem) {
		close(enqueueStarted)
		time.Sleep(20 * time.Millisecond)
		enqueueFinished.Store(true)
		close(enqueueDone)
	}

	scheduler := newReconcileRetryScheduler(enqueueFunc, nil)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"},
	}
	scheduler.Schedule(cr)

	// Wait until callback claims ownership and enters enqueueFunc
	<-enqueueStarted

	// Call Close() while enqueue is in progress
	closeReturned := make(chan struct{})
	go func() {
		scheduler.Close()
		close(closeReturned)
	}()

	// Close() must NOT return before enqueue completes
	select {
	case <-closeReturned:
		t.Fatal("Close() returned before in-flight enqueue completed!")
	case <-enqueueDone:
		// Enqueue completed while Close() was waiting
	}

	// Now Close() should return cleanly
	<-closeReturned
	assert.True(t, enqueueFinished.Load(), "Enqueue must be finished when Close() returns")
}

func TestRetryScheduler_Linearization_CancelAfterClaim(t *testing.T) {
	// Proves that once callback claims ownership at the linearization point,
	// subsequent Cancel() does not prevent the claimed enqueue, corrupt state, or deadlock.
	enqueueStarted := make(chan struct{})
	enqueueDone := make(chan struct{})
	gate := make(chan struct{})
	var enqueuedCount atomic.Int32

	enqueueFunc := func(item queue.PriorityQueueItem) {
		close(enqueueStarted)
		<-gate
		enqueuedCount.Add(1)
		close(enqueueDone)
	}

	scheduler := newReconcileRetryScheduler(enqueueFunc, nil)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"},
	}
	scheduler.Schedule(cr)

	<-enqueueStarted

	// Cancel while enqueue is in flight (after claim point)
	scheduler.Cancel("ns/chi")

	close(gate)
	<-enqueueDone

	assert.Equal(t, int32(1), enqueuedCount.Load(), "Claimed callback must successfully complete enqueue despite subsequent Cancel()")
}

func TestRetryScheduler_StaleCRState_UsesLatestFromGetter(t *testing.T) {
	var enqueuedCR *api.ClickHouseInstallation
	var mu sync.Mutex

	// CR captured at Schedule() time had Generation 1
	staleCR := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace:  "ns",
			Name:       "chi",
			Generation: 1,
		},
	}

	// CR in cluster was updated to Generation 2 before timer fired
	freshCR := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace:  "ns",
			Name:       "chi",
			Generation: 2,
		},
	}

	getCR := func(ctx context.Context, ns, name string) (*api.ClickHouseInstallation, error) {
		return freshCR, nil
	}

	scheduler := newReconcileRetryScheduler(func(item queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		if cmd, ok := item.(*cmd_queue.ReconcileCHI); ok {
			enqueuedCR = cmd.New
		}
	}, getCR)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	scheduler.Schedule(staleCR)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return enqueuedCR != nil
	}, 1*time.Second, 2*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, int64(2), enqueuedCR.Generation, "Retry must enqueue the latest CR from getCR, not stale captured state")
}

func TestRetryScheduler_CRDeletionBeforeTimerExecution(t *testing.T) {
	var enqueuedCount int
	var mu sync.Mutex

	// Getter returns CR that is being deleted
	now := meta.Now()
	deletingCR := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace:         "ns",
			Name:              "chi",
			DeletionTimestamp: &now,
		},
	}

	getCR := func(ctx context.Context, ns, name string) (*api.ClickHouseInstallation, error) {
		return deletingCR, nil
	}

	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		enqueuedCount++
	}, getCR)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"},
	}
	scheduler.Schedule(cr)

	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, enqueuedCount, "CR with DeletionTimestamp must not be retried")
}

func TestRetryScheduler_CRDeletedFromClusterBeforeTimerExecution(t *testing.T) {
	var enqueuedCount int
	var mu sync.Mutex

	getCR := func(ctx context.Context, ns, name string) (*api.ClickHouseInstallation, error) {
		return nil, errors.New("not found")
	}

	scheduler := newReconcileRetryScheduler(func(queue.PriorityQueueItem) {
		mu.Lock()
		defer mu.Unlock()
		enqueuedCount++
	}, getCR)
	scheduler.SetDelays(5*time.Millisecond, 20*time.Millisecond)
	defer scheduler.Close()

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"},
	}
	scheduler.Schedule(cr)

	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, enqueuedCount, "CR deleted from cluster must not be retried")
}

func TestIsCRDeleting(t *testing.T) {
	// 1. Nil CR
	assert.False(t, isCRDeleting(nil))

	// 2. Normal CR with nil DeletionTimestamp
	crNormal := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Name: "chi"},
	}
	assert.Nil(t, crNormal.DeletionTimestamp)
	assert.False(t, isCRDeleting(crNormal))

	// 3. CR with zero DeletionTimestamp
	zeroTime := meta.Time{}
	crZeroTime := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Name:              "chi",
			DeletionTimestamp: &zeroTime,
		},
	}
	assert.False(t, isCRDeleting(crZeroTime))

	// 4. CR being deleted (non-nil, non-zero DeletionTimestamp)
	activeTime := meta.Now()
	crDeleting := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Name:              "chi",
			DeletionTimestamp: &activeTime,
		},
	}
	assert.True(t, isCRDeleting(crDeleting))
}
