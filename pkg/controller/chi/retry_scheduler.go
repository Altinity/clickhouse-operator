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
	"sync"
	"sync/atomic"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	cmd_queue "github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/queue"
)

const (
	defaultInitialRetryDelay = 10 * time.Second
	defaultMaxRetryDelay     = 300 * time.Second
	defaultGetterTimeout     = 10 * time.Second
)

// crGetter retrieves the latest ClickHouseInstallation object from cache or apiserver.
type crGetter func(ctx context.Context, namespace, name string) (*api.ClickHouseInstallation, error)

// retryToken tracks the cancellation and claim status of a specific scheduled retry instance.
type retryToken struct {
	cancelled atomic.Bool
	claimed   atomic.Bool
}

type retryEntry struct {
	cr           *api.ClickHouseInstallation
	timer        *time.Timer
	currentDelay time.Duration
	failures     int
	generation   uint64
	token        *retryToken
}

// reconcileRetryScheduler coordinates bounded, delayed retries for failed reconciliations
// (such as transient Keeper replica cleanup failures) without blocking worker threads.
//
// Synchronization Invariants:
// 1. Entry Map Protection: s.mu protects s.closed, s.retries, initialDelay, and maxDelay.
// 2. Generation & Token Ownership: Every scheduled retry possesses a monotonically increasing
//    generation counter and a shared retryToken. Replacing (Schedule), cancelling (Cancel/Reset),
//    or shutting down (Close) increments the generation, sets token.cancelled=true, and stops the timer.
// 3. Linearization Point: The second lock acquisition under s.mu in handleTimerCallback is the
//    authoritative linearization point. Prior to this claim, any concurrent Cancel, Reset, Schedule
//    (replacement), or Close sets token.cancelled=true or increments entry.generation, cleanly aborting
//    the callback. After this point, ownership is committed to enqueue.
// 4. Safe Shutdown: Close() sets s.closed=true, cancels all active tokens, cancels s.ctx (unblocking
//    any in-flight API calls), stops timers under s.mu, and then waits on s.wg.Wait(). When Close()
//    returns, no callbacks are running and no callback can ever call s.enqueue.
// 5. Deadlock Prevention: s.mu is never held across queue operations (s.enqueue) or external fetches (s.getCR).
type reconcileRetryScheduler struct {
	mu           sync.Mutex
	closed       bool
	initialDelay time.Duration
	maxDelay     time.Duration
	retries      map[string]*retryEntry
	enqueue      func(queue.PriorityQueueItem)
	getCR        crGetter
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// isCRDeleting safely reports whether the CR has a non-nil, non-zero DeletionTimestamp.
func isCRDeleting(cr *api.ClickHouseInstallation) bool {
	if cr == nil {
		return false
	}
	ts := cr.GetDeletionTimestamp()
	return ts != nil && !ts.IsZero()
}

func newReconcileRetryScheduler(enqueue func(queue.PriorityQueueItem), getCR crGetter) *reconcileRetryScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &reconcileRetryScheduler{
		initialDelay: defaultInitialRetryDelay,
		maxDelay:     defaultMaxRetryDelay,
		retries:      make(map[string]*retryEntry),
		enqueue:      enqueue,
		getCR:        getCR,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// SetDelays overrides retry delays (useful for fast unit tests without sleeps).
func (s *reconcileRetryScheduler) SetDelays(initial, max time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if initial > 0 {
		s.initialDelay = initial
	}
	if max > 0 {
		s.maxDelay = max
	}
}

// Schedule arranges a delayed reconcile retry for the given CHI using bounded exponential backoff.
func (s *reconcileRetryScheduler) Schedule(cr *api.ClickHouseInstallation) {
	if cr == nil || isCRDeleting(cr) || s == nil || s.enqueue == nil {
		return
	}

	key := cr.Namespace + "/" + cr.Name

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	entry, exists := s.retries[key]
	if !exists {
		entry = &retryEntry{
			cr: cr,
		}
		s.retries[key] = entry
	} else {
		// Invalidate previous callback and stop previous timer
		if entry.token != nil {
			entry.token.cancelled.Store(true)
		}
		if entry.timer != nil {
			entry.timer.Stop()
			entry.timer = nil
		}
		entry.cr = cr
	}

	entry.generation++
	entry.token = &retryToken{}
	entry.failures++

	delay := entry.currentDelay
	if delay <= 0 {
		delay = s.initialDelay
	} else {
		delay *= 2
		if delay > s.maxDelay {
			delay = s.maxDelay
		}
	}
	entry.currentDelay = delay

	myGen := entry.generation
	myToken := entry.token
	ns := cr.Namespace
	name := cr.Name
	fallbackCR := entry.cr

	log.V(1).Info(
		"Scheduling delayed retry for CHI %s (failure %d, delay %v, gen %d)",
		key, entry.failures, delay, myGen,
	)

	entry.timer = time.AfterFunc(delay, func() {
		s.handleTimerCallback(key, ns, name, myGen, myToken, fallbackCR)
	})
}

func (s *reconcileRetryScheduler) handleTimerCallback(
	key, ns, name string,
	myGen uint64,
	myToken *retryToken,
	fallbackCR *api.ClickHouseInstallation,
) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	entry, ok := s.retries[key]
	if !ok || entry == nil || entry.generation != myGen || myToken.cancelled.Load() {
		s.mu.Unlock()
		return
	}
	entry.timer = nil
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	// Pre-fetch check: abort if cancelled while waiting for goroutine execution
	if myToken.cancelled.Load() {
		return
	}

	// Fetch latest CR to avoid reconciling stale spec or status
	var targetCR *api.ClickHouseInstallation
	if s.getCR != nil {
		fetchCtx, fetchCancel := context.WithTimeout(s.ctx, defaultGetterTimeout)
		latest, err := s.getCR(fetchCtx, ns, name)
		fetchCancel()

		if err != nil || latest == nil {
			// CR no longer exists or API call failed; reset retry entry
			s.Reset(key)
			return
		}
		targetCR = latest
	} else {
		targetCR = fallbackCR
	}

	// Verify target CR is not currently being deleted
	if isCRDeleting(targetCR) {
		s.Reset(key)
		return
	}

	// Linearization Point: Atomically commit callback ownership before enqueueing
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	entry, ok = s.retries[key]
	if !ok || entry.generation != myGen || myToken.cancelled.Load() {
		s.mu.Unlock()
		return
	}
	myToken.claimed.Store(true)
	s.mu.Unlock()

	log.V(1).Info("Triggering scheduled retry for CHI %s (gen %d)", key, myGen)
	if s.enqueue != nil {
		s.enqueue(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, targetCR))
	}
}

// Cancel stops any pending retry timer for the CHI while preserving backoff history
// so consecutive failures continue exponential backoff.
func (s *reconcileRetryScheduler) Cancel(key string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, exists := s.retries[key]; exists {
		entry.generation++
		if entry.token != nil {
			entry.token.cancelled.Store(true)
		}
		if entry.timer != nil {
			entry.timer.Stop()
			entry.timer = nil
		}
	}
}

// Reset clears pending retry timers and completely removes the entry, resetting backoff state.
func (s *reconcileRetryScheduler) Reset(key string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, exists := s.retries[key]; exists {
		entry.generation++
		if entry.token != nil {
			entry.token.cancelled.Store(true)
		}
		if entry.timer != nil {
			entry.timer.Stop()
			entry.timer = nil
		}
		delete(s.retries, key)
	}
}

// Close stops all active timers, cancels in-flight fetches, and prevents future scheduling (called on controller shutdown).
func (s *reconcileRetryScheduler) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	for _, entry := range s.retries {
		entry.generation++
		if entry.token != nil {
			entry.token.cancelled.Store(true)
		}
		if entry.timer != nil {
			entry.timer.Stop()
			entry.timer = nil
		}
	}
	s.retries = make(map[string]*retryEntry)
	s.mu.Unlock()

	s.wg.Wait()
}
