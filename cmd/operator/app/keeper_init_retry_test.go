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

package app

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// An ordinary API-server-state failure. Its shape is deliberately unremarkable: the retry must
// key off the sentinel initKeeper attaches, never off how controller-runtime happens to wrap a
// dial error today, so an error with no special structure has to be retried.
func outageErr() error {
	return errors.New("failed to get server groups: connection refused")
}

// shrinkBackoff keeps the multi-attempt cases instant. The shipped schedule is asserted separately
// by TestKeeperInitRetryBackoffSchedule.
//
// It mutates a package var and restores it in Cleanup, which is safe only because nothing in this
// package calls t.Parallel(). Do not add it here without giving this a different seam.
func shrinkBackoff(t *testing.T) {
	t.Helper()
	saved := keeperInitRetryBackoff
	keeperInitRetryBackoff = wait.Backoff{Duration: time.Microsecond, Factor: 1.0, Steps: 1}
	t.Cleanup(func() { keeperInitRetryBackoff = saved })
}

func TestInitKeeperWithRetryStopsOnFirstSuccess(t *testing.T) {
	shrinkBackoff(t)

	calls := 0
	err := initKeeperWithRetry(context.Background(), func(context.Context) error {
		calls++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, calls, "a successful init must not be retried")
}

func TestInitKeeperWithRetryRecoversFromTransientFailure(t *testing.T) {
	shrinkBackoff(t)

	calls := 0
	err := initKeeperWithRetry(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return outageErr()
		}
		return nil
	})

	require.NoError(t, err, "an outage that clears must not surface as an error")
	require.Equal(t, 3, calls)
}

// The property that replaced the attempt budget: initKeeper runs only at start-up, so giving up
// disables Keeper for the lifetime of the process. An outage lasting longer than any budget we
// might have picked must therefore still be survivable.
func TestInitKeeperWithRetryNeverGivesUp(t *testing.T) {
	shrinkBackoff(t)

	// Large enough that no plausible budget could pass this by accident - the design it replaced
	// allowed nine attempts.
	const outageAttempts = 500

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	err := initKeeperWithRetry(ctx, func(context.Context) error {
		calls++
		if calls < outageAttempts {
			return outageErr()
		}
		return nil
	})

	require.NoError(t, err, "an outage of any length must still resolve once the API server returns")
	require.Equal(t, outageAttempts, calls, "no fixed budget may cut the retry short")
}

// Scheme registration is deterministic, so retrying it would retry forever without ever changing
// the outcome. initKeeper marks such failures at the source; only that marking stops the retry.
func TestInitKeeperWithRetryDoesNotRetryTerminalError(t *testing.T) {
	shrinkBackoff(t)

	cause := errors.New("unable to AddToScheme")
	calls := 0
	err := initKeeperWithRetry(context.Background(), func(context.Context) error {
		calls++
		return fmt.Errorf("%w: %w", errKeeperInitTerminal, cause)
	})

	require.ErrorIs(t, err, errKeeperInitTerminal)
	require.ErrorIs(t, err, cause, "the underlying cause must survive the wrap, it is what gets logged")
	require.Equal(t, 1, calls, "a terminal error must not be retried")
}

// The failures worth waiting out - RBAC that has not propagated yet, apps/v1 briefly absent from
// discovery - arrive as ordinary errors, and an earlier revision classified exactly these as
// terminal by routing them through IsTransientAPIError. Nothing about the error's content is
// matched here; the string is illustrative, and what is pinned is that an unmarked error retries.
func TestInitKeeperWithRetryRetriesUnmarkedError(t *testing.T) {
	shrinkBackoff(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	_ = initKeeperWithRetry(ctx, func(context.Context) error {
		calls++
		if calls < 4 {
			return errors.New("forbidden: User cannot list resource statefulsets")
		}
		return nil
	})

	require.Equal(t, 4, calls, "a boot-time API-server condition must be waited out, not given up on")
}

// A cancelled context means the operator is shutting down, and the retry is otherwise unbounded -
// so this is the only thing that ends it. Without it the goroutine would keep rebuilding a manager
// nobody will start, and the WaitGroup in Run would never come back.
func TestInitKeeperWithRetryAbandonsOnContextDone(t *testing.T) {
	// Deliberately NOT shrunk: a loop that ignored ctx would have to wait out a real back-off, so
	// this also proves the abandon happens during the wait rather than after it.
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	err := initKeeperWithRetry(ctx, func(context.Context) error {
		calls++
		cancel()
		return outageErr()
	})

	require.Error(t, err)
	require.Equal(t, 1, calls, "retrying past cancellation would keep the process from shutting down")
	require.True(t, util.IsContextDone(ctx),
		"launchKeeper reports shutdown differently from a real failure, and uses ctx to tell them apart")
}

// The schedule has to satisfy two opposing needs: catch a brief blip quickly, then stop filling
// the log forever in an environment that will never come good.
func TestKeeperInitRetryBackoffSchedule(t *testing.T) {
	backoff := keeperInitRetryBackoff

	var elapsed time.Duration
	attemptsInFirstMinute := 0
	for elapsed < time.Minute {
		elapsed += backoff.Step()
		attemptsInFirstMinute++
	}
	require.GreaterOrEqual(t, attemptsInFirstMinute, 5,
		"a blip that clears in seconds must not wait minutes to be noticed")

	// Step() keeps returning the cap once Steps is spent, which is what makes the retry unbounded
	// in time. Walk well past Steps to pin that it neither stops nor grows without limit.
	ceiling := time.Duration(float64(backoff.Cap) * (1.0 + backoff.Jitter))
	for step := 0; step < 100; step++ {
		delay := backoff.Step()
		require.Greater(t, delay, time.Duration(0), "the retry must never spin without delay")
		require.LessOrEqual(t, delay, ceiling, "an uncapped doubling would stop retrying in practice")
	}
	require.GreaterOrEqual(t, backoff.Step(), time.Duration(float64(backoff.Cap)*0.5),
		"the steady-state delay must stay near the cap, not decay back to rapid retries")
}
