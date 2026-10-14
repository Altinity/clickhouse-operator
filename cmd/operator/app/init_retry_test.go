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
// key off the sentinel an initializer attaches, never off how controller-runtime happens to wrap a
// dial error today, so an error with no special structure has to be retried.
func outageErr() error {
	return errors.New("failed to get server groups: connection refused")
}

// shrinkBackoff keeps the multi-attempt cases instant. The shipped schedule is asserted separately
// by TestInitRetryBackoffSchedule.
//
// It mutates a package var and restores it in Cleanup, which is safe only because nothing in this
// package calls t.Parallel(). Do not add it here without giving this a different seam.
func shrinkBackoff(t *testing.T) {
	t.Helper()
	saved := initRetryBackoff
	initRetryBackoff = wait.Backoff{Duration: time.Microsecond, Factor: 1.0, Steps: 1}
	t.Cleanup(func() { initRetryBackoff = saved })
}

func TestInitWithRetryStopsOnFirstSuccess(t *testing.T) {
	shrinkBackoff(t)

	calls := 0
	err := initWithRetry(context.Background(), keeperComponent, func(context.Context) error {
		calls++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, calls, "a successful init must not be retried")
}

func TestInitWithRetryRecoversFromTransientFailure(t *testing.T) {
	shrinkBackoff(t)

	calls := 0
	err := initWithRetry(context.Background(), keeperComponent, func(context.Context) error {
		calls++
		if calls < 3 {
			return outageErr()
		}
		return nil
	})

	require.NoError(t, err, "an outage that clears must not surface as an error")
	require.Equal(t, 3, calls)
}

// The property that replaced the attempt budget: an initializer runs only at start-up, so giving
// up disables its controllers for the lifetime of the process. An outage lasting longer than any budget we
// might have picked must therefore still be survivable.
func TestInitWithRetryNeverGivesUp(t *testing.T) {
	shrinkBackoff(t)

	// Large enough that no plausible budget could pass this by accident - the design it replaced
	// allowed nine attempts.
	const outageAttempts = 500

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	err := initWithRetry(ctx, keeperComponent, func(context.Context) error {
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
// the outcome. The initializer marks such failures at the source; only that marking stops the retry.
func TestInitWithRetryDoesNotRetryTerminalError(t *testing.T) {
	shrinkBackoff(t)

	cause := errors.New("unable to AddToScheme")
	calls := 0
	err := initWithRetry(context.Background(), keeperComponent, func(context.Context) error {
		calls++
		return fmt.Errorf("%w: %w", errInitTerminal, cause)
	})

	require.ErrorIs(t, err, errInitTerminal)
	require.ErrorIs(t, err, cause, "the underlying cause must survive the wrap, it is what gets logged")
	require.Equal(t, 1, calls, "a terminal error must not be retried")
}

// The failures worth waiting out - RBAC that has not propagated yet, apps/v1 briefly absent from
// discovery - arrive as ordinary errors, and an earlier revision classified exactly these as
// terminal by routing them through IsTransientAPIError. Nothing about the error's content is
// matched here; the string is illustrative, and what is pinned is that an unmarked error retries.
func TestInitWithRetryRetriesUnmarkedError(t *testing.T) {
	shrinkBackoff(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	_ = initWithRetry(ctx, keeperComponent, func(context.Context) error {
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
func TestInitWithRetryAbandonsOnContextDone(t *testing.T) {
	// Deliberately NOT shrunk: a loop that ignored ctx would have to wait out a real back-off, so
	// this also proves the abandon happens during the wait rather than after it.
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	err := initWithRetry(ctx, keeperComponent, func(context.Context) error {
		calls++
		cancel()
		return outageErr()
	})

	require.Error(t, err)
	require.Equal(t, 1, calls, "retrying past cancellation would keep the process from shutting down")
	require.True(t, util.IsContextDone(ctx),
		"launchKeeper and launchBackup report shutdown differently from a real failure, and use ctx to tell them apart")
}

// Keeper and backup initialize side by side through the same helper. Neither may inherit the
// other's progress - Step() mutates the backoff it is called on - nor its fate: a backup that can
// never start must leave Keeper's retry untouched, and the other way round.
func TestInitWithRetryKeepsComponentsIndependent(t *testing.T) {
	shrinkBackoff(t)

	keeperCalls, backupCalls := 0, 0
	keeperErr := make(chan error, 1)
	backupErr := make(chan error, 1)

	go func() {
		keeperErr <- initWithRetry(context.Background(), keeperComponent, func(context.Context) error {
			keeperCalls++
			if keeperCalls < 3 {
				return outageErr()
			}
			return nil
		})
	}()
	go func() {
		backupErr <- initWithRetry(context.Background(), backupComponent, func(context.Context) error {
			backupCalls++
			return fmt.Errorf("%w: %w", errInitTerminal, errors.New("unable to AddToScheme"))
		})
	}()

	require.NoError(t, <-keeperErr, "a terminal backup failure must not stop Keeper from starting")
	require.ErrorIs(t, <-backupErr, errInitTerminal)
	require.Equal(t, 3, keeperCalls)
	require.Equal(t, 1, backupCalls)
}

// The schedule has to satisfy two opposing needs: catch a brief blip quickly, then stop filling
// the log forever in an environment that will never come good.
func TestInitRetryBackoffSchedule(t *testing.T) {
	backoff := initRetryBackoff

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
