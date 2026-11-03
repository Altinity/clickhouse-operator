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

package poller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A spent budget must be distinguishable from a Get that failed, because a caller escalates to a
// destructive action on one and not the other. Nothing else pins this: every consumer test injects
// an already-wrapped error through a fake, so dropping the %w here would leave those tests green
// while the real escalation silently stopped firing.
func TestPollReturnsErrTimeoutWhenBudgetSpent(t *testing.T) {
	err := New(context.Background(), "test").
		WithOptions(&Options{
			Timeout:      10 * time.Millisecond,
			MainInterval: time.Millisecond,
		}).
		WithFunctions(&Functions{
			Get:    func(context.Context) (any, error) { return struct{}{}, nil },
			IsDone: func(context.Context, any) bool { return false },
		}).
		Poll()

	require.Error(t, err)
	require.ErrorIs(t, err, ErrTimeout, "a spent budget must carry ErrTimeout so callers can tell it from a Get failure")
	require.Contains(t, err.Error(), "poll(test)", "the rendered message must keep naming the poll")
}

// A Get failure is NOT a spent budget: the poller abandons on it within milliseconds, so a caller
// that escalates on any error would act against an object it never actually waited for.
func TestPollGetFailureIsNotErrTimeout(t *testing.T) {
	getErr := errors.New("etcdserver: request timed out")

	err := New(context.Background(), "test").
		WithOptions(&Options{
			Timeout:      time.Minute,
			MainInterval: time.Millisecond,
		}).
		WithFunctions(&Functions{
			Get:    func(context.Context) (any, error) { return nil, getErr },
			IsDone: func(context.Context, any) bool { return false },
		}).
		Poll()

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrTimeout, "a Get failure must not masquerade as a spent budget")
}
