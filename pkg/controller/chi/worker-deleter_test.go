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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
)

type fakeSchemer struct {
	isHostActiveReplicaFunc func(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool
	hostDropReplicaFunc     func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error

	activeCalls int
	dropCalls   int
}

func (f *fakeSchemer) IsHostActiveReplica(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool {
	f.activeCalls++
	if f.isHostActiveReplicaFunc != nil {
		return f.isHostActiveReplicaFunc(ctx, hostToRunOn, hostToCheck)
	}
	return false
}

func (f *fakeSchemer) HostDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.Host) error {
	f.dropCalls++
	if f.hostDropReplicaFunc != nil {
		return f.hostDropReplicaFunc(ctx, hostToRunOn, hostToDrop, f.dropCalls)
	}
	return nil
}

func (f *fakeSchemer) HostSyncTables(ctx context.Context, host *api.Host) error                       { return nil }
func (f *fakeSchemer) HostCreateTables(ctx context.Context, host *api.Host) error                     { return nil }
func (f *fakeSchemer) HostDropTables(ctx context.Context, host *api.Host) error                       { return nil }
func (f *fakeSchemer) IsHostInCluster(ctx context.Context, host *api.Host) bool                       { return true }
func (f *fakeSchemer) HostActiveQueriesNum(ctx context.Context, host *api.Host) (int, error)          { return 0, nil }
func (f *fakeSchemer) HostClickHouseVersion(ctx context.Context, host *api.Host) (string, error)       { return "", nil }
func (f *fakeSchemer) HostMaxReplicaDelay(ctx context.Context, host *api.Host) (int, error)           { return 0, nil }
func (f *fakeSchemer) HostShutdown(ctx context.Context, host *api.Host) error                         { return nil }
func (f *fakeSchemer) ExecHost(ctx context.Context, host *api.Host, SQLs []string, _opts ...*clickhouse.QueryOptions) error { return nil }
func (f *fakeSchemer) ExecCluster(ctx context.Context, cluster *api.Cluster, SQLs []string, _opts ...*clickhouse.QueryOptions) error { return nil }
func (f *fakeSchemer) HostClusterDoesNotExistErrorCount(ctx context.Context, host *api.Host) (int, error) { return 0, nil }

func TestWaitHostReplicaInactive_NilHost(t *testing.T) {
	w := &worker{}
	err := w.waitHostReplicaInactive(context.Background(), &api.Host{}, nil, 0, 0)
	assert.NoError(t, err)
}

func TestWaitHostReplicaInactive_ContextDone(t *testing.T) {
	w := &worker{}
	hostToRunOn := &api.Host{}
	hostToDrop := &api.Host{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel context immediately

	err := w.waitHostReplicaInactive(ctx, hostToRunOn, hostToDrop, 1*time.Millisecond, 0)
	assert.Equal(t, context.Canceled, err)
}

func TestIsReplicaActiveError(t *testing.T) {
	err305 := errors.New("Code: 305, Can't drop replica chi-dev-0-1, because it's active")
	errTextVariant1 := errors.New("Can't drop replica chi-dev-0-1, because it's active")
	errTextVariant2 := errors.New("DB::Exception: Replica is active")
	errOther := errors.New("Code: 192, Unknown table")

	assert.True(t, isReplicaActiveError(err305))
	assert.True(t, isReplicaActiveError(errTextVariant1))
	assert.True(t, isReplicaActiveError(errTextVariant2))
	assert.False(t, isReplicaActiveError(errOther))
	assert.False(t, isReplicaActiveError(nil))
}

func TestGetCoordinatorHost(t *testing.T) {
	w := &worker{}
	assert.Nil(t, w.getCoordinatorHost(nil))

	hostNoShard := &api.Host{}
	assert.Nil(t, w.getCoordinatorHost(hostNoShard))
}

func TestWaitHostReplicaInactive_ImmediateSuccess(t *testing.T) {
	fake := &fakeSchemer{
		isHostActiveReplicaFunc: func(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool {
			return false
		},
	}
	w := &worker{schemer: fake}
	err := w.waitHostReplicaInactive(context.Background(), &api.Host{}, &api.Host{}, 1*time.Millisecond, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, fake.activeCalls)
}

func TestWaitHostReplicaInactive_PollingTransitions(t *testing.T) {
	calls := 0
	fake := &fakeSchemer{
		isHostActiveReplicaFunc: func(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool {
			calls++
			return calls < 2 // Active on call 1, inactive on call 2
		},
	}

	w := &worker{schemer: fake}
	hostToRunOn := &api.Host{}
	hostToDrop := &api.Host{}

	err := w.waitHostReplicaInactive(context.Background(), hostToRunOn, hostToDrop, 1*time.Millisecond, 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, fake.activeCalls)
}

func TestWaitHostReplicaInactive_ParentContextExpires(t *testing.T) {
	fake := &fakeSchemer{
		isHostActiveReplicaFunc: func(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool {
			return true
		},
	}
	w := &worker{schemer: fake}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := w.waitHostReplicaInactive(ctx, &api.Host{}, &api.Host{}, 1*time.Millisecond, 0)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
}

func TestWaitHostReplicaInactive_InternalTimeout(t *testing.T) {
	fake := &fakeSchemer{
		isHostActiveReplicaFunc: func(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool {
			return true
		},
	}
	w := &worker{schemer: fake}

	// Test waitHostReplicaInactive internal timeout branch directly by specifying 10ms waitTimeout
	err := w.waitHostReplicaInactive(context.Background(), &api.Host{}, &api.Host{}, 1*time.Millisecond, 10*time.Millisecond)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestWaitTimeoutProceedsToRetryAndSucceeds(t *testing.T) {
	fake := &fakeSchemer{
		isHostActiveReplicaFunc: func(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool {
			return true // Always active during wait phase -> waitHostReplicaInactive times out
		},
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			if calls == 1 {
				return errors.New("Code: 305, Can't drop replica, because it's active")
			}
			return nil // Succeeds on attempt 2
		},
	}

	w := &worker{schemer: fake}
	hostToRunOn := &api.Host{}
	hostToDrop := &api.Host{}

	// Step 1: waitHostReplicaInactive times out after 10ms
	errWait := w.waitHostReplicaInactive(context.Background(), hostToRunOn, hostToDrop, 1*time.Millisecond, 10*time.Millisecond)
	assert.Error(t, errWait)
	assert.True(t, errors.Is(errWait, context.DeadlineExceeded))

	// Step 2: retryDropReplica proceeds despite wait timeout and succeeds on attempt 2
	errRetry := w.retryDropReplica(context.Background(), hostToRunOn, hostToDrop, 1*time.Millisecond)
	assert.NoError(t, errRetry)
	assert.Equal(t, 2, fake.dropCalls)
}

func TestDropZKReplica_NilGuards(t *testing.T) {
	w := &worker{}
	assert.Nil(t, w.dropZKReplica(context.Background(), nil, NewDropReplicaOptions()))

	hostToDropNoShard := &api.Host{}
	assert.Nil(t, w.dropZKReplica(context.Background(), hostToDropNoShard, NewDropReplicaOptions()))
}

func TestDropReplicaOptions(t *testing.T) {
	opts := NewDropReplicaOptions()
	assert.False(t, opts.RegularDrop())
	assert.False(t, opts.ForceDropUponStorageLoss())

	opts.SetRegularDrop()
	assert.True(t, opts.RegularDrop())

	opts.SetForceDropUponStorageLoss()
	assert.True(t, opts.ForceDropUponStorageLoss())

	var nilOpts *dropReplicaOptions
	assert.False(t, nilOpts.RegularDrop())
	assert.False(t, nilOpts.ForceDropUponStorageLoss())
	assert.Nil(t, nilOpts.SetRegularDrop())
	assert.Nil(t, nilOpts.SetForceDropUponStorageLoss())

	arr := NewDropReplicaOptionsArr(opts)
	assert.Equal(t, opts, arr.First())

	emptyArr := NewDropReplicaOptionsArr()
	assert.Nil(t, emptyArr.First())
}

func TestRetryDropReplica_SuccessOnFirstTry(t *testing.T) {
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			return nil
		},
	}
	w := &worker{schemer: fake}
	err := w.retryDropReplica(context.Background(), &api.Host{}, &api.Host{}, 1*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, 1, fake.dropCalls)
}

func TestRetryDropReplica_RetriesOn305ThenSucceeds(t *testing.T) {
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			if calls == 1 {
				return errors.New("Code: 305, Can't drop replica, because it's active")
			}
			return nil
		},
	}

	w := &worker{schemer: fake}
	hostToRunOn := &api.Host{}
	hostToDrop := &api.Host{}
	ctx := context.Background()

	err := w.retryDropReplica(ctx, hostToRunOn, hostToDrop, 1*time.Millisecond)

	assert.NoError(t, err)
	assert.Equal(t, 2, fake.dropCalls)
}

func TestRetryDropReplica_ExhaustsRetriesReturnsLastError(t *testing.T) {
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			return errors.New("Code: 305, Can't drop replica, because it's active")
		},
	}

	w := &worker{schemer: fake}
	hostToRunOn := &api.Host{}
	hostToDrop := &api.Host{}
	ctx := context.Background()

	err := w.retryDropReplica(ctx, hostToRunOn, hostToDrop, 1*time.Millisecond)

	assert.Error(t, err)
	assert.True(t, isReplicaActiveError(err))
	assert.Equal(t, 6, fake.dropCalls)
}

func TestRetryDropReplica_NonRetryableErrorFailsFast(t *testing.T) {
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			return errors.New("Code: 192, Unknown table")
		},
	}

	w := &worker{schemer: fake}
	hostToRunOn := &api.Host{}
	hostToDrop := &api.Host{}
	ctx := context.Background()

	err := w.retryDropReplica(ctx, hostToRunOn, hostToDrop, 1*time.Millisecond)

	assert.Error(t, err)
	assert.False(t, isReplicaActiveError(err))
	assert.Equal(t, 1, fake.dropCalls)
}

func TestRetryDropReplica_ContextCancelledMidRetry_ReturnsErrNotNil(t *testing.T) {
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			return errors.New("Code: 305, Can't drop replica, because it's active")
		},
	}

	w := &worker{schemer: fake}
	hostToRunOn := &api.Host{}
	hostToDrop := &api.Host{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before loop

	err := w.retryDropReplica(ctx, hostToRunOn, hostToDrop, 1*time.Millisecond)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, 0, fake.dropCalls)
}

func TestRetryDropReplica_ContextCancelledDuringRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(c context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			cancel() // Cancel context during first retry attempt
			return errors.New("Code: 305, Can't drop replica, because it's active")
		},
	}
	w := &worker{schemer: fake}
	err := w.retryDropReplica(ctx, &api.Host{}, &api.Host{}, 1*time.Millisecond)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, 1, fake.dropCalls)
}

func TestDefaultFallbackParameters(t *testing.T) {
	w := &worker{schemer: &fakeSchemer{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err1 := w.waitHostReplicaInactive(ctx, &api.Host{}, &api.Host{}, 0, 0)
	assert.Error(t, err1)

	err2 := w.retryDropReplica(ctx, &api.Host{}, &api.Host{}, 0)
	assert.Error(t, err2)
}
