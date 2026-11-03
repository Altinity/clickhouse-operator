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
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
)

var deleteHostSTSResource = schema.GroupResource{Group: "apps", Resource: "statefulsets"}

// deleteHostFakeSTS answers deleteHost's first step - the StatefulSet lookup. Every
// mutating method panics: reaching one means deleteHost went on to touch k8s objects
// after a Get it could not classify, which is exactly what these tests forbid.
type deleteHostFakeSTS struct {
	getCalls int
	getErr   error
}

func (f *deleteHostFakeSTS) Get(ctx context.Context, params ...any) (*apps.StatefulSet, error) {
	f.getCalls++
	if f.getErr != nil {
		// Mirror client-go: a typed Get hands back a non-nil zero object alongside the error.
		return &apps.StatefulSet{}, f.getErr
	}
	return &apps.StatefulSet{}, nil
}

func (f *deleteHostFakeSTS) Create(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	panic("deleteHost must not create a StatefulSet when the StatefulSet Get failed")
}

func (f *deleteHostFakeSTS) Update(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	panic("deleteHost must not update a StatefulSet when the StatefulSet Get failed")
}

func (f *deleteHostFakeSTS) Delete(ctx context.Context, namespace, name string) error {
	panic("deleteHost must not delete a StatefulSet when the StatefulSet Get failed")
}

func (f *deleteHostFakeSTS) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]apps.StatefulSet, error) {
	return nil, nil
}

// deleteHostFakeKube exposes STS() only. IKube is embedded as a nil interface, so any
// other accessor panics - a second guard that the paths under test reach nothing else.
type deleteHostFakeKube struct {
	interfaces.IKube
	sts interfaces.IKubeSTS
}

func (k *deleteHostFakeKube) STS() interfaces.IKubeSTS { return k.sts }

// newDeleteHostFixture builds the smallest worker/CR/host trio that carries deleteHost
// from entry down to the StatefulSet Get: a live context so the IsContextDone guard does
// not short-circuit, a CR attached to the host so the DeleteStarted announce can format,
// and an announcer with no event emitter - capable() is then false, so WithEvent and
// friends stay inert and never reach the nil status updater.
func newDeleteHostFixture(fake interfaces.IKubeSTS) (*worker, *api.ClickHouseInstallation, *api.Host) {
	const (
		namespace   = "test-ns"
		crName      = "test-chi"
		clusterName = "cluster"
		hostName    = "chi-test-chi-cluster-0-0"
	)

	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: crName},
	}
	host := &api.Host{Name: hostName}
	host.Runtime.Address.Namespace = namespace
	host.Runtime.Address.CHIName = crName
	host.Runtime.Address.ClusterName = clusterName
	host.Runtime.Address.HostName = hostName
	host.Runtime.SetCR(cr)

	w := &worker{
		c: &Controller{kube: &deleteHostFakeKube{sts: fake}},
		a: a.NewAnnouncer(nil, nil),
	}
	return w, cr, host
}

// TestDeleteHostStatefulSetGetErrorClassification pins how deleteHost reads a failed
// StatefulSet Get.
//
// Only IsNotFound proves the host is gone. Every other error means "unable to tell", and
// reporting those as a completed deletion drops the host's objects on the floor: the
// cleanup below the Get is skipped, and the host's PVCs carry no owner reference
// (model/common/creator/pvc.go), so nothing else ever reclaims them.
func TestDeleteHostStatefulSetGetErrorClassification(t *testing.T) {
	tests := []struct {
		name     string
		injected error
		wantErr  bool
	}{
		{
			name:     "NotFound - StatefulSet is gone for sure, host already deleted",
			injected: apiErrors.NewNotFound(deleteHostSTSResource, "chi-test-chi-cluster-0-0"),
		},
		{
			name:     "Forbidden - RBAC revoked, existence of the StatefulSet is unknown",
			injected: apiErrors.NewForbidden(deleteHostSTSResource, "chi-test-chi-cluster-0-0", errors.New("no permission")),
			wantErr:  true,
		},
		{
			name:     "InternalError - API server failure, existence of the StatefulSet is unknown",
			injected: apiErrors.NewInternalError(errors.New("etcd unavailable")),
			wantErr:  true,
		},
		{
			name:     "TooManyRequests - throttled, existence of the StatefulSet is unknown",
			injected: apiErrors.NewTooManyRequests("client throttled", 1),
			wantErr:  true,
		},
		{
			name:     "opaque non-API error - transport failure, not classifiable at all",
			injected: errors.New("dial tcp 10.96.0.1:443: connect: connection refused"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &deleteHostFakeSTS{getErr: tc.injected}
			w, cr, host := newDeleteHostFixture(fake)

			err := w.deleteHost(context.Background(), cr, host)

			// Classification first: it is the behaviour under test, and asserting it before
			// the invariants below keeps a failure here attributable to the classification
			// rather than to an unrelated invariant tripping first.
			if tc.wantErr {
				require.Error(t, err, "an unclassifiable Get error must not be reported as a completed deletion")
				require.ErrorIs(t, err, tc.injected, "the original API error must reach the caller")
			} else {
				require.NoError(t, err, "a NotFound StatefulSet means the host is already deleted")
			}

			// Invariants that hold for every class. assert, not require, so one failing
			// invariant still reports the other.
			assert.Equal(t, 1, fake.getCalls, "deleteHost must consult the StatefulSet exactly once")
			// A Get that failed read nothing, so the host must not be left holding a
			// zero-valued or stale StatefulSet that later steps could act on.
			assert.Nil(t, host.Runtime.CurStatefulSet, "a failed Get must not leave a StatefulSet behind")
		})
	}
}

// A deferred pass skips clean() and dropZKReplicas, so the hosts the action plan removed are
// still on disk. announceCleanupPostponed is the only signal a user gets, and it is only worth
// emitting when there is actually something left behind - events here are created directly
// against the API with nothing to aggregate them, so an ungated call mints a new Event object
// on every deferred pass forever.
//
// WalkRemoved dispatches on the diff entry's TYPE: a dropped shard arrives as one IShard entry,
// never as its hosts, which is why the walk expands clusters and shards back into hosts.
func TestRemovedHostFQDNs(t *testing.T) {
	nameFQDN := func(host *api.Host) string { return "fqdn-" + host.GetName() }

	crWith := func(hostNames ...string) *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{}
		shard := &api.ChiShard{}
		for _, n := range hostNames {
			shard.Hosts = append(shard.Hosts, &api.Host{Name: n})
		}
		cr.Spec.Configuration = &api.Configuration{
			Clusters: []*api.Cluster{{Layout: &api.ChiClusterLayout{Shards: []*api.ChiShard{shard}}}},
		}
		return cr
	}

	t.Run("nothing removed means nothing to announce", func(t *testing.T) {
		cr := crWith("h0")
		cr.EnsureRuntime().ActionPlan = api.MakeActionPlan(crWith("h0"), cr)
		require.Empty(t, removedHostFQDNs(cr, nameFQDN),
			"an unchanged plan must stay silent - otherwise every deferred pass mints an Event")
	})

	t.Run("a nil CR is handled", func(t *testing.T) {
		require.Empty(t, removedHostFQDNs(nil, nameFQDN))
	})

	t.Run("a removed host is named", func(t *testing.T) {
		cr := crWith("h0")
		cr.EnsureRuntime().ActionPlan = api.MakeActionPlan(crWith("h0", "h1"), cr)
		require.Contains(t, removedHostFQDNs(cr, nameFQDN), "fqdn-h1",
			"the FQDN of a host the plan drops must reach the event")
	})
}

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
