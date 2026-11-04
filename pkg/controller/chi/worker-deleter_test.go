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
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	goch "github.com/mailru/go-clickhouse/v2"
	core "k8s.io/api/core/v1"
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
	hostDropReplicaFunc func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error
	dropCalls           int
}

func (f *fakeSchemer) HostDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.Host) error {
	f.dropCalls++
	if f.hostDropReplicaFunc != nil {
		return f.hostDropReplicaFunc(ctx, hostToRunOn, hostToDrop, f.dropCalls)
	}
	return nil
}

type fakeStoragePVC struct {
	interfaces.IKubeStoragePVC
}

func (f *fakeStoragePVC) ListForHost(ctx context.Context, host *api.Host) (*core.PersistentVolumeClaimList, error) {
	return &core.PersistentVolumeClaimList{}, nil
}

type fakeKubeStorage struct {
	interfaces.IKube
	storage interfaces.IKubeStoragePVC
}

func (k *fakeKubeStorage) Storage() interfaces.IKubeStoragePVC {
	return k.storage
}

func newTestWorker(s schemer.ISchemer) *worker {
	return &worker{
		c: &Controller{
			kube: &fakeKubeStorage{
				storage: &fakeStoragePVC{},
			},
		},
		schemer:            s,
		dropReplicaDelay:   1 * time.Millisecond,
		dropReplicaTimeout: 50 * time.Millisecond,
	}
}

func createTestCHIWithTopology(clusterName string, shards [][]string) *api.ClickHouseInstallation {
	cr := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Name:      "test-chi",
			Namespace: "default",
		},
	}
	var chiShards []*api.ChiShard
	for sIdx, hosts := range shards {
		shard := &api.ChiShard{
			Name: fmt.Sprintf("shard-%d", sIdx),
		}
		for rIdx, hName := range hosts {
			host := &api.Host{
				Name: hName,
			}
			host.Runtime.Address.ClusterName = clusterName
			host.Runtime.Address.ClusterIndex = 0
			host.Runtime.Address.ShardName = shard.Name
			host.Runtime.Address.ShardIndex = sIdx
			host.Runtime.Address.ReplicaIndex = rIdx
			host.Runtime.Address.HostName = hName
			host.SetCR(cr)
			shard.Hosts = append(shard.Hosts, host)
		}
		chiShards = append(chiShards, shard)
	}
	cluster := &api.Cluster{
		Name: clusterName,
		Layout: &api.ChiClusterLayout{
			Shards: chiShards,
		},
	}
	cluster.Runtime.CHI = cr
	cr.Spec.Configuration = &api.Configuration{
		Clusters: []*api.Cluster{cluster},
	}
	return cr
}

func TestIsReplicaActiveError(t *testing.T) {
	// 1. Actual ClickHouse structured Code 305 error
	struct305 := &goch.Error{Code: 305, Message: "Can't drop replica chi-dev-0-1, because it's active"}
	assert.True(t, isReplicaActiveError(struct305), "structured 305 must match")

	// 2. Wrapped Code 305 error
	wrapped305 := fmt.Errorf("query execution failed: %w", struct305)
	assert.True(t, isReplicaActiveError(wrapped305), "wrapped structured 305 must match")

	// ClickHouse string error variants
	err305 := errors.New("Code: 305, DB::Exception: Can't drop replica chi-dev-0-1, because it's active")
	err305Dot := errors.New("Code: 305. DB::Exception: Replica chi-dev-0-1 is active. (TABLE_WAS_NOT_DROPPED)")
	errTextVariant1 := errors.New("Can't drop replica chi-dev-0-1, because it's active")
	errTextVariant2 := errors.New("DB::Exception: Replica is active")
	assert.True(t, isReplicaActiveError(err305))
	assert.True(t, isReplicaActiveError(err305Dot))
	assert.True(t, isReplicaActiveError(errTextVariant1))
	assert.True(t, isReplicaActiveError(errTextVariant2))

	// 3. An unrelated error containing the substring "Code: 305"
	unrelatedTextWith305 := errors.New("invalid configuration line 42: Code: 305 is not a valid parameter")
	assert.False(t, isReplicaActiveError(unrelatedTextWith305), "unrelated error containing 'Code: 305' must NOT match")

	// 4. An unrelated ClickHouse error code (structured and string)
	struct192 := &goch.Error{Code: 192, Message: "Unknown table"}
	assert.False(t, isReplicaActiveError(struct192), "structured 192 must not match")
	errOther := errors.New("Code: 192, DB::Exception: Unknown table")
	assert.False(t, isReplicaActiveError(errOther))

	// Generic connection error
	errConn := errors.New("dial tcp: connection refused")
	assert.False(t, isReplicaActiveError(errConn))

	// 5. Nil error
	assert.False(t, isReplicaActiveError(nil))
}

func TestIsRetryableReplicaDropError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		// Nil error
		{
			name:      "nil error",
			err:       nil,
			retryable: false,
		},
		// Code 305: Replica is active (primary target of #1943)
		{
			name:      "structured ClickHouse 305 replica active",
			err:       &goch.Error{Code: 305, Message: "Can't drop replica chi-0-1, because it's active"},
			retryable: true,
		},
		{
			name:      "wrapped ClickHouse 305 from retry timeout",
			err:       fmt.Errorf("SYSTEM DROP REPLICA timed out: %w", &goch.Error{Code: 305, Message: "replica is active"}),
			retryable: true,
		},
		{
			name:      "string ClickHouse 305 with DB::Exception",
			err:       errors.New("Code: 305, DB::Exception: Can't drop replica chi-0-1, because it's active"),
			retryable: true,
		},
		// Transient Keeper / Cluster Error Codes
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with Connection loss",
			err:       &goch.Error{Code: 999, Message: "Coordination::Exception: Connection loss"},
			retryable: true,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with Operation timeout",
			err:       &goch.Error{Code: 999, Message: "Coordination::Exception: Operation timeout"},
			retryable: true,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with Session expired",
			err:       &goch.Error{Code: 999, Message: "Coordination::Exception: Session expired"},
			retryable: true,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with lowercase connection loss",
			err:       &goch.Error{Code: 999, Message: "coordination error: connection loss"},
			retryable: true,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with operation timed out variation",
			err:       &goch.Error{Code: 999, Message: "coordination error: operation timed out"},
			retryable: true,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with session moved",
			err:       &goch.Error{Code: 999, Message: "coordination exception: session moved"},
			retryable: true,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with Not authenticated (permanent)",
			err:       &goch.Error{Code: 999, Message: "Coordination::Exception: Not authenticated"},
			retryable: false,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with lowercase not authenticated (permanent)",
			err:       &goch.Error{Code: 999, Message: "coordination exception: not authenticated"},
			retryable: false,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with Invalid ACL (permanent)",
			err:       &goch.Error{Code: 999, Message: "Coordination::Exception: Invalid ACL"},
			retryable: false,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with Bad arguments (permanent)",
			err:       &goch.Error{Code: 999, Message: "Coordination::Exception: Bad arguments"},
			retryable: false,
		},
		{
			name:      "ClickHouse 999 KEEPER_EXCEPTION with Runtime inconsistency (permanent)",
			err:       &goch.Error{Code: 999, Message: "Coordination::Exception: Runtime inconsistency"},
			retryable: false,
		},
		{
			name:      "ClickHouse 225 NO_ZOOKEEPER",
			err:       &goch.Error{Code: 225, Message: "NO_ZOOKEEPER: ZooKeeper is not available"},
			retryable: true,
		},
		{
			name:      "ClickHouse 159 TIMEOUT_EXCEEDED",
			err:       &goch.Error{Code: 159, Message: "TIMEOUT_EXCEEDED: Timeout waiting for ZooKeeper response"},
			retryable: true,
		},
		{
			name:      "ClickHouse 209 SOCKET_TIMEOUT",
			err:       &goch.Error{Code: 209, Message: "SOCKET_TIMEOUT: Socket timeout reached"},
			retryable: true,
		},
		{
			name:      "ClickHouse 210 NETWORK_ERROR",
			err:       &goch.Error{Code: 210, Message: "NETWORK_ERROR: Connection reset by peer"},
			retryable: true,
		},
		// Network errors
		{
			name:      "transient net.Error timeout",
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")},
			retryable: true,
		},
		{
			name:      "transient net.OpError connection reset",
			err:       &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")},
			retryable: true,
		},
		{
			name:      "io.EOF",
			err:       io.EOF,
			retryable: true,
		},
		{
			name:      "io.ErrUnexpectedEOF",
			err:       io.ErrUnexpectedEOF,
			retryable: true,
		},
		// Permanent errors - MUST NOT be retryable
		{
			name:      "ClickHouse 515 INCORRECT_ACCESS_ENTITY_DEFINITION",
			err:       &goch.Error{Code: 515, Message: "INCORRECT_ACCESS_ENTITY_DEFINITION: malformed user definition"},
			retryable: false,
		},
		{
			name:      "ClickHouse 516 AUTHENTICATION_FAILED",
			err:       &goch.Error{Code: 516, Message: "AUTHENTICATION_FAILED: password is incorrect"},
			retryable: false,
		},
		{
			name:      "ClickHouse 192 UNKNOWN_USER",
			err:       &goch.Error{Code: 192, Message: "UNKNOWN_USER: user 'default' does not exist"},
			retryable: false,
		},
		{
			name:      "ClickHouse 62 SYNTAX_ERROR",
			err:       &goch.Error{Code: 62, Message: "SYNTAX_ERROR: Syntax error near 'DROP'"},
			retryable: false,
		},
		{
			name:      "ClickHouse 493 ACCESS_DENIED",
			err:       &goch.Error{Code: 493, Message: "ACCESS_DENIED: not enough privileges"},
			retryable: false,
		},
		{
			name:      "ClickHouse 60 UNKNOWN_TABLE",
			err:       &goch.Error{Code: 60, Message: "UNKNOWN_TABLE: table does not exist"},
			retryable: false,
		},
		{
			name:      "ClickHouse 81 UNKNOWN_DATABASE",
			err:       &goch.Error{Code: 81, Message: "UNKNOWN_DATABASE: database does not exist"},
			retryable: false,
		},
		{
			name:      "ClickHouse 47 UNKNOWN_IDENTIFIER",
			err:       &goch.Error{Code: 47, Message: "UNKNOWN_IDENTIFIER: unknown identifier"},
			retryable: false,
		},
		{
			name:      "ClickHouse 164 READONLY",
			err:       &goch.Error{Code: 164, Message: "READONLY: server is in read-only mode"},
			retryable: false,
		},
		{
			name:      "unknown ClickHouse error code 42",
			err:       &goch.Error{Code: 42, Message: "UNKNOWN_ERROR: random error"},
			retryable: false,
		},
		{
			name:      "generic arbitrary SQL error",
			err:       errors.New("invalid SQL: table corruption"),
			retryable: false,
		},
		// Context cancellation & deadlines - MUST NOT be retryable
		{
			name:      "context.Canceled",
			err:       context.Canceled,
			retryable: false,
		},
		{
			name:      "context.DeadlineExceeded",
			err:       context.DeadlineExceeded,
			retryable: false,
		},
		{
			name:      "net.OpError wrapping context.Canceled",
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: context.Canceled},
			retryable: false,
		},
		{
			name:      "net.OpError wrapping context.DeadlineExceeded",
			err:       &net.OpError{Op: "read", Net: "tcp", Err: context.DeadlineExceeded},
			retryable: false,
		},
		{
			name:      "wrapped ClickHouse error containing context.Canceled",
			err:       fmt.Errorf("execute failed: %w", context.Canceled),
			retryable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetryableReplicaDropError(tt.err)
			assert.Equal(t, tt.retryable, got, "case: %s", tt.name)
		})
	}
}

func TestGetCoordinatorHost(t *testing.T) {
	w := &worker{}

	t.Run("nil hostToDrop returns nil", func(t *testing.T) {
		assert.Nil(t, w.getCoordinatorHost(nil, nil))
	})

	t.Run("host without CR returns nil", func(t *testing.T) {
		hostNoCR := &api.Host{Name: "orphan"}
		assert.Nil(t, w.getCoordinatorHost(nil, hostNoCR))
	})

	t.Run("nil cr returns nil without panic", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
		hostToDrop0 := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
		assert.Nil(t, w.getCoordinatorHost(nil, hostToDrop0))
	})

	t.Run("2-replica shard: deleting replica 1 chooses surviving replica 0", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[1]

		newCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})

		coord := w.getCoordinatorHost(newCR, hostToDrop)
		require.NotNil(t, coord)
		assert.Equal(t, "h0", coord.GetName())
		assert.NotEqual(t, hostToDrop.GetName(), coord.GetName())
	})

	t.Run("2-replica shard: deleting replica 0 chooses surviving replica 1", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]

		newCR := createTestCHIWithTopology("c1", [][]string{{"h1"}})

		coord := w.getCoordinatorHost(newCR, hostToDrop)
		require.NotNil(t, coord)
		assert.Equal(t, "h1", coord.GetName())
		assert.NotEqual(t, hostToDrop.GetName(), coord.GetName())
	})

	t.Run("3-replica shard: deleting middle replica 1 chooses replica 0", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1", "h2"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[1]

		newCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h2"}})

		coord := w.getCoordinatorHost(newCR, hostToDrop)
		require.NotNil(t, coord)
		assert.Equal(t, "h0", coord.GetName())
		assert.NotEqual(t, hostToDrop.GetName(), coord.GetName())
	})

	t.Run("3-replica shard: simultaneous deletion of h0 and h1 chooses surviving h2", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1", "h2"}})
		hostToDrop0 := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
		hostToDrop1 := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[1]

		newCR := createTestCHIWithTopology("c1", [][]string{{"h2"}})

		coord0 := w.getCoordinatorHost(newCR, hostToDrop0)
		require.NotNil(t, coord0)
		assert.Equal(t, "h2", coord0.GetName())

		coord1 := w.getCoordinatorHost(newCR, hostToDrop1)
		require.NotNil(t, coord1)
		assert.Equal(t, "h2", coord1.GetName())
	})

	t.Run("single-replica shard deletion returns nil (cannot self-coordinate)", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]

		newCR := createTestCHIWithTopology("c1", [][]string{})

		coord := w.getCoordinatorHost(newCR, hostToDrop)
		assert.Nil(t, coord, "single-replica shard deletion must return nil, never select hostToDrop")
	})

	t.Run("entire shard deletion returns nil without selecting dead hosts", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0-0", "h0-1"}, {"h1-0", "h1-1"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[1].Hosts[0]

		newCR := createTestCHIWithTopology("c1", [][]string{{"h0-0", "h0-1"}})

		coord := w.getCoordinatorHost(newCR, hostToDrop)
		assert.Nil(t, coord, "whole shard deletion must return nil rather than executing against unrelated shard")
	})

	t.Run("typed-nil IShard (*ChiShard)(nil) returns nil safely via IsZero()", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
		// newCR has no cluster or shard named "c1"
		newCR := &api.ClickHouseInstallation{
			Spec: api.ChiSpec{
				Configuration: &api.Configuration{},
			},
		}
		assert.Nil(t, w.getCoordinatorHost(newCR, hostToDrop))
	})
}

func TestRetryDropReplica(t *testing.T) {
	hostToRunOn := &api.Host{Name: "coord"}
	hostToDrop := &api.Host{Name: "victim"}

	t.Run("immediate success takes 1 call without delay", func(t *testing.T) {
		fake := &fakeSchemer{
			hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
				return nil
			},
		}
		w := newTestWorker(fake)
		err := w.retryDropReplica(context.Background(), hostToRunOn, hostToDrop)
		assert.NoError(t, err)
		assert.Equal(t, 1, fake.dropCalls)
	})

	t.Run("retries on Code 305 and succeeds on attempt 2", func(t *testing.T) {
		fake := &fakeSchemer{
			hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
				if calls == 1 {
					return errors.New("Code: 305, Can't drop replica, because it's active")
				}
				return nil
			},
		}
		w := newTestWorker(fake)
		err := w.retryDropReplica(context.Background(), hostToRunOn, hostToDrop)
		assert.NoError(t, err)
		assert.Equal(t, 2, fake.dropCalls)
	})

	t.Run("exhausts timeout on persistent Code 305 and returns last error with context", func(t *testing.T) {
		fake := &fakeSchemer{
			hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
				return errors.New("Code: 305, Can't drop replica, because it's active")
			},
		}
		w := newTestWorker(fake)
		w.dropReplicaTimeout = 10 * time.Millisecond
		err := w.retryDropReplica(context.Background(), hostToRunOn, hostToDrop)
		assert.Error(t, err)
		assert.True(t, isReplicaActiveError(err))
		assert.Contains(t, err.Error(), "timed out after")
		assert.GreaterOrEqual(t, fake.dropCalls, 2)
	})

	t.Run("non-305 error fails fast on attempt 1", func(t *testing.T) {
		fake := &fakeSchemer{
			hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
				return errors.New("Code: 192, Unknown table")
			},
		}
		w := newTestWorker(fake)
		err := w.retryDropReplica(context.Background(), hostToRunOn, hostToDrop)
		assert.Error(t, err)
		assert.False(t, isReplicaActiveError(err))
		assert.Contains(t, err.Error(), "failed with non-retryable error")
		assert.Equal(t, 1, fake.dropCalls)
	})

	t.Run("pre-cancelled context aborts before any call", func(t *testing.T) {
		fake := &fakeSchemer{}
		w := newTestWorker(fake)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := w.retryDropReplica(ctx, hostToRunOn, hostToDrop)
		assert.Equal(t, context.Canceled, err)
		assert.Equal(t, 0, fake.dropCalls)
	})

	t.Run("context cancelled mid-retry returns context.Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fake := &fakeSchemer{
			hostDropReplicaFunc: func(c context.Context, runOn, drop *api.Host, calls int) error {
				cancel()
				return errors.New("Code: 305, Can't drop replica, because it's active")
			},
		}
		w := newTestWorker(fake)
		w.dropReplicaDelay = 10 * time.Millisecond
		w.dropReplicaTimeout = 100 * time.Millisecond
		err := w.retryDropReplica(ctx, hostToRunOn, hostToDrop)
		assert.Equal(t, context.Canceled, err)
		assert.Equal(t, 1, fake.dropCalls)
	})

	t.Run("inherited ZooKeeper session timeout accounts for configured session timeout + grace period", func(t *testing.T) {
		cr := &api.ClickHouseInstallation{
			Spec: api.ChiSpec{
				Configuration: &api.Configuration{
					Zookeeper: &api.ZookeeperConfig{
						SessionTimeoutMs: 20000,
					},
					Clusters: []*api.Cluster{
						{
							Name: "c1",
							Layout: &api.ChiClusterLayout{
								Shards: []*api.ChiShard{
									{
										Name: "s0",
										Hosts: []*api.Host{
											{Name: "h0"},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		cluster := cr.Spec.Configuration.Clusters[0]
		cluster.Runtime.CHI = cr
		cluster.Zookeeper = &api.ZookeeperConfig{SessionTimeoutMs: 20000}
		h := cluster.Layout.Shards[0].Hosts[0]
		h.SetCR(cr)
		h.Runtime.Address.ClusterName = "c1"

		zk := h.GetZookeeper()
		require.NotNil(t, zk)
		assert.Equal(t, 20000, zk.SessionTimeoutMs)

		expectedTimeout := 20*time.Second + replicaWaitGracePeriod
		assert.Equal(t, 50*time.Second, expectedTimeout)
	})
}

func TestDropZKReplica(t *testing.T) {
	w := newTestWorker(&fakeSchemer{})

	t.Run("nil hostToDrop returns error", func(t *testing.T) {
		dropped, err := w.dropZKReplica(context.Background(), nil, nil, NewDropReplicaOptions())
		assert.False(t, dropped)
		assert.Error(t, err)
	})

	t.Run("host without CR returns error", func(t *testing.T) {
		hostNoCR := &api.Host{Name: "orphan"}
		dropped, err := w.dropZKReplica(context.Background(), nil, hostNoCR, NewDropReplicaOptions())
		assert.False(t, dropped)
		assert.Error(t, err)
	})

	t.Run("no coordinator host in shard returns dropped=false, err=nil (postponed, non-blocking)", func(t *testing.T) {
		cr := createTestCHIWithTopology("c1", [][]string{{"single-host"}})
		host := cr.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]

		dropped, err := w.dropZKReplica(context.Background(), cr, host, NewDropReplicaOptions())
		assert.False(t, dropped)
		assert.NoError(t, err, "whole-shard / single-replica deletion must not return a fatal error that blocks reconciliation")
	})

	t.Run("prohibited drop (OnDelete is false) returns dropped=false, err=nil", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
		newCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[1]

		oldCR.Spec.Configuration.Clusters[0].GetReconcile().Host.Drop.Replicas = &api.ReconcileHostDropReplicas{
			OnDelete: types.NewStringBool(false),
		}

		dropped, err := w.dropZKReplica(context.Background(), newCR, hostToDrop, NewDropReplicaOptions().SetRegularDrop())
		assert.False(t, dropped)
		assert.NoError(t, err)
	})

	t.Run("successful drop returns dropped=true, err=nil", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
		newCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
		hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[1]

		fake := &fakeSchemer{}
		worker := newTestWorker(fake)

		dropped, err := worker.dropZKReplica(context.Background(), newCR, hostToDrop, NewDropReplicaOptions().SetRegularDrop())
		assert.True(t, dropped)
		assert.NoError(t, err)
		assert.Equal(t, 1, fake.dropCalls)
	})
}

func TestDropZKReplicas_Lifecycle_FailureAbortsReconcile(t *testing.T) {
	// When a same-shard coordinator exists but drop fails, dropZKReplicas must return an error
	// so that reconcileCHI aborts before finalizeReconcileAndMarkCompleted advances Ancestor.
	oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	newCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(oldCR, newCR)

	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			return errors.New("dial tcp: connection refused")
		},
	}
	w := newTestWorker(fake)

	err := w.dropZKReplicas(context.Background(), newCR)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to drop 1 ZK replica(s)")
	assert.True(t, errors.Is(err, ErrZKReplicaDropFailed))
}

func TestDropZKReplicas_RetryableVsPermanentError(t *testing.T) {
	oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	newCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(oldCR, newCR)

	// Case 1: Retryable error (Code 305)
	fakeRetryable := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			return &goch.Error{Code: 305, Message: "replica is active"}
		},
	}
	wRetryable := newTestWorker(fakeRetryable)
	wRetryable.dropReplicaDelay = 1 * time.Millisecond
	wRetryable.dropReplicaTimeout = 5 * time.Millisecond

	errRetryable := wRetryable.dropZKReplicas(context.Background(), newCR)
	require.Error(t, errRetryable)
	assert.True(t, errors.Is(errRetryable, ErrZKReplicaDropFailed), "must wrap ErrZKReplicaDropFailed")
	assert.True(t, errors.Is(errRetryable, ErrZKReplicaDropRetryable), "must wrap ErrZKReplicaDropRetryable")

	// Case 2: Permanent error (Code 515 Authentication failed)
	fakePermanent := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
			return &goch.Error{Code: 515, Message: "authentication failed"}
		},
	}
	wPermanent := newTestWorker(fakePermanent)
	wPermanent.dropReplicaDelay = 1 * time.Millisecond
	wPermanent.dropReplicaTimeout = 5 * time.Millisecond

	errPermanent := wPermanent.dropZKReplicas(context.Background(), newCR)
	require.Error(t, errPermanent)
	assert.True(t, errors.Is(errPermanent, ErrZKReplicaDropFailed), "must wrap ErrZKReplicaDropFailed")
	assert.True(t, errors.Is(errPermanent, ErrZKReplicaDropPermanent), "must wrap ErrZKReplicaDropPermanent")
	assert.False(t, errors.Is(errPermanent, ErrZKReplicaDropRetryable), "must NOT wrap ErrZKReplicaDropRetryable")
}

func TestDropZKReplicas_Lifecycle_WholeShardDoesNotAbortReconcile(t *testing.T) {
	// When an entire shard is removed, no same-shard coordinator exists.
	// dropZKReplicas must return nil so reconciliation completes rather than looping indefinitely.
	oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	newCR := createTestCHIWithTopology("c1", [][]string{})
	newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(oldCR, newCR)

	fake := &fakeSchemer{}
	w := newTestWorker(fake)

	err := w.dropZKReplicas(context.Background(), newCR)
	require.NoError(t, err, "Whole-shard deletion must not fail reconciliation")
}

func TestDropZKReplicas_Lifecycle_RecoveryOnNextReconcile(t *testing.T) {
	// Demonstrates the recovery lifecycle across reconciliations:
	// 1. Initial scale down from 2 to 1 replica (h1 removed).
	// 2. Pass 1: Coordinator h0 is down (fails). Reconciliation aborts; Ancestor is NOT updated.
	// 3. Pass 2: Coordinator h0 recovers. Ancestor still has h1. ActionPlan still includes h1 in WalkRemoved.
	// 4. Pass 2 succeeds, replica is dropped, and reconciliation completes!
	oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	newCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	newCR.SetAncestor(oldCR)

	// Pass 1: Coordinator fails
	pass1Fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
			return errors.New("dial tcp: connection refused")
		},
	}
	w1 := newTestWorker(pass1Fake)
	newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(newCR.GetAncestorT(), newCR)
	err1 := w1.dropZKReplicas(context.Background(), newCR)
	require.Error(t, err1)

	// Ancestor was NOT updated because pass 1 aborted before finalize
	require.Equal(t, oldCR, newCR.GetAncestorT(), "Ancestor must remain oldCR on failure")

	// Pass 2: Coordinator recovers
	pass2Fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
			return nil
		},
	}
	w2 := newTestWorker(pass2Fake)
	// ActionPlan in pass 2 diffs against oldCR (which was preserved!)
	newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(newCR.GetAncestorT(), newCR)
	var removedHostsCount int
	newCR.EnsureRuntime().ActionPlan.WalkRemoved(func(api.ICluster) {}, func(api.IShard) {}, func(*api.Host) {
		removedHostsCount++
	})
	require.Equal(t, 1, removedHostsCount, "Removed host must still be present in ActionPlan")

	err2 := w2.dropZKReplicas(context.Background(), newCR)
	require.NoError(t, err2, "Pass 2 must succeed after coordinator recovers")
	assert.Equal(t, 1, pass2Fake.dropCalls)
}

func TestDropZKReplicas_Lifecycle_ReplicaReaddedBeforeRetry(t *testing.T) {
	// Demonstrates safe handling when user re-adds a replica before failed cleanup retried:
	// 1. Initial scale down from 2 to 1 replica (h1 removed).
	// 2. Pass 1: Drop fails (e.g. coordinator network blip). Reconcile aborts; Ancestor still has h1.
	// 3. User updates CR to scale BACK UP to 2 replicas (h1 re-added) before pass 2 runs.
	// 4. Pass 2: ActionPlan computes diff between Ancestor (h0, h1) and newCR (h0, h1).
	//    Diff sees 0 removed hosts. dropZKReplicas performs 0 drops, avoiding accidental deletion
	//    of the re-added replica's metadata!
	oldCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	downscaledCR := createTestCHIWithTopology("c1", [][]string{{"h0"}})
	downscaledCR.SetAncestor(oldCR)

	// Pass 1: Coordinator fails during downscale
	pass1Fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
			return errors.New("dial tcp: connection refused")
		},
	}
	w1 := newTestWorker(pass1Fake)
	downscaledCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(downscaledCR.GetAncestorT(), downscaledCR)
	err1 := w1.dropZKReplicas(context.Background(), downscaledCR)
	require.Error(t, err1)

	// Ancestor is preserved as oldCR
	require.Equal(t, oldCR, downscaledCR.GetAncestorT())

	// Now user scales back up: re-adds h1 before pass 2 runs
	readdedCR := createTestCHIWithTopology("c1", [][]string{{"h0", "h1"}})
	readdedCR.SetAncestor(downscaledCR.GetAncestorT()) // inherits preserved Ancestor

	pass2Fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, runOn, drop *api.Host, calls int) error {
			t.Fatalf("unexpected call to HostDropReplica: re-added replica must not be dropped!")
			return nil
		},
	}
	w2 := newTestWorker(pass2Fake)
	readdedCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(readdedCR.GetAncestorT(), readdedCR)

	var removedHostsCount int
	readdedCR.EnsureRuntime().ActionPlan.WalkRemoved(func(api.ICluster) {}, func(api.IShard) {}, func(*api.Host) {
		removedHostsCount++
	})
	require.Equal(t, 0, removedHostsCount, "No hosts should be marked as removed after re-addition")

	err2 := w2.dropZKReplicas(context.Background(), readdedCR)
	require.NoError(t, err2)
	assert.Equal(t, 0, pass2Fake.dropCalls, "HostDropReplica must not be called when replica is re-added")
}

func TestDropZKReplicas_Accounting(t *testing.T) {
	t.Run("accurately accounts succeeded and failed drops", func(t *testing.T) {
		oldCR := createTestCHIWithTopology("c1", [][]string{
			{"h0", "h1", "h2"},
		})
		newCR := createTestCHIWithTopology("c1", [][]string{
			{"h0"},
		})
		newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(oldCR, newCR)

		fake := &fakeSchemer{
			hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hostToDrop *api.Host, calls int) error {
				if hostToDrop.GetName() == "h2" {
					return errors.New("Code: 305, Can't drop replica, because it's active")
				}
				return nil
			},
		}

		w := newTestWorker(fake)
		err := w.dropZKReplicas(context.Background(), newCR)
		require.Error(t, err)

		assert.GreaterOrEqual(t, fake.dropCalls, 2)
	})
}

func TestDropZKReplicas_WholeShardDeletion(t *testing.T) {
	oldCR := createTestCHIWithTopology("c1", [][]string{
		{"h0-0", "h0-1"},
		{"h1-0", "h1-1"},
	})
	newCR := createTestCHIWithTopology("c1", [][]string{
		{"h0-0", "h0-1"},
	})
	newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(oldCR, newCR)

	fake := &fakeSchemer{}
	w := newTestWorker(fake)

	err := w.dropZKReplicas(context.Background(), newCR)
	require.NoError(t, err, "Whole shard deletion must not return error to block reconcile")
	assert.Equal(t, 0, fake.dropCalls, "Must never execute DROP REPLICA on unrelated shard 0")
}

func TestDropZKReplica_Regression_Issue1943(t *testing.T) {
	// Replicate Issue #1943:
	// A 2-replica shard scales down to 1 replica (replica 1 removed, replica 0 surviving).
	// SYSTEM DROP REPLICA initially fails with Code 305 because Keeper session hasn't expired.
	// After retrying, the replica becomes inactive and DROP REPLICA succeeds.
	oldCR := createTestCHIWithTopology("c1", [][]string{{"chi-dev-bmch-0-0", "chi-dev-bmch-0-1"}})
	hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[1]
	survivingCR := createTestCHIWithTopology("c1", [][]string{{"chi-dev-bmch-0-0"}})

	var executedOnHost string
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hDrop *api.Host, calls int) error {
			executedOnHost = hostToRunOn.GetName()
			if calls == 1 {
				return errors.New("Code: 305, Can't drop replica chi-dev-bmch-0-1, because it's active")
			}
			return nil
		},
	}

	w := newTestWorker(fake)
	dropped, err := w.dropZKReplica(context.Background(), survivingCR, hostToDrop, NewDropReplicaOptions().SetRegularDrop())

	require.NoError(t, err)
	assert.True(t, dropped)
	assert.Equal(t, "chi-dev-bmch-0-0", executedOnHost, "SYSTEM DROP REPLICA must execute on surviving replica 0")
	assert.Equal(t, 2, fake.dropCalls, "Must succeed on second attempt after retrying Code 305")
}

func TestDropZKReplica_Regression_Replica0Deleted(t *testing.T) {
	// When replica 0 is deleted instead of replica 1, replica 1 must be chosen as coordinator.
	oldCR := createTestCHIWithTopology("c1", [][]string{{"chi-dev-bmch-0-0", "chi-dev-bmch-0-1"}})
	hostToDrop := oldCR.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
	survivingCR := createTestCHIWithTopology("c1", [][]string{{"chi-dev-bmch-0-1"}})

	var executedOnHost string
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hDrop *api.Host, calls int) error {
			executedOnHost = hostToRunOn.GetName()
			if calls == 1 {
				return errors.New("Code: 305, Can't drop replica chi-dev-bmch-0-0, because it's active")
			}
			return nil
		},
	}

	w := newTestWorker(fake)
	dropped, err := w.dropZKReplica(context.Background(), survivingCR, hostToDrop, NewDropReplicaOptions().SetRegularDrop())

	require.NoError(t, err)
	assert.True(t, dropped)
	assert.Equal(t, "chi-dev-bmch-0-1", executedOnHost, "SYSTEM DROP REPLICA must execute on surviving replica 1")
	assert.Equal(t, 2, fake.dropCalls)
}

func TestDropZKReplicas_ActionPlan_Issue1943(t *testing.T) {
	// Replicate Issue #1943 through the full ActionPlan walk:
	// A 2-replica shard scaled down to 1 replica (bmch-0-1 removed).
	oldCR := createTestCHIWithTopology("c1", [][]string{{"chi-dev-bmch-0-0", "chi-dev-bmch-0-1"}})
	newCR := createTestCHIWithTopology("c1", [][]string{{"chi-dev-bmch-0-0"}})
	newCR.EnsureRuntime().ActionPlan = api.MakeActionPlan(oldCR, newCR)

	var executedOnHost string
	var droppedHost string
	fake := &fakeSchemer{
		hostDropReplicaFunc: func(ctx context.Context, hostToRunOn, hDrop *api.Host, calls int) error {
			executedOnHost = hostToRunOn.GetName()
			droppedHost = hDrop.GetName()
			return nil
		},
	}

	w := newTestWorker(fake)
	err := w.dropZKReplicas(context.Background(), newCR)
	require.NoError(t, err)

	assert.Equal(t, "chi-dev-bmch-0-0", executedOnHost, "Action plan drop must execute on surviving replica 0")
	assert.Equal(t, "chi-dev-bmch-0-1", droppedHost, "Action plan must target removed replica 1")
	assert.Equal(t, 1, fake.dropCalls)
}

func TestHasHostVolumesToRetain_FailSafe(t *testing.T) {
	h := &api.Host{Name: "victim"}
	w := &worker{
		c: &Controller{
			kube: &fakeKubeStorage{storage: nil},
		},
	}
	assert.True(t, w.hasHostVolumesToRetain(context.Background(), h),
		"Missing storage manager must fail-safe to true to protect retained volumes")
}
