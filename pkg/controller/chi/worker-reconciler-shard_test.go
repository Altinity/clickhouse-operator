package chi

import (
	"context"
	"reflect"
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func makeTestShardFixture(hostNames ...string) (*api.ClickHouseInstallation, *api.ChiShard, []*api.Host) {
	shard := &api.ChiShard{
		Name:  "shard-0",
		Hosts: make([]*api.Host, 0, len(hostNames)),
	}
	cluster := &api.Cluster{
		Name:   "cluster-0",
		Layout: &api.ChiClusterLayout{Shards: []*api.ChiShard{shard}},
	}
	chi := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{Clusters: []*api.Cluster{cluster}},
		},
	}
	cluster.Runtime.CHI = chi
	shard.Runtime.CHI = chi

	hosts := make([]*api.Host, 0, len(hostNames))
	for i, hostName := range hostNames {
		host := &api.Host{Name: hostName}
		host.SetCR(chi)
		host.Runtime.Address.ClusterName = cluster.Name
		host.Runtime.Address.ShardName = shard.Name
		host.Runtime.Address.HostName = hostName
		host.Runtime.Address.ReplicaIndex = i
		host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
		shard.Hosts = append(shard.Hosts, host)
		hosts = append(hosts, host)
	}

	return chi, shard, hosts
}

func Test_isShardSafeToDisruptHost(t *testing.T) {
	_, _, hosts := makeTestShardFixture("host-a", "host-b")
	hostA := hosts[0]
	hostB := hosts[1]

	health := map[string]bool{
		hostA.GetName(): true,
		hostB.GetName(): false,
	}

	w := &worker{
		hostHealthyFn: func(ctx context.Context, host *api.Host) bool {
			return health[host.GetName()]
		},
	}

	if w.isShardSafeToDisruptHost(context.Background(), hostA) {
		t.Fatalf("expected shard to be unsafe to disrupt %s when peer is unhealthy", hostA.GetName())
	}

	health[hostB.GetName()] = true
	if !w.isShardSafeToDisruptHost(context.Background(), hostA) {
		t.Fatalf("expected shard to be safe to disrupt %s after peer recovery", hostA.GetName())
	}
}

func Test_reconcileShardWithHosts_RecoveryFirstOrdering(t *testing.T) {
	_, shard, hosts := makeTestShardFixture("host-a", "host-b")
	hostA := hosts[0]
	hostB := hosts[1]

	health := map[string]bool{
		hostA.GetName(): true,
		hostB.GetName(): false,
	}
	order := make([]string, 0)

	w := &worker{
		hostHealthyFn: func(ctx context.Context, host *api.Host) bool {
			return health[host.GetName()]
		},
		reconcileShardFn: func(ctx context.Context, shard api.IShard) error {
			return nil
		},
		reconcileHostFn: func(ctx context.Context, host *api.Host) error {
			order = append(order, host.GetName())
			if host.GetName() == hostB.GetName() {
				// Simulate steady-state recovery before rollout continues.
				health[hostB.GetName()] = true
			}
			return nil
		},
	}

	if err := w.reconcileShardWithHosts(context.Background(), shard); err != nil {
		t.Fatalf("reconcileShardWithHosts() unexpected error: %v", err)
	}

	wantOrder := []string{hostB.GetName(), hostA.GetName()}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("unexpected reconcile order, got=%v want=%v", order, wantOrder)
	}
}

func Test_reconcileShardWithHosts_InterruptedRolloutAfterRestart_RecoversMissingReplicaFirst(t *testing.T) {
	_, shard, hosts := makeTestShardFixture("chi-foo-1-0-0-0", "chi-foo-1-0-1-0")
	host00 := hosts[0]
	host01 := hosts[1]

	// Simulate restart in the middle of rollout: host01 is still down, host00 is up.
	health := map[string]bool{
		host00.GetName(): true,
		host01.GetName(): false,
	}
	order := make([]string, 0)

	w := &worker{
		hostHealthyFn: func(ctx context.Context, host *api.Host) bool {
			return health[host.GetName()]
		},
		reconcileShardFn: func(ctx context.Context, shard api.IShard) error {
			return nil
		},
		reconcileHostFn: func(ctx context.Context, host *api.Host) error {
			order = append(order, host.GetName())
			if host.GetName() == host01.GetName() {
				health[host01.GetName()] = true
			}
			return nil
		},
	}

	if err := w.reconcileShardWithHosts(context.Background(), shard); err != nil {
		t.Fatalf("reconcileShardWithHosts() unexpected error: %v", err)
	}

	// Regression assertion: missing replica is recovered before any further disruption on the healthy replica.
	wantOrder := []string{host01.GetName(), host00.GetName()}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("unexpected reconcile order after interrupted rollout, got=%v want=%v", order, wantOrder)
	}
}
