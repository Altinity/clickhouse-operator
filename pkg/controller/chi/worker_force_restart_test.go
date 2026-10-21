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
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model"
)

func hostWithAddress(name string) *api.Host {
	host := &api.Host{Name: name}
	host.Runtime.Address.ClusterName = "c1"
	host.Runtime.Address.ShardName = "s1"
	host.Runtime.Address.HostName = name
	return host
}

func hostWithServerSettingChange() *api.Host {
	ancestor := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Settings: api.NewSettings().SetScalarsFromMap(map[string]string{"max_concurrent_queries": "10"}),
			},
		},
	}
	cr := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Settings: api.NewSettings().SetScalarsFromMap(map[string]string{"max_concurrent_queries": "20"}),
			},
		},
	}
	cr.SetAncestor(ancestor)
	host := hostWithAddress("r1")
	host.SetCR(cr)
	return host
}

func TestShouldForceRestartHostFollowsCurrentPolicy(t *testing.T) {
	cr := &api.ClickHouseInstallation{}
	host := hostWithAddress("r1")
	host.SetCR(cr)
	w := &worker{a: a.NewAnnouncer(nil, nil)}

	if w.shouldForceRestartHost(context.Background(), host) {
		t.Fatal("a host with no ancestor does not restart")
	}

	ancestor := makeCompletedCR("c1", "s1", "r1")
	shard := ancestor.Spec.Configuration.Clusters[0].Layout.Shards[0]
	shard.Hosts[0].SetCR(ancestor)
	otherAncestor := hostWithAddress("r2")
	otherAncestor.SetCR(ancestor)
	shard.Hosts = append(shard.Hosts, otherAncestor)
	cr.SetAncestor(ancestor)
	cr.Spec.Restart = types.NewString(api.RestartRollingUpdate)

	if !w.shouldForceRestartHost(context.Background(), host) {
		t.Fatal("RollingUpdate is evaluated against the host's current state")
	}
}

func TestReconcileReloadabilityAsksOnce(t *testing.T) {
	host := hostWithServerSettingChange()
	if !model.IsServerSettingsChanged(host) {
		t.Fatal("fixture must change a server setting")
	}
	w := &worker{a: a.NewAnnouncer(nil, nil)}
	cache := &reconcileReloadability{}

	remembered := &hostReloadability{
		settings: model.NewSettingsChangeableWithoutRestart([]string{"logger.level"}),
	}
	remembered.once.Do(func() {})
	cache.hosts.Store(host, remembered)
	if !cache.get(context.Background(), w, host).Contains("logger.level") {
		t.Fatal("a list already fetched for this host is reused")
	}

	// A cancelled query is remembered. A later live call must not dial ClickHouse.
	changed := hostWithServerSettingChange()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if cache.get(ctx, w, changed) != nil {
		t.Fatal("a cancelled query leaves configurationRestartPolicy in charge")
	}
	if cache.get(context.Background(), w, changed) != nil {
		t.Fatal("the failed query stays the answer for this reconcile")
	}

	fresh := &reconcileReloadability{}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if fresh.get(ctx, w, host).Contains("logger.level") {
		t.Fatal("the next reconcile does not keep the previous list")
	}
}

func TestReconcileReloadabilityConcurrentHosts(t *testing.T) {
	w := &worker{a: a.NewAnnouncer(nil, nil)}
	cache := &reconcileReloadability{}
	host := hostWithServerSettingChange()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cache.get(ctx, w, host) != nil {
				t.Error("a cancelled query leaves configurationRestartPolicy in charge")
			}
		}()
	}
	wg.Wait()
	if cache.get(context.Background(), w, host) != nil {
		t.Fatal("the failed query stays the answer for this reconcile")
	}
}
