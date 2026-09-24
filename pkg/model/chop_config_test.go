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

package model

import (
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

func init() {
	chop.New(nil, nil, "")
}

func yes() types.StringBool { return types.StringBool("yes") }
func no() types.StringBool  { return types.StringBool("no") }

// applyRestartPolicy installs a restart policy for the duration of the test, restoring whatever
// was there before on cleanup. chop.Config() hands back a process-global, so an unconditional
// overwrite leaks into sibling subtests and into any later test that reads the policy without
// setting one first - an ordering-dependent failure rather than an obvious one. Mirrors
// withFIPSImagePolicy in pkg/model/chi/normalizer/normalizer_fips_test.go.
func applyRestartPolicy(t *testing.T, zookeeperRequiresReboot bool) {
	t.Helper()
	cfg := chop.Config()
	prev := cfg.ClickHouse.ConfigRestartPolicy
	t.Cleanup(func() { cfg.ClickHouse.ConfigRestartPolicy = prev })

	zk := no()
	if zookeeperRequiresReboot {
		zk = yes()
	}
	cfg.ClickHouse.ConfigRestartPolicy = api.OperatorConfigRestartPolicy{
		Rules: []api.OperatorConfigRestartPolicyRule{
			{
				Version: "*",
				Rules: []api.OperatorConfigRestartPolicyRuleSet{
					{types.Matchable("settings/*"): yes()},
					{types.Matchable("zookeeper/*"): zk},
				},
			},
		},
	}
}

func zkConfig(hosts ...string) *api.ZookeeperConfig {
	nodes := make(api.ZookeeperNodes, 0, len(hosts))
	for _, host := range hosts {
		nodes = append(nodes, api.ZookeeperNode{
			Host: host,
			Port: types.NewInt32(2181),
		})
	}
	return &api.ZookeeperConfig{Nodes: nodes}
}

func hostWithZKAndSettings(oldZK, newZK *api.ZookeeperConfig, oldSettings, newSettings *api.Settings) *api.Host {
	mkCHI := func(zk *api.ZookeeperConfig, settings *api.Settings, host *api.Host) *api.ClickHouseInstallation {
		host.Runtime.Address.ClusterName = "cluster"
		host.Runtime.Address.ShardName = "shard0"
		host.Runtime.Address.HostName = "host-0"
		chi := &api.ClickHouseInstallation{
			Spec: api.ChiSpec{
				Configuration: &api.Configuration{
					Zookeeper: zk,
					Settings:  settings,
					Clusters: []*api.Cluster{
						{
							Name:      "cluster",
							Zookeeper: zk,
							Layout: &api.ChiClusterLayout{
								Shards: []*api.ChiShard{
									{
										Name:  "shard0",
										Hosts: []*api.Host{host},
									},
								},
							},
						},
					},
				},
			},
		}
		host.SetCR(chi)
		return chi
	}

	oldHost := &api.Host{Name: "host-0"}
	newHost := &api.Host{Name: "host-0"}
	ancestor := mkCHI(oldZK, oldSettings, oldHost)
	current := mkCHI(newZK, newSettings, newHost)
	current.SetAncestor(ancestor)
	return newHost
}

func TestIsConfigurationChangeRequiresReboot_ZookeeperEndpointsOnly(t *testing.T) {
	applyRestartPolicy(t, false)

	oldZK := zkConfig("keeper-0", "keeper-1", "keeper-2")
	newZK := zkConfig("keeper-new-0", "keeper-new-1", "keeper-new-2")
	host := hostWithZKAndSettings(oldZK, newZK, nil, nil)

	if oldZK.Equals(newZK) {
		t.Fatal("expected ZooKeeper configuration to change")
	}
	if got := IsConfigurationChangeRequiresReboot(host); got {
		t.Fatalf("restart required = true, want false for ZooKeeper-only change")
	}
}

func TestIsConfigurationChangeRequiresReboot_ZookeeperAndRestartRequiredSetting(t *testing.T) {
	applyRestartPolicy(t, false)

	oldZK := zkConfig("keeper-0", "keeper-1", "keeper-2")
	newZK := zkConfig("keeper-new-0", "keeper-new-1", "keeper-new-2")
	oldSettings := api.NewSettings().SetScalarsFromMap(map[string]string{
		"max_concurrent_queries_for_all_users": "10",
	})
	newSettings := api.NewSettings().SetScalarsFromMap(map[string]string{
		"max_concurrent_queries_for_all_users": "20",
	})
	host := hostWithZKAndSettings(oldZK, newZK, oldSettings, newSettings)

	if got := IsConfigurationChangeRequiresReboot(host); !got {
		t.Fatalf("restart required = false, want true when ZooKeeper change is combined with a restart-required setting")
	}
}

func TestIsConfigurationChangeRequiresReboot_ZookeeperPolicyYesStillRestarts(t *testing.T) {
	applyRestartPolicy(t, true)

	host := hostWithZKAndSettings(
		zkConfig("keeper-0"),
		zkConfig("keeper-new-0"),
		nil,
		nil,
	)

	if got := IsConfigurationChangeRequiresReboot(host); !got {
		t.Fatalf("restart required = false, want true when operator policy zookeeper/* is yes")
	}
}

// TestIsConfigurationChangeRequiresReboot_ZookeeperUnchangedNeverRestarts pins the function's
// most basic property, which nothing else covered: an identical ZooKeeper config is not a change,
// so it never restarts no matter what the policy says.
//
// Every other case here feeds two DIFFERENT configs, so the Equals early return could be deleted
// without failing a single test - and deleting it makes an unchanged config fall through to the
// policy, where "yes" would restart every host for nothing on each reconcile.
func TestIsConfigurationChangeRequiresReboot_ZookeeperUnchangedNeverRestarts(t *testing.T) {
	// Policy "yes" is the only setting under which the early return is observable.
	applyRestartPolicy(t, true)

	host := hostWithZKAndSettings(zkConfig("zk-0", "zk-1"), zkConfig("zk-0", "zk-1"), nil, nil)
	if IsConfigurationChangeRequiresReboot(host) {
		t.Fatal("an unchanged ZooKeeper config is not a change and must never restart")
	}
}

// TestIsConfigurationChangeRequiresReboot_ZookeeperLastMatchingRuleWins pins the precedence the
// release note promises operators: rule sets from a ClickHouseOperatorConfiguration are appended
// after the operator's own, and the last matching rule wins, so a user entry overrides the
// shipped zookeeper/*: "no".
//
// That a user "yes" produces a restart is already covered by ZookeeperPolicyYesStillRestarts;
// what had no coverage is the PRECEDENCE when two matching rules coexist, which is the part the
// release note relies on. Nothing else distinguishes "the shipped rule matched and said no" from
// "no rule matched at all" either, since isListedChangeRequiresReboot returns false for both.
//
// The fixture stacks two rule sets inside one version block, whereas a real chopconf merge
// appends at the outer version-block level. Last-match-wins is the same in both arrangements -
// getLatestConfigMatchValue overwrites across both loops - and one block keeps the fixture small.
func TestIsConfigurationChangeRequiresReboot_ZookeeperLastMatchingRuleWins(t *testing.T) {
	cfg := chop.Config()
	prev := cfg.ClickHouse.ConfigRestartPolicy
	t.Cleanup(func() { cfg.ClickHouse.ConfigRestartPolicy = prev })

	// Two appended rule sets in one version block, shipped default first, user override second.
	cfg.ClickHouse.ConfigRestartPolicy = api.OperatorConfigRestartPolicy{
		Rules: []api.OperatorConfigRestartPolicyRule{
			{
				Version: "*",
				Rules: []api.OperatorConfigRestartPolicyRuleSet{
					{types.Matchable("zookeeper/*"): no()},
					{types.Matchable("zookeeper/*"): yes()},
				},
			},
		},
	}

	host := hostWithZKAndSettings(zkConfig("zk-0"), zkConfig("zk-1"), nil, nil)
	if !IsConfigurationChangeRequiresReboot(host) {
		t.Fatal("the later rule set says yes and must win over the earlier no")
	}
}

// TestIsConfigurationChangeRequiresReboot_ZookeeperEnableDisableAlwaysRestarts pins that
// turning ZooKeeper on or off restarts regardless of the restart policy.
//
// getHostZookeeper emits <distributed_ddl> in the same generated file as <zookeeper> and
// returns nothing at all when no nodes are configured, so this transition adds or removes
// that section rather than editing a value inside it. ClickHouse builds its DDLWorker during
// startup and never on a config reload, so a suppressed restart here brings the cluster up
// with replication configured and ON CLUSTER DDL dead on every pre-existing host - the one
// case where the policy must not be honoured.
func TestIsConfigurationChangeRequiresReboot_ZookeeperEnableDisableAlwaysRestarts(t *testing.T) {
	// The default policy, i.e. the one under which endpoint edits are NOT supposed to restart.
	applyRestartPolicy(t, false)

	t.Run("enabling zookeeper restarts", func(t *testing.T) {
		host := hostWithZKAndSettings(nil, zkConfig("zk-0"), nil, nil)
		if !IsConfigurationChangeRequiresReboot(host) {
			t.Fatal("adding zookeeper to a live cluster must restart: it introduces <distributed_ddl>")
		}
	})

	t.Run("disabling zookeeper restarts", func(t *testing.T) {
		host := hostWithZKAndSettings(zkConfig("zk-0"), nil, nil, nil)
		if !IsConfigurationChangeRequiresReboot(host) {
			t.Fatal("removing zookeeper must restart: it drops <distributed_ddl> and leaves a stale session")
		}
	})

	t.Run("an empty-to-empty change is still no change", func(t *testing.T) {
		host := hostWithZKAndSettings(nil, api.NewZookeeperConfig(), nil, nil)
		if IsConfigurationChangeRequiresReboot(host) {
			t.Fatal("nil and a zero-value config are both empty - nothing changed, nothing to restart")
		}
	})

	// Same no-op, but with the policy set to restart. Under the default "no" the case above
	// reaches the policy lookup and returns false for the wrong reason, so only this one
	// distinguishes "both empty is not a change" from "the policy happened to say no".
	// Equals sees a nil and a zero-value config as different pointers, and InheritZookeeperFrom
	// mints exactly such a config, so a real cluster can hit this.
	t.Run("an empty-to-empty change does not restart even under policy yes", func(t *testing.T) {
		applyRestartPolicy(t, true)

		host := hostWithZKAndSettings(nil, api.NewZookeeperConfig(), nil, nil)
		if IsConfigurationChangeRequiresReboot(host) {
			t.Fatal("both sides describe no ZooKeeper: restarting would be a no-op restart")
		}
	})

	t.Run("endpoint edit between two non-empty configs still honours the policy", func(t *testing.T) {
		host := hostWithZKAndSettings(zkConfig("zk-0"), zkConfig("zk-1"), nil, nil)
		if IsConfigurationChangeRequiresReboot(host) {
			t.Fatal("the transition guard must not swallow the policy for ordinary endpoint edits")
		}
	})
}
