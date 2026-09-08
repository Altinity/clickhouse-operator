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

func applyRestartPolicy(zookeeperRequiresReboot bool) {
	zk := no()
	if zookeeperRequiresReboot {
		zk = yes()
	}
	chop.Config().ClickHouse.ConfigRestartPolicy = api.OperatorConfigRestartPolicy{
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
	applyRestartPolicy(false)

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
	applyRestartPolicy(false)

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
	applyRestartPolicy(true)

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
