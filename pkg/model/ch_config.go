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
	"bytes"
	"fmt"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	xmlbuilder "github.com/altinity/clickhouse-operator/pkg/model/builder/xml"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	distributedDDLPathPattern = "/clickhouse/%s/task_queue/ddl"

	// Special auto-generated clusters. Each of these clusters lay over all replicas in CHI
	// 1. Cluster with one shard and all replicas. Used to duplicate data over all replicas.
	// 2. Cluster with all shards (1 replica). Used to gather/scatter data over all replicas.

	oneShardAllReplicasClusterName = "all-replicated"
	allShardsOneReplicaClusterName = "all-sharded"
)

type ClickHouseConfigGenerator struct {
	chi *chiv1.ClickHouseInstallation
}

// NewClickHouseConfigGenerator returns new ClickHouseConfigGenerator struct
func NewClickHouseConfigGenerator(chi *chiv1.ClickHouseInstallation) *ClickHouseConfigGenerator {
	return &ClickHouseConfigGenerator{
		chi: chi,
	}
}

// GetUsers creates data for "users.xml"
func (c *ClickHouseConfigGenerator) GetUsers() string {
	return c.generateXMLConfig(c.chi.Spec.Configuration.Users, configUsers)
}

// GetProfiles creates data for "profiles.xml"
func (c *ClickHouseConfigGenerator) GetProfiles() string {
	return c.generateXMLConfig(c.chi.Spec.Configuration.Profiles, configProfiles)
}

// GetQuotas creates data for "quotas.xml"
func (c *ClickHouseConfigGenerator) GetQuotas() string {
	return c.generateXMLConfig(c.chi.Spec.Configuration.Quotas, configQuotas)
}

// GetSettings creates data for "settings.xml"
func (c *ClickHouseConfigGenerator) GetSettings() string {
	return c.generateXMLConfig(c.chi.Spec.Configuration.Settings, "")
}

// GetZookeeper creates data for "zookeeper.xml"
func (c *ClickHouseConfigGenerator) GetZookeeper() string {
	if len(c.chi.Spec.Configuration.Zookeeper.Nodes) == 0 {
		// No Zookeeper nodes provided
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	//		<zookeeper>
	cline(b, 0, "<"+xmlTagYandex+">")
	cline(b, 4, "<zookeeper>")
	// Append Zookeeper nodes
	for i := range c.chi.Spec.Configuration.Zookeeper.Nodes {
		// Convenience wrapper
		node := &c.chi.Spec.Configuration.Zookeeper.Nodes[i]
		// <node>
		//		<host>HOST</host>
		//		<port>PORT</port>
		// </node>
		cline(b, 8, "<node>")
		cline(b, 8, "    <host>%s</host>", node.Host)
		cline(b, 8, "    <port>%d</port>", node.Port)
		cline(b, 8, "</node>")
	}
	// </zookeeper>
	cline(b, 4, "</zookeeper>")

	// <distributed_ddl>
	//      <path>/x/y/chi.name/z</path>
	//      <profile>X</profile>
	cline(b, 4, "<distributed_ddl>")
	cline(b, 4, "    <path>%s</path>", c.getDistributedDDLPath())
	if c.chi.Spec.Defaults.DistributedDDL.Profile != "" {
		cline(b, 4, "    <profile>%s</profile>", c.chi.Spec.Defaults.DistributedDDL.Profile)
	}
	//		</distributed_ddl>
	// </yandex>
	cline(b, 4, "</distributed_ddl>")
	cline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// GetRemoteServers creates "remote_servers.xml" content and calculates data generation parameters for other sections
func (c *ClickHouseConfigGenerator) GetRemoteServers() string {
	b := &bytes.Buffer{}

	// <yandex>
	//		<remote_servers>
	cline(b, 0, "<"+xmlTagYandex+">")
	cline(b, 4, "<remote_servers>")

	cline(b, 8, "<!-- User-specified clusters -->")

	// Build each cluster XML
	for clusterIndex := range c.chi.Spec.Configuration.Clusters {
		// Convenience wrapper
		cluster := &c.chi.Spec.Configuration.Clusters[clusterIndex]

		// <my_cluster_name>
		cline(b, 8, "<%s>", cluster.Name)

		// Build each shard XML
		for shardIndex := range cluster.Layout.Shards {
			// Convenience wrapper
			shard := &cluster.Layout.Shards[shardIndex]

			// <shard>
			//		<internal_replication>VALUE(true/false)</internal_replication>
			cline(b, 12, "<shard>")
			cline(b, 16, "<internal_replication>%s</internal_replication>", shard.InternalReplication)

			//		<weight>X</weight>
			if shard.Weight > 0 {
				cline(b, 16, "<weight>%d</weight>", shard.Weight)
			}

			// Build each replica's XML
			for replicaIndex := range shard.Replicas {
				// Convenience wrapper
				replica := &shard.Replicas[replicaIndex]

				// <replica>
				//		<host>XXX</host>
				//		<port>XXX</port>
				// </replica>
				cline(b, 16, "<replica>")
				cline(b, 16, "    <host>%s</host>", c.getRemoteServersReplicaHostname(replica))
				cline(b, 16, "    <port>%d</port>", replica.Port)
				cline(b, 16, "</replica>")
			}
			// </shard>
			cline(b, 12, "</shard>")
		}
		// </my_cluster_name>
		cline(b, 8, "</%s>", cluster.Name)
	}

	cline(b, 8, "<!-- Autogenerated clusters -->")

	// One Shard All Replicas

	// <my_cluster_name>
	//     <shard>
	//         <internal_replication>
	clusterName := oneShardAllReplicasClusterName
	cline(b, 8, "<%s>", clusterName)
	cline(b, 8, "    <shard>")
	cline(b, 8, "        <internal_replication>true</internal_replication>")

	// Build each cluster XML
	for clusterIndex := range c.chi.Spec.Configuration.Clusters {
		// Convenience wrapper
		cluster := &c.chi.Spec.Configuration.Clusters[clusterIndex]
		// Build each shard XML
		for shardIndex := range cluster.Layout.Shards {
			// Convenience wrapper
			shard := &cluster.Layout.Shards[shardIndex]

			// Build each replica's XML
			for replicaIndex := range shard.Replicas {
				// Convenience wrapper
				replica := &shard.Replicas[replicaIndex]

				// <replica>
				//		<host>XXX</host>
				//		<port>XXX</port>
				// </replica>
				cline(b, 16, "<replica>")
				cline(b, 16, "    <host>%s</host>", c.getRemoteServersReplicaHostname(replica))
				cline(b, 16, "    <port>%d</port>", replica.Port)
				cline(b, 16, "</replica>")
			}
		}
	}

	//     </shard>
	// </my_cluster_name>
	cline(b, 8, "    </shard>")
	cline(b, 8, "</%s>", clusterName)

	// All Shards One Replica

	// <my_cluster_name>
	clusterName = allShardsOneReplicaClusterName
	cline(b, 8, "<%s>", clusterName)

	// Build each cluster XML
	for clusterIndex := range c.chi.Spec.Configuration.Clusters {
		// Convenience wrapper
		cluster := &c.chi.Spec.Configuration.Clusters[clusterIndex]
		// Build each shard XML
		for shardIndex := range cluster.Layout.Shards {
			// Convenience wrapper
			shard := &cluster.Layout.Shards[shardIndex]

			// Build each replica's XML
			for replicaIndex := range shard.Replicas {
				// Convenience wrapper
				replica := &shard.Replicas[replicaIndex]

				// <shard>
				//     <internal_replication>
				cline(b, 12, "<shard>")
				cline(b, 12, "    <internal_replication>true</internal_replication>")

				// <replica>
				//		<host>XXX</host>
				//		<port>XXX</port>
				// </replica>
				cline(b, 16, "<replica>")
				cline(b, 16, "    <host>%s</host>", c.getRemoteServersReplicaHostname(replica))
				cline(b, 16, "    <port>%d</port>", replica.Port)
				cline(b, 16, "</replica>")

				// </shard>
				cline(b, 12, "</shard>")
			}
		}
	}

	// </my_cluster_name>
	cline(b, 8, "</%s>", clusterName)

	// 		</remote_servers>
	// </yandex>
	cline(b, 0, "    </remote_servers>")
	cline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// GetHostMacros creates "macros.xml" content
func (c *ClickHouseConfigGenerator) GetHostMacros(replica *chiv1.ChiReplica) string {
	b := &bytes.Buffer{}

	// <yandex>
	//     <macros>
	cline(b, 0, "<"+xmlTagYandex+">")
	cline(b, 0, "    <macros>")

	// <installation>CHI-name-macros-value</installation>
	cline(b, 8, "<installation>%s</installation>", replica.Address.ChiName)

	// <CLUSTER_NAME>cluster-name-macros-value</CLUSTER_NAME>
	// cline(b, 8, "<%s>%[2]s</%[1]s>", replica.Address.ClusterName, c.getMacrosCluster(replica.Address.ClusterName))
	// <CLUSTER_NAME-shard>0-based shard index within cluster</CLUSTER_NAME-shard>
	// cline(b, 8, "<%s-shard>%d</%[1]s-shard>", replica.Address.ClusterName, replica.Address.ShardIndex)

	// All Shards One Replica Cluster
	// <CLUSTER_NAME-shard>0-based shard index within all-shards-one-replica-cluster would always be GlobalReplicaIndex</CLUSTER_NAME-shard>
	cline(b, 8, "<%s-shard>%d</%[1]s-shard>", allShardsOneReplicaClusterName, replica.Address.GlobalReplicaIndex)

	// <cluster> and <shard> macros are applicable to main cluster only. All aux clusters do not have ambiguous macros
	// <cluster></cluster> macro
	cline(b, 8, "<cluster>%s</cluster>", replica.Address.ClusterName)
	// <shard></shard> macro
	cline(b, 8, "<shard>%s</shard>", replica.Address.ShardName)
	// <replica>replica id = full deployment id</replica>
	// full deployment id is unique to identify replica within the cluster
	cline(b, 8, "<replica>%s</replica>", CreatePodHostname(replica))

	// 		</macros>
	// </yandex>
	cline(b, 0, "    </macros>")
	cline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// generateXMLConfig creates XML using map[string]string definitions
func (c *ClickHouseConfigGenerator) generateXMLConfig(data map[string]interface{}, prefix string) string {
	if len(data) == 0 {
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	// XML code
	// </yandex>
	cline(b, 0, "<"+xmlTagYandex+">")
	xmlbuilder.GenerateXML(b, data, prefix)
	cline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

//
// Paths and Names section
//

// getDistributedDDLPath returns string path used in <distributed_ddl><path>XXX</path></distributed_ddl>
func (c *ClickHouseConfigGenerator) getDistributedDDLPath() string {
	return fmt.Sprintf(distributedDDLPathPattern, c.chi.Name)
}

// getRemoteServersReplicaHostname returns hostname (podhostname + service or FQDN) for "remote_servers.xml"
// based on .Spec.Defaults.ReplicasUseFQDN
func (c *ClickHouseConfigGenerator) getRemoteServersReplicaHostname(replica *chiv1.ChiReplica) string {
	if util.IsStringBoolTrue(c.chi.Spec.Defaults.ReplicasUseFQDN) {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		return CreatePodFQDN(replica)
	} else {
		return CreatePodHostname(replica)
	}
}

// getMacrosInstallation returns macros value for <installation-name> macros
func (c *ClickHouseConfigGenerator) getMacrosInstallation(name string) string {
	return util.CreateStringID(name, 6)
}

// getMacrosCluster returns macros value for <cluster-name> macros
func (c *ClickHouseConfigGenerator) getMacrosCluster(name string) string {
	return util.CreateStringID(name, 4)
}
