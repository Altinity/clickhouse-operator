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

package models

import (
	"bytes"
	"fmt"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	xmlbuilder "github.com/altinity/clickhouse-operator/pkg/models/builders/xml"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// Special auto-generated clusters. Each of these clusters lay over all replicas in CHI
	// 1. Cluster with one shard and all replicas. Used to duplicate data over all replicas.
	// 2. Cluster with all shards (1 replica). Used to gather/scatter data over all replicas.

	oneShardAllReplicasClusterName = "all-replicated"
	allShardsOneReplicaClusterName = "all-sharded"
)

type ClickHouseConfigGenerator struct {
	chi *chiv1.ClickHouseInstallation
}

func NewClickHouseConfigGenerator(chi *chiv1.ClickHouseInstallation) *ClickHouseConfigGenerator {
	return &ClickHouseConfigGenerator{
		chi: chi,
	}
}

// GetUsers creates data for "users.xml"
func (c *ClickHouseConfigGenerator) GetUsers() string {
	return generateXMLConfig(c.chi.Spec.Configuration.Users, configUsers)
}

// GetProfiles creates data for "profiles.xml"
func (c *ClickHouseConfigGenerator) GetProfiles() string {
	return generateXMLConfig(c.chi.Spec.Configuration.Profiles, configProfiles)
}

// GetQuotas creates data for "quotas.xml"
func (c *ClickHouseConfigGenerator) GetQuotas() string {
	return generateXMLConfig(c.chi.Spec.Configuration.Quotas, configQuotas)
}

// GetSettings creates data for "settings.xml"
func (c *ClickHouseConfigGenerator) GetSettings() string {
	return generateXMLConfig(c.chi.Spec.Configuration.Settings, "")
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
	fprintf(b, "<%s>\n", xmlTagYandex)
	fprintf(b, "%4s<zookeeper>\n", " ")
	// Append Zookeeper nodes
	for i := range c.chi.Spec.Configuration.Zookeeper.Nodes {
		// Convenience wrapper
		node := &c.chi.Spec.Configuration.Zookeeper.Nodes[i]
		// <node>
		//		<host>HOST</host>
		//		<port>PORT</port>
		// </node>
		fprintf(b, "%8s<node>\n", " ")
		fprintf(b, "%12s<host>%s</host>\n", " ", node.Host)
		fprintf(b, "%12s<port>%d</port>\n", " ", node.Port)
		fprintf(b, "%8s</node>\n", " ")
	}
	// </zookeeper>
	fprintf(b, "%4s</zookeeper>\n", " ")

	// <distributed_ddl>
	//      <path>/x/y/chi.name/z</path>
	//      <profile>X</prpfile>
	fprintf(b, "%4s<distributed_ddl>\n", " ")
	fprintf(b, "%8s<path>%s</path>\n", " ", fmt.Sprintf(distributedDDLPattern, c.chi.Name))
	if c.chi.Spec.Defaults.DistributedDDL.Profile != "" {
		fprintf(b, "%8s<profile>%s</profile>\n", " ", c.chi.Spec.Defaults.DistributedDDL.Profile)
	}
	//		</distributed_ddl>
	// </yandex>
	fprintf(b, "%4s</distributed_ddl>\n", " ")
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// getRemoteServersReplicaHostname creates hostname (podhostname + service or FQDN) for "remote_servers.xml"
// based on .Spec.Defaults.ReplicasUseFQDN
func (c *ClickHouseConfigGenerator) getRemoteServersReplicaHostname(replica *chiv1.ChiReplica) string {
	if util.IsStringBoolTrue(c.chi.Spec.Defaults.ReplicasUseFQDN) {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		return CreatePodHostname(replica) + "." + CreateNamespaceDomainName(replica.Address.Namespace)
	} else {
		return CreatePodHostname(replica)
	}
}

// GetRemoteServers creates "remote_servers.xml" content and calculates data generation parameters for other sections
func (c *ClickHouseConfigGenerator) GetRemoteServers() string {
	b := &bytes.Buffer{}

	// <yandex>
	//		<remote_servers>
	fprintf(b, "<%s>\n", xmlTagYandex)
	fprintf(b, "%4s<remote_servers>\n", " ")

	fprintf(b, "\n")
	fprintf(b, "%8s<!-- User-specified clusters -->\n", " ")
	fprintf(b, "\n")

	// Build each cluster XML
	for clusterIndex := range c.chi.Spec.Configuration.Clusters {
		// Convenience wrapper
		cluster := &c.chi.Spec.Configuration.Clusters[clusterIndex]

		// <my_cluster_name>
		fprintf(b, "%8s<%s>\n", " ", cluster.Name)

		// Build each shard XML
		for shardIndex := range cluster.Layout.Shards {
			// Convenience wrapper
			shard := &cluster.Layout.Shards[shardIndex]

			// <shard>
			//		<internal_replication>VALUE(yes/no)</internal_replication>
			fprintf(b, "%12s<shard>\n", " ")
			fprintf(b, "%16s<internal_replication>%s</internal_replication>\n", " ", shard.InternalReplication)

			//		<weight>X</weight>
			if shard.Weight > 0 {
				fprintf(b, "%16s<weight>%d</weight>\n", " ", shard.Weight)
			}

			// Build each replica's XML
			for replicaIndex := range shard.Replicas {
				// Convenience wrapper
				replica := &shard.Replicas[replicaIndex]

				// <replica>
				//		<host>XXX</host>
				//		<port>XXX</port>
				// </replica>
				fprintf(b, "%16s<replica>\n", " ")
				fprintf(b, "%20s<host>%s</host>\n", " ", c.getRemoteServersReplicaHostname(replica))
				fprintf(b, "%20s<port>%d</port>\n", " ", replica.Port)
				fprintf(b, "%16s</replica>\n", " ")
			}
			// </shard>
			fprintf(b, "%12s</shard>\n", " ")
		}
		// </my_cluster_name>
		fprintf(b, "%8s</%s>\n", " ", cluster.Name)
	}

	fprintf(b, "\n")
	fprintf(b, "%8s<!-- Autogenerated clusters -->\n", " ")
	fprintf(b, "\n")

	// One Shard All Replicas

	// <my_cluster_name>
	clusterName := oneShardAllReplicasClusterName
	fprintf(b, "%8s<%s>\n", " ", clusterName)
	// <shard>
	fprintf(b, "%12s<shard>\n", " ")
	// <internal_replication>
	fprintf(b, "%16s<internal_replication>%s</internal_replication>\n", " ", "yes")

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
				fprintf(b, "%16s<replica>\n", " ")
				fprintf(b, "%20s<host>%s</host>\n", " ", c.getRemoteServersReplicaHostname(replica))
				fprintf(b, "%20s<port>%d</port>\n", " ", replica.Port)
				fprintf(b, "%16s</replica>\n", " ")
			}
		}
	}

	// </shard>
	fprintf(b, "%12s</shard>\n", " ")
	// </my_cluster_name>
	fprintf(b, "%8s</%s>\n", " ", clusterName)

	// All Shards One Replica

	// <my_cluster_name>
	clusterName = allShardsOneReplicaClusterName
	fprintf(b, "%8s<%s>\n", " ", clusterName)

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
				fprintf(b, "%12s<shard>\n", " ")
				// <internal_replication>
				fprintf(b, "%16s<internal_replication>%s</internal_replication>\n", " ", "yes")

				// <replica>
				//		<host>XXX</host>
				//		<port>XXX</port>
				// </replica>
				fprintf(b, "%16s<replica>\n", " ")
				fprintf(b, "%20s<host>%s</host>\n", " ", c.getRemoteServersReplicaHostname(replica))
				fprintf(b, "%20s<port>%d</port>\n", " ", replica.Port)
				fprintf(b, "%16s</replica>\n", " ")

				// </shard>
				fprintf(b, "%12s</shard>\n", " ")
			}
		}
	}

	// </my_cluster_name>
	fprintf(b, "%8s</%s>\n", " ", clusterName)

	// 		</remote_servers>
	// </yandex>
	fprintf(b, "%4s</remote_servers>\n", " ")
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// GetHostMacros creates "macros.xml" content
func (c *ClickHouseConfigGenerator) GetHostMacros(replica *chiv1.ChiReplica) string {
	b := &bytes.Buffer{}

	// <yandex>
	fprintf(b, "<%s>\n", xmlTagYandex)
	// <macros>
	fprintf(b, "%4s<macros>\n", " ")

	// <installation>CHI name</installation>
	fprintf(b, "%8s<installation>%s</installation>\n", " ", replica.Address.ChiName)

	// <CLUSTER_NAME>cluster name</CLUSTER_NAME>
	fprintf(b,
		"%8s<%s>%[2]s</%[2]s>\n",
		" ",
		replica.Address.ClusterName,
	)
	// <CLUSTER_NAME-shard>0-based shard index within cluster</CLUSTER_NAME-shard>
	fprintf(b,
		"%8s<%s-shard>%d</%[2]s-shard>\n",
		" ",
		replica.Address.ClusterName,
		replica.Address.ShardIndex,
	)

	// One Shard All Replicas Cluster
	// <CLUSTER_NAME>cluster name</CLUSTER_NAME>
	fprintf(b,
		"%8s<%s>%[2]s</%[2]s>\n",
		" ",
		oneShardAllReplicasClusterName,
	)
	// <CLUSTER_NAME-shard>0-based shard index within one-shard-cluster would always be 0</CLUSTER_NAME-shard>
	fprintf(b,
		"%8s<%s-shard>%d</%[2]s-shard>\n",
		" ",
		oneShardAllReplicasClusterName,
		0,
	)

	// All Shards One Replica Cluster
	// <CLUSTER_NAME>cluster name</CLUSTER_NAME>
	fprintf(b,
		"%8s<%s>%[2]s</%[2]s>\n",
		" ",
		allShardsOneReplicaClusterName,
	)
	// <CLUSTER_NAME-shard>0-based shard index within all-shards-one-replica-cluster would always be GlobalReplicaIndex</CLUSTER_NAME-shard>
	fprintf(b,
		"%8s<%s-shard>%d</%[2]s-shard>\n",
		" ",
		allShardsOneReplicaClusterName,
		replica.Address.GlobalReplicaIndex,
	)

	// <replica>replica id = full deployment id</replica>
	// full deployment id is unique to identify replica within the cluster
	fprintf(b, "%8s<replica>%s</replica>\n", " ", CreatePodHostname(replica))

	// 		</macros>
	// </yandex>
	fprintf(b, "%4s</macros>\n", " ")
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateXMLConfig creates XML using map[string]string definitions
func generateXMLConfig(data map[string]interface{}, prefix string) string {
	if len(data) == 0 {
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	fprintf(b, "<%s>\n", xmlTagYandex)
	xmlbuilder.GenerateXML(b, data, prefix)
	// <yandex>
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}
