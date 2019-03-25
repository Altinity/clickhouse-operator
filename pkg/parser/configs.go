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

package parser

import (
	"bytes"
	"fmt"
	"github.com/golang/glog"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	xmlbuilder "github.com/altinity/clickhouse-operator/pkg/parser/builders/xml"
)

// generateProfilesConfig creates data for "profiles.xml"
func generateProfilesConfig(chi *chiv1.ClickHouseInstallation) string {
	return genConfigXML(chi.Spec.Configuration.Profiles, configProfiles)
}

// generateQuotasConfig creates data for "quotas.xml"
func generateQuotasConfig(chi *chiv1.ClickHouseInstallation) string {
	return genConfigXML(chi.Spec.Configuration.Quotas, configQuotas)
}

// generateUsersConfig creates data for "users.xml"
func generateUsersConfig(chi *chiv1.ClickHouseInstallation) string {
	return genConfigXML(chi.Spec.Configuration.Users, configUsers)
}

// genConfigXML creates XML using map[string]string definitions
func genConfigXML(data map[string]string, section string) string {
	if len(data) == 0 {
		return ""
	}

	b := &bytes.Buffer{}

	// <yandex>
	//		<SECTION>
	fprintf(b, "<%s>\n", xmlTagYandex)
	fprintf(b, "%4s<%s>\n", " ", section)

	err := xmlbuilder.GenerateXML(b, data, 4, 4)
	if err != nil {
		glog.V(2).Infof(err.Error())
		return ""
	}
	//		<SECTION>
	// <yandex>
	fprintf(b, "%4s</%s>\n", " ", section)
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateSettingsConfig creates data for "settings.xml"
func generateSettingsConfig(chi *chiv1.ClickHouseInstallation) string {
	if len(chi.Spec.Configuration.Settings) == 0 {
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	fprintf(b, "<%s>\n", xmlTagYandex)
	err := xmlbuilder.GenerateXML(b, chi.Spec.Configuration.Settings, 0, 4, configUsers, configProfiles, configQuotas)
	if err != nil {
		glog.V(2).Infof(err.Error())
		return ""
	}
	// </yandex>
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateListenConfig creates data for "listen.xml"
func generateListenConfig(chi *chiv1.ClickHouseInstallation) string {
	return `<yandex>
    <!-- Listen wildcard address to allow accepting connections from other containers and host network. -->
    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>
    <listen_try>1</listen_try>

    <!--
    <logger>
        <console>1</console>
    </logger>
    -->
</yandex>
`
}

// generateZookeeperConfig creates data for "zookeeper.xml"
func generateZookeeperConfig(chi *chiv1.ClickHouseInstallation) string {
	if len(chi.Spec.Configuration.Zookeeper.Nodes) == 0 {
		// No Zookeeper nodes provided
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	//		<zookeeper>
	fprintf(b, "<%s>\n", xmlTagYandex)
	fprintf(b, "%4s<zookeeper>\n", " ")
	// Append Zookeeper nodes
	for i := range chi.Spec.Configuration.Zookeeper.Nodes {
		// Convenience wrapper
		node := &chi.Spec.Configuration.Zookeeper.Nodes[i]
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
	fprintf(b, "%8s<path>%s</path>\n", " ", fmt.Sprintf(distributedDDLPattern, chi.Name))
	if chi.Spec.Defaults.DistributedDDL.Profile != "" {
		fprintf(b, "%8s<profile>%s</profile>\n", " ", chi.Spec.Defaults.DistributedDDL.Profile)
	}
	//		</distributed_ddl>
	// </yandex>
	fprintf(b, "%4s</distributed_ddl>\n", " ")
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateRemoteServersConfigReplicaHostname creates hostname (podhostname + service or FQDN) for "remote_servers.xml"
// based on .Spec.Defaults.ReplicasUseFQDN
func generateRemoteServersConfigReplicaHostname(
	chi *chiv1.ClickHouseInstallation,
	replica *chiv1.ChiClusterLayoutShardReplica,
) string {
	if chi.Spec.Defaults.ReplicasUseFQDN == 1 {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		return CreatePodHostname(replica) + "." + CreateNamespaceDomainName(replica.Address.Namespace)
	} else {
		return CreatePodHostname(replica)
	}
}

// generateRemoteServersConfig creates "remote_servers.xml" content and calculates data generation parameters for other sections
func generateRemoteServersConfig(chi *chiv1.ClickHouseInstallation) string {
	b := &bytes.Buffer{}

	// <yandex>
	//		<remote_servers>
	fprintf(b, "<%s>\n", xmlTagYandex)
	fprintf(b, "%4s<remote_servers>\n", " ")

	// Build each cluster XML
	for clusterIndex := range chi.Spec.Configuration.Clusters {
		// Convenience wrapper
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]

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
				fprintf(b, "%20s<host>%s</host>\n", " ", generateRemoteServersConfigReplicaHostname(chi, replica))
				fprintf(b, "%20s<port>%d</port>\n", " ", replica.Port)
				fprintf(b, "%16s</replica>\n", " ")
			}
			// </shard>
			fprintf(b, "%12s</shard>\n", " ")
		}
		// </my_cluster_name>
		fprintf(b, "%8s</%s>\n", " ", cluster.Name)
	}
	// 		</remote_servers>
	// </yandex>
	fprintf(b, "%4s</remote_servers>\n", " ")
	fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateHostMacros creates "macros.xml" content
func generateHostMacros(replica *chiv1.ChiClusterLayoutShardReplica) string {
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
	// <CLUSTER_NAME-shard>1-based shard index within cluster</CLUSTER_NAME-shard>
	fprintf(b,
		"%8s<%s-shard>%d</%[2]s-shard>\n",
		" ",
		replica.Address.ClusterName,
		replica.Address.ShardIndex,
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
