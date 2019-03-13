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

	// += <yandex>
	//      <SECTION>
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	fmt.Fprintf(b, "%4s<%s>\n", " ", section)

	err := xmlbuilder.GenerateXML(b, data, 4, 4)
	if err != nil {
		glog.V(2).Infof(err.Error())
		return ""
	}
	// += <SECTION>
	//  <yandex>
	fmt.Fprintf(b, "%4s</%s>\n", " ", section)
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateSettingsConfig creates data for "settings.xml"
func generateSettingsConfig(chi *chiv1.ClickHouseInstallation) string {
	if len(chi.Spec.Configuration.Settings) == 0 {
		return ""
	}

	b := &bytes.Buffer{}
	// += <yandex>
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	err := xmlbuilder.GenerateXML(b, chi.Spec.Configuration.Settings, 0, 4, configUsers, configProfiles, configQuotas)
	if err != nil {
		glog.V(2).Infof(err.Error())
		return ""
	}
	// += </yandex>
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateZookeeperConfig creates data for "zookeeper.xml"
func generateZookeeperConfig(chi *chiv1.ClickHouseInstallation) string {
	if len(chi.Spec.Configuration.Zookeeper.Nodes) == 0 {
		// No Zookeeper nodes provided
		return ""
	}

	b := &bytes.Buffer{}
	// += <yandex>
	//      <zookeeper>
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	fmt.Fprintf(b, "%4s<zookeeper>\n", " ")
	// Append Zookeeper nodes
	for i := range chi.Spec.Configuration.Zookeeper.Nodes {
		// Convenience wrapper
		node := &chi.Spec.Configuration.Zookeeper.Nodes[i]
		// += <node>
		//      <host>HOST</host>
		//      <port>PORT</port>
		//    </node>
		fmt.Fprintf(b, "%8s<node>\n", " ")
		fmt.Fprintf(b, "%12s<host>%s</host>\n", " ", node.Host)
		fmt.Fprintf(b, "%12s<port>%d</port>\n", " ", node.Port)
		fmt.Fprintf(b, "%8s</node>\n", " ")
	}
	// += </zookeeper>
	fmt.Fprintf(b, "%4s</zookeeper>\n", " ")

	// += <distributed_ddl>
	//      <path>/x/y/chi.name/z</path>
	//      <profile>X</prpfile>
	fmt.Fprintf(b, "%4s<distributed_ddl>\n", " ")
	fmt.Fprintf(b, "%8s<path>%s</path>\n", " ", fmt.Sprintf(distributedDDLPattern, chi.Name))
	if chi.Spec.Defaults.DistributedDDL.Profile != "" {
		fmt.Fprintf(b, "%8s<profile>%s</profile>\n", " ", chi.Spec.Defaults.DistributedDDL.Profile)
	}
	// += </distributed_ddl>
	//  </yandex>
	fmt.Fprintf(b, "%4s</distributed_ddl>\n", " ")
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateRemoteServersConfigReplicaHostname creates hostname (podhostname + service or FQDN) for "remote_servers.xml"
// based on .Spec.Defaults.ReplicasUseFQDN
func generateRemoteServersConfigReplicaHostname(chi *chiv1.ClickHouseInstallation, fullDeploymentID string) string {
	var namespaceDomainName string
	if chi.Spec.Defaults.ReplicasUseFQDN == 1 {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		namespaceDomainName = "." + CreateNamespaceDomainName(chi.Namespace)
	}
	return CreatePodHostname(fullDeploymentID) + namespaceDomainName
}

// generateRemoteServersConfig creates "remote_servers.xml" content and calculates data generation parameters for other sections
func generateRemoteServersConfig(chi *chiv1.ClickHouseInstallation, options *genOptions) string {
	b := &bytes.Buffer{}
	dRefIndex := make(map[string]int)

	// Prepare deployment IDs out of deployment fingerprints
	deploymentID := make(map[string]string)
	for deploymentFingerprint := range options.deploymentCountMax {
		deploymentID[deploymentFingerprint] = generateDeploymentID(deploymentFingerprint)
	}

	// += <yandex>
	// 		<remote_servers>
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	fmt.Fprintf(b, "%4s<remote_servers>\n", " ")

	// Build each cluster XML
	for clusterIndex := range chi.Spec.Configuration.Clusters {
		// Convenience wrapper
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]

		// += <my_cluster_name>
		fmt.Fprintf(b, "%8s<%s>\n", " ", cluster.Name)

		// Build each shard XML
		for shardIndex := range cluster.Layout.Shards {
			// Convenience wrapper
			shard := &cluster.Layout.Shards[shardIndex]

			// += <shard>
			//		<internal_replication>VALUE(yes/no)</internal_replication>
			fmt.Fprintf(b, "%12s<shard>\n", " ")
			fmt.Fprintf(b, "%16s<internal_replication>%s</internal_replication>\n", " ", shard.InternalReplication)

			// += <weight>X</weight>
			if shard.Weight > 0 {
				fmt.Fprintf(b, "%16s<weight>%d</weight>\n", " ", shard.Weight)
			}

			// Build each replica's XML
			for replicaIndex := range shard.Replicas {
				// Convenience wrapper
				replica := &shard.Replicas[replicaIndex]

				fingerprint := replica.Deployment.Fingerprint
				idx, ok := dRefIndex[fingerprint]
				if ok {
					// fingerprint listed - deployment known
					idx++
					if idx > options.deploymentCountMax[fingerprint] {
						idx = 1
					}
				} else {
					// new fingerprint
					idx = 1
				}
				dRefIndex[fingerprint] = idx

				// 1eb454-2 (deployment id - sequential index of this deployment id)
				fullDeploymentID := generateFullDeploymentID(deploymentID[fingerprint], idx)
				options.fullDeploymentIDToFingerprint[fullDeploymentID] = fingerprint
				options.ssDeployments[fingerprint] = &replica.Deployment

				// += <replica>
				//		<host>XXX</host>
				fmt.Fprintf(b, "%16s<replica>\n", " ")
				fmt.Fprintf(b, "%20s<host>%s</host>\n", " ", generateRemoteServersConfigReplicaHostname(chi, fullDeploymentID))

				options.macrosData[fullDeploymentID] = append(
					options.macrosData[fullDeploymentID],
					&macrosDataShardDescription{
						clusterName: cluster.Name,
						index:       shardIndex + 1,
					},
				)

				port := 9000
				if replica.Port > 0 {
					port = int(replica.Port)
				}
				// +=	<port>XXX</port>
				//	</replica>
				fmt.Fprintf(b, "%20s<port>%d</port>\n", " ", port)
				fmt.Fprintf(b, "%16s</replica>\n", " ")
			}
			// += </shard>
			fmt.Fprintf(b, "%12s</shard>\n", " ")
		}
		// += </my_cluster_name>
		fmt.Fprintf(b, "%8s</%s>\n", " ", cluster.Name)
	}
	// += </remote_servers>
	// </yandex>
	fmt.Fprintf(b, "%4s</remote_servers>\n", " ")
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}

// generateHostMacros creates "macros.xml" content
func generateHostMacros(chiName, fullDeploymentID string, shardDescriptions macrosDataShardDescriptionList) string {
	b := &bytes.Buffer{}

	// += <yandex>
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	// += <macros>
	fmt.Fprintf(b, "%4s<macros>\n", " ")

	// += <installation>CHI name</installation>
	fmt.Fprintf(b, "%8s<installation>%s</installation>\n", " ", chiName)

	for i := range shardDescriptions {
		// += <CLUSTER_NAME>cluster name</CLUSTER_NAME>
		fmt.Fprintf(b,
			"%8s<%s>%[2]s</%[2]s>\n",
			" ",
			shardDescriptions[i].clusterName,
		)
		// += <CLUSTER_NAME-shard>1-based shard index within cluster</CLUSTER_NAME-shard>
		fmt.Fprintf(b,
			"%8s<%s-shard>%d</%[2]s-shard>\n",
			" ",
			shardDescriptions[i].clusterName,
			shardDescriptions[i].index,
		)
	}

	// += <replica>replica id = full deployment id</replica>
	// full deployment id is unique to identify replica within the cluster
	fmt.Fprintf(b, "%8s<replica>%s</replica>\n", " ", fullDeploymentID)

	// += </macros>
	// </yandex>
	fmt.Fprintf(b, "%4s</macros>\n", " ")
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}
