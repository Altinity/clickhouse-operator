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

// genProfilesConfig creates data for "profiles.xml"
func genProfilesConfig(chi *chiv1.ClickHouseInstallation) string {
	return genConfigXML(chi.Spec.Configuration.Profiles, configProfiles)
}

// genQuotasConfig creates data for "quotas.xml"
func genQuotasConfig(chi *chiv1.ClickHouseInstallation) string {
	return genConfigXML(chi.Spec.Configuration.Quotas, configQuotas)
}

// genUsersConfig creates data for "users.xml"
func genUsersConfig(chi *chiv1.ClickHouseInstallation) string {
	return genConfigXML(chi.Spec.Configuration.Users, configUsers)
}

// genConfigXML creates XML using map[string]string definitions
func genConfigXML(data map[string]string, section string) string {
	b := &bytes.Buffer{}
	c := len(data)
	if c == 0 {
		return ""
	}

	// += <yandex>
	//      <SECTION>
	fmt.Fprintf(b, "<%s>\n%4s<%s>\n", xmlTagYandex, " ", section)
	err := xmlbuilder.GenerateXML(b, data, 4, 4)
	if err != nil {
		glog.V(2).Infof(err.Error())
		return ""
	}
	// += <SECTION>
	//  <yandex>
	fmt.Fprintf(b, "%4s</%s>\n</%s>\n", " ", section, xmlTagYandex)

	return b.String()
}

// genSettingsConfig creates data for "settings.xml"
func genSettingsConfig(chi *chiv1.ClickHouseInstallation) string {
	b := &bytes.Buffer{}
	c := len(chi.Spec.Configuration.Settings)
	if c == 0 {
		return ""
	}

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

// genZookeeperConfig creates data for "zookeeper.xml"
func genZookeeperConfig(chi *chiv1.ClickHouseInstallation) string {
	b := &bytes.Buffer{}
	zkNodesNum := len(chi.Spec.Configuration.Zookeeper.Nodes)
	if zkNodesNum == 0 {
		return ""
	}

	// += <yandex>
	//      <zookeeper>
	fmt.Fprintf(b, "<%s>\n%4s<zookeeper>\n", xmlTagYandex, " ")
	// Append Zookeeper nodes
	for i := 0; i < zkNodesNum; i++ {
		// += <node>
		//      <host>HOST</host>
		//      <port>PORT</port>
		//    </node>
		fmt.Fprintf(b, "%8s<node>\n%12[1]s<host>%s</host>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Host)
		fmt.Fprintf(b, "%12s<port>%d</port>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Port)
		fmt.Fprintf(b, "%8s</node>\n", " ")
	}
	// += </zookeeper>
	fmt.Fprintf(b, "%4s</zookeeper>\n", " ")

	// += <distributed_ddl>
	//      <path>/x/y/chi.name/z</path>
	//      <profile>X</prpfile>
	fmt.Fprintf(b, "%4s<distributed_ddl>\n%8[1]s<path>%s</path>\n", " ", fmt.Sprintf(distributedDDLPattern, chi.Name))
	if chi.Spec.Defaults.DistributedDDL.Profile != "" {
		fmt.Fprintf(b, "%8s<profile>%s</profile>\n", " ", chi.Spec.Defaults.DistributedDDL.Profile)
	}
	// += </distributed_ddl>
	//  </yandex>
	fmt.Fprintf(b, "%4[1]s</distributed_ddl>\n</%s>\n", " ", xmlTagYandex)

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
	return CreatePodHostnamePlusService(fullDeploymentID) + namespaceDomainName
}

// generateRemoteServersConfig creates "remote_servers.xml" content and calculates data generation parameters for other sections
func generateRemoteServersConfig(chi *chiv1.ClickHouseInstallation, opts *genOptions, clusters []*chiv1.ChiCluster) string {
	b := &bytes.Buffer{}
	dRefIndex := make(map[string]int)

	// Prepare deployment IDs out of deployment fingerprints
	deploymentID := make(map[string]string)
	for deploymentFingerprint := range opts.deploymentCountMax {
		deploymentID[deploymentFingerprint] = generateDeploymentID(deploymentFingerprint)
	}

	// += <yandex>
	// 		<remote_servers>
	fmt.Fprintf(b, "<%s>\n%4s<remote_servers>\n", xmlTagYandex, " ")
	// Build each cluster XML
	for clusterIndex := range clusters {

		// += <my_cluster_name>
		fmt.Fprintf(b, "%8s<%s>\n", " ", clusters[clusterIndex].Name)

		// Build each shard XML
		for shardIndex := range clusters[clusterIndex].Layout.Shards {

			// += <shard>
			//		<internal_replication>yes</internal_replication>
			fmt.Fprintf(b,
				"%12s<shard>\n%16[1]s<internal_replication>%s</internal_replication>\n",
				" ",
				clusters[clusterIndex].Layout.Shards[shardIndex].InternalReplication,
			)

			// += <weight>X</weight>
			if clusters[clusterIndex].Layout.Shards[shardIndex].Weight > 0 {
				fmt.Fprintf(b, "%16s<weight>%d</weight>\n", " ", clusters[clusterIndex].Layout.Shards[shardIndex].Weight)
			}

			// Build each replica XML
			for _, replica := range clusters[clusterIndex].Layout.Shards[shardIndex].Replicas {
				fingerprint := replica.Deployment.Fingerprint
				idx, ok := dRefIndex[fingerprint]
				if ok {
					// fingerprint listed - deployment known
					idx++
					if idx > opts.deploymentCountMax[fingerprint] {
						idx = 1
					}
				} else {
					// new fingerprint
					idx = 1
				}
				dRefIndex[fingerprint] = idx

				// 1eb454-2 (deployment id - sequential index of this deployment id)
				fullDeploymentID := generateFullDeploymentID(deploymentID[fingerprint], idx)
				opts.fullDeploymentIDToFingerprint[fullDeploymentID] = fingerprint
				opts.ssDeployments[fingerprint] = &replica.Deployment

				// += <replica>
				//		<host>XXX</host>
				fmt.Fprintf(b, "%16s<replica>\n%20[1]s<host>%s</host>\n", " ", generateRemoteServersConfigReplicaHostname(chi, fullDeploymentID))

				opts.macrosData[fullDeploymentID] = append(
					opts.macrosData[fullDeploymentID],
					&macrosDataShardDescription{
						clusterName: clusters[clusterIndex].Name,
						index:   	 shardIndex + 1,
					},
				)

				port := 9000
				if replica.Port > 0 {
					port = int(replica.Port)
				}
				// +=	<port>XXX</port>
				//	</replica>
				fmt.Fprintf(b, "%20s<port>%d</port>\n%16[1]s</replica>\n", " ", port)
			}
			// += </shard>
			fmt.Fprintf(b, "%12s</shard>\n", " ")
		}
		// += </my_cluster_name>
		fmt.Fprintf(b, "%8s</%s>\n", " ", clusters[clusterIndex].Name)
	}
	// += </remote_servers>
	// </yandex>
	fmt.Fprintf(b, "%4s</remote_servers>\n</%s>\n", " ", xmlTagYandex)

	return b.String()
}

// generateHostMacros creates "macros.xml" content
func generateHostMacros(chiName, fullDeploymentID string, shardDescriptions shardsIndex) string {
	b := &bytes.Buffer{}

	// += <yandex>
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	// += <macros>
	fmt.Fprintf(b, "%4s<macros>\n", " ")

	// +=     <installation>CHI name</installation>
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

	// += <replica>replica id</replica>
	fmt.Fprintf(b, "%8s<replica>%s</replica>\n", " ", fullDeploymentID)

	// += </macros>
	fmt.Fprintf(b, "%4s</macros>\n", " ")
	// += </yandex>
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}
