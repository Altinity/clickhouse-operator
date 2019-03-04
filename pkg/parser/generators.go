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

// genRemoteServersConfig creates data for "remote_servers.xml" and calculates data generation parameters for other sections
func genRemoteServersConfig(chi *chiv1.ClickHouseInstallation, opts *genOptions, clusters []*chiv1.ChiCluster) string {
	var domainName string
	b := &bytes.Buffer{}
	dRefIndex := make(map[string]int)
	dID := make(map[string]string)

	for k := range opts.deploymentCountMax {
		dID[k] = randomString()
	}
	if chi.Spec.Defaults.ReplicasUseFQDN == 1 {
		domainName = CreateDomainName(chi.Namespace)
	}

	// += <yandex>
	// 		<remote_servers>
	fmt.Fprintf(b, "<%s>\n%4s<remote_servers>\n", xmlTagYandex, " ")
	// Build each cluster XML
	for i := range clusters {

		// += <my_cluster_name>
		fmt.Fprintf(b, "%8s<%s>\n", " ", clusters[i].Name)

		// Build each shard XML
		for j := range clusters[i].Layout.Shards {

			// += <shard>
			//		<internal_replication>yes</internal_replication>
			fmt.Fprintf(b,
				"%12s<shard>\n%16[1]s<internal_replication>%s</internal_replication>\n",
				" ",
				clusters[i].Layout.Shards[j].InternalReplication,
			)

			// += <weight>X</weight>
			if clusters[i].Layout.Shards[j].Weight > 0 {
				fmt.Fprintf(b, "%16s<weight>%d</weight>\n", " ", clusters[i].Layout.Shards[j].Weight)
			}

			// Build each replica XML
			for _, replica := range clusters[i].Layout.Shards[j].Replicas {
				fingerprint := replica.Deployment.Fingerprint
				idx, ok := dRefIndex[fingerprint]
				if !ok {
					idx = 1
				} else {
					idx++
					if idx > opts.deploymentCountMax[fingerprint] {
						idx = 1
					}
				}
				dRefIndex[fingerprint] = idx
				ssNameID := fmt.Sprintf(ssNameIDPattern, dID[fingerprint], idx)
				opts.ssNames[ssNameID] = fingerprint
				opts.ssDeployments[fingerprint] = &replica.Deployment

				// += <replica>
				//		<host>XXX</host>
				fmt.Fprintf(b, "%16s<replica>\n%20[1]s<host>%s</host>\n", " ", fmt.Sprintf(hostnamePattern, ssNameID, domainName))

				opts.macrosDataIndex[ssNameID] = append(
					opts.macrosDataIndex[ssNameID],
					&shardsIndexItem{
						cluster: clusters[i].Name,
						index:   j + 1,
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
		fmt.Fprintf(b, "%8s</%s>\n", " ", clusters[i].Name)
	}
	// += </remote_servers>
	// </yandex>
	fmt.Fprintf(b, "%4s</remote_servers>\n</%s>\n", " ", xmlTagYandex)

	return b.String()
}

// generateHostMacros creates data for particular "macros.xml"
func generateHostMacros(chiName, ssName string, dataIndex shardsIndex) string {
	b := &bytes.Buffer{}

	// += <yandex>
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)

	// += <macros>
	//      <installation>CHI name</installation>
	fmt.Fprintf(b, "%4s<macros>\n%8[1]s<installation>%s</installation>\n", " ", chiName)
	for i := range dataIndex {
		// += <CLUSTER_NAME>CLUSTER_NAME</CLUSTER_NAME>
		//    <CLUSTER_NAMEs-shard>SHARD_NAME</CLUSTER_NAMEs-shard>
		fmt.Fprintf(b, "%8s<%s>%[2]s</%[2]s>\n%8[1]s<%[2]s-shard>%d</%[2]s-shard>\n", " ", dataIndex[i].cluster, dataIndex[i].index)
	}

	// += <replica>DNS name</replica>
	//   </macros>
	fmt.Fprintf(b, "%8s<replica>%s</replica>\n%4[1]s</macros>\n", " ", ssName)
	// += </yandex>
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)

	return b.String()
}
