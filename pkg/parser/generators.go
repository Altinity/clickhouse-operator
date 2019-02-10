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
	fmt.Fprintf(b, "<%s>\n%4s<%s>\n", xmlTagYandex, " ", section)
	err := xmlbuilder.GenerateXML(b, data, 4, 4)
	if err != nil {
		glog.V(2).Infof(err.Error())
		return ""
	}
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
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	err := xmlbuilder.GenerateXML(b, chi.Spec.Configuration.Settings, 0, 4, configUsers, configProfiles, configQuotas)
	if err != nil {
		glog.V(2).Infof(err.Error())
		return ""
	}
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)
	return b.String()
}

// genZookeeperConfig creates data for "zookeeper.xml"
func genZookeeperConfig(chi *chiv1.ClickHouseInstallation) string {
	b := &bytes.Buffer{}
	c := len(chi.Spec.Configuration.Zookeeper.Nodes)
	if c == 0 {
		return ""
	}
	fmt.Fprintf(b, "<%s>\n%4s<zookeeper>\n", xmlTagYandex, " ")
	for i := 0; i < c; i++ {
		fmt.Fprintf(b, "%8s<node>\n%12[1]s<host>%s</host>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Host)
		fmt.Fprintf(b, "%12s<port>%d</port>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Port)
		fmt.Fprintf(b, "%8s</node>\n", " ")
	}
	fmt.Fprintf(b, "%4s</zookeeper>\n", " ")
	fmt.Fprintf(b, "%4s<distributed_ddl>\n%8[1]s<path>%s</path>\n", " ", fmt.Sprintf(distributedDDLPattern, chi.Name))
	if chi.Spec.Defaults.DistributedDDL.Profile != "" {
		fmt.Fprintf(b, "%8s<profile>%s</profile>\n", " ", chi.Spec.Defaults.DistributedDDL.Profile)
	}
	fmt.Fprintf(b, "%4[1]s</distributed_ddl>\n</%s>\n", " ", xmlTagYandex)
	return b.String()
}

// genRemoteServersConfig creates data for "remote_servers.xml" and calculates data generation parametes for other sections
func genRemoteServersConfig(chi *chiv1.ClickHouseInstallation, o *genOptions, c []*chiv1.ChiCluster) string {
	var hostDomain string
	b := &bytes.Buffer{}
	dRefIndex := make(map[string]int)
	dID := make(map[string]string)
	for k := range o.dRefsMax {
		dID[k] = randomString()
	}
	if chi.Spec.Defaults.ReplicasUseFQDN == 1 {
		hostDomain = fmt.Sprintf(domainPattern, chi.Namespace)
	}
	fmt.Fprintf(b, "<%s>\n%4s<remote_servers>\n", xmlTagYandex, " ")
	for i := range c {
		fmt.Fprintf(b, "%8s<%s>\n", " ", c[i].Name)
		for j := range c[i].Layout.Shards {
			fmt.Fprintf(b, "%12s<shard>\n%16[1]s<internal_replication>%s</internal_replication>\n",
				" ", c[i].Layout.Shards[j].InternalReplication)
			if c[i].Layout.Shards[j].Weight > 0 {
				fmt.Fprintf(b, "%16s<weight>%d</weight>\n", " ", c[i].Layout.Shards[j].Weight)
			}
			for _, r := range c[i].Layout.Shards[j].Replicas {
				k := r.Deployment.Key
				idx, ok := dRefIndex[k]
				if !ok {
					idx = 1
				} else {
					idx++
					if idx > o.dRefsMax[k] {
						idx = 1
					}
				}
				dRefIndex[k] = idx
				ssNameID := fmt.Sprintf(ssNameIDPattern, dID[k], idx)
				o.ssNames[ssNameID] = k
				o.ssDeployments[k] = &r.Deployment
				fmt.Fprintf(b, "%16s<replica>\n%20[1]s<host>%s</host>\n", " ", fmt.Sprintf(hostnamePattern, ssNameID, hostDomain))
				o.macrosDataIndex[ssNameID] = append(o.macrosDataIndex[ssNameID], &shardsIndexItem{
					cluster: c[i].Name,
					index:   j + 1,
				})
				rPort := 9000
				if r.Port > 0 {
					rPort = int(r.Port)
				}
				fmt.Fprintf(b, "%20s<port>%d</port>\n%16[1]s</replica>\n", " ", rPort)
			}
			fmt.Fprintf(b, "%12s</shard>\n", " ")
		}
		fmt.Fprintf(b, "%8s</%s>\n", " ", c[i].Name)
	}
	fmt.Fprintf(b, "%4s</remote_servers>\n</%s>\n", " ", xmlTagYandex)
	return b.String()
}

// generateHostMacros creates data for particular "macros.xml"
func generateHostMacros(chiName, ssName string, dataIndex shardsIndex) string {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "<%s>\n", xmlTagYandex)
	fmt.Fprintf(b, "%4s<macros>\n%8[1]s<installation>%s</installation>\n", " ", chiName)
	for i := range dataIndex {
		fmt.Fprintf(b, "%8s<%s>%[2]s</%[2]s>\n%8[1]s<%[2]s-shard>%d</%[2]s-shard>\n", " ", dataIndex[i].cluster, dataIndex[i].index)
	}
	fmt.Fprintf(b, "%8s<replica>%s</replica>\n%4[1]s</macros>\n", " ", ssName)
	fmt.Fprintf(b, "</%s>\n", xmlTagYandex)
	return b.String()
}
