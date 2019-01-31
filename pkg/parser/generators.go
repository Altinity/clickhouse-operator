package parser

import (
	"bytes"
	"fmt"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func genUsersConfig(chi *chiv1.ClickHouseInstallation) string {
	b := &bytes.Buffer{}
	return b.String()
}

func genZookeeperConfig(chi *chiv1.ClickHouseInstallation) string {
	b := &bytes.Buffer{}
	c := len(chi.Spec.Configuration.Zookeeper.Nodes)
	if c == 0 {
		return ""
	}
	fmt.Fprintf(b, "<yandex>\n%4s<zookeeper>\n", " ")
	for i := 0; i < c; i++ {
		fmt.Fprintf(b, "%8s<node>\n%12[1]s<host>%s</host>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Host)
		if chi.Spec.Configuration.Zookeeper.Nodes[i].Port > 0 {
			fmt.Fprintf(b, "%12s<port>%d</port>\n", " ", chi.Spec.Configuration.Zookeeper.Nodes[i].Port)
		}
		fmt.Fprintf(b, "%8s</node>\n", " ")
	}
	fmt.Fprintf(b, "%4s</zookeeper>\n", " ")
	fmt.Fprintf(b, "%4s<distributed_ddl>\n%8[1]s<path>%s</path>\n", " ", fmt.Sprintf(distributedDDLPattern, chi.Name))
	if chi.Spec.Defaults.DistributedDDL.Profile != "" {
		fmt.Fprintf(b, "%8s<profile>%s</profile>\n", " ", chi.Spec.Defaults.DistributedDDL.Profile)
	}
	fmt.Fprintf(b, "%4[1]s</distributed_ddl>\n</yandex>\n", " ")
	return b.String()
}

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
	fmt.Fprintf(b, "<yandex>\n%4s<remote_servers>\n", " ")
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
				o.hostNames[ssNameID] = fmt.Sprintf(hostnamePattern, ssNameID, hostDomain)
				o.ssDeployments[k] = &r.Deployment
				fmt.Fprintf(b, "%16s<replica>\n%20[1]s<host>%s</host>\n", " ", o.hostNames[ssNameID])
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
	fmt.Fprintf(b, "%4s</remote_servers>\n</yandex>\n", " ")
	return b.String()
}

func generateHostMacros(chiName, ssName string, dataIndex shardsIndex) string {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "<yandex>\n%4s<macros>\n%8[1]s<installation>%s</installation>\n", " ", chiName)
	for i := range dataIndex {
		fmt.Fprintf(b, "%8s<%s>%[2]s</%[2]s>\n%8[1]s<%[2]s-shard>%d</%[2]s-shard>\n", " ", dataIndex[i].cluster, dataIndex[i].index)
	}
	fmt.Fprintf(b, "%8s<replica>%s</replica>\n%4[1]s</macros>\n</yandex>\n", " ", ssName)
	return b.String()
}
