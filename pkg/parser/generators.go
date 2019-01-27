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
	fmt.Fprintf(b, "%4s</zookeeper>\n</yandex>\n", " ")
	return b.String()
}

func genRemoteServersConfig(chi *chiv1.ClickHouseInstallation, o *genOptions, c []*chiv1.ChiCluster) string {
	b := &bytes.Buffer{}
	dRefIndex := make(map[string]int)
	dID := make(map[string]string)
	for k := range o.dRefsMax {
		dID[k] = randomString()
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
				prefix := fmt.Sprintf(ssNamePattern, chi.Name, dID[k], idx)
				o.ssNames[prefix] = struct{}{}
				o.ssIndex[prefix] = k
				o.ssDeployments[k] = &r.Deployment
				fmt.Fprintf(b, "%16s<replica>\n%20[1]s<host>%s</host>\n", " ", instanceHostname(chi, prefix))
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

func instanceHostname(chi *chiv1.ClickHouseInstallation, prefix string) string {
	return fmt.Sprintf(hostnamePattern, prefix, chi.Namespace)
}
