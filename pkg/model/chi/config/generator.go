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

package config

import (
	"bytes"
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/model/common/config"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	InternodeClusterSecretEnvName = "CLICKHOUSE_INTERNODE_CLUSTER_SECRET"
)

const (
	// Pattern for string path used in <distributed_ddl><path>XXX</path></distributed_ddl>
	DistributedDDLPathPattern = "/clickhouse/%s/task_queue/ddl"

	// Special auto-generated clusters. Each of these clusters lay over all replicas in CHI
	// 1. Cluster with one shard and all replicas. Used to duplicate data over all replicas.
	// 2. Cluster with all shards (1 replica). Used to gather/scatter data over all replicas.
	OneShardAllReplicasClusterName = "all-replicated"
	AllShardsOneReplicaClusterName = "all-sharded"
	AllClustersClusterName         = "all-clusters"
)

// Generator generates configuration files content for specified CR
// Configuration files content is an XML ATM, so config generator provides set of Get*() functions
// which produces XML which are parts of configuration and can/should be used as content of config files.
type Generator struct {
	cr    chi.ICustomResource
	namer interfaces.INameManager
	opts  *GeneratorOptions
}

// newGenerator returns new Generator struct
func newGenerator(cr chi.ICustomResource, namer interfaces.INameManager, opts *GeneratorOptions) *Generator {
	return &Generator{
		cr:    cr,
		namer: namer,
		opts:  opts,
	}
}

// getGlobalSettings creates data for global section of "settings.xml"
func (c *Generator) getGlobalSettings() string {
	// No host specified means request to generate common config
	return c.opts.Settings.ClickHouseConfig()
}

// getHostSettings creates data for host section of "settings.xml"
func (c *Generator) getHostSettings(host *chi.Host) string {
	// Generate config for the specified host
	return host.Settings.ClickHouseConfig()
}

// getSectionFromFiles creates data for custom common config files
func (c *Generator) getSectionFromFiles(section chi.SettingsSection, includeUnspecified bool, host *chi.Host) map[string]string {
	var files *chi.Settings
	if host == nil {
		// We are looking into Common files
		files = c.opts.Files
	} else {
		// We are looking into host's personal files
		files = host.Files
	}

	// Extract particular section from files

	return files.GetSection(section, includeUnspecified)
}

// getUsers creates data for users section. Used as "users.xml"
func (c *Generator) getUsers() string {
	return c.opts.Users.ClickHouseConfig(configUsers)
}

// getProfiles creates data for profiles section. Used as "profiles.xml"
func (c *Generator) getProfiles() string {
	return c.opts.Profiles.ClickHouseConfig(configProfiles)
}

// getQuotas creates data for "quotas.xml"
func (c *Generator) getQuotas() string {
	return c.opts.Quotas.ClickHouseConfig(configQuotas)
}

// getHostZookeeper creates data for "zookeeper.xml"
func (c *Generator) getHostZookeeper(host *chi.Host) string {
	zk := host.GetZookeeper()

	if zk.IsEmpty() {
		// No Zookeeper nodes provided
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	//		<zookeeper>
	util.Iline(b, 0, "<"+xmlTagYandex+">")
	util.Iline(b, 4, "<zookeeper>")

	// Append Zookeeper nodes
	for i := range zk.Nodes {
		// Convenience wrapper
		node := &zk.Nodes[i]

		if !node.Port.IsValid() {
			// Node has to have correct port specified
			continue
		}

		// <node>
		//		<host>HOST</host>
		//		<port>PORT</port>
		//		<secure>%d</secure>
		// </node>
		util.Iline(b, 8, "<node>")
		util.Iline(b, 8, "    <host>%s</host>", node.Host)
		util.Iline(b, 8, "    <port>%d</port>", node.Port.Value())
		if node.Secure.HasValue() {
			util.Iline(b, 8, "    <secure>%d</secure>", c.getSecure(node))
		}
		util.Iline(b, 8, "</node>")
	}

	// Append session_timeout_ms
	if zk.SessionTimeoutMs > 0 {
		util.Iline(b, 8, "<session_timeout_ms>%d</session_timeout_ms>", zk.SessionTimeoutMs)
	}

	// Append operation_timeout_ms
	if zk.OperationTimeoutMs > 0 {
		util.Iline(b, 8, "<operation_timeout_ms>%d</operation_timeout_ms>", zk.OperationTimeoutMs)
	}

	// Append root
	if len(zk.Root) > 0 {
		util.Iline(b, 8, "<root>%s</root>", zk.Root)
	}

	// Append identity
	if len(zk.Identity) > 0 {
		util.Iline(b, 8, "<identity>%s</identity>", zk.Identity)
	}

	// </zookeeper>
	util.Iline(b, 4, "</zookeeper>")

	// <distributed_ddl>
	//      <path>/x/y/chi.name/z</path>
	//      <profile>X</profile>
	util.Iline(b, 4, "<distributed_ddl>")
	util.Iline(b, 4, "    <path>%s</path>", c.getDistributedDDLPath())
	if c.opts.DistributedDDL.HasProfile() {
		util.Iline(b, 4, "    <profile>%s</profile>", c.opts.DistributedDDL.GetProfile())
	}
	//		</distributed_ddl>
	// </yandex>
	util.Iline(b, 4, "</distributed_ddl>")
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// chiHostsNum count hosts according to the options
func (c *Generator) chiHostsNum(selector *config.HostSelector) int {
	num := 0
	c.cr.WalkHosts(func(host *chi.Host) error {
		if selector.Include(host) {
			num++
		}
		return nil
	})
	return num
}

// clusterHostsNum count hosts according to the options
func (c *Generator) clusterHostsNum(cluster chi.ICluster, selector *config.HostSelector) int {
	num := 0
	// Build each shard XML
	cluster.WalkShards(func(index int, shard chi.IShard) error {
		num += c.shardHostsNum(shard, selector)
		return nil
	})
	return num
}

// shardHostsNum count hosts according to the options
func (c *Generator) shardHostsNum(shard chi.IShard, selector *config.HostSelector) int {
	num := 0
	shard.WalkHosts(func(host *chi.Host) error {
		if selector.Include(host) {
			num++
		}
		return nil
	})
	return num
}

func (c *Generator) getRemoteServersReplica(host *chi.Host, b *bytes.Buffer) {
	// <replica>
	//		<host>XXX</host>
	//		<port>XXX</port>
	//		<secure>XXX</secure>
	// </replica>
	var port int32
	if host.IsSecure() {
		port = host.TLSPort.Value()
	} else {
		port = host.TCPPort.Value()
	}
	util.Iline(b, 16, "<replica>")
	util.Iline(b, 16, "    <host>%s</host>", c.getRemoteServersReplicaHostname(host))
	util.Iline(b, 16, "    <port>%d</port>", port)
	util.Iline(b, 16, "    <secure>%d</secure>", c.getSecure(host))
	util.Iline(b, 16, "</replica>")
}

// getRemoteServers creates "remote_servers.xml" content and calculates data generation parameters for other sections
func (c *Generator) getRemoteServers(selector *config.HostSelector) string {
	if selector == nil {
		selector = defaultIncludeAllSelector()
	}

	b := &bytes.Buffer{}

	// <yandex>
	//		<remote_servers>
	util.Iline(b, 0, "<"+xmlTagYandex+">")
	util.Iline(b, 4, "<remote_servers>")

	util.Iline(b, 8, "<!-- User-specified clusters -->")

	// Build each cluster XML
	c.cr.WalkClusters(func(cluster chi.ICluster) error {
		if c.clusterHostsNum(cluster, selector) < 1 {
			// Skip empty cluster
			return nil
		}
		// <my_cluster_name>
		util.Iline(b, 8, "<%s>", cluster.GetName())

		// <secret>VALUE</secret>
		switch cluster.GetSecret().Source() {
		case chi.ClusterSecretSourcePlaintext:
			// Secret value is explicitly specified
			util.Iline(b, 12, "<secret>%s</secret>", cluster.GetSecret().Value)
		case chi.ClusterSecretSourceSecretRef, chi.ClusterSecretSourceAuto:
			// Use secret via ENV var from secret
			util.Iline(b, 12, `<secret from_env="%s" />`, InternodeClusterSecretEnvName)
		}

		// Build each shard XML
		cluster.WalkShards(func(index int, shard chi.IShard) error {
			if c.shardHostsNum(shard, selector) < 1 {
				// Skip empty shard
				return nil
			}

			// <shard>
			//		<internal_replication>VALUE(true/false)</internal_replication>
			util.Iline(b, 12, "<shard>")
			util.Iline(b, 16, "<internal_replication>%s</internal_replication>", shard.GetInternalReplication())

			//		<weight>X</weight>
			if shard.HasWeight() {
				util.Iline(b, 16, "<weight>%d</weight>", shard.GetWeight())
			}

			shard.WalkHosts(func(host *chi.Host) error {
				if selector.Include(host) {
					c.getRemoteServersReplica(host, b)
					log.V(2).M(host).Info("Adding host to remote servers: %s", host.GetName())
				} else {
					log.V(1).M(host).Info("SKIP host from remote servers: %s", host.GetName())
				}
				return nil
			})

			// </shard>
			util.Iline(b, 12, "</shard>")

			return nil
		})
		// </my_cluster_name>
		util.Iline(b, 8, "</%s>", cluster.GetName())

		return nil
	})

	// Auto-generated clusters

	if c.chiHostsNum(selector) < 1 {
		util.Iline(b, 8, "<!-- Autogenerated clusters are skipped due to absence of hosts -->")
	} else {
		util.Iline(b, 8, "<!-- Autogenerated clusters -->")
		// One Shard All Replicas

		// <sll-replicated>
		//     <shard>
		//         <internal_replication>
		clusterName := OneShardAllReplicasClusterName
		util.Iline(b, 8, "<%s>", clusterName)
		util.Iline(b, 8, "    <shard>")
		util.Iline(b, 8, "        <internal_replication>true</internal_replication>")
		c.cr.WalkHosts(func(host *chi.Host) error {
			if selector.Include(host) {
				c.getRemoteServersReplica(host, b)
			}
			return nil
		})

		//     </shard>
		// </sll-replicated>
		util.Iline(b, 8, "    </shard>")
		util.Iline(b, 8, "</%s>", clusterName)

		// All Shards One Replica

		// <all-sharded>
		clusterName = AllShardsOneReplicaClusterName
		util.Iline(b, 8, "<%s>", clusterName)
		c.cr.WalkHosts(func(host *chi.Host) error {
			if selector.Include(host) {
				// <shard>
				//     <internal_replication>
				util.Iline(b, 12, "<shard>")
				util.Iline(b, 12, "    <internal_replication>false</internal_replication>")

				c.getRemoteServersReplica(host, b)

				// </shard>
				util.Iline(b, 12, "</shard>")
			}
			return nil
		})
		// </all-sharded>
		util.Iline(b, 8, "</%s>", clusterName)

		// All shards from all clusters

		// <all-clusters>
		clusterName = AllClustersClusterName
		util.Iline(b, 8, "<%s>", clusterName)
		c.cr.WalkClusters(func(cluster chi.ICluster) error {
			cluster.WalkShards(func(index int, shard chi.IShard) error {
				if c.shardHostsNum(shard, selector) < 1 {
					// Skip empty shard
					return nil
				}
				util.Iline(b, 12, "<shard>")
				util.Iline(b, 12, "    <internal_replication>%s</internal_replication>", shard.GetInternalReplication())

				shard.WalkHosts(func(host *chi.Host) error {
					if selector.Include(host) {
						c.getRemoteServersReplica(host, b)
					}
					return nil
				})
				util.Iline(b, 12, "</shard>")

				return nil
			})

			return nil
		})
		// </all-clusters>
		util.Iline(b, 8, "</%s>", clusterName)
	}

	// 		</remote_servers>
	// </yandex>
	util.Iline(b, 0, "    </remote_servers>")
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// getHostMacros creates "macros.xml" content
func (c *Generator) getHostMacros(host *chi.Host) string {
	b := &bytes.Buffer{}

	// <yandex>
	//     <macros>
	util.Iline(b, 0, "<"+xmlTagYandex+">")
	util.Iline(b, 0, "    <macros>")

	// <installation>CHI-name-macros-value</installation>
	util.Iline(b, 8, "<installation>%s</installation>", host.Runtime.Address.CHIName)

	// <CLUSTER_NAME>cluster-name-macros-value</CLUSTER_NAME>
	// util.Iline(b, 8, "<%s>%[2]s</%[1]s>", replica.Address.ClusterName, c.getMacrosCluster(replica.Address.ClusterName))
	// <CLUSTER_NAME-shard>0-based shard index within cluster</CLUSTER_NAME-shard>
	// util.Iline(b, 8, "<%s-shard>%d</%[1]s-shard>", replica.Address.ClusterName, replica.Address.ShardIndex)

	// All Shards One Replica ChkCluster
	// <CLUSTER_NAME-shard>0-based shard index within all-shards-one-replica-cluster</CLUSTER_NAME-shard>
	util.Iline(b, 8, "<%s-shard>%d</%[1]s-shard>", AllShardsOneReplicaClusterName, host.Runtime.Address.CHIScopeIndex)

	// <cluster> and <shard> macros are applicable to main cluster only. All aux clusters do not have ambiguous macros
	// <cluster></cluster> macro
	util.Iline(b, 8, "<cluster>%s</cluster>", host.Runtime.Address.ClusterName)
	// <shard></shard> macro
	util.Iline(b, 8, "<shard>%s</shard>", host.Runtime.Address.ShardName)
	// <replica>replica id = full deployment id</replica>
	// full deployment id is unique to identify replica within the cluster
	util.Iline(b, 8, "<replica>%s</replica>", c.namer.Name(interfaces.NamePodHostname, host))

	// 		</macros>
	// </yandex>
	util.Iline(b, 0, "    </macros>")
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// getHostHostnameAndPorts creates "ports.xml" content
func (c *Generator) getHostHostnameAndPorts(host *chi.Host) string {

	b := &bytes.Buffer{}

	// <yandex>
	util.Iline(b, 0, "<"+xmlTagYandex+">")

	if host.TCPPort.Value() != chi.ChDefaultTCPPortNumber {
		util.Iline(b, 4, "<tcp_port>%d</tcp_port>", host.TCPPort.Value())
	}
	if host.TLSPort.Value() != chi.ChDefaultTLSPortNumber {
		util.Iline(b, 4, "<tcp_port_secure>%d</tcp_port_secure>", host.TLSPort.Value())
	}
	if host.HTTPPort.Value() != chi.ChDefaultHTTPPortNumber {
		util.Iline(b, 4, "<http_port>%d</http_port>", host.HTTPPort.Value())
	}
	if host.HTTPSPort.Value() != chi.ChDefaultHTTPSPortNumber {
		util.Iline(b, 4, "<https_port>%d</https_port>", host.HTTPSPort.Value())
	}

	// Interserver host and port
	util.Iline(b, 4, "<interserver_http_host>%s</interserver_http_host>", c.getRemoteServersReplicaHostname(host))
	if host.InterserverHTTPPort.Value() != chi.ChDefaultInterserverHTTPPortNumber {
		util.Iline(b, 4, "<interserver_http_port>%d</interserver_http_port>", host.InterserverHTTPPort.Value())
	}

	// </yandex>
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

//
// Paths and Names section
//

// getDistributedDDLPath returns string path used in <distributed_ddl><path>XXX</path></distributed_ddl>
func (c *Generator) getDistributedDDLPath() string {
	return fmt.Sprintf(DistributedDDLPathPattern, c.cr.GetName())
}

// getRemoteServersReplicaHostname returns hostname (podhostname + service or FQDN) for "remote_servers.xml"
// based on .Spec.Defaults.ReplicasUseFQDN
func (c *Generator) getRemoteServersReplicaHostname(host *chi.Host) string {
	return c.namer.Name(interfaces.NameInstanceHostname, host)
}

// Secured interface for nodes and hosts
type Secured interface {
	IsSecure() bool
}

// getSecure gets config-usable value for host or node secure flag
func (c *Generator) getSecure(host Secured) int {
	if host.IsSecure() {
		return 1
	}
	return 0
}
