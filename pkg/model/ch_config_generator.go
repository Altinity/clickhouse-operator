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
	// Pattern for string path used in <distributed_ddl><path>XXX</path></distributed_ddl>
	distributedDDLPathPattern = "/clickhouse/%s/task_queue/ddl"

	// Special auto-generated clusters. Each of these clusters lay over all replicas in CHI
	// 1. Cluster with one shard and all replicas. Used to duplicate data over all replicas.
	// 2. Cluster with all shards (1 replica). Used to gather/scatter data over all replicas.
	oneShardAllReplicasClusterName = "all-replicated"
	allShardsOneReplicaClusterName = "all-sharded"
)

// ClickHouseConfigGenerator generates ClickHouse configuration files content for specified CHI
// ClickHouse configuration files content is an XML ATM, so config generator provides set of Get*() functions
// which produces XML which are parts of ClickHouse configuration and can/should be used as ClickHouse config files.
type ClickHouseConfigGenerator struct {
	chi *chiv1.ClickHouseInstallation
}

// NewClickHouseConfigGenerator returns new ClickHouseConfigGenerator struct
func NewClickHouseConfigGenerator(chi *chiv1.ClickHouseInstallation) *ClickHouseConfigGenerator {
	return &ClickHouseConfigGenerator{
		chi: chi,
	}
}

// GetUsers creates data for users section. Used as "users.xml"
func (c *ClickHouseConfigGenerator) GetUsers() string {
	return c.generateXMLConfig(c.chi.Spec.Configuration.Users, configUsers)
}

// GetProfiles creates data for profiles section. Used as "profiles.xml"
func (c *ClickHouseConfigGenerator) GetProfiles() string {
	return c.generateXMLConfig(c.chi.Spec.Configuration.Profiles, configProfiles)
}

// GetQuotas creates data for "quotas.xml"
func (c *ClickHouseConfigGenerator) GetQuotas() string {
	return c.generateXMLConfig(c.chi.Spec.Configuration.Quotas, configQuotas)
}

// GetSettings creates data for "settings.xml"
func (c *ClickHouseConfigGenerator) GetSettings(host *chiv1.ChiHost) string {
	if host == nil {
		// No host specified means request to generate common config
		return c.generateXMLConfig(c.chi.Spec.Configuration.Settings, "")
	}
	// Generate config for the specified host
	return c.generateXMLConfig(host.Settings, "")
}

// GetFiles creates data for custom common config files
func (c *ClickHouseConfigGenerator) GetFiles(section chiv1.SettingsSection, includeUnspecified bool, host *chiv1.ChiHost) map[string]string {
	var files *chiv1.Settings
	if host == nil {
		// We are looking into Common files
		files = c.chi.Spec.Configuration.Files
	} else {
		files = host.Files
	}

	// Extract particular section from cm

	return files.GetSectionStringMap(section, includeUnspecified)
}

// GetHostZookeeper creates data for "zookeeper.xml"
func (c *ClickHouseConfigGenerator) GetHostZookeeper(host *chiv1.ChiHost) string {
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
		// <node>
		//		<host>HOST</host>
		//		<port>PORT</port>
		// </node>
		util.Iline(b, 8, "<node>")
		util.Iline(b, 8, "    <host>%s</host>", node.Host)
		util.Iline(b, 8, "    <port>%d</port>", node.Port)
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
	if c.chi.Spec.Defaults.DistributedDDL.HasProfile() {
		util.Iline(b, 4, "    <profile>%s</profile>", c.chi.Spec.Defaults.DistributedDDL.GetProfile())
	}
	//		</distributed_ddl>
	// </yandex>
	util.Iline(b, 4, "</distributed_ddl>")
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// RemoteServersGeneratorOptions specifies options for remote-servers generator
type RemoteServersGeneratorOptions struct {
	exclude struct {
		reconcileAttributes *chiv1.ChiHostReconcileAttributes
		hosts               []*chiv1.ChiHost
	}
}

// NewRemoteServersGeneratorOptions creates new remote-servers generator options
func NewRemoteServersGeneratorOptions() *RemoteServersGeneratorOptions {
	return &RemoteServersGeneratorOptions{}
}

// ExcludeHost specifies to exclude host
func (o *RemoteServersGeneratorOptions) ExcludeHost(host *chiv1.ChiHost) *RemoteServersGeneratorOptions {
	if (o == nil) || (host == nil) {
		return o
	}

	o.exclude.hosts = append(o.exclude.hosts, host)
	return o
}

// ExcludeReconcileAttributes specifies to exclude reconcile attributes
func (o *RemoteServersGeneratorOptions) ExcludeReconcileAttributes(attrs *chiv1.ChiHostReconcileAttributes) *RemoteServersGeneratorOptions {
	if (o == nil) || (attrs == nil) {
		return o
	}

	o.exclude.reconcileAttributes = attrs
	return o
}

// Skip specifies to skip the host
func (o *RemoteServersGeneratorOptions) Skip(host *chiv1.ChiHost) bool {
	if o == nil {
		return false
	}

	if o.exclude.reconcileAttributes.Any(host.ReconcileAttributes) {
		return true
	}

	for _, val := range o.exclude.hosts {
		if val == host {
			return true
		}
	}

	return false
}

// Include specifies to include the host
func (o *RemoteServersGeneratorOptions) Include(host *chiv1.ChiHost) bool {
	if o == nil {
		return false
	}

	if o.exclude.reconcileAttributes.Any(host.ReconcileAttributes) {
		return false
	}

	for _, val := range o.exclude.hosts {
		if val == host {
			return false
		}
	}

	return true
}

// defaultRemoteServersGeneratorOptions
func defaultRemoteServersGeneratorOptions() *RemoteServersGeneratorOptions {
	return NewRemoteServersGeneratorOptions()
}

// CHIHostsNum count hosts according to the options
func (c *ClickHouseConfigGenerator) CHIHostsNum(options *RemoteServersGeneratorOptions) int {
	num := 0
	c.chi.WalkHosts(func(host *chiv1.ChiHost) error {
		if options.Include(host) {
			num++
		}
		return nil
	})
	return num
}

// ClusterHostsNum count hosts according to the options
func (c *ClickHouseConfigGenerator) ClusterHostsNum(cluster *chiv1.ChiCluster, options *RemoteServersGeneratorOptions) int {
	num := 0
	// Build each shard XML
	cluster.WalkShards(func(index int, shard *chiv1.ChiShard) error {
		num += c.ShardHostsNum(shard, options)
		return nil
	})
	return num
}

// ShardHostsNum count hosts according to the options
func (c *ClickHouseConfigGenerator) ShardHostsNum(shard *chiv1.ChiShard, options *RemoteServersGeneratorOptions) int {
	num := 0
	shard.WalkHosts(func(host *chiv1.ChiHost) error {
		if options.Include(host) {
			num++
		}
		return nil
	})
	return num
}

// GetRemoteServers creates "remote_servers.xml" content and calculates data generation parameters for other sections
func (c *ClickHouseConfigGenerator) GetRemoteServers(options *RemoteServersGeneratorOptions) string {
	if options == nil {
		options = defaultRemoteServersGeneratorOptions()
	}

	b := &bytes.Buffer{}

	// <yandex>
	//		<remote_servers>
	util.Iline(b, 0, "<"+xmlTagYandex+">")
	util.Iline(b, 4, "<remote_servers>")

	util.Iline(b, 8, "<!-- User-specified clusters -->")

	// Build each cluster XML
	c.chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		if c.ClusterHostsNum(cluster, options) < 1 {
			// Skip empty cluster
			return nil
		}
		// <my_cluster_name>
		util.Iline(b, 8, "<%s>", cluster.Name)

		// Build each shard XML
		cluster.WalkShards(func(index int, shard *chiv1.ChiShard) error {
			if c.ShardHostsNum(shard, options) < 1 {
				// Skip empty shard
				return nil
			}

			// <shard>
			//		<internal_replication>VALUE(true/false)</internal_replication>
			util.Iline(b, 12, "<shard>")
			util.Iline(b, 16, "<internal_replication>%s</internal_replication>", shard.InternalReplication)

			//		<weight>X</weight>
			if shard.Weight > 0 {
				util.Iline(b, 16, "<weight>%d</weight>", shard.Weight)
			}

			shard.WalkHosts(func(host *chiv1.ChiHost) error {
				if options.Include(host) {
					// <replica>
					//		<host>XXX</host>
					//		<port>XXX</port>
					// </replica>
					util.Iline(b, 16, "<replica>")
					util.Iline(b, 16, "    <host>%s</host>", c.getRemoteServersReplicaHostname(host))
					util.Iline(b, 16, "    <port>%d</port>", host.TCPPort)
					util.Iline(b, 16, "</replica>")
				}
				return nil
			})

			// </shard>
			util.Iline(b, 12, "</shard>")

			return nil
		})
		// </my_cluster_name>
		util.Iline(b, 8, "</%s>", cluster.Name)

		return nil
	})

	// Auto-generated clusters

	if c.CHIHostsNum(options) < 1 {
		util.Iline(b, 8, "<!-- Autogenerated clusters are skipped due to absence of hots -->")
	} else {
		util.Iline(b, 8, "<!-- Autogenerated clusters -->")
		// One Shard All Replicas

		// <my_cluster_name>
		//     <shard>
		//         <internal_replication>
		clusterName := oneShardAllReplicasClusterName
		util.Iline(b, 8, "<%s>", clusterName)
		util.Iline(b, 8, "    <shard>")
		util.Iline(b, 8, "        <internal_replication>true</internal_replication>")
		c.chi.WalkHosts(func(host *chiv1.ChiHost) error {
			if options.Include(host) {
				// <replica>
				//		<host>XXX</host>
				//		<port>XXX</port>
				// </replica>
				util.Iline(b, 16, "<replica>")
				util.Iline(b, 16, "    <host>%s</host>", c.getRemoteServersReplicaHostname(host))
				util.Iline(b, 16, "    <port>%d</port>", host.TCPPort)
				util.Iline(b, 16, "</replica>")
			}
			return nil
		})

		//     </shard>
		// </my_cluster_name>
		util.Iline(b, 8, "    </shard>")
		util.Iline(b, 8, "</%s>", clusterName)

		// All Shards One Replica

		// <my_cluster_name>
		clusterName = allShardsOneReplicaClusterName
		util.Iline(b, 8, "<%s>", clusterName)
		c.chi.WalkHosts(func(host *chiv1.ChiHost) error {
			if options.Include(host) {
				// <shard>
				//     <internal_replication>
				util.Iline(b, 12, "<shard>")
				util.Iline(b, 12, "    <internal_replication>false</internal_replication>")

				// <replica>
				//		<host>XXX</host>
				//		<port>XXX</port>
				// </replica>
				util.Iline(b, 16, "<replica>")
				util.Iline(b, 16, "    <host>%s</host>", c.getRemoteServersReplicaHostname(host))
				util.Iline(b, 16, "    <port>%d</port>", host.TCPPort)
				util.Iline(b, 16, "</replica>")

				// </shard>
				util.Iline(b, 12, "</shard>")
			}
			return nil
		})
		// </my_cluster_name>
		util.Iline(b, 8, "</%s>", clusterName)
	}

	// 		</remote_servers>
	// </yandex>
	util.Iline(b, 0, "    </remote_servers>")
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// GetHostMacros creates "macros.xml" content
func (c *ClickHouseConfigGenerator) GetHostMacros(host *chiv1.ChiHost) string {
	b := &bytes.Buffer{}

	// <yandex>
	//     <macros>
	util.Iline(b, 0, "<"+xmlTagYandex+">")
	util.Iline(b, 0, "    <macros>")

	// <installation>CHI-name-macros-value</installation>
	util.Iline(b, 8, "<installation>%s</installation>", host.Address.CHIName)

	// <CLUSTER_NAME>cluster-name-macros-value</CLUSTER_NAME>
	// util.Iline(b, 8, "<%s>%[2]s</%[1]s>", replica.Address.ClusterName, c.getMacrosCluster(replica.Address.ClusterName))
	// <CLUSTER_NAME-shard>0-based shard index within cluster</CLUSTER_NAME-shard>
	// util.Iline(b, 8, "<%s-shard>%d</%[1]s-shard>", replica.Address.ClusterName, replica.Address.ShardIndex)

	// All Shards One Replica Cluster
	// <CLUSTER_NAME-shard>0-based shard index within all-shards-one-replica-cluster</CLUSTER_NAME-shard>
	util.Iline(b, 8, "<%s-shard>%d</%[1]s-shard>", allShardsOneReplicaClusterName, host.Address.CHIScopeIndex)

	// <cluster> and <shard> macros are applicable to main cluster only. All aux clusters do not have ambiguous macros
	// <cluster></cluster> macro
	util.Iline(b, 8, "<cluster>%s</cluster>", host.Address.ClusterName)
	// <shard></shard> macro
	util.Iline(b, 8, "<shard>%s</shard>", host.Address.ShardName)
	// <replica>replica id = full deployment id</replica>
	// full deployment id is unique to identify replica within the cluster
	util.Iline(b, 8, "<replica>%s</replica>", CreatePodHostname(host))

	// 		</macros>
	// </yandex>
	util.Iline(b, 0, "    </macros>")
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// noCustomPorts
func noCustomPorts(host *chiv1.ChiHost) bool {
	if host.TCPPort != chDefaultTCPPortNumber {
		return false
	}

	if host.HTTPPort != chDefaultHTTPPortNumber {
		return false
	}

	if host.InterserverHTTPPort != chDefaultInterserverHTTPPortNumber {
		return false
	}

	return true
}

// GetHostPorts creates "ports.xml" content
func (c *ClickHouseConfigGenerator) GetHostPorts(host *chiv1.ChiHost) string {

	if noCustomPorts(host) {
		return ""
	}

	b := &bytes.Buffer{}

	// <yandex>
	util.Iline(b, 0, "<"+xmlTagYandex+">")

	if host.TCPPort != chDefaultTCPPortNumber {
		util.Iline(b, 4, "<tcp_port>%d</tcp_port>", host.TCPPort)
	}
	if host.HTTPPort != chDefaultHTTPPortNumber {
		util.Iline(b, 4, "<http_port>%d</http_port>", host.HTTPPort)
	}
	if host.InterserverHTTPPort != chDefaultInterserverHTTPPortNumber {
		util.Iline(b, 4, "<interserver_http_port>%d</interserver_http_port>", host.InterserverHTTPPort)
	}

	// </yandex>
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// generateXMLConfig creates XML using map[string]string definitions
func (c *ClickHouseConfigGenerator) generateXMLConfig(settings *chiv1.Settings, prefix string) string {
	if settings.Len() == 0 {
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	// XML code
	// </yandex>
	util.Iline(b, 0, "<"+xmlTagYandex+">")
	xmlbuilder.GenerateXML(b, settings, prefix)
	util.Iline(b, 0, "</"+xmlTagYandex+">")

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
func (c *ClickHouseConfigGenerator) getRemoteServersReplicaHostname(host *chiv1.ChiHost) string {
	return CreateReplicaHostname(host)
}

// getMacrosInstallation returns macros value for <installation-name> macros
func (c *ClickHouseConfigGenerator) getMacrosInstallation(name string) string {
	return util.CreateStringID(name, 6)
}

// getMacrosCluster returns macros value for <cluster-name> macros
func (c *ClickHouseConfigGenerator) getMacrosCluster(name string) string {
	return util.CreateStringID(name, 4)
}
