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

package namer

import (
	"fmt"
	"strconv"

	apps "k8s.io/api/apps/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type namerContext string
type namer struct {
	ctx namerContext
}

// NewNamer creates new namer with specified context
func NewNamer(ctx namerContext) *namer {
	return &namer{
		ctx: ctx,
	}
}

func (n *namer) lenCHI() int {
	if n.ctx == NamerContextLabels {
		return namePartChiMaxLenLabelsCtx
	} else {
		return namePartChiMaxLenNamesCtx
	}
}

// namePartNamespace
func (n *namer) namePartNamespace(name string) string {
	return sanitize(util.StringHead(name, n.lenCHI()))
}

// namePartChiName
func (n *namer) namePartChiName(name string) string {
	return sanitize(util.StringHead(name, n.lenCHI()))
}

// namePartChiNameID
func (n *namer) namePartChiNameID(name string) string {
	return util.CreateStringID(name, n.lenCHI())
}

func (n *namer) lenCluster() int {
	if n.ctx == NamerContextLabels {
		return namePartClusterMaxLenLabelsCtx
	} else {
		return namePartClusterMaxLenNamesCtx
	}
}

// namePartClusterName
func (n *namer) namePartClusterName(name string) string {
	return sanitize(util.StringHead(name, n.lenCluster()))
}

// namePartClusterNameID
func (n *namer) namePartClusterNameID(name string) string {
	return util.CreateStringID(name, n.lenCluster())
}

func (n *namer) lenShard() int {
	if n.ctx == NamerContextLabels {
		return namePartShardMaxLenLabelsCtx
	} else {
		return namePartShardMaxLenNamesCtx
	}

}

// namePartShardName
func (n *namer) namePartShardName(name string) string {
	return sanitize(util.StringHead(name, n.lenShard()))
}

// namePartShardNameID
func (n *namer) namePartShardNameID(name string) string {
	return util.CreateStringID(name, n.lenShard())
}

func (n *namer) lenReplica() int {
	if n.ctx == NamerContextLabels {
		return namePartReplicaMaxLenLabelsCtx
	} else {
		return namePartReplicaMaxLenNamesCtx
	}

}

// namePartReplicaName
func (n *namer) namePartReplicaName(name string) string {
	return sanitize(util.StringHead(name, n.lenReplica()))
}

// namePartReplicaNameID
func (n *namer) namePartReplicaNameID(name string) string {
	return util.CreateStringID(name, n.lenReplica())
}

// namePartHostName
func (n *namer) namePartHostName(name string) string {
	return sanitize(util.StringHead(name, n.lenReplica()))
}

// namePartHostNameID
func (n *namer) namePartHostNameID(name string) string {
	return util.CreateStringID(name, n.lenReplica())
}

// GetNamePartNamespace
func (n *namer) GetNamePartNamespace(obj interface{}) string {
	switch obj.(type) {
	case *api.ClickHouseInstallation:
		chi := obj.(*api.ClickHouseInstallation)
		return n.namePartChiName(chi.Namespace)
	case *api.Cluster:
		cluster := obj.(*api.Cluster)
		return n.namePartChiName(cluster.Runtime.Address.Namespace)
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartChiName(shard.Runtime.Address.Namespace)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartChiName(host.Runtime.Address.Namespace)
	}

	return "ERROR"
}

// GetNamePartCHIName
func (n *namer) GetNamePartCHIName(obj interface{}) string {
	switch obj.(type) {
	case *api.ClickHouseInstallation:
		chi := obj.(*api.ClickHouseInstallation)
		return n.namePartChiName(chi.Name)
	case *api.Cluster:
		cluster := obj.(*api.Cluster)
		return n.namePartChiName(cluster.Runtime.Address.CHIName)
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartChiName(shard.Runtime.Address.CHIName)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartChiName(host.Runtime.Address.CHIName)
	}

	return "ERROR"
}

// GetNamePartClusterName
func (n *namer) GetNamePartClusterName(obj interface{}) string {
	switch obj.(type) {
	case *api.Cluster:
		cluster := obj.(*api.Cluster)
		return n.namePartClusterName(cluster.Runtime.Address.ClusterName)
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartClusterName(shard.Runtime.Address.ClusterName)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartClusterName(host.Runtime.Address.ClusterName)
	}

	return "ERROR"
}

// GetNamePartShardName
func (n *namer) GetNamePartShardName(obj interface{}) string {
	switch obj.(type) {
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartShardName(shard.Runtime.Address.ShardName)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartShardName(host.Runtime.Address.ShardName)
	}

	return "ERROR"
}

// GetNamePartReplicaName
func (n *namer) GetNamePartReplicaName(host *api.Host) string {
	return n.namePartReplicaName(host.Runtime.Address.ReplicaName)
}

// getNamePartHostName
func (n *namer) getNamePartHostName(host *api.Host) string {
	return n.namePartHostName(host.Runtime.Address.HostName)
}

// GetNamePartCHIScopeCycleSize
func GetNamePartCHIScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleSize)
}

// GetNamePartCHIScopeCycleIndex
func GetNamePartCHIScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleIndex)
}

// GetNamePartCHIScopeCycleOffset
func GetNamePartCHIScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleOffset)
}

// GetNamePartClusterScopeCycleSize
func GetNamePartClusterScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleSize)
}

// GetNamePartClusterScopeCycleIndex
func GetNamePartClusterScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleIndex)
}

// GetNamePartClusterScopeCycleOffset
func GetNamePartClusterScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleOffset)
}

// GetNamePartCHIScopeIndex
func GetNamePartCHIScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeIndex)
}

// GetNamePartClusterScopeIndex
func GetNamePartClusterScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeIndex)
}

// GetNamePartShardScopeIndex
func GetNamePartShardScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ShardScopeIndex)
}

// GetNamePartReplicaScopeIndex
func GetNamePartReplicaScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ReplicaScopeIndex)
}

// CreateConfigMapHostName returns a name for a ConfigMap for replica's personal config
func CreateConfigMapHostName(host *api.Host) string {
	return Macro(host).Line(configMapHostNamePattern)
}

// CreateConfigMapHostMigrationName returns a name for a ConfigMap for replica's personal config
//func CreateConfigMapHostMigrationName(host *api.Host) string {
//	return macro(host).Line(configMapHostMigrationNamePattern)
//}

// CreateConfigMapCommonName returns a name for a ConfigMap for replica's common config
func CreateConfigMapCommonName(chi api.ICustomResource) string {
	return Macro(chi).Line(configMapCommonNamePattern)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common users config
func CreateConfigMapCommonUsersName(chi api.ICustomResource) string {
	return Macro(chi).Line(configMapCommonUsersNamePattern)
}

// CreateCHIServiceName creates a name of a root ClickHouseInstallation Service resource
func CreateCHIServiceName(chi api.ICustomResource) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := chiServiceNamePattern

	// ServiceTemplate may have personal name pattern specified
	if template, ok := chi.GetRootServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return Macro(chi).Line(pattern)
}

// CreateCHIServiceFQDN creates a FQD name of a root ClickHouseInstallation Service resource
func CreateCHIServiceFQDN(chi api.ICustomResource, namespaceDomainPattern *api.String) string {
	// FQDN can be generated either from default pattern,
	// or from personal pattern provided

	// Start with default pattern
	pattern := serviceFQDNPattern

	if namespaceDomainPattern.HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		pattern = "%s." + namespaceDomainPattern.Value()
	}

	// Create FQDN based on pattern available
	return fmt.Sprintf(
		pattern,
		CreateCHIServiceName(chi),
		chi.GetNamespace(),
	)
}

// CreateClusterServiceName returns a name of a cluster's Service
func CreateClusterServiceName(cluster api.ICluster) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := clusterServiceNamePattern

	// ServiceTemplate may have personal name pattern specified
	if template, ok := cluster.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return Macro(cluster).Line(pattern)
}

// CreateShardServiceName returns a name of a shard's Service
func CreateShardServiceName(shard api.IShard) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := shardServiceNamePattern

	// ServiceTemplate may have personal name pattern specified
	if template, ok := shard.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return Macro(shard).Line(pattern)
}

// CreateShardName returns a name of a shard
func CreateShardName(shard api.IShard, index int) string {
	return strconv.Itoa(index)
}

// CreateReplicaName returns a name of a replica.
// Here replica is a CHOp-internal replica - i.e. a vertical slice of hosts field.
// In case you are looking for replica name in terms of a hostname to address particular host as in remote_servers.xml
// you need to take a look on CreateInstanceHostname function
func CreateReplicaName(replica api.IReplica, index int) string {
	return strconv.Itoa(index)
}

// CreateHostName returns a name of a host
func CreateHostName(host *api.Host, shard api.IShard, shardIndex int, replica api.IReplica, replicaIndex int) string {
	return fmt.Sprintf("%s-%s", shard.GetName(), replica.GetName())
}

// CreateHostTemplateName returns a name of a HostTemplate
func CreateHostTemplateName(host *api.Host) string {
	return "HostTemplate" + host.Name
}

// CreateInstanceHostname returns hostname (pod-hostname + service or FQDN) which can be used as a replica name
// in all places where ClickHouse requires replica name. These are such places as:
// 1. "remote_servers.xml" config file
// 2. statements like SYSTEM DROP REPLICA <replica_name>
// any other places
// Function operations are based on .Spec.Defaults.ReplicasUseFQDN
func CreateInstanceHostname(host *api.Host) string {
	if host.GetCR().GetSpec().Defaults.ReplicasUseFQDN.IsTrue() {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		return createPodFQDN(host)
	}

	return CreatePodHostname(host)
}

// CreateStatefulSetName creates a name of a StatefulSet for ClickHouse instance
func CreateStatefulSetName(host *api.Host) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in PodTemplate

	// Start with default name pattern
	pattern := statefulSetNamePattern

	// PodTemplate may have personal name pattern specified
	if template, ok := host.GetPodTemplate(); ok {
		// PodTemplate available
		if template.GenerateName != "" {
			// PodTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create StatefulSet name based on name pattern available
	return Macro(host).Line(pattern)
}

// CreateStatefulSetServiceName returns a name of a StatefulSet-related Service for ClickHouse instance
func CreateStatefulSetServiceName(host *api.Host) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := statefulSetServiceNamePattern

	// ServiceTemplate may have personal name pattern specified
	if template, ok := host.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return Macro(host).Line(pattern)
}

// CreatePodHostname returns a hostname of a Pod of a ClickHouse instance.
// Is supposed to be used where network connection to a Pod is required.
// NB: right now Pod's hostname points to a Service, through which Pod can be accessed.
func CreatePodHostname(host *api.Host) string {
	// Do not use Pod own hostname - point to appropriate StatefulSet's Service
	return CreateStatefulSetServiceName(host)
}

// createPodFQDN creates a fully qualified domain name of a pod
// ss-1eb454-2-0.my-dev-domain.svc.cluster.local
func createPodFQDN(host *api.Host) string {
	// FQDN can be generated either from default pattern,
	// or from personal pattern provided

	// Start with default pattern
	pattern := podFQDNPattern

	if host.GetCR().GetSpec().NamespaceDomainPattern.HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		pattern = "%s." + host.GetCR().GetSpec().NamespaceDomainPattern.Value()
	}

	// Create FQDN based on pattern available
	return fmt.Sprintf(
		pattern,
		CreatePodHostname(host),
		host.GetRuntime().GetAddress().GetNamespace(),
	)
}

// createPodFQDNsOfCluster creates fully qualified domain names of all pods in a cluster
func createPodFQDNsOfCluster(cluster api.ICluster) (fqdns []string) {
	cluster.WalkHosts(func(host *api.Host) error {
		fqdns = append(fqdns, createPodFQDN(host))
		return nil
	})
	return fqdns
}

// createPodFQDNsOfShard creates fully qualified domain names of all pods in a shard
func createPodFQDNsOfShard(shard api.IShard) (fqdns []string) {
	shard.WalkHosts(func(host *api.Host) error {
		fqdns = append(fqdns, createPodFQDN(host))
		return nil
	})
	return fqdns
}

// createPodFQDNsOfCHI creates fully qualified domain names of all pods in a CHI
func createPodFQDNsOfCHI(chi api.ICustomResource) (fqdns []string) {
	chi.WalkHosts(func(host *api.Host) error {
		fqdns = append(fqdns, createPodFQDN(host))
		return nil
	})
	return fqdns
}

// CreateFQDN is a wrapper over pod FQDN function
func CreateFQDN(host *api.Host) string {
	return createPodFQDN(host)
}

// CreateFQDNs is a wrapper over set of create FQDN functions
// obj specifies source object to create FQDNs from
// scope specifies target scope - what entity to create FQDNs for - be it CHI, cluster, shard or a host
// excludeSelf specifies whether to exclude the host itself from the result. Applicable only in case obj is a host
func CreateFQDNs(obj interface{}, scope interface{}, excludeSelf bool) []string {
	switch typed := obj.(type) {
	case api.ICustomResource:
		return createPodFQDNsOfCHI(typed)
	case api.ICluster:
		return createPodFQDNsOfCluster(typed)
	case api.IShard:
		return createPodFQDNsOfShard(typed)
	case *api.Host:
		self := ""
		if excludeSelf {
			self = createPodFQDN(typed)
		}
		switch scope.(type) {
		case api.Host:
			return util.RemoveFromArray(self, []string{createPodFQDN(typed)})
		case api.ChiShard:
			return util.RemoveFromArray(self, createPodFQDNsOfShard(any(typed.GetShard()).(api.IShard)))
		case api.Cluster:
			return util.RemoveFromArray(self, createPodFQDNsOfCluster(any(typed.GetCluster()).(api.ICluster)))
		case api.ClickHouseInstallation:
			return util.RemoveFromArray(self, createPodFQDNsOfCHI(any(typed.GetCR()).(api.ICustomResource)))
		}
	}
	return nil
}

// CreatePodHostnameRegexp creates pod hostname regexp.
// For example, `template` can be defined in operator config:
// HostRegexpTemplate: chi-{chi}-[^.]+\\d+-\\d+\\.{namespace}.svc.cluster.local$"
func CreatePodHostnameRegexp(chi api.ICustomResource, template string) string {
	return Macro(chi).Line(template)
}

// CreatePodName creates Pod name based on specified StatefulSet or Host
func CreatePodName(obj interface{}) string {
	switch obj.(type) {
	case *apps.StatefulSet:
		statefulSet := obj.(*apps.StatefulSet)
		return fmt.Sprintf(podNamePattern, statefulSet.Name)
	case *api.Host:
		host := obj.(*api.Host)
		return fmt.Sprintf(podNamePattern, CreateStatefulSetName(host))
	}
	return "unknown-type"
}

// CreatePVCNameByVolumeClaimTemplate creates PVC name
func CreatePVCNameByVolumeClaimTemplate(host *api.Host, volumeClaimTemplate *api.VolumeClaimTemplate) string {
	return createPVCName(host, volumeClaimTemplate.Name)
}	

// createPVCName is an internal function
func createPVCName(host *api.Host, volumeMountName string) string {
	return volumeMountName + "-" + CreatePodName(host)
}

// CreateClusterAutoSecretName creates Secret name where auto-generated secret is kept
func CreateClusterAutoSecretName(cluster api.ICluster) string {
	if cluster.GetName() == "" {
		return fmt.Sprintf(
			"%s-auto-secret",
			cluster.GetRuntime().GetRoot().GetName(),
		)
	}

	return fmt.Sprintf(
		"%s-%s-auto-secret",
		cluster.GetRuntime().GetRoot().GetName(),
		cluster.GetName(),
	)
}
