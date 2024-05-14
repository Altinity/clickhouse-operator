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

package chi

import (
	"fmt"
	"strconv"
	"strings"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// Names context length
	namePartChiMaxLenNamesCtx     = 60
	namePartClusterMaxLenNamesCtx = 15
	namePartShardMaxLenNamesCtx   = 15
	namePartReplicaMaxLenNamesCtx = 15

	// Labels context length
	namePartChiMaxLenLabelsCtx     = 63
	namePartClusterMaxLenLabelsCtx = 63
	namePartShardMaxLenLabelsCtx   = 63
	namePartReplicaMaxLenLabelsCtx = 63
)

const (
	// chiServiceNamePattern is a template of CHI Service name. "clickhouse-{chi}"
	chiServiceNamePattern = "clickhouse-" + macrosChiName

	// clusterServiceNamePattern is a template of cluster Service name. "cluster-{chi}-{cluster}"
	clusterServiceNamePattern = "cluster-" + macrosChiName + "-" + macrosClusterName

	// shardServiceNamePattern is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	shardServiceNamePattern = "shard-" + macrosChiName + "-" + macrosClusterName + "-" + macrosShardName

	// replicaServiceNamePattern is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	replicaServiceNamePattern = "shard-" + macrosChiName + "-" + macrosClusterName + "-" + macrosReplicaName

	// statefulSetNamePattern is a template of hosts's StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	statefulSetNamePattern = "chi-" + macrosChiName + "-" + macrosClusterName + "-" + macrosHostName

	// statefulSetServiceNamePattern is a template of hosts's StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	statefulSetServiceNamePattern = "chi-" + macrosChiName + "-" + macrosClusterName + "-" + macrosHostName

	// configMapCommonNamePattern is a template of common settings for the CHI ConfigMap. "chi-{chi}-common-configd"
	configMapCommonNamePattern = "chi-" + macrosChiName + "-common-configd"

	// configMapCommonUsersNamePattern is a template of common users settings for the CHI ConfigMap. "chi-{chi}-common-usersd"
	configMapCommonUsersNamePattern = "chi-" + macrosChiName + "-common-usersd"

	// configMapHostNamePattern is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	configMapHostNamePattern = "chi-" + macrosChiName + "-deploy-confd-" + macrosClusterName + "-" + macrosHostName

	// configMapHostMigrationNamePattern is a template of macros ConfigMap. "chi-{chi}-migration-{cluster}-{shard}-{host}"
	//configMapHostMigrationNamePattern = "chi-" + macrosChiName + "-migration-" + macrosClusterName + "-" + macrosHostName

	// namespaceDomainPattern presents Domain Name pattern of a namespace
	// In this pattern "%s" is substituted namespace name's value
	// Ex.: my-dev-namespace.svc.cluster.local
	namespaceDomainPattern = "%s.svc.cluster.local"

	// ServiceName.domain.name
	serviceFQDNPattern = "%s" + "." + namespaceDomainPattern

	// podFQDNPattern consists of 3 parts:
	// 1. nameless service of of stateful set
	// 2. namespace name
	// Hostname.domain.name
	podFQDNPattern = "%s" + "." + namespaceDomainPattern

	// podNamePattern is a name of a Pod within StatefulSet. In our setup each StatefulSet has only 1 pod,
	// so all pods would have '-0' suffix after StatefulSet name
	// Ex.: StatefulSetName-0
	podNamePattern = "%s-0"
)

// sanitize makes string fulfil kubernetes naming restrictions
// String can't end with '-', '_' and '.'
func sanitize(s string) string {
	return strings.Trim(s, "-_.")
}

const (
	namerContextLabels = "labels"
	namerContextNames  = "names"
)

type namerContext string
type namer struct {
	ctx namerContext
}

// newNamer creates new namer with specified context
func newNamer(ctx namerContext) *namer {
	return &namer{
		ctx: ctx,
	}
}

func (n *namer) lenCHI() int {
	if n.ctx == namerContextLabels {
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
	if n.ctx == namerContextLabels {
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
	if n.ctx == namerContextLabels {
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
	if n.ctx == namerContextLabels {
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

// getNamePartNamespace
func (n *namer) getNamePartNamespace(obj interface{}) string {
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

// getNamePartCHIName
func (n *namer) getNamePartCHIName(obj interface{}) string {
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

// getNamePartClusterName
func (n *namer) getNamePartClusterName(obj interface{}) string {
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

// getNamePartShardName
func (n *namer) getNamePartShardName(obj interface{}) string {
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

// getNamePartReplicaName
func (n *namer) getNamePartReplicaName(host *api.Host) string {
	return n.namePartReplicaName(host.Runtime.Address.ReplicaName)
}

// getNamePartHostName
func (n *namer) getNamePartHostName(host *api.Host) string {
	return n.namePartHostName(host.Runtime.Address.HostName)
}

// getNamePartCHIScopeCycleSize
func getNamePartCHIScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleSize)
}

// getNamePartCHIScopeCycleIndex
func getNamePartCHIScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleIndex)
}

// getNamePartCHIScopeCycleOffset
func getNamePartCHIScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleOffset)
}

// getNamePartClusterScopeCycleSize
func getNamePartClusterScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleSize)
}

// getNamePartClusterScopeCycleIndex
func getNamePartClusterScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleIndex)
}

// getNamePartClusterScopeCycleOffset
func getNamePartClusterScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleOffset)
}

// getNamePartCHIScopeIndex
func getNamePartCHIScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeIndex)
}

// getNamePartClusterScopeIndex
func getNamePartClusterScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeIndex)
}

// getNamePartShardScopeIndex
func getNamePartShardScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ShardScopeIndex)
}

// getNamePartReplicaScopeIndex
func getNamePartReplicaScopeIndex(host *api.Host) string {
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
func CreateConfigMapCommonName(chi api.IChi) string {
	return Macro(chi).Line(configMapCommonNamePattern)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common users config
func CreateConfigMapCommonUsersName(chi api.IChi) string {
	return Macro(chi).Line(configMapCommonUsersNamePattern)
}

// CreateCHIServiceName creates a name of a root ClickHouseInstallation Service resource
func CreateCHIServiceName(chi api.IChi) string {
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
func CreateCHIServiceFQDN(chi api.IChi) string {
	// FQDN can be generated either from default pattern,
	// or from personal pattern provided

	// Start with default pattern
	pattern := serviceFQDNPattern

	if chi.GetSpec().GetNamespaceDomainPattern().HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		pattern = "%s." + chi.GetSpec().GetNamespaceDomainPattern().Value()
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

// IsAutoGeneratedShardName checks whether provided name is auto-generated
func IsAutoGeneratedShardName(name string, shard api.IShard, index int) bool {
	return name == CreateShardName(shard, index)
}

// CreateReplicaName returns a name of a replica.
// Here replica is a CHOp-internal replica - i.e. a vertical slice of hosts field.
// In case you are looking for replica name in terms of a hostname to address particular host as in remote_servers.xml
// you need to take a look on CreateInstanceHostname function
func CreateReplicaName(replica api.IReplica, index int) string {
	return strconv.Itoa(index)
}

// IsAutoGeneratedReplicaName checks whether provided name is auto-generated
func IsAutoGeneratedReplicaName(name string, replica api.IReplica, index int) bool {
	return name == CreateReplicaName(replica, index)
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
	if host.GetCHI().GetSpec().Defaults.ReplicasUseFQDN.IsTrue() {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		return createPodFQDN(host)
	}

	return CreatePodHostname(host)
}

// IsAutoGeneratedHostName checks whether name is auto-generated
func IsAutoGeneratedHostName(
	name string,
	host *api.Host,
	shard api.IShard,
	shardIndex int,
	replica api.IReplica,
	replicaIndex int,
) bool {
	switch {
	case name == CreateHostName(host, shard, shardIndex, replica, replicaIndex):
		// Current version of the name
		return true
	case name == fmt.Sprintf("%d-%d", shardIndex, replicaIndex):
		// old version - index-index
		return true
	case name == fmt.Sprintf("%d", shardIndex):
		// old version - index
		return true
	case name == fmt.Sprintf("%d", replicaIndex):
		// old version - index
		return true
	default:
		return false
	}
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

	if host.GetCHI().GetSpec().NamespaceDomainPattern.HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		pattern = "%s." + host.GetCHI().GetSpec().NamespaceDomainPattern.Value()
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
func createPodFQDNsOfCHI(chi api.IChi) (fqdns []string) {
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
	case api.IChi:
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
			return util.RemoveFromArray(self, createPodFQDNsOfCHI(any(typed.GetCHI()).(api.IChi)))
		}
	}
	return nil
}

// CreatePodHostnameRegexp creates pod hostname regexp.
// For example, `template` can be defined in operator config:
// HostRegexpTemplate: chi-{chi}-[^.]+\\d+-\\d+\\.{namespace}.svc.cluster.local$"
func CreatePodHostnameRegexp(chi api.IChi, template string) string {
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

// CreatePodNames is a wrapper over set of create pod names functions
// obj specifies source object to create names from
func CreatePodNames(obj interface{}) []string {
	switch typed := obj.(type) {
	case api.IChi:
		return createPodNamesOfCHI(typed)
	case api.ICluster:
		return createPodNamesOfCluster(typed)
	case api.IShard:
		return createPodNamesOfShard(typed)
	case
		*api.Host,
		*apps.StatefulSet:
		return []string{
			CreatePodName(typed),
		}
	}
	return nil
}

// createPodNamesOfCluster creates pod names of all pods in a cluster
func createPodNamesOfCluster(cluster api.ICluster) (names []string) {
	cluster.WalkHosts(func(host *api.Host) error {
		names = append(names, CreatePodName(host))
		return nil
	})
	return names
}

// createPodNamesOfShard creates pod names of all pods in a shard
func createPodNamesOfShard(shard api.IShard) (names []string) {
	shard.WalkHosts(func(host *api.Host) error {
		names = append(names, CreatePodName(host))
		return nil
	})
	return names
}

// createPodNamesOfCHI creates fully qualified domain names of all pods in a CHI
func createPodNamesOfCHI(chi api.IChi) (names []string) {
	chi.WalkHosts(func(host *api.Host) error {
		names = append(names, CreatePodName(host))
		return nil
	})
	return names
}

// CreatePVCNameByVolumeClaimTemplate creates PVC name
func CreatePVCNameByVolumeClaimTemplate(host *api.Host, volumeClaimTemplate *api.VolumeClaimTemplate) string {
	return createPVCName(host, volumeClaimTemplate.Name)
}

// CreatePVCNameByVolumeMount creates PVC name
func CreatePVCNameByVolumeMount(host *api.Host, volumeMount *core.VolumeMount) (string, bool) {
	volumeClaimTemplate, ok := GetVolumeClaimTemplate(host, volumeMount)
	if !ok {
		// Unable to find VolumeClaimTemplate related to this volumeMount.
		// May be this volumeMount is not created from VolumeClaimTemplate, it may be a reference to a ConfigMap
		return "", false
	}
	return createPVCName(host, volumeClaimTemplate.Name), true
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
