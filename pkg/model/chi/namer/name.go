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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer/macro"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// createConfigMapHostName returns a name for a ConfigMap for replica's personal config
func createConfigMapHostName(host *api.Host) string {
	return macro.Macro(host).Line(configMapHostNamePattern)
}

// createConfigMapCommonName returns a name for a ConfigMap for replica's common config
func createConfigMapCommonName(chi api.ICustomResource) string {
	return macro.Macro(chi).Line(configMapCommonNamePattern)
}

// createConfigMapCommonUsersName returns a name for a ConfigMap for replica's common users config
func createConfigMapCommonUsersName(chi api.ICustomResource) string {
	return macro.Macro(chi).Line(configMapCommonUsersNamePattern)
}

// createCHIServiceName creates a name of a root ClickHouseInstallation Service resource
func createCHIServiceName(chi api.ICustomResource) string {
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
	return macro.Macro(chi).Line(pattern)
}

// createCHIServiceFQDN creates a FQD name of a root ClickHouseInstallation Service resource
func createCHIServiceFQDN(chi api.ICustomResource, namespaceDomainPattern *api.String) string {
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
		createCHIServiceName(chi),
		chi.GetNamespace(),
	)
}

// createClusterServiceName returns a name of a cluster's Service
func createClusterServiceName(cluster api.ICluster) string {
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
	return macro.Macro(cluster).Line(pattern)
}

// createShardServiceName returns a name of a shard's Service
func createShardServiceName(shard api.IShard) string {
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
	return macro.Macro(shard).Line(pattern)
}

// createShardName returns a name of a shard
func createShardName(shard api.IShard, index int) string {
	return strconv.Itoa(index)
}

// createReplicaName returns a name of a replica.
// Here replica is a CHOp-internal replica - i.e. a vertical slice of hosts field.
// In case you are looking for replica name in terms of a hostname to address particular host as in remote_servers.xml
// you need to take a look on CreateInstanceHostname function
func createReplicaName(replica api.IReplica, index int) string {
	return strconv.Itoa(index)
}

// createHostName returns a name of a host
func createHostName(host *api.Host, shard api.IShard, shardIndex int, replica api.IReplica, replicaIndex int) string {
	return fmt.Sprintf("%s-%s", shard.GetName(), replica.GetName())
}

// createHostTemplateName returns a name of a HostTemplate
func createHostTemplateName(host *api.Host) string {
	return "HostTemplate" + host.Name
}

// createInstanceHostname returns hostname (pod-hostname + service or FQDN) which can be used as a replica name
// in all places where ClickHouse requires replica name. These are such places as:
// 1. "remote_servers.xml" config file
// 2. statements like SYSTEM DROP REPLICA <replica_name>
// any other places
// Function operations are based on .Spec.Defaults.ReplicasUseFQDN
func createInstanceHostname(host *api.Host) string {
	if host.GetCR().GetSpec().Defaults.ReplicasUseFQDN.IsTrue() {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		return createPodFQDN(host)
	}

	return createPodHostname(host)
}

// createStatefulSetName creates a name of a StatefulSet for ClickHouse instance
func createStatefulSetName(host *api.Host) string {
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
	return macro.Macro(host).Line(pattern)
}

// createStatefulSetServiceName returns a name of a StatefulSet-related Service for ClickHouse instance
func createStatefulSetServiceName(host *api.Host) string {
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
	return macro.Macro(host).Line(pattern)
}

// createPodHostname returns a hostname of a Pod of a ClickHouse instance.
// Is supposed to be used where network connection to a Pod is required.
// NB: right now Pod's hostname points to a Service, through which Pod can be accessed.
func createPodHostname(host *api.Host) string {
	// Do not use Pod own hostname - point to appropriate StatefulSet's Service
	return createStatefulSetServiceName(host)
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
		createPodHostname(host),
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

// createFQDN is a wrapper over pod FQDN function
func createFQDN(host *api.Host) string {
	return createPodFQDN(host)
}

// createFQDNs is a wrapper over set of create FQDN functions
// obj specifies source object to create FQDNs from
// scope specifies target scope - what entity to create FQDNs for - be it CHI, cluster, shard or a host
// excludeSelf specifies whether to exclude the host itself from the result. Applicable only in case obj is a host
func createFQDNs(obj interface{}, scope interface{}, excludeSelf bool) []string {
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

// createPodHostnameRegexp creates pod hostname regexp.
// For example, `template` can be defined in operator config:
// HostRegexpTemplate: chi-{chi}-[^.]+\\d+-\\d+\\.{namespace}.svc.cluster.local$"
func createPodHostnameRegexp(chi api.ICustomResource, template string) string {
	return macro.Macro(chi).Line(template)
}

// createPodName creates Pod name based on specified StatefulSet or Host
func createPodName(obj interface{}) string {
	switch obj.(type) {
	case *apps.StatefulSet:
		statefulSet := obj.(*apps.StatefulSet)
		return fmt.Sprintf(podNamePattern, statefulSet.Name)
	case *api.Host:
		host := obj.(*api.Host)
		return fmt.Sprintf(podNamePattern, createStatefulSetName(host))
	}
	return "unknown-type"
}

// createPVCNameByVolumeClaimTemplate creates PVC name
func createPVCNameByVolumeClaimTemplate(host *api.Host, volumeClaimTemplate *api.VolumeClaimTemplate) string {
	return createPVCName(host, volumeClaimTemplate.Name)
}

// createPVCName is an internal function
func createPVCName(host *api.Host, volumeMountName string) string {
	return volumeMountName + "-" + createPodName(host)
}

// createClusterAutoSecretName creates Secret name where auto-generated secret is kept
func createClusterAutoSecretName(cluster api.ICluster) string {
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
