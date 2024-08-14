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

	apps "k8s.io/api/apps/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// createConfigMapNameHost returns a name for a ConfigMap for replica's personal config
func (n *Namer) createConfigMapNameHost(host *api.Host) string {
	return n.macro.Scope(host).Line(patterns.Get(namePatternConfigMapHost))
}

// createCRServiceName creates a name of a root ClickHouseInstallation Service resource
func (n *Namer) createCRServiceName(cr api.ICustomResource) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := patterns.Get(namePatternCRService)

	// ServiceTemplate may have personal name pattern specified
	if template, ok := cr.GetRootServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return n.macro.Scope(cr).Line(pattern)
}

// createCRServiceFQDN creates a FQD name of a root ClickHouseInstallation Service resource
func (n *Namer) createCRServiceFQDN(cr api.ICustomResource, namespaceDomainPattern *types.String) string {
	// FQDN can be generated either from default pattern,
	// or from personal pattern provided

	// Start with default pattern
	pattern := patterns.Get(patternServiceFQDN)

	if namespaceDomainPattern.HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		pattern = "%s." + namespaceDomainPattern.Value()
	}

	// Create FQDN based on pattern available
	return fmt.Sprintf(
		pattern,
		n.createCRServiceName(cr),
		cr.GetNamespace(),
	)
}

// createClusterServiceName returns a name of a cluster's Service
func (n *Namer) createClusterServiceName(cluster api.ICluster) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := patterns.Get(namePatternClusterService)

	// ServiceTemplate may have personal name pattern specified
	if template, ok := cluster.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return n.macro.Scope(cluster).Line(pattern)
}

// createShardServiceName returns a name of a shard's Service
func (n *Namer) createShardServiceName(shard api.IShard) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := patterns.Get(namePatternShardService)

	// ServiceTemplate may have personal name pattern specified
	if template, ok := shard.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return n.macro.Scope(shard).Line(pattern)
}

// createStatefulSetName creates a name of a StatefulSet for ClickHouse instance
func (n *Namer) createStatefulSetName(host *api.Host) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in PodTemplate

	// Start with default name pattern
	pattern := patterns.Get(namePatternStatefulSet)

	// PodTemplate may have personal name pattern specified
	if template, ok := host.GetPodTemplate(); ok {
		// PodTemplate available
		if template.GenerateName != "" {
			// PodTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create StatefulSet name based on name pattern available
	return n.macro.Scope(host).Line(pattern)
}

// createStatefulSetServiceName returns a name of a StatefulSet-related Service for ClickHouse instance
func (n *Namer) createStatefulSetServiceName(host *api.Host) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := patterns.Get(namePatternStatefulSetService)

	// ServiceTemplate may have personal name pattern specified
	if template, ok := host.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.GenerateName != "" {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GenerateName
		}
	}

	// Create Service name based on name pattern available
	return n.macro.Scope(host).Line(pattern)
}

// createPodHostname returns a hostname of a Pod of a ClickHouse instance.
// Is supposed to be used where network connection to a Pod is required.
// NB: right now Pod's hostname points to a Service, through which Pod can be accessed.
func (n *Namer) createPodHostname(host *api.Host) string {
	// Do not use Pod own hostname - point to appropriate StatefulSet's Service
	return n.createStatefulSetServiceName(host)
}

// createInstanceHostname returns hostname (pod-hostname + service or FQDN) which can be used as a replica name
// in all places where ClickHouse requires replica name. These are such places as:
// 1. "remote_servers.xml" config file
// 2. statements like SYSTEM DROP REPLICA <replica_name>
// any other places
// Function operations are based on .Spec.Defaults.ReplicasUseFQDN
func (n *Namer) createInstanceHostname(host *api.Host) string {
	if host.GetCR().GetSpec().GetDefaults().ReplicasUseFQDN.IsTrue() {
		// In case .Spec.Defaults.ReplicasUseFQDN is set replicas would use FQDN pod hostname,
		// otherwise hostname+service name (unique within namespace) would be used
		// .my-dev-namespace.svc.cluster.local
		return n.createPodFQDN(host)
	}

	return n.createPodHostname(host)
}

// createPodFQDN creates a fully qualified domain name of a pod
// ss-1eb454-2-0.my-dev-domain.svc.cluster.local
func (n *Namer) createPodFQDN(host *api.Host) string {
	// FQDN can be generated either from default pattern,
	// or from personal pattern provided

	// Start with default pattern
	pattern := patterns.Get(patternPodFQDN)

	if host.GetCR().GetSpec().GetNamespaceDomainPattern().HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		pattern = "%s." + host.GetCR().GetSpec().GetNamespaceDomainPattern().Value()
	}

	// Create FQDN based on pattern available
	return fmt.Sprintf(
		pattern,
		n.createPodHostname(host),
		host.GetRuntime().GetAddress().GetNamespace(),
	)
}

// createFQDN is a wrapper over pod FQDN function
func (n *Namer) createFQDN(host *api.Host) string {
	return n.createPodFQDN(host)
}

// createPodName creates Pod name based on specified StatefulSet or Host
func (n *Namer) createPodName(obj interface{}) string {
	switch obj.(type) {
	case *apps.StatefulSet:
		statefulSet := obj.(*apps.StatefulSet)
		return fmt.Sprintf(patterns.Get(patternPodName), statefulSet.Name)
	case *api.Host:
		host := obj.(*api.Host)
		return fmt.Sprintf(patterns.Get(patternPodName), n.createStatefulSetName(host))
	}
	return "unknown-type"
}

// createPVCName is an internal function
func (n *Namer) createPVCName(host *api.Host, volumeMountName string) string {
	return volumeMountName + "-" + n.createPodName(host)
}

// createPVCNameByVolumeClaimTemplate creates PVC name
func (n *Namer) createPVCNameByVolumeClaimTemplate(host *api.Host, volumeClaimTemplate *api.VolumeClaimTemplate) string {
	return n.createPVCName(host, volumeClaimTemplate.Name)
}
