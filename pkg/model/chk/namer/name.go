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

// createConfigMapNameCommon returns a name for a ConfigMap for replica's common config
func (n *Namer) createConfigMapNameCommon(chi api.ICustomResource) string {
	return n.macro.Scope(chi).Line(patterns.Get(patternConfigMapCommonName))
}

// createConfigMapNameCommonUsers returns a name for a ConfigMap for replica's common users config
func (n *Namer) createConfigMapNameCommonUsers(chi api.ICustomResource) string {
	return n.macro.Scope(chi).Line(patterns.Get(patternConfigMapCommonUsersName))
}

// createConfigMapNameHost returns a name for a ConfigMap for replica's personal config
func (n *Namer) createConfigMapNameHost(host *api.Host) string {
	return n.macro.Scope(host).Line(patterns.Get(patternConfigMapHostName))
}

// createCRServiceName creates a name of a root ClickHouseInstallation Service resource
func (n *Namer) createCRServiceName(cr api.ICustomResource, templates ...*api.ServiceTemplate) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := patterns.Get(patternCRServiceName)

	// May have service template specified
	if len(templates) > 0 {
		template := templates[0]
		// ServiceTemplate may have personal name pattern specified
		if template.HasGenerateName() {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GetGenerateName()
		}
	}

	// Create Service name based on name pattern available
	return n.macro.Scope(cr).Line(pattern)
}

// createCRServiceFQDN creates a FQD name of a root ClickHouseInstallation Service resource
func (n *Namer) createCRServiceFQDN(cr api.ICustomResource, namespaceDomainPattern *types.String, templates ...*api.ServiceTemplate) string {
	// FQDN can be generated either from default pattern,
	// or from personal pattern provided

	// Start with default pattern
	pattern := patternServiceFQDN

	if namespaceDomainPattern.HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		pattern = "%s." + namespaceDomainPattern.Value()
	}

	service := ""
	// May have service template specified
	if len(templates) > 0 {
		template := templates[0]
		service = n.createCRServiceName(cr, template)
	} else {
		service = n.createCRServiceName(cr)
	}

	// Create FQDN based on pattern available
	return fmt.Sprintf(
		pattern,
		service,
		cr.GetNamespace(),
	)
}

// createClusterServiceName returns a name of a cluster's Service
func (n *Namer) createClusterServiceName(cluster api.ICluster) string {
	// Name can be generated either from default name pattern,
	// or from personal name pattern provided in ServiceTemplate

	// Start with default name pattern
	pattern := patterns.Get(patternClusterServiceName)

	// ServiceTemplate may have personal name pattern specified
	if template, ok := cluster.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.HasGenerateName() {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GetGenerateName()
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
	pattern := patterns.Get(patternShardServiceName)

	// ServiceTemplate may have personal name pattern specified
	if template, ok := shard.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.HasGenerateName() {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GetGenerateName()
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
	pattern := patterns.Get(patternStatefulSetName)

	// PodTemplate may have personal name pattern specified
	if template, ok := host.GetPodTemplate(); ok {
		// PodTemplate available
		if template.HasGenerateName() {
			// PodTemplate has explicitly specified name pattern
			pattern = template.GetGenerateName()
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
	pattern := patterns.Get(patternStatefulSetServiceName)

	// ServiceTemplate may have personal name pattern specified
	if template, ok := host.GetServiceTemplate(); ok {
		// ServiceTemplate available
		if template.HasGenerateName() {
			// ServiceTemplate has explicitly specified name pattern
			pattern = template.GetGenerateName()
		}
	}

	// Create Service name based on name pattern available
	return n.macro.Scope(host).Line(pattern)
}

// createPodHostname returns a hostname of a Pod of a ClickHouse Keeper instance.
// Is supposed to be used where network connection to a Pod is required.
// For StatefulSet pods, this returns <pod-name>.<headless-service-name> to ensure DNS resolution works.
func (n *Namer) createPodHostname(host *api.Host) string {
	// For StatefulSet pods, we need <pod-name>.<headless-service-name> format 
	// to ensure proper DNS resolution within the cluster
	return fmt.Sprintf("%s.%s", n.createPodName(host), n.createStatefulSetServiceName(host))
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
// chk-keeper-cluster-0-0.chk-keeper-cluster-0.my-dev-namespace.svc.cluster.local
func (n *Namer) createPodFQDN(host *api.Host) string {
	// FQDN can be generated either from default pattern,
	// or from personal pattern provided

	if host.GetCR().GetSpec().GetNamespaceDomainPattern().HasValue() {
		// NamespaceDomainPattern has been explicitly specified
		// Use custom pattern: <pod-name>.<headless-service-name>.<custom-domain>
		pattern := "%s.%s." + host.GetCR().GetSpec().GetNamespaceDomainPattern().Value()
		return fmt.Sprintf(
			pattern,
			n.createPodName(host),
			n.createStatefulSetServiceName(host),
		)
	}

	// Use standard Kubernetes StatefulSet DNS pattern:
	// <pod-name>.<headless-service-name>.<namespace>.svc.cluster.local
	// This fixes the hostname mismatch issue in remote_servers.xml where
	// StatefulSet pods have names like "chk-keeper-cluster-0-0" but
	// the generated hostnames were missing the headless service component
	return fmt.Sprintf(
		"%s.%s.%s.svc.cluster.local",
		n.createPodName(host),
		n.createStatefulSetServiceName(host),
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
		return fmt.Sprintf(patternPodName, statefulSet.Name)
	case *api.Host:
		host := obj.(*api.Host)
		return fmt.Sprintf(patternPodName, n.createStatefulSetName(host))
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

// createClusterPDBName creates a name of a cluster-scope PDB
func (n *Namer) createClusterPDBName(cluster api.ICluster) string {
	// Start with default name pattern
	pattern := patterns.Get(patternClusterPDBName)

	// Create PDB name based on name pattern available
	return n.macro.Scope(cluster).Line(pattern)
}
