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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallation describes the Installation of a ClickHouse Database Cluster
type ClickHouseInstallation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ChiSpec   `json:"spec" diff:"spec"`
	Status            ChiStatus `json:"status"`
}

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	FullDeploymentIDs []string `json:"fullDeploymentIDs"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	Defaults      ChiDefaults      `json:"defaults,omitempty"`
	Configuration ChiConfiguration `json:"configuration" diff:"configuration"`
	Templates     ChiTemplates     `json:"templates,omitempty"`
}

// ChiDefaults defines defaults section of .spec
type ChiDefaults struct {
	ReplicasUseFQDN int               `json:"replicasUseFQDN,omitempty"`
	DistributedDDL  ChiDistributedDDL `json:"distributedDDL,omitempty"`
	Deployment      ChiDeployment     `json:"deployment,omitempty"`
}

// ChiDistributedDDL defines distributedDDL section of .spec.defaults
type ChiDistributedDDL struct {
	Profile string `json:"profile,omitempty"`
}

// ChiDeployment defines deployment section of .spec
type ChiDeployment struct {
	// PodTemplate specifies which Pod template from
	// .spec.templates.podTemplates should be used
	PodTemplate string `json:"podTemplate,omitempty" diff:"podTemplate"`

	// VolumeClaimTemplate specifies which VolumeClaim template
	// from .spec.templates.volumeClaimTemplates should be used
	VolumeClaimTemplate string `json:"volumeClaimTemplate,omitempty" diff:"volumeClaimTemplate"`

	Zone     ChiDeploymentZone `json:"zone,omitempty" diff:"zone"`
	Scenario string            `json:"scenario,omitempty" diff:"scenario"`

	// Fingerprint is a fingerprint of the ChiDeployment. Used to find equal deployments
	Fingerprint string `json:"fingerprint,omitempty" diff:"fingerprint"`
}

// ChiDeploymentZone defines zone section of *.deployment
type ChiDeploymentZone struct {
	MatchLabels map[string]string `json:"matchLabels" diff:"matchLabels"`
}

// ChiConfiguration defines configuration section of .spec
type ChiConfiguration struct {
	Zookeeper ChiConfigurationZookeeper `json:"zookeeper,omitempty" diff:"zookeeper"`
	Users     map[string]string         `json:"users,omitempty"     diff:"users"`
	Profiles  map[string]string         `json:"profiles,omitempty"  diff:"profiles"`
	Quotas    map[string]string         `json:"quotas,omitempty"    diff:"quotas"`
	Settings  map[string]string         `json:"settings,omitempty"  diff:"settings"`
	Clusters  []ChiCluster              `json:"clusters,omitempty"  diff:"clusters"`
}

// ChiConfigurationZookeeper defines zookeeper section of .spec.configuration
type ChiConfigurationZookeeper struct {
	Nodes []ChiConfigurationZookeeperNode `json:"nodes,omitempty" diff:"nodes"`
}

// ChiConfigurationZookeeperNode defines item of nodes section of .spec.configuration.zookeeper
type ChiConfigurationZookeeperNode struct {
	Host string `json:"host" diff:"host"`
	Port int32  `json:"port" diff:"port"`
}

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name       string           `json:"name"   diff:"name"`
	Layout     ChiClusterLayout `json:"layout" diff:"layout"`
	Deployment ChiDeployment    `json:"deployment,omitempty"`
}

// ChiClusterLayout defines layout section of .spec.configuration.clusters
type ChiClusterLayout struct {
	Type          string                  `json:"type"`
	ShardsCount   int                     `json:"shardsCount,omitempty"`
	ReplicasCount int                     `json:"replicasCount,omitempty"`
	Shards        []ChiClusterLayoutShard `json:"shards,omitempty" diff:"shards"`
}

// ChiClusterLayoutShard defines item of a shard section of .spec.configuration.clusters[n].shards
type ChiClusterLayoutShard struct {
	DefinitionType      string                         `json:"definitionType"`
	ReplicasCount       int                            `json:"replicasCount,omitempty"`
	Weight              int                            `json:"weight,omitempty" diff:"weight"`
	InternalReplication string                         `json:"internalReplication,omitempty" diff:"internalReplication"`
	Deployment          ChiDeployment                  `json:"deployment,omitempty"`
	Replicas            []ChiClusterLayoutShardReplica `json:"replicas,omitempty" diff:"replicas"`
}

// ChiClusterLayoutShardReplica defines item of a replicas section of .spec.configuration.clusters[n].shards[m]
type ChiClusterLayoutShardReplica struct {
	Port       int32         `json:"port,omitempty"       diff:"port"`
	Deployment ChiDeployment `json:"deployment,omitempty" diff:"deployment"`
}

// ChiTemplates defines templates section of .spec
type ChiTemplates struct {
	PodTemplates         []ChiPodTemplate         `json:"podTemplates,omitempty"`
	VolumeClaimTemplates []ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty"`
}

// ChiVolumeClaimTemplate defines item of .spec.templates.volumeClaimTemplates
type ChiVolumeClaimTemplate struct {
	Name                  string                       `json:"name"`
	PersistentVolumeClaim corev1.PersistentVolumeClaim `json:"persistentVolumeClaim"`
}

// ChiPodTemplate defines item of a podTemplates section of .spec.templates
type ChiPodTemplate struct {
	Name       string             `json:"name"`
	Containers []corev1.Container `json:"containers"`
	Volumes    []corev1.Volume    `json:"volumes"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallationList defines a list of ClickHouseInstallation resources
type ClickHouseInstallationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ClickHouseInstallation `json:"items"`
}

const (
	// ClickHouseInstallationCRDResourceKind defines kind of CRD resource
	ClickHouseInstallationCRDResourceKind = "ClickHouseInstallation"
)

// IsNew checks whether CHI is a new one or already known and was processed/created earlier
func (chi *ClickHouseInstallation) IsNew() bool {
	// New CHI does not have FullDeploymentIDs specified
	return chi.Status.FullDeploymentIDs == nil || len(chi.Status.FullDeploymentIDs) == 0
}
