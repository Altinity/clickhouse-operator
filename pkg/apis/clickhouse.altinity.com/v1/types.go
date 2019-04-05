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
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata"`
	Spec              ChiSpec   `json:"spec"     yaml:"spec"`
	Status            ChiStatus `json:"status"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	Defaults      ChiDefaults      `json:"defaults,omitempty"  yaml:"defaults"`
	Configuration ChiConfiguration `json:"configuration"       yaml:"configuration"`
	Templates     ChiTemplates     `json:"templates,omitempty" yaml:"templates"`
}

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	IsKnown int `json:"isKnown"`
}

// ChiDefaults defines defaults section of .spec
type ChiDefaults struct {
	ReplicasUseFQDN int               `json:"replicasUseFQDN,omitempty" yaml:"replicasUseFQDN"`
	DistributedDDL  ChiDistributedDDL `json:"distributedDDL,omitempty"  yaml:"distributedDDL"`
	Deployment      ChiDeployment     `json:"deployment,omitempty"      yaml:"deployment"`
}

// ChiConfiguration defines configuration section of .spec
type ChiConfiguration struct {
	Zookeeper ChiConfigurationZookeeper `json:"zookeeper,omitempty" yaml:"zookeeper"`
	Users     map[string]string         `json:"users,omitempty"     yaml:"users"`
	Profiles  map[string]string         `json:"profiles,omitempty"  yaml:"profiles"`
	Quotas    map[string]string         `json:"quotas,omitempty"    yaml:"quotas"`
	Settings  map[string]string         `json:"settings,omitempty"  yaml:"settings"`
	// TODO refactor into map[string]ChiCluster
	Clusters []ChiCluster `json:"clusters,omitempty"`
}

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name       string           `json:"name"`
	Layout     ChiClusterLayout `json:"layout"`
	Deployment ChiDeployment    `json:"deployment,omitempty"`

	Address ChiClusterAddress `json:"address"`
}

type ChiClusterAddress struct {
	Namespace    string `json:"namespace"`
	CHIName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
}

// ChiClusterLayout defines layout section of .spec.configuration.clusters
type ChiClusterLayout struct {
	Type          string                  `json:"type"`
	ShardsCount   int                     `json:"shardsCount,omitempty"`
	ReplicasCount int                     `json:"replicasCount,omitempty"`
	Shards        []ChiClusterLayoutShard `json:"shards,omitempty"`
}

// ChiClusterLayoutShard defines item of a shard section of .spec.configuration.clusters[n].shards
type ChiClusterLayoutShard struct {
	DefinitionType      string                         `json:"definitionType"`
	ReplicasCount       int                            `json:"replicasCount,omitempty"`
	Weight              int                            `json:"weight,omitempty"`
	InternalReplication string                         `json:"internalReplication,omitempty"`
	Deployment          ChiDeployment                  `json:"deployment,omitempty"`
	Replicas            []ChiClusterLayoutShardReplica `json:"replicas,omitempty"`

	Address ChiClusterLayoutShardAddress `json:"address"`
}

type ChiClusterLayoutShardAddress struct {
	Namespace    string `json:"namespace"`
	CHIName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ShardIndex   int    `json:"shardIndex"`
}

// ChiClusterLayoutShardReplica defines item of a replicas section of .spec.configuration.clusters[n].shards[m]
type ChiClusterLayoutShardReplica struct {
	Port       int32         `json:"port,omitempty"`
	Deployment ChiDeployment `json:"deployment,omitempty"`

	Address ChiClusterLayoutShardReplicaAddress `json:"address"`
	Config  ChiClusterLayoutShardReplicaConfig  `json:"config"`
}

type ChiClusterLayoutShardReplicaAddress struct {
	Namespace    string `json:"namespace"`
	ChiName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ShardIndex   int    `json:"shardIndex"`
	ReplicaIndex int    `json:"replicaIndex"`
}

type ChiClusterLayoutShardReplicaConfig struct {
	ZkFingerprint string `json:"zkfingerprint"`
}

// ChiTemplates defines templates section of .spec
type ChiTemplates struct {
	// TODO refactor into [string]ChiPodTemplate
	PodTemplates []ChiPodTemplate `json:"podTemplates,omitempty" yaml:"podTemplates"`
	// TODO refactor into [string]ChiVolumeClaimTemplate
	VolumeClaimTemplates []ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates"`
}

// ChiDistributedDDL defines distributedDDL section of .spec.defaults
type ChiDistributedDDL struct {
	Profile string `json:"profile,omitempty" yaml:"profile"`
}

// ChiDeployment defines deployment section of .spec
type ChiDeployment struct {
	// PodTemplate specifies which Pod template from
	// .spec.templates.podTemplates should be used
	PodTemplate string `json:"podTemplate,omitempty" yaml:"podTemplate"`

	// VolumeClaimTemplate specifies which VolumeClaim template
	// from .spec.templates.volumeClaimTemplates should be used
	VolumeClaimTemplate string `json:"volumeClaimTemplate,omitempty" yaml:"volumeClaimTemplate"`

	Zone     ChiDeploymentZone `json:"zone,omitempty"     yaml:"zone"`
	Scenario string            `json:"scenario,omitempty" yaml:"scenario"`

	// Fingerprint is a fingerprint of the ChiDeployment. Used to find equal deployments
	Fingerprint string `json:"fingerprint,omitempty"`

	// Index is an index of this Deployment within Cluster
	Index int `json:"index,omitempty"`
}

// ChiDeploymentZone defines zone section of *.deployment
type ChiDeploymentZone struct {
	MatchLabels map[string]string `json:"matchLabels" yaml:"matchLabels"`
}

// ChiConfigurationZookeeper defines zookeeper section of .spec.configuration
type ChiConfigurationZookeeper struct {
	Nodes []ChiConfigurationZookeeperNode `json:"nodes,omitempty" yaml:"nodes"`
}

// ChiConfigurationZookeeperNode defines item of nodes section of .spec.configuration.zookeeper
type ChiConfigurationZookeeperNode struct {
	Host string `json:"host" yaml:"host"`
	Port int32  `json:"port" yaml:"port"`
}

// ChiVolumeClaimTemplate defines item of .spec.templates.volumeClaimTemplates
type ChiVolumeClaimTemplate struct {
	Name                  string                       `json:"name"                  yaml:"name"`
	PersistentVolumeClaim corev1.PersistentVolumeClaim `json:"persistentVolumeClaim" yaml:"persistentVolumeClaim"`
	UseDefaultName        bool                         `json:"useDefaultName"`
}

// ChiPodTemplate defines item of a podTemplates section of .spec.templates
type ChiPodTemplate struct {
	Name       string             `json:"name"       yaml:"name"`
	Containers []corev1.Container `json:"containers" yaml:"containers"`
	Volumes    []corev1.Volume    `json:"volumes"    yaml:"volumes"`
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
