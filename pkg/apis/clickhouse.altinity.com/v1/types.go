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

// ClickHouseInstallation defines the Installation of a ClickHouse Database Cluster
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
	IsKnown       int      `json:"isKnown"`
	Version       string   `json:"version"`
	ClustersCount int      `json:"clusters"`
	ReplicasCount int      `json:"replicas"`
	Pods          []string `json:"pods"`
	Endpoint      string   `json:"endpoint"`
}

// ChiDefaults defines defaults section of .spec
type ChiDefaults struct {
	ReplicasUseFQDN string            `json:"replicasUseFQDN,omitempty" yaml:"replicasUseFQDN"`
	DistributedDDL  ChiDistributedDDL `json:"distributedDDL,omitempty"  yaml:"distributedDDL"`
	Templates       ChiTemplateNames  `json:"templates" yaml:"templates"`
}

// ChiTemplateNames defines references to .spec.templates to be used on current level of cluster
type ChiTemplateNames struct {
	PodTemplate         string `json:"podTemplate,omitempty"      yaml:"podTemplate"`
	VolumeClaimTemplate string `json:"volumeClaimTemplate,omitempty" yaml:"volumeClaimTemplate"`
}

// ChiConfiguration defines configuration section of .spec
type ChiConfiguration struct {
	Zookeeper ChiZookeeperConfig     `json:"zookeeper,omitempty" yaml:"zookeeper"`
	Users     map[string]interface{} `json:"users,omitempty"     yaml:"users"`
	Profiles  map[string]interface{} `json:"profiles,omitempty"  yaml:"profiles"`
	Quotas    map[string]interface{} `json:"quotas,omitempty"    yaml:"quotas"`
	Settings  map[string]interface{} `json:"settings,omitempty"  yaml:"settings"`
	// TODO refactor into map[string]ChiCluster
	Clusters []ChiCluster `json:"clusters,omitempty"`
}

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name      string           `json:"name"`
	Layout    ChiLayout        `json:"layout"`
	Templates ChiTemplateNames `json:"templates,omitempty"`

	Address ChiClusterAddress `json:"address"`
}

// ChiClusterAddress defines address of a cluster within ClickHouseInstallation
type ChiClusterAddress struct {
	Namespace    string `json:"namespace"`
	ChiName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
}

// ChiLayout defines layout section of .spec.configuration.clusters
type ChiLayout struct {
	// DEPRECATED - to be removed soon
	Type          string     `json:"type"`
	ShardsCount   int        `json:"shardsCount,omitempty"`
	ReplicasCount int        `json:"replicasCount,omitempty"`
	Shards        []ChiShard `json:"shards,omitempty"`
}

// ChiShard defines item of a shard section of .spec.configuration.clusters[n].shards
type ChiShard struct {
	// DEPRECATED - to be removed soon
	DefinitionType      string           `json:"definitionType"`
	Name                string           `json:"name,omitempty"`
	Weight              int              `json:"weight,omitempty"`
	InternalReplication string           `json:"internalReplication,omitempty"`
	Templates           ChiTemplateNames `json:"templates,omitempty"`
	ReplicasCount       int              `json:"replicasCount,omitempty"`
	Replicas            []ChiReplica     `json:"replicas,omitempty"`

	Address ChiShardAddress `json:"address"`
}

// ChiShardAddress defines address of a shard within ClickHouseInstallation
type ChiShardAddress struct {
	Namespace    string `json:"namespace"`
	ChiName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ShardName    string `json:"shardName,omitempty"`
	ShardIndex   int    `json:"shardIndex"`
}

// ChiReplica defines item of a replicas section of .spec.configuration.clusters[n].shards[m]
type ChiReplica struct {
	Name      string           `json:"name,omitempty"`
	Port      int32            `json:"port,omitempty"`
	Templates ChiTemplateNames `json:"templates,omitempty"`

	Address ChiReplicaAddress `json:"address"`
	Config  ChiReplicaConfig  `json:"config"`
}

// ChiReplicaAddress defines address of a replica within ClickHouseInstallation
type ChiReplicaAddress struct {
	Namespace          string `json:"namespace"`
	ChiName            string `json:"chiName"`
	ClusterName        string `json:"clusterName"`
	ClusterIndex       int    `json:"clusterIndex"`
	ShardName          string `json:"shardName,omitempty"`
	ShardIndex         int    `json:"shardIndex"`
	ReplicaName        string `json:"replicaName,omitempty"`
	ReplicaIndex       int    `json:"replicaIndex"`
	GlobalReplicaIndex int    `json:"globalReplicaIndex"`
}

// ChiReplicaConfig defines additional data related to replica
type ChiReplicaConfig struct {
	ZkFingerprint string `json:"zkfingerprint"`
}

// ChiTemplates defines templates section of .spec
type ChiTemplates struct {
	// TODO refactor into [string]ChiPodTemplate
	PodTemplates []ChiPodTemplate `json:"podTemplates,omitempty" yaml:"podTemplates"`
	// TODO refactor into [string]ChiVolumeClaimTemplate
	VolumeClaimTemplates []ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates"`
}

// ChiPodTemplate defines full Pod Template, directly used by StatefulSet
type ChiPodTemplate struct {
	Name string         `json:"name" yaml:"name"`
	Spec corev1.PodSpec `json:"spec" yaml:"spec"`
}

// ChiVolumeClaimTemplate defines PersistentVolumeClaim Template, directly used by StatefulSet
type ChiVolumeClaimTemplate struct {
	Name string                           `json:"name" yaml:"name"`
	Spec corev1.PersistentVolumeClaimSpec `json:"spec" yaml:"spec"`
}

// ChiDistributedDDL defines distributedDDL section of .spec.defaults
type ChiDistributedDDL struct {
	Profile string `json:"profile,omitempty" yaml:"profile"`
}

// ChiZookeeperConfig defines zookeeper section of .spec.configuration
type ChiZookeeperConfig struct {
	Nodes []ChiZookeeperNode `json:"nodes,omitempty" yaml:"nodes"`
}

// ChiZookeeperNode defines item of nodes section of .spec.configuration.zookeeper
type ChiZookeeperNode struct {
	Host string `json:"host" yaml:"host"`
	Port int32  `json:"port" yaml:"port"`
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
