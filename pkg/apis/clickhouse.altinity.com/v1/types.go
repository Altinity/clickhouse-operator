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

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	Defaults      ChiDefaults      `json:"defaults,omitempty"`
	Configuration ChiConfiguration `json:"configuration" diff:"configuration"`
	Templates     ChiTemplates     `json:"templates,omitempty"`
}

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	IsKnown int `json:"isKnown"`
}

// ChiDefaults defines defaults section of .spec
type ChiDefaults struct {
	ReplicasUseFQDN int               `json:"replicasUseFQDN,omitempty"`
	DistributedDDL  ChiDistributedDDL `json:"distributedDDL,omitempty"`
	Deployment      ChiDeployment     `json:"deployment,omitempty"`
}

// ChiConfiguration defines configuration section of .spec
type ChiConfiguration struct {
	Zookeeper ChiConfigurationZookeeper `json:"zookeeper,omitempty" diff:"zookeeper"`
	Users     map[string]string         `json:"users,omitempty"     diff:"users"`
	Profiles  map[string]string         `json:"profiles,omitempty"  diff:"profiles"`
	Quotas    map[string]string         `json:"quotas,omitempty"    diff:"quotas"`
	Settings  map[string]string         `json:"settings,omitempty"  diff:"settings"`
	// TODO refactor into map[string]ChiCluster
	Clusters []ChiCluster `json:"clusters,omitempty"  diff:"clusters"`
}

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name       string           `json:"name"   diff:"name"`
	Layout     ChiClusterLayout `json:"layout" diff:"layout"`
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
	Port       int32         `json:"port,omitempty"       diff:"port"`
	Deployment ChiDeployment `json:"deployment,omitempty" diff:"deployment"`

	Address ChiClusterLayoutShardReplicaAddress `json:"address"`
}

type ChiClusterLayoutShardReplicaAddress struct {
	Namespace    string `json:"namespace"`
	ChiName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ShardIndex   int    `json:"shardIndex"`
	ReplicaIndex int    `json:"replicaIndex"`
}

// ChiTemplates defines templates section of .spec
type ChiTemplates struct {
	// TODO refactor into [string]ChiPodTemplate
	PodTemplates []ChiPodTemplate `json:"podTemplates,omitempty"`
	// TODO refactor into [string]ChiVolumeClaimTemplate
	VolumeClaimTemplates []ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty"`
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

	// Index is an index of this Deployment within Cluster
	Index int `json:"index,omitempty" diff:"fingerprint"`
}

// ChiDeploymentZone defines zone section of *.deployment
type ChiDeploymentZone struct {
	MatchLabels map[string]string `json:"matchLabels" diff:"matchLabels"`
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

// ChiVolumeClaimTemplate defines item of .spec.templates.volumeClaimTemplates
type ChiVolumeClaimTemplate struct {
	Name                  string                       `json:"name"`
	PersistentVolumeClaim corev1.PersistentVolumeClaim `json:"persistentVolumeClaim"`
	UseDefaultName        bool                         `json:"useDefaultName"`
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
func (chi *ClickHouseInstallation) IsKnown() bool {
	// New CHI does not have FullDeploymentIDs specified
	return chi.Status.IsKnown > 0
}

func (chi *ClickHouseInstallation) SetKnown() {
	// New CHI does not have FullDeploymentIDs specified
	chi.Status.IsKnown = 1
}

func (chi *ClickHouseInstallation) IsFilled() bool {
	filled := true
	clusters := 0
	chi.WalkClusters(func(cluster *ChiCluster) error {
		clusters++
		if cluster.Address.Namespace == "" {
			filled = false
		}
		return nil
	})
	return (clusters > 0) && filled
}

func (chi *ClickHouseInstallation) DeploymentsCount() int {
	replicasCount := 0

	shardProcessor := func(
		shard *ChiClusterLayoutShard,
	) error {
		replicasCount += shard.ReplicasCount
		return nil
	}
	chi.WalkShards(shardProcessor)

	return replicasCount
}

func (chi *ClickHouseInstallation) FillAddressInfo() int {
	replicasCount := 0

	replicaProcessor := func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiClusterLayoutShard,
		replicaIndex int,
		replica *ChiClusterLayoutShardReplica,
	) error {
		cluster.Address.Namespace = chi.Namespace
		cluster.Address.CHIName = chi.Name
		cluster.Address.ClusterName = cluster.Name
		cluster.Address.ClusterIndex = clusterIndex

		shard.Address.Namespace = chi.Namespace
		shard.Address.CHIName = chi.Name
		shard.Address.ClusterName = cluster.Name
		shard.Address.ClusterIndex = clusterIndex
		shard.Address.ShardIndex = shardIndex

		replica.Address.Namespace = chi.Namespace
		replica.Address.ChiName = chi.Name
		replica.Address.ClusterName = cluster.Name
		replica.Address.ClusterIndex = clusterIndex
		replica.Address.ShardIndex = shardIndex
		replica.Address.ReplicaIndex = replicaIndex

		return nil
	}
	chi.WalkReplicasFullPath(replicaProcessor)

	return replicasCount
}

func (chi *ClickHouseInstallation) WalkClustersFullPath(
	f func(chi *ClickHouseInstallation, clusterIndex int, cluster *ChiCluster) error,
) []error {
	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		res = append(res, f(chi, clusterIndex, cluster))
	}

	return res
}

func (chi *ClickHouseInstallation) WalkClusters(
	f func(cluster *ChiCluster) error,
) []error {
	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		res = append(res, f(cluster))
	}

	return res
}

func (chi *ClickHouseInstallation) WalkShardsFullPath(
	f func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiClusterLayoutShard,
	) error,
) []error {

	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			res = append(res, f(chi, clusterIndex, cluster, shardIndex, shard))
		}
	}

	return res
}

func (chi *ClickHouseInstallation) WalkShards(
	f func(
		shard *ChiClusterLayoutShard,
	) error,
) []error {

	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			res = append(res, f(shard))
		}
	}

	return res
}

func (chi *ClickHouseInstallation) WalkReplicasFullPath(
	f func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiClusterLayoutShard,
		replicaIndex int,
		replica *ChiClusterLayoutShardReplica,
	) error,
) []error {

	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			for replicaIndex := range shard.Replicas {
				replica := &shard.Replicas[replicaIndex]
				res = append(res, f(chi, clusterIndex, cluster, shardIndex, shard, replicaIndex, replica))
			}
		}
	}

	return res
}

func (chi *ClickHouseInstallation) WalkReplicas(
	f func(
		replica *ChiClusterLayoutShardReplica,
	) error,
) []error {

	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			for replicaIndex := range shard.Replicas {
				replica := &shard.Replicas[replicaIndex]
				res = append(res, f(replica))
			}
		}
	}

	return res
}

func (cluster *ChiCluster) WalkShards(
	f func(shardIndex int, shard *ChiClusterLayoutShard) error,
) []error {
	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := &cluster.Layout.Shards[shardIndex]
		res = append(res, f(shardIndex, shard))
	}

	return res
}

func (cluster *ChiCluster) WalkReplicas(
	f func(
		replica *ChiClusterLayoutShardReplica,
	) error,
) []error {

	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := &cluster.Layout.Shards[shardIndex]
		for replicaIndex := range shard.Replicas {
			replica := &shard.Replicas[replicaIndex]
			res = append(res, f(replica))
		}
	}

	return res
}


func (shard *ChiClusterLayoutShard) WalkReplicas(
	f func(replica *ChiClusterLayoutShardReplica) error,
) []error {
	res := make([]error, 0)

	for replicaIndex := range shard.Replicas {
		replica := &shard.Replicas[replicaIndex]
		res = append(res, f(replica))
	}

	return res
}
