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
	"github.com/altinity/clickhouse-operator/pkg/version"
)

// IsNew checks whether CHI is a new one or already known and was processed/created earlier
func (chi *ClickHouseInstallation) IsKnown() bool {
	// New CHI does not have FullDeploymentIDs specified
	return chi.Status.IsKnown > 0
}

// StatusFill fills .Status
func (chi *ClickHouseInstallation) StatusFill(endpoint string, pods []string) {
	// New CHI does not have FullDeploymentIDs specified
	chi.Status.IsKnown = 1
	chi.Status.Version = version.Version
	chi.Status.ClustersCount = chi.ClustersCount()
	chi.Status.ShardsCount   = chi.ShardsCount()
	chi.Status.ReplicasCount = chi.ReplicasCount()/chi.ShardsCount()
	chi.Status.Pods = pods
	chi.Status.Endpoint = endpoint
}

func (chi *ClickHouseInstallation) IsNormalized() bool {
	filled := true
	clusters := 0
	chi.WalkClusters(func(cluster *ChiCluster) error {
		clusters++
		if cluster.Chi == nil {
			filled = false
		}
		return nil
	})
	return (clusters > 0) && filled
}

func (chi *ClickHouseInstallation) FillAddressInfo() int {
	replicasCount := 0

	replicaProcessor := func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiShard,
		replicaIndex int,
		replica *ChiReplica,
	) error {
		cluster.Address.Namespace = chi.Namespace
		cluster.Address.ChiName = chi.Name
		cluster.Address.ClusterName = cluster.Name
		cluster.Address.ClusterIndex = clusterIndex

		shard.Address.Namespace = chi.Namespace
		shard.Address.ChiName = chi.Name
		shard.Address.ClusterName = cluster.Name
		shard.Address.ClusterIndex = clusterIndex
		shard.Address.ShardName = shard.Name
		shard.Address.ShardIndex = shardIndex

		replica.Address.Namespace = chi.Namespace
		replica.Address.ChiName = chi.Name
		replica.Address.ClusterName = cluster.Name
		replica.Address.ClusterIndex = clusterIndex
		replica.Address.ShardName = shard.Name
		replica.Address.ShardIndex = shardIndex
		replica.Address.ReplicaName = replica.Name
		replica.Address.ReplicaIndex = replicaIndex
		replica.Address.GlobalReplicaIndex = replicasCount

		replicasCount++
		return nil
	}
	chi.WalkReplicasFullPath(replicaProcessor)

	return replicasCount
}

func (chi *ClickHouseInstallation) FillChiPointer() {

	replicaProcessor := func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiShard,
		replicaIndex int,
		replica *ChiReplica,
	) error {
		cluster.Chi = chi
		shard.Chi = chi
		replica.Chi = chi
		return nil
	}
	chi.WalkReplicasFullPath(replicaProcessor)
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
		shard *ChiShard,
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
		shard *ChiShard,
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
		shard *ChiShard,
		replicaIndex int,
		replica *ChiReplica,
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
		replica *ChiReplica,
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

func (chi *ClickHouseInstallation) WalkReplicasTillError(
	f func(replica *ChiReplica) error,
) error {
	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			for replicaIndex := range shard.Replicas {
				replica := &shard.Replicas[replicaIndex]
				if err := f(replica); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (chi *ClickHouseInstallation) WalkClusterTillError(
	fChi func(chi *ClickHouseInstallation) error,
	fCluster func(cluster *ChiCluster) error,
	fShard func(shard *ChiShard) error,
	fReplica func(replica *ChiReplica) error,
) error {

	if err := fChi(chi); err != nil {
		return err
	}

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		if err := fCluster(cluster); err != nil {
			return err
		}
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			if err := fShard(shard); err != nil {
				return err
			}
			for replicaIndex := range shard.Replicas {
				replica := &shard.Replicas[replicaIndex]
				if err := fReplica(replica); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (chi *ClickHouseInstallation) MergeFrom(from *ClickHouseInstallation) {
	if from == nil {
		return
	}

	// Copy ObjectMeta for now
	chi.ObjectMeta = from.ObjectMeta
	// Do actual merge for Spec
	(&chi.Spec).MergeFrom(&from.Spec)
	// Copy Status for now
	chi.Status = from.Status
}

func (chi *ClickHouseInstallation) FindCluster(name string) *ChiCluster {
	var cluster *ChiCluster
	chi.WalkClusters(func(c *ChiCluster) error {
		if c.Name == name {
			cluster = c
		}
		return nil
	})
	return cluster
}

func (chi *ClickHouseInstallation) ClustersCount() int {
	count := 0
	chi.WalkClusters(func(cluster *ChiCluster) error {
		count++
		return nil
	})
	return count
}

func (chi *ClickHouseInstallation) ReplicasCount() int {
	count := 0
	chi.WalkReplicas(func(replica *ChiReplica) error {
		count++
		return nil
	})
	return count
}

func (chi *ClickHouseInstallation) ShardsCount() int {
	count := 0
	chi.WalkShards(func(replica *ChiShard) error {
		count++
		return nil
	})
	return count
}


// GetPodTemplate gets ChiPodTemplate by name
func (chi *ClickHouseInstallation) GetPodTemplate(name string) (*ChiPodTemplate, bool) {
	if chi.Spec.Templates.PodTemplatesIndex == nil {
		return nil, false
	} else {
		template, ok := chi.Spec.Templates.PodTemplatesIndex[name]
		return template, ok
	}
}

// GetVolumeClaimTemplate gets ChiVolumeClaimTemplate by name
func (chi *ClickHouseInstallation) GetVolumeClaimTemplate(name string) (*ChiVolumeClaimTemplate, bool) {
	if chi.Spec.Templates.VolumeClaimTemplatesIndex == nil {
		return nil, false
	} else {
		template, ok := chi.Spec.Templates.VolumeClaimTemplatesIndex[name]
		return template, ok
	}
}

// GetServiceTemplate gets ChiServiceTemplate by name
func (chi *ClickHouseInstallation) GetServiceTemplate(name string) (*ChiServiceTemplate, bool) {
	if chi.Spec.Templates.ServiceTemplatesIndex == nil {
		return nil, false
	} else {
		template, ok := chi.Spec.Templates.ServiceTemplatesIndex[name]
		return template, ok
	}
}

// GetServiceTemplate gets own ChiServiceTemplate
func (chi *ClickHouseInstallation) GetOwnServiceTemplate() (*ChiServiceTemplate, bool) {
	name := chi.Spec.Defaults.Templates.ServiceTemplate
	template, ok := chi.GetServiceTemplate(name)
	return template, ok
}
