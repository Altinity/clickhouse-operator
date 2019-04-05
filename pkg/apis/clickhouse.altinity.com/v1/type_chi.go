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
