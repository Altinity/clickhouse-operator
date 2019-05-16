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

func (cluster *ChiCluster) InheritTemplates(chi *ClickHouseInstallation) {
	if cluster.Templates.PodTemplate == "" {
		cluster.Templates.PodTemplate = chi.Spec.Defaults.Templates.PodTemplate
	}
	if cluster.Templates.VolumeClaimTemplate == "" {
		cluster.Templates.VolumeClaimTemplate = chi.Spec.Defaults.Templates.VolumeClaimTemplate
	}
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
	f func(replica *ChiClusterLayoutShardReplica) error,
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
