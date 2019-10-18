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

// StatusFill fills .Status
func (chi *ClickHouseInstallation) StatusFill(endpoint string, pods []string) {
	chi.Status.Version = version.Version
	chi.Status.ClustersCount = chi.ClustersCount()
	chi.Status.ShardsCount = chi.ShardsCount()
	chi.Status.HostsCount = chi.HostsCount()
	chi.Status.UpdatedHostsCount = 0
	chi.Status.DeleteHostsCount = 0
	chi.Status.DeletedHostsCount = 0
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
	hostsCount := 0

	hostProcessor := func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiShard,
		replicaIndex int,
		host *ChiHost,
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

		host.Address.Namespace = chi.Namespace
		host.Address.ChiName = chi.Name
		host.Address.ClusterName = cluster.Name
		host.Address.ClusterIndex = clusterIndex
		host.Address.ShardName = shard.Name
		host.Address.ShardIndex = shardIndex
		host.Address.ReplicaName = host.Name
		host.Address.ReplicaIndex = replicaIndex
		host.Address.HostIndex = hostsCount

		hostsCount++
		return nil
	}
	chi.WalkHostsFullPath(hostProcessor)

	return hostsCount
}

func (chi *ClickHouseInstallation) FillChiPointer() {

	hostProcessor := func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiShard,
		replicaIndex int,
		host *ChiHost,
	) error {
		cluster.Chi = chi
		shard.Chi = chi
		host.Chi = chi
		return nil
	}
	chi.WalkHostsFullPath(hostProcessor)
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

func (chi *ClickHouseInstallation) WalkHostsFullPath(
	f func(
		chi *ClickHouseInstallation,
		clusterIndex int,
		cluster *ChiCluster,
		shardIndex int,
		shard *ChiShard,
		replicaIndex int,
		host *ChiHost,
	) error,
) []error {

	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			for replicaIndex := range shard.Replicas {
				host := &shard.Replicas[replicaIndex]
				res = append(res, f(chi, clusterIndex, cluster, shardIndex, shard, replicaIndex, host))
			}
		}
	}

	return res
}

func (chi *ClickHouseInstallation) WalkHosts(
	f func(host *ChiHost) error,
) []error {

	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			for replicaIndex := range shard.Replicas {
				host := &shard.Replicas[replicaIndex]
				res = append(res, f(host))
			}
		}
	}

	return res
}

func (chi *ClickHouseInstallation) WalkHostsTillError(
	f func(host *ChiHost) error,
) error {
	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			for replicaIndex := range shard.Replicas {
				host := &shard.Replicas[replicaIndex]
				if err := f(host); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (chi *ClickHouseInstallation) WalkTillError(
	fChi func(chi *ClickHouseInstallation) error,
	fCluster func(cluster *ChiCluster) error,
	fShard func(shard *ChiShard) error,
	fHost func(host *ChiHost) error,
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
				host := &shard.Replicas[replicaIndex]
				if err := fHost(host); err != nil {
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

func (spec *ChiSpec) MergeFrom(from *ChiSpec) {
	if from == nil {
		return
	}

	if spec.Stop == "" {
		spec.Stop = from.Stop
	}
	(&spec.Defaults).MergeFrom(&from.Defaults)
	(&spec.Configuration).MergeFrom(&from.Configuration)
	(&spec.Templates).MergeFrom(&from.Templates)
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

func (chi *ClickHouseInstallation) HostsCount() int {
	count := 0
	chi.WalkHosts(func(host *ChiHost) error {
		count++
		return nil
	})
	return count
}

func (chi *ClickHouseInstallation) ShardsCount() int {
	count := 0
	chi.WalkShards(func(shard *ChiShard) error {
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

// WalkVolumeClaimTemplates walks over all VolumeClaimTemplates
func (chi *ClickHouseInstallation) WalkVolumeClaimTemplates(f func(template *ChiVolumeClaimTemplate)) {
	if chi.Spec.Templates.VolumeClaimTemplatesIndex == nil {
		return
	}

	for name := range chi.Spec.Templates.VolumeClaimTemplatesIndex {
		template, _ := chi.Spec.Templates.VolumeClaimTemplatesIndex[name]
		f(template)
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
