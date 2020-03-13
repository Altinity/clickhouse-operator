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
	"math"
)

// fillStatus fills .Status
func (chi *ClickHouseInstallation) FillStatus(endpoint string, pods, fqdns []string) {
	chi.Status.Version = version.Version
	chi.Status.ClustersCount = chi.ClustersCount()
	chi.Status.ShardsCount = chi.ShardsCount()
	chi.Status.HostsCount = chi.HostsCount()
	chi.Status.UpdatedHostsCount = 0
	chi.Status.DeleteHostsCount = 0
	chi.Status.DeletedHostsCount = 0
	chi.Status.Pods = pods
	chi.Status.FQDNs = fqdns
	chi.Status.Endpoint = endpoint
	chi.Status.NormalizedCHI = chi.Spec
}

func (chi *ClickHouseInstallation) FillAddressInfo() {
	// What is the max number of Pods allowed per Node
	// TODO need to support multi-cluster
	maxNumberOfPodsPerNode := 0
	chi.WalkPodTemplates(func(template *ChiPodTemplate) {
		for i := range template.PodDistribution {
			podDistribution := &template.PodDistribution[i]
			if podDistribution.Type == PodDistributionMaxNumberPerNode {
				maxNumberOfPodsPerNode = podDistribution.Number
			}
		}
	})

	//          1perNode   2perNode  3perNode  4perNode  5perNode
	// sh1r1    n1   a     n1  a     n1 a      n1  a     n1  a
	// sh1r2    n2   a     n2  a     n2 a      n2  a     n2  a
	// sh1r3    n3   a     n3  a     n3 a      n3  a     n3  a
	// sh2r1    n4   a     n4  a     n4 a      n4  a     n1  b
	// sh2r2    n5   a     n5  a     n5 a      n1  b     n2  b
	// sh2r3    n6   a     n6  a     n1 b      n2  b     n3  b
	// sh3r1    n7   a     n7  a     n2 b      n3  b     n1  c
	// sh3r2    n8   a     n8  a     n3 b      n4  b     n2  c
	// sh3r3    n9   a     n1  b     n4 b      n1  c     n3  c
	// sh4r1    n10  a     n2  b     n5 b      n2  c     n1  d
	// sh4r2    n11  a     n3  b     n1 c      n3  c     n2  d
	// sh4r3    n12  a     n4  b     n2 c      n4  c     n3  d
	// sh5r1    n13  a     n5  b     n3 c      n1  d     n1  e
	// sh5r2    n14  a     n6  b     n4 c      n2  d     n2  e
	// sh5r3    n15  a     n7  b     n5 c      n3  d     n3  e
	// 1perNode = ceil(15 / 1 'cycles num') = 15 'cycle len'
	// 2perNode = ceil(15 / 2 'cycles num') = 8  'cycle len'
	// 3perNode = ceil(15 / 3 'cycles num') = 5  'cycle len'
	// 4perNode = ceil(15 / 4 'cycles num') = 4  'cycle len'
	// 5perNode = ceil(15 / 5 'cycles num') = 3  'cycle len'

	// Number of requested cycles equals to max number of ClickHouses per node, but can't be less than 1
	requestedClusterScopeCyclesNum := maxNumberOfPodsPerNode
	if requestedClusterScopeCyclesNum <= 0 {
		requestedClusterScopeCyclesNum = 1
	}

	chiScopeCycleSize := 0 // Unlimited
	clusterScopeCycleSize := 0
	if requestedClusterScopeCyclesNum == 1 {
		// One cycle only requested
		clusterScopeCycleSize = 0 // Unlimited
	} else {
		clusterScopeCycleSize = int(math.Ceil(float64(chi.HostsCount()) / float64(requestedClusterScopeCyclesNum)))
	}

	chi.WalkHostsFullPath(chiScopeCycleSize, clusterScopeCycleSize, func(
		chi *ClickHouseInstallation,

		chiScopeIndex int,
		chiScopeCycleSize int,
		chiScopeCycleIndex int,
		chiScopeCycleOffset int,

		clusterScopeIndex int,
		clusterScopeCycleSize int,
		clusterScopeCycleIndex int,
		clusterScopeCycleOffset int,

		clusterIndex int,
		cluster *ChiCluster,

		shardIndex int,
		shard *ChiShard,

		replicaIndex int,
		replica *ChiReplica,

		host *ChiHost,
	) error {
		cluster.Address.Namespace = chi.Namespace
		cluster.Address.CHIName = chi.Name
		cluster.Address.ClusterName = cluster.Name
		cluster.Address.ClusterIndex = clusterIndex

		shard.Address.Namespace = chi.Namespace
		shard.Address.CHIName = chi.Name
		shard.Address.ClusterName = cluster.Name
		shard.Address.ClusterIndex = clusterIndex
		shard.Address.ShardName = shard.Name
		shard.Address.ShardIndex = shardIndex

		replica.Address.Namespace = chi.Namespace
		replica.Address.CHIName = chi.Name
		replica.Address.ClusterName = cluster.Name
		replica.Address.ClusterIndex = clusterIndex
		replica.Address.ReplicaName = replica.Name
		replica.Address.ReplicaIndex = replicaIndex

		host.Address.Namespace = chi.Namespace
		host.Address.CHIName = chi.Name
		host.Address.ClusterName = cluster.Name
		host.Address.ClusterIndex = clusterIndex
		host.Address.ShardName = shard.Name
		host.Address.ShardIndex = shardIndex
		host.Address.ReplicaName = replica.Name
		host.Address.ReplicaIndex = replicaIndex
		host.Address.HostName = host.Name
		host.Address.CHIScopeIndex = chiScopeIndex
		host.Address.CHIScopeCycleSize = chiScopeCycleSize
		host.Address.CHIScopeCycleIndex = chiScopeCycleIndex
		host.Address.CHIScopeCycleOffset = chiScopeCycleOffset
		host.Address.ClusterScopeIndex = clusterScopeIndex
		host.Address.ClusterScopeCycleSize = clusterScopeCycleSize
		host.Address.ClusterScopeCycleIndex = clusterScopeCycleIndex
		host.Address.ClusterScopeCycleOffset = clusterScopeCycleOffset
		host.Address.ShardScopeIndex = replicaIndex
		host.Address.ReplicaScopeIndex = shardIndex

		return nil
	})
}

func (chi *ClickHouseInstallation) FillCHIPointer() {
	chi.WalkHostsFullPath(0, 0, func(
		chi *ClickHouseInstallation,

		chiScopeIndex int,
		chiScopeCycleSize int,
		chiScopeCycleIndex int,
		chiScopeCycleOffset int,

		clusterScopeIndex int,
		clusterScopeCycleSize int,
		clusterScopeCycleIndex int,
		clusterScopeCycleOffset int,

		clusterIndex int,
		cluster *ChiCluster,

		shardIndex int,
		shard *ChiShard,

		replicaIndex int,
		replica *ChiReplica,

		host *ChiHost,
	) error {
		cluster.CHI = chi
		shard.CHI = chi
		replica.CHI = chi
		host.CHI = chi
		return nil
	})
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
	chiScopeCycleSize int,
	clusterScopeCycleSize int,
	f func(
		chi *ClickHouseInstallation,

		chiScopeIndex int,
		chiScopeCycleSize int,
		chiScopeCycleIndex int,
		chiScopeCycleOffset int,

		clusterScopeIndex int,
		clusterScopeCycleSize int,
		clusterScopeCycleIndex int,
		clusterScopeCycleOffset int,

		clusterIndex int,
		cluster *ChiCluster,

		shardIndex int,
		shard *ChiShard,

		replicaIndex int,
		replica *ChiReplica,

		host *ChiHost,
	) error,
) []error {

	res := make([]error, 0)

	chiScopeIndex := 0
	chiScopeCycleIndex := 0
	chiScopeCycleOffset := 0

	clusterScopeIndex := 0
	clusterScopeCycleIndex := 0
	clusterScopeCycleOffset := 0

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := &chi.Spec.Configuration.Clusters[clusterIndex]

		clusterScopeIndex = 0
		clusterScopeCycleIndex = 0
		clusterScopeCycleOffset = 0

		for shardIndex := range cluster.Layout.Shards {
			shard := cluster.GetShard(shardIndex)
			for replicaIndex, host := range shard.Hosts {
				replica := cluster.GetReplica(replicaIndex)

				res = append(res, f(
					chi,

					chiScopeIndex,
					chiScopeCycleSize,
					chiScopeCycleIndex,
					chiScopeCycleOffset,

					clusterScopeIndex,
					clusterScopeCycleSize,
					clusterScopeCycleIndex,
					clusterScopeCycleOffset,

					clusterIndex,
					cluster,

					shardIndex,
					shard,

					replicaIndex,
					replica,

					host,
				))

				// CHI-scope counters
				chiScopeIndex++
				chiScopeCycleOffset++
				if (chiScopeCycleSize > 0) && (chiScopeCycleOffset >= chiScopeCycleSize) {
					chiScopeCycleOffset = 0
					chiScopeCycleIndex++
				}

				// Cluster-scope counters
				clusterScopeIndex++
				clusterScopeCycleOffset++
				if (clusterScopeCycleSize > 0) && (clusterScopeCycleOffset >= clusterScopeCycleSize) {
					clusterScopeCycleOffset = 0
					clusterScopeCycleIndex++
				}
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
			for replicaIndex := range shard.Hosts {
				host := shard.Hosts[replicaIndex]
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
			for replicaIndex := range shard.Hosts {
				host := shard.Hosts[replicaIndex]
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
			for replicaIndex := range shard.Hosts {
				host := shard.Hosts[replicaIndex]
				if err := fHost(host); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (chi *ClickHouseInstallation) MergeFrom(from *ClickHouseInstallation, _type MergeType) {
	if from == nil {
		return
	}

	// Copy metadata for now
	chi.TypeMeta = from.TypeMeta
	chi.ObjectMeta = from.ObjectMeta

	// Do actual merge for Spec
	(&chi.Spec).MergeFrom(&from.Spec, _type)

	// Copy Status for now
	chi.Status = from.Status
}

func (spec *ChiSpec) MergeFrom(from *ChiSpec, _type MergeType) {
	if from == nil {
		return
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if spec.Stop == "" {
			spec.Stop = from.Stop
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.Stop != "" {
			// Override by non-empty values only
			spec.Stop = from.Stop
		}
	}

	(&spec.Defaults).MergeFrom(&from.Defaults, _type)
	(&spec.Configuration).MergeFrom(&from.Configuration, _type)
	(&spec.Templates).MergeFrom(&from.Templates, _type)
	// TODO may be it would be wiser to make more intelligent merge
	spec.UseTemplates = append(spec.UseTemplates, from.UseTemplates...)
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

func (chi *ClickHouseInstallation) ShardsCount() int {
	count := 0
	chi.WalkShards(func(shard *ChiShard) error {
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

// GetHostTemplate gets ChiHostTemplate by name
func (chi *ClickHouseInstallation) GetHostTemplate(name string) (*ChiHostTemplate, bool) {
	if chi.Spec.Templates.HostTemplatesIndex == nil {
		return nil, false
	} else {
		template, ok := chi.Spec.Templates.HostTemplatesIndex[name]
		return template, ok
	}
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

// WalkPodTemplates walks over all PodTemplates
func (chi *ClickHouseInstallation) WalkPodTemplates(f func(template *ChiPodTemplate)) {
	if chi.Spec.Templates.PodTemplatesIndex == nil {
		return
	}

	for name := range chi.Spec.Templates.PodTemplatesIndex {
		template, _ := chi.Spec.Templates.PodTemplatesIndex[name]
		f(template)
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

// GetServiceTemplate gets ChiServiceTemplate of a CHI
func (chi *ClickHouseInstallation) GetCHIServiceTemplate() (*ChiServiceTemplate, bool) {
	name := chi.Spec.Defaults.Templates.ServiceTemplate
	template, ok := chi.GetServiceTemplate(name)
	return template, ok
}

func (chi *ClickHouseInstallation) MatchFullName(namespace, name string) bool {
	if chi == nil {
		return false
	}
	return (chi.Namespace == namespace) && (chi.Name == name)
}
