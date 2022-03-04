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
	"context"
	"math"
	"strings"

	"github.com/imdario/mergo"

	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/version"
)

// FillStatus fills .Status
func (chi *ClickHouseInstallation) FillStatus(endpoint string, pods, fqdns []string, normalized bool) {
	chi.Status.CHOpVersion = version.Version
	chi.Status.CHOpCommit = version.GitSHA
	chi.Status.CHOpDate = version.BuiltAt
	chi.Status.ClustersCount = chi.ClustersCount()
	chi.Status.ShardsCount = chi.ShardsCount()
	chi.Status.HostsCount = chi.HostsCount()
	chi.Status.TaskID = *chi.Spec.TaskID
	chi.Status.UpdatedHostsCount = 0
	chi.Status.DeleteHostsCount = 0
	chi.Status.DeletedHostsCount = 0
	chi.Status.Pods = pods
	chi.Status.FQDNs = fqdns
	chi.Status.Endpoint = endpoint
	chi.Status.Generation = 0
	if normalized {
		chi.Status.NormalizedCHI = &ClickHouseInstallation{
			TypeMeta:   chi.TypeMeta,
			ObjectMeta: chi.ObjectMeta,
			Spec:       chi.Spec,
			// Skip status
		}
	} else {
		chi.Status.NormalizedCHI = nil
	}
}

// FillSelfCalculatedAddressInfo calculates and fills address info
func (chi *ClickHouseInstallation) FillSelfCalculatedAddressInfo() {
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
		// Skip StatefulSet as impossible to self-calculate
		// host.Address.StatefulSet = CreateStatefulSetName(host)
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

// FillCHIPointer fills CHI pointer
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

// WalkClustersFullPath walks clusters with full path
func (chi *ClickHouseInstallation) WalkClustersFullPath(
	f func(chi *ClickHouseInstallation, clusterIndex int, cluster *ChiCluster) error,
) []error {
	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		res = append(res, f(chi, clusterIndex, chi.Spec.Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkClusters walks clusters
func (chi *ClickHouseInstallation) WalkClusters(f func(cluster *ChiCluster) error) []error {
	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		res = append(res, f(chi.Spec.Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkShardsFullPath walks shards with full path
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
		cluster := chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			res = append(res, f(chi, clusterIndex, cluster, shardIndex, shard))
		}
	}

	return res
}

// WalkShards walks shards
func (chi *ClickHouseInstallation) WalkShards(
	f func(
		shard *ChiShard,
	) error,
) []error {

	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := chi.Spec.Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			res = append(res, f(shard))
		}
	}

	return res
}

// WalkHostsFullPath walks hosts with full path
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
		cluster := chi.Spec.Configuration.Clusters[clusterIndex]

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

// WalkHosts walks hosts
func (chi *ClickHouseInstallation) WalkHosts(f func(host *ChiHost) error) []error {
	res := make([]error, 0)

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := chi.Spec.Configuration.Clusters[clusterIndex]
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

// WalkHostsTillError walks hosts until an error met
func (chi *ClickHouseInstallation) WalkHostsTillError(f func(host *ChiHost) error) error {
	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := chi.Spec.Configuration.Clusters[clusterIndex]
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

// WalkTillError walks until an error met
func (chi *ClickHouseInstallation) WalkTillError(
	ctx context.Context,
	fCHIPreliminary func(ctx context.Context, chi *ClickHouseInstallation) error,
	fCluster func(ctx context.Context, cluster *ChiCluster) error,
	fShard func(ctx context.Context, shard *ChiShard) error,
	fHost func(ctx context.Context, host *ChiHost) error,
	fCHI func(ctx context.Context, chi *ClickHouseInstallation) error,
) error {

	if err := fCHIPreliminary(ctx, chi); err != nil {
		return err
	}

	for clusterIndex := range chi.Spec.Configuration.Clusters {
		cluster := chi.Spec.Configuration.Clusters[clusterIndex]
		if err := fCluster(ctx, cluster); err != nil {
			return err
		}
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			if err := fShard(ctx, shard); err != nil {
				return err
			}
			for replicaIndex := range shard.Hosts {
				host := shard.Hosts[replicaIndex]
				if err := fHost(ctx, host); err != nil {
					return err
				}
			}
		}
	}

	if err := fCHI(ctx, chi); err != nil {
		return err
	}

	return nil
}

// MergeFrom merges from CHI
func (chi *ClickHouseInstallation) MergeFrom(from *ClickHouseInstallation, _type MergeType) {
	if from == nil {
		return
	}

	// Merge Meta
	switch _type {
	case MergeTypeFillEmptyValues:
		_ = mergo.Merge(&chi.TypeMeta, from.TypeMeta)
		_ = mergo.Merge(&chi.ObjectMeta, from.ObjectMeta)
	case MergeTypeOverrideByNonEmptyValues:
		_ = mergo.Merge(&chi.TypeMeta, from.TypeMeta, mergo.WithOverride)
		_ = mergo.Merge(&chi.ObjectMeta, from.ObjectMeta, mergo.WithOverride)
	}
	// Exclude skipped annotations
	chi.Annotations = util.CopyMapFilter(
		chi.Annotations,
		nil,
		util.ListSkippedAnnotations(),
	)

	// Do actual merge for Spec
	(&chi.Spec).MergeFrom(&from.Spec, _type)

	// Copy Status for now
	chi.Status = from.Status

	// Copy service attributes
	chi.Attributes = from.Attributes
}

// MergeFrom merges from spec
func (spec *ChiSpec) MergeFrom(from *ChiSpec, _type MergeType) {
	if from == nil {
		return
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if spec.Stop == "" {
			spec.Stop = from.Stop
		}
		if spec.Restart == "" {
			spec.Restart = from.Restart
		}
		if spec.Troubleshoot == "" {
			spec.Troubleshoot = from.Troubleshoot
		}
		if spec.NamespaceDomainPattern == "" {
			spec.NamespaceDomainPattern = from.NamespaceDomainPattern
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.Stop != "" {
			// Override by non-empty values only
			spec.Stop = from.Stop
		}
		if from.Restart != "" {
			// Override by non-empty values only
			spec.Restart = from.Restart
		}
		if from.Troubleshoot != "" {
			// Override by non-empty values only
			spec.Troubleshoot = from.Troubleshoot
		}
		if from.NamespaceDomainPattern != "" {
			spec.NamespaceDomainPattern = from.NamespaceDomainPattern
		}
	}

	spec.Templating = spec.Templating.MergeFrom(from.Templating, _type)
	spec.Reconciling = spec.Reconciling.MergeFrom(from.Reconciling, _type)
	spec.Defaults = spec.Defaults.MergeFrom(from.Defaults, _type)
	spec.Configuration = spec.Configuration.MergeFrom(from.Configuration, _type)
	spec.Templates = spec.Templates.MergeFrom(from.Templates, _type)
	// TODO may be it would be wiser to make more intelligent merge
	spec.UseTemplates = append(spec.UseTemplates, from.UseTemplates...)
}

// FindCluster finds cluster by name or index
func (chi *ClickHouseInstallation) FindCluster(needle interface{}) *ChiCluster {
	var resultCluster *ChiCluster
	chi.WalkClustersFullPath(func(chi *ClickHouseInstallation, clusterIndex int, cluster *ChiCluster) error {
		switch v := needle.(type) {
		case string:
			if cluster.Name == v {
				resultCluster = cluster
			}
		case int:
			if clusterIndex == v {
				resultCluster = cluster
			}
		}
		return nil
	})
	return resultCluster
}

// FindShard finds shard by name or index
func (chi *ClickHouseInstallation) FindShard(needleCluster interface{}, needleShard interface{}) *ChiShard {
	if cluster := chi.FindCluster(needleCluster); cluster != nil {
		return cluster.FindShard(needleShard)
	}
	return nil
}

// ClustersCount counts clusters
func (chi *ClickHouseInstallation) ClustersCount() int {
	count := 0
	chi.WalkClusters(func(cluster *ChiCluster) error {
		count++
		return nil
	})
	return count
}

// ShardsCount counts shards
func (chi *ClickHouseInstallation) ShardsCount() int {
	count := 0
	chi.WalkShards(func(shard *ChiShard) error {
		count++
		return nil
	})
	return count
}

// HostsCount counts hosts
func (chi *ClickHouseInstallation) HostsCount() int {
	count := 0
	chi.WalkHosts(func(host *ChiHost) error {
		count++
		return nil
	})
	return count
}

// HostsCountAttributes counts hosts by attributes
func (chi *ClickHouseInstallation) HostsCountAttributes(a ChiHostReconcileAttributes) int {
	count := 0
	chi.WalkHosts(func(host *ChiHost) error {
		if host.ReconcileAttributes.Any(a) {
			count++
		}
		return nil
	})
	return count
}

// GetHostTemplate gets ChiHostTemplate by name
func (chi *ClickHouseInstallation) GetHostTemplate(name string) (*ChiHostTemplate, bool) {
	if !chi.Spec.Templates.GetHostTemplatesIndex().Has(name) {
		return nil, false
	}
	return chi.Spec.Templates.GetHostTemplatesIndex().Get(name), true
}

// GetPodTemplate gets ChiPodTemplate by name
func (chi *ClickHouseInstallation) GetPodTemplate(name string) (*ChiPodTemplate, bool) {
	if !chi.Spec.Templates.GetPodTemplatesIndex().Has(name) {
		return nil, false
	}
	return chi.Spec.Templates.GetPodTemplatesIndex().Get(name), true
}

// WalkPodTemplates walks over all PodTemplates
func (chi *ClickHouseInstallation) WalkPodTemplates(f func(template *ChiPodTemplate)) {
	chi.Spec.Templates.GetPodTemplatesIndex().Walk(f)
}

// GetVolumeClaimTemplate gets ChiVolumeClaimTemplate by name
func (chi *ClickHouseInstallation) GetVolumeClaimTemplate(name string) (*ChiVolumeClaimTemplate, bool) {
	if chi.Spec.Templates.GetVolumeClaimTemplatesIndex().Has(name) {
		return chi.Spec.Templates.GetVolumeClaimTemplatesIndex().Get(name), true
	}
	return nil, false
}

// WalkVolumeClaimTemplates walks over all VolumeClaimTemplates
func (chi *ClickHouseInstallation) WalkVolumeClaimTemplates(f func(template *ChiVolumeClaimTemplate)) {
	chi.Spec.Templates.GetVolumeClaimTemplatesIndex().Walk(f)
}

// GetServiceTemplate gets ChiServiceTemplate by name
func (chi *ClickHouseInstallation) GetServiceTemplate(name string) (*ChiServiceTemplate, bool) {
	if !chi.Spec.Templates.GetServiceTemplatesIndex().Has(name) {
		return nil, false
	}
	return chi.Spec.Templates.GetServiceTemplatesIndex().Get(name), true
}

// GetCHIServiceTemplate gets ChiServiceTemplate of a CHI
func (chi *ClickHouseInstallation) GetCHIServiceTemplate() (*ChiServiceTemplate, bool) {
	if !chi.Spec.Defaults.Templates.HasServiceTemplate() {
		return nil, false
	}
	name := chi.Spec.Defaults.Templates.GetServiceTemplate()
	return chi.GetServiceTemplate(name)
}

// MatchFullName matches full name
func (chi *ClickHouseInstallation) MatchFullName(namespace, name string) bool {
	if chi == nil {
		return false
	}
	return (chi.Namespace == namespace) && (chi.Name == name)
}

// Possible templating policies
const (
	TemplatingPolicyManual = "manual"
	TemplatingPolicyAuto   = "auto"
)

// IsAuto checks whether templating policy is auto
func (chi *ClickHouseInstallation) IsAuto() bool {
	if chi == nil {
		return false
	}
	if (chi.Namespace == "") && (chi.Name == "") {
		return false
	}
	return strings.ToLower(chi.Spec.Templating.GetPolicy()) == TemplatingPolicyAuto
}

// IsStopped checks whether CHI is stopped
func (chi *ClickHouseInstallation) IsStopped() bool {
	return util.IsStringBoolTrue(chi.Spec.Stop)
}

// Restart const presents possible values for .spec.restart
const (
	RestartAll = "Restart"
	// RestartRollingUpdate requires to roll over all hosts in the cluster and shutdown and reconcile each of it.
	// This restart policy means that all hosts in the cluster would pass through shutdown/reconcile cycle.
	RestartRollingUpdate = "RollingUpdate"
)

// IsRollingUpdate checks whether CHI should perform rolling update
func (chi *ClickHouseInstallation) IsRollingUpdate() bool {
	return chi.Spec.Restart == RestartRollingUpdate
}

// IsNoRestartSpecified checks whether CHI has no restart request
func (chi *ClickHouseInstallation) IsNoRestartSpecified() bool {
	return chi.Spec.Restart == ""
}

// IsTroubleshoot checks whether CHI is in troubleshoot mode
func (chi *ClickHouseInstallation) IsTroubleshoot() bool {
	return util.IsStringBoolTrue(chi.Spec.Troubleshoot)
}

// GetReconciling gets reconciling spec
func (chi *ClickHouseInstallation) GetReconciling() *ChiReconciling {
	if chi == nil {
		return nil
	}
	return chi.Spec.Reconciling
}
