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
	"encoding/json"
	"fmt"
	"math"

	"github.com/imdario/mergo"
	"gopkg.in/yaml.v3"

	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type IRoot interface {
	GetName() string
	WalkHosts(f func(host *Host) error) []error
}

func (chi *ClickHouseInstallation) GetSpec() *ChiSpec {
	return &chi.Spec
}

func (chi *ClickHouseInstallation) GetUsedTemplates() []*TemplateRef {
	return chi.GetSpec().UseTemplates
}

// FillStatus fills .Status
func (chi *ClickHouseInstallation) FillStatus(endpoint string, pods, fqdns []string, ip string) {
	chi.EnsureStatus().Fill(&FillStatusParams{
		CHOpIP:              ip,
		ClustersCount:       chi.ClustersCount(),
		ShardsCount:         chi.ShardsCount(),
		HostsCount:          chi.HostsCount(),
		TaskID:              chi.GetSpec().GetTaskID(),
		HostsUpdatedCount:   0,
		HostsAddedCount:     0,
		HostsUnchangedCount: 0,
		HostsCompletedCount: 0,
		HostsDeleteCount:    0,
		HostsDeletedCount:   0,
		Pods:                pods,
		FQDNs:               fqdns,
		Endpoint:            endpoint,
		NormalizedCHI: chi.Copy(CopyCHIOptions{
			SkipStatus:        true,
			SkipManagedFields: true,
		}),
	})
}

// FillSelfCalculatedAddressInfo calculates and fills address info
func (chi *ClickHouseInstallation) FillSelfCalculatedAddressInfo() {
	// What is the max number of Pods allowed per Node
	// TODO need to support multi-cluster
	maxNumberOfPodsPerNode := 0
	chi.WalkPodTemplates(func(template *PodTemplate) {
		for i := range template.PodDistribution {
			podDistribution := &template.PodDistribution[i]
			if podDistribution.Type == deployment.PodDistributionMaxNumberPerNode {
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

	chi.WalkHostsFullPathAndScope(
		chiScopeCycleSize,
		clusterScopeCycleSize,
		func(
			chi *ClickHouseInstallation,
			cluster *Cluster,
			shard *ChiShard,
			replica *ChiReplica,
			host *Host,
			address *HostScopeAddress,
		) error {
			cluster.Runtime.Address.Namespace = chi.Namespace
			cluster.Runtime.Address.CHIName = chi.Name
			cluster.Runtime.Address.ClusterName = cluster.Name
			cluster.Runtime.Address.ClusterIndex = address.ClusterIndex

			shard.Runtime.Address.Namespace = chi.Namespace
			shard.Runtime.Address.CHIName = chi.Name
			shard.Runtime.Address.ClusterName = cluster.Name
			shard.Runtime.Address.ClusterIndex = address.ClusterIndex
			shard.Runtime.Address.ShardName = shard.Name
			shard.Runtime.Address.ShardIndex = address.ShardIndex

			replica.Runtime.Address.Namespace = chi.Namespace
			replica.Runtime.Address.CHIName = chi.Name
			replica.Runtime.Address.ClusterName = cluster.Name
			replica.Runtime.Address.ClusterIndex = address.ClusterIndex
			replica.Runtime.Address.ReplicaName = replica.Name
			replica.Runtime.Address.ReplicaIndex = address.ReplicaIndex

			host.Runtime.Address.Namespace = chi.Namespace
			// Skip StatefulSet as impossible to self-calculate
			// host.Address.StatefulSet = CreateStatefulSetName(host)
			host.Runtime.Address.CHIName = chi.Name
			host.Runtime.Address.ClusterName = cluster.Name
			host.Runtime.Address.ClusterIndex = address.ClusterIndex
			host.Runtime.Address.ShardName = shard.Name
			host.Runtime.Address.ShardIndex = address.ShardIndex
			host.Runtime.Address.ReplicaName = replica.Name
			host.Runtime.Address.ReplicaIndex = address.ReplicaIndex
			host.Runtime.Address.HostName = host.Name
			host.Runtime.Address.CHIScopeIndex = address.CHIScopeAddress.Index
			host.Runtime.Address.CHIScopeCycleSize = address.CHIScopeAddress.CycleSpec.Size
			host.Runtime.Address.CHIScopeCycleIndex = address.CHIScopeAddress.CycleAddress.CycleIndex
			host.Runtime.Address.CHIScopeCycleOffset = address.CHIScopeAddress.CycleAddress.Index
			host.Runtime.Address.ClusterScopeIndex = address.ClusterScopeAddress.Index
			host.Runtime.Address.ClusterScopeCycleSize = address.ClusterScopeAddress.CycleSpec.Size
			host.Runtime.Address.ClusterScopeCycleIndex = address.ClusterScopeAddress.CycleAddress.CycleIndex
			host.Runtime.Address.ClusterScopeCycleOffset = address.ClusterScopeAddress.CycleAddress.Index
			host.Runtime.Address.ShardScopeIndex = address.ReplicaIndex
			host.Runtime.Address.ReplicaScopeIndex = address.ShardIndex

			return nil
		},
	)
}

// FillCHIPointer fills CHI pointer
func (chi *ClickHouseInstallation) FillCHIPointer() {
	chi.WalkHostsFullPath(
		func(
			chi *ClickHouseInstallation,
			cluster *Cluster,
			shard *ChiShard,
			replica *ChiReplica,
			host *Host,
			address *HostScopeAddress,
		) error {
			cluster.Runtime.CHI = chi
			shard.Runtime.CHI = chi
			replica.Runtime.CHI = chi
			host.SetCR(chi)
			return nil
		},
	)
}

func (chi *ClickHouseInstallation) Fill() {
	chi.FillSelfCalculatedAddressInfo()
	chi.FillCHIPointer()
}

// WalkClustersFullPath walks clusters with full path
func (chi *ClickHouseInstallation) WalkClustersFullPath(
	f func(chi *ClickHouseInstallation, clusterIndex int, cluster *Cluster) error,
) []error {
	if chi == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range chi.GetSpec().Configuration.Clusters {
		res = append(res, f(chi, clusterIndex, chi.GetSpec().Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkClusters walks clusters
func (chi *ClickHouseInstallation) WalkClusters(f func(i ICluster) error) []error {
	if chi == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range chi.GetSpec().Configuration.Clusters {
		res = append(res, f(chi.GetSpec().Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkShards walks shards
func (chi *ClickHouseInstallation) WalkShards(
	f func(
		shard *ChiShard,
	) error,
) []error {
	if chi == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range chi.GetSpec().Configuration.Clusters {
		cluster := chi.GetSpec().Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := &cluster.Layout.Shards[shardIndex]
			res = append(res, f(shard))
		}
	}

	return res
}

// WalkHostsFullPathAndScope walks hosts with full path
func (chi *ClickHouseInstallation) WalkHostsFullPathAndScope(
	chiScopeCycleSize int,
	clusterScopeCycleSize int,
	f WalkHostsAddressFn,
) (res []error) {
	if chi == nil {
		return nil
	}
	address := NewHostScopeAddress(chiScopeCycleSize, clusterScopeCycleSize)
	for clusterIndex := range chi.GetSpec().Configuration.Clusters {
		cluster := chi.GetSpec().Configuration.Clusters[clusterIndex]
		address.ClusterScopeAddress.Init()
		for shardIndex := range cluster.Layout.Shards {
			shard := cluster.GetShard(shardIndex)
			for replicaIndex, host := range shard.Hosts {
				replica := cluster.GetReplica(replicaIndex)
				address.ClusterIndex = clusterIndex
				address.ShardIndex = shardIndex
				address.ReplicaIndex = replicaIndex
				res = append(res, f(chi, cluster, shard, replica, host, address))
				address.CHIScopeAddress.Inc()
				address.ClusterScopeAddress.Inc()
			}
		}
	}
	return res
}

// WalkHostsFullPath walks hosts with a function
func (chi *ClickHouseInstallation) WalkHostsFullPath(f WalkHostsAddressFn) []error {
	return chi.WalkHostsFullPathAndScope(0, 0, f)
}

// WalkHosts walks hosts with a function
func (chi *ClickHouseInstallation) WalkHosts(f func(host *Host) error) []error {
	if chi == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range chi.GetSpec().Configuration.Clusters {
		cluster := chi.GetSpec().Configuration.Clusters[clusterIndex]
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

// WalkTillError walks hosts with a function until an error met
func (chi *ClickHouseInstallation) WalkTillError(
	ctx context.Context,
	fCHIPreliminary func(ctx context.Context, chi *ClickHouseInstallation) error,
	fCluster func(ctx context.Context, cluster *Cluster) error,
	fShards func(ctx context.Context, shards []*ChiShard) error,
	fCHIFinal func(ctx context.Context, chi *ClickHouseInstallation) error,
) error {
	if err := fCHIPreliminary(ctx, chi); err != nil {
		return err
	}

	for clusterIndex := range chi.GetSpec().Configuration.Clusters {
		cluster := chi.GetSpec().Configuration.Clusters[clusterIndex]
		if err := fCluster(ctx, cluster); err != nil {
			return err
		}

		shards := make([]*ChiShard, 0, len(cluster.Layout.Shards))
		for shardIndex := range cluster.Layout.Shards {
			shards = append(shards, &cluster.Layout.Shards[shardIndex])
		}
		if err := fShards(ctx, shards); err != nil {
			return err
		}
	}

	if err := fCHIFinal(ctx, chi); err != nil {
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
	chi.GetSpec().MergeFrom(from.GetSpec(), _type)

	// Copy service attributes
	chi.ensureRuntime().attributes = from.ensureRuntime().attributes

	chi.EnsureStatus().CopyFrom(from.Status, CopyCHIStatusOptions{
		InheritableFields: true,
	})
}

// HasTaskID checks whether task id is specified
func (spec *ChiSpec) HasTaskID() bool {
	return len(spec.TaskID.Value()) > 0
}

// GetTaskID gets task id as a string
func (spec *ChiSpec) GetTaskID() string {
	return spec.TaskID.Value()
}

// MergeFrom merges from spec
func (spec *ChiSpec) MergeFrom(from *ChiSpec, _type MergeType) {
	if from == nil {
		return
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if !spec.HasTaskID() {
			spec.TaskID = spec.TaskID.MergeFrom(from.TaskID)
		}
		if !spec.Stop.HasValue() {
			spec.Stop = spec.Stop.MergeFrom(from.Stop)
		}
		if !spec.Restart.HasValue() {
			spec.Restart = spec.Restart.MergeFrom(from.Restart)
		}
		if !spec.Troubleshoot.HasValue() {
			spec.Troubleshoot = spec.Troubleshoot.MergeFrom(from.Troubleshoot)
		}
		if !spec.NamespaceDomainPattern.HasValue() {
			spec.NamespaceDomainPattern = spec.NamespaceDomainPattern.MergeFrom(from.NamespaceDomainPattern)
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.HasTaskID() {
			spec.TaskID = spec.TaskID.MergeFrom(from.TaskID)
		}
		if from.Stop.HasValue() {
			// Override by non-empty values only
			spec.Stop = from.Stop
		}
		if from.Restart.HasValue() {
			// Override by non-empty values only
			spec.Restart = spec.Restart.MergeFrom(from.Restart)
		}
		if from.Troubleshoot.HasValue() {
			// Override by non-empty values only
			spec.Troubleshoot = from.Troubleshoot
		}
		if from.NamespaceDomainPattern.HasValue() {
			spec.NamespaceDomainPattern = spec.NamespaceDomainPattern.MergeFrom(from.NamespaceDomainPattern)
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

// FindCluster finds cluster by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (chi *ClickHouseInstallation) FindCluster(needle interface{}) *Cluster {
	var resultCluster *Cluster
	chi.WalkClustersFullPath(func(chi *ClickHouseInstallation, clusterIndex int, cluster *Cluster) error {
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
// Expectations: name is expected to be a string, index is expected to be an int.
func (chi *ClickHouseInstallation) FindShard(needleCluster interface{}, needleShard interface{}) *ChiShard {
	return chi.FindCluster(needleCluster).FindShard(needleShard)
}

// FindHost finds shard by name or index
// Expectations: name is expected to be a string, index is expected to be an int.
func (chi *ClickHouseInstallation) FindHost(needleCluster interface{}, needleShard interface{}, needleHost interface{}) *Host {
	return chi.FindCluster(needleCluster).FindHost(needleShard, needleHost)
}

// ClustersCount counts clusters
func (chi *ClickHouseInstallation) ClustersCount() int {
	count := 0
	chi.WalkClusters(func(cluster ICluster) error {
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
	chi.WalkHosts(func(host *Host) error {
		count++
		return nil
	})
	return count
}

// HostsCountAttributes counts hosts by attributes
func (chi *ClickHouseInstallation) HostsCountAttributes(a *HostReconcileAttributes) int {
	count := 0
	chi.WalkHosts(func(host *Host) error {
		if host.GetReconcileAttributes().Any(a) {
			count++
		}
		return nil
	})
	return count
}

// GetHostTemplate gets HostTemplate by name
func (chi *ClickHouseInstallation) GetHostTemplate(name string) (*HostTemplate, bool) {
	if !chi.GetSpec().Templates.GetHostTemplatesIndex().Has(name) {
		return nil, false
	}
	return chi.GetSpec().Templates.GetHostTemplatesIndex().Get(name), true
}

// GetPodTemplate gets PodTemplate by name
func (chi *ClickHouseInstallation) GetPodTemplate(name string) (*PodTemplate, bool) {
	if !chi.GetSpec().Templates.GetPodTemplatesIndex().Has(name) {
		return nil, false
	}
	return chi.GetSpec().Templates.GetPodTemplatesIndex().Get(name), true
}

// WalkPodTemplates walks over all PodTemplates
func (chi *ClickHouseInstallation) WalkPodTemplates(f func(template *PodTemplate)) {
	chi.GetSpec().Templates.GetPodTemplatesIndex().Walk(f)
}

// GetVolumeClaimTemplate gets VolumeClaimTemplate by name
func (chi *ClickHouseInstallation) GetVolumeClaimTemplate(name string) (*VolumeClaimTemplate, bool) {
	if chi.GetSpec().Templates.GetVolumeClaimTemplatesIndex().Has(name) {
		return chi.GetSpec().Templates.GetVolumeClaimTemplatesIndex().Get(name), true
	}
	return nil, false
}

// WalkVolumeClaimTemplates walks over all VolumeClaimTemplates
func (chi *ClickHouseInstallation) WalkVolumeClaimTemplates(f func(template *VolumeClaimTemplate)) {
	if chi == nil {
		return
	}
	chi.GetSpec().Templates.GetVolumeClaimTemplatesIndex().Walk(f)
}

// GetServiceTemplate gets ServiceTemplate by name
func (chi *ClickHouseInstallation) GetServiceTemplate(name string) (*ServiceTemplate, bool) {
	if !chi.GetSpec().Templates.GetServiceTemplatesIndex().Has(name) {
		return nil, false
	}
	return chi.GetSpec().Templates.GetServiceTemplatesIndex().Get(name), true
}

// GetRootServiceTemplate gets ServiceTemplate of a CHI
func (chi *ClickHouseInstallation) GetRootServiceTemplate() (*ServiceTemplate, bool) {
	if !chi.GetSpec().Defaults.Templates.HasServiceTemplate() {
		return nil, false
	}
	name := chi.GetSpec().Defaults.Templates.GetServiceTemplate()
	return chi.GetServiceTemplate(name)
}

// MatchNamespace matches namespace
func (chi *ClickHouseInstallation) MatchNamespace(namespace string) bool {
	if chi == nil {
		return false
	}
	return chi.Namespace == namespace
}

// MatchFullName matches full name
func (chi *ClickHouseInstallation) MatchFullName(namespace, name string) bool {
	if chi == nil {
		return false
	}
	return (chi.Namespace == namespace) && (chi.Name == name)
}

// FoundIn checks whether CHI can be found in haystack
func (chi *ClickHouseInstallation) FoundIn(haystack []*ClickHouseInstallation) bool {
	if chi == nil {
		return false
	}

	for _, candidate := range haystack {
		if candidate.MatchFullName(chi.Namespace, chi.Name) {
			return true
		}
	}

	return false
}

// Possible templating policies
const (
	TemplatingPolicyAuto   = "auto"
	TemplatingPolicyManual = "manual"
)

// IsAuto checks whether templating policy is auto
func (chi *ClickHouseInstallation) IsAuto() bool {
	if chi == nil {
		return false
	}
	if (chi.Namespace == "") && (chi.Name == "") {
		return false
	}
	return chi.GetSpec().Templating.GetPolicy() == TemplatingPolicyAuto
}

// IsStopped checks whether CHI is stopped
func (chi *ClickHouseInstallation) IsStopped() bool {
	if chi == nil {
		return false
	}
	return chi.GetSpec().Stop.Value()
}

// Restart constants present available values for .spec.restart
// Controlling the operator's Clickhouse instances restart policy
const (
	// RestartRollingUpdate requires to roll over all hosts in the cluster and shutdown and reconcile each of it.
	// This restart policy means that all hosts in the cluster would pass through shutdown/reconcile cycle.
	RestartRollingUpdate = "RollingUpdate"
)

// IsRollingUpdate checks whether CHI should perform rolling update
func (chi *ClickHouseInstallation) IsRollingUpdate() bool {
	if chi == nil {
		return false
	}
	return chi.GetSpec().Restart.Value() == RestartRollingUpdate
}

// IsTroubleshoot checks whether CHI is in troubleshoot mode
func (chi *ClickHouseInstallation) IsTroubleshoot() bool {
	if chi == nil {
		return false
	}
	return chi.GetSpec().Troubleshoot.Value()
}

// GetReconciling gets reconciling spec
func (chi *ClickHouseInstallation) GetReconciling() *ChiReconciling {
	if chi == nil {
		return nil
	}
	return chi.GetSpec().Reconciling
}

// CopyCHIOptions specifies options for CHI copier
type CopyCHIOptions struct {
	// SkipStatus specifies whether to copy status
	SkipStatus bool
	// SkipManagedFields specifies whether to copy managed fields
	SkipManagedFields bool
}

// Copy makes copy of a CHI, filtering fields according to specified CopyOptions
func (chi *ClickHouseInstallation) Copy(opts CopyCHIOptions) *ClickHouseInstallation {
	if chi == nil {
		return nil
	}
	jsonBytes, err := json.Marshal(chi)
	if err != nil {
		return nil
	}

	var chi2 *ClickHouseInstallation
	if err := json.Unmarshal(jsonBytes, &chi2); err != nil {
		return nil
	}

	if opts.SkipStatus {
		chi2.Status = nil
	}

	if opts.SkipManagedFields {
		chi2.SetManagedFields(nil)
	}

	return chi2
}

// JSON returns JSON string
func (chi *ClickHouseInstallation) JSON(opts CopyCHIOptions) string {
	if chi == nil {
		return ""
	}

	filtered := chi.Copy(opts)
	jsonBytes, err := json.MarshalIndent(filtered, "", "  ")
	if err != nil {
		return fmt.Sprintf("unable to parse. err: %v", err)
	}
	return string(jsonBytes)

}

// YAML return YAML string
func (chi *ClickHouseInstallation) YAML(opts CopyCHIOptions) string {
	if chi == nil {
		return ""
	}

	filtered := chi.Copy(opts)
	yamlBytes, err := yaml.Marshal(filtered)
	if err != nil {
		return fmt.Sprintf("unable to parse. err: %v", err)
	}
	return string(yamlBytes)
}

func (chi *ClickHouseInstallation) GetRuntime() ICustomResourceRuntime {
	return chi.ensureRuntime()
}

func (chi *ClickHouseInstallation) ensureRuntime() *ClickHouseInstallationRuntime {
	if chi == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if chi.runtime != nil {
		return chi.runtime
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	chi.runtimeCreatorMutex.Lock()
	defer chi.runtimeCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if chi.runtime == nil {
		chi.runtime = newClickHouseInstallationRuntime()
	}
	return chi.runtime
}

func (chi *ClickHouseInstallation) IEnsureStatus() IStatus {
	return any(chi.EnsureStatus()).(IStatus)
}

// EnsureStatus ensures status
func (chi *ClickHouseInstallation) EnsureStatus() *ChiStatus {
	if chi == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if chi.Status != nil {
		return chi.Status
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	chi.statusCreatorMutex.Lock()
	defer chi.statusCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if chi.Status == nil {
		chi.Status = &ChiStatus{}
	}
	return chi.Status
}

// GetStatus gets Status
func (chi *ClickHouseInstallation) GetStatus() *ChiStatus {
	if chi == nil {
		return nil
	}
	return chi.Status
}

// HasStatus checks whether CHI has Status
func (chi *ClickHouseInstallation) HasStatus() bool {
	if chi == nil {
		return false
	}
	return chi.Status != nil
}

// HasAncestor checks whether CHI has an ancestor
func (chi *ClickHouseInstallation) HasAncestor() bool {
	if !chi.HasStatus() {
		return false
	}
	return chi.Status.HasNormalizedCHICompleted()
}

// GetAncestor gets ancestor of a CHI
func (chi *ClickHouseInstallation) GetAncestor() *ClickHouseInstallation {
	if !chi.HasAncestor() {
		return nil
	}
	return chi.Status.GetNormalizedCHICompleted()
}

// SetAncestor sets ancestor of a CHI
func (chi *ClickHouseInstallation) SetAncestor(a *ClickHouseInstallation) {
	if chi == nil {
		return
	}
	chi.EnsureStatus().NormalizedCHICompleted = a
}

// HasTarget checks whether CHI has a target
func (chi *ClickHouseInstallation) HasTarget() bool {
	if !chi.HasStatus() {
		return false
	}
	return chi.Status.HasNormalizedCHI()
}

// GetTarget gets target of a CHI
func (chi *ClickHouseInstallation) GetTarget() *ClickHouseInstallation {
	if !chi.HasTarget() {
		return nil
	}
	return chi.Status.GetNormalizedCHI()
}

// SetTarget sets target of a CHI
func (chi *ClickHouseInstallation) SetTarget(a *ClickHouseInstallation) {
	if chi == nil {
		return
	}
	chi.EnsureStatus().NormalizedCHI = a
}

// FirstHost returns first host of the CHI
func (chi *ClickHouseInstallation) FirstHost() *Host {
	var result *Host
	chi.WalkHosts(func(host *Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

func (chi *ClickHouseInstallation) GetName() string {
	if chi == nil {
		return ""
	}
	return chi.Name
}

func (chi *ClickHouseInstallation) GetNamespace() string {
	if chi == nil {
		return ""
	}
	return chi.Namespace
}

func (chi *ClickHouseInstallation) GetLabels() map[string]string {
	if chi == nil {
		return nil
	}
	return chi.Labels
}

func (chi *ClickHouseInstallation) GetAnnotations() map[string]string {
	if chi == nil {
		return nil
	}
	return chi.Annotations
}
