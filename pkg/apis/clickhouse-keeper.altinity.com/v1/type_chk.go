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
	"github.com/imdario/mergo"

	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (chk *ClickHouseKeeperInstallation) GetSpec() *ChkSpec {
	return &chk.Spec
}

func (chk *ClickHouseKeeperInstallation) GetSpecA() any {
	return &chk.Spec
}

func (chk *ClickHouseKeeperInstallation) GetRuntime() apiChi.ICustomResourceRuntime {
	return chk.ensureRuntime()
}

func (chk *ClickHouseKeeperInstallation) ensureRuntime() *ClickHouseKeeperInstallationRuntime {
	if chk == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if chk.runtime != nil {
		return chk.runtime
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	chk.runtimeCreatorMutex.Lock()
	defer chk.runtimeCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if chk.runtime == nil {
		chk.runtime = newClickHouseKeeperInstallationRuntime()
	}
	return chk.runtime
}

func (chk *ClickHouseKeeperInstallation) IEnsureStatus() apiChi.IStatus {
	return any(chk.EnsureStatus()).(apiChi.IStatus)
}

// EnsureStatus ensures status
func (chk *ClickHouseKeeperInstallation) EnsureStatus() *ChkStatus {
	if chk == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if chk.Status != nil {
		return chk.Status
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	chk.statusCreatorMutex.Lock()
	defer chk.statusCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if chk.Status == nil {
		chk.Status = &ChkStatus{}
	}
	return chk.Status
}

// GetStatus gets Status
func (chk *ClickHouseKeeperInstallation) GetStatus() *ChkStatus {
	if chk == nil {
		return nil
	}
	return chk.Status
}

// HasStatus checks whether CHI has Status
func (chk *ClickHouseKeeperInstallation) HasStatus() bool {
	if chk == nil {
		return false
	}
	return chk.Status != nil
}

// HasAncestor checks whether CHI has an ancestor
func (chk *ClickHouseKeeperInstallation) HasAncestor() bool {
	if !chk.HasStatus() {
		return false
	}
	return chk.Status.HasNormalizedCHKCompleted()
}

// GetAncestor gets ancestor of a CHI
func (chk *ClickHouseKeeperInstallation) GetAncestor() *ClickHouseKeeperInstallation {
	if !chk.HasAncestor() {
		return nil
	}
	return chk.Status.GetNormalizedCHKCompleted()
}

// SetAncestor sets ancestor of a CHI
func (chk *ClickHouseKeeperInstallation) SetAncestor(a *ClickHouseKeeperInstallation) {
	if chk == nil {
		return
	}
	chk.EnsureStatus().NormalizedCHKCompleted = a
}

// HasTarget checks whether CHI has a target
func (chk *ClickHouseKeeperInstallation) HasTarget() bool {
	if !chk.HasStatus() {
		return false
	}
	return chk.Status.HasNormalizedCHK()
}

// GetTarget gets target of a CHI
func (chk *ClickHouseKeeperInstallation) GetTarget() *ClickHouseKeeperInstallation {
	if !chk.HasTarget() {
		return nil
	}
	return chk.Status.GetNormalizedCHK()
}

// SetTarget sets target of a CHI
func (chk *ClickHouseKeeperInstallation) SetTarget(a *ClickHouseKeeperInstallation) {
	if chk == nil {
		return
	}
	chk.EnsureStatus().NormalizedCHK = a
}

func (chk *ClickHouseKeeperInstallation) Fill() {
	apiChi.FillCR(chk)
}

// MergeFrom merges from CHI
func (chk *ClickHouseKeeperInstallation) MergeFrom(from *ClickHouseKeeperInstallation, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	// Merge Meta
	switch _type {
	case apiChi.MergeTypeFillEmptyValues:
		_ = mergo.Merge(&chk.TypeMeta, from.TypeMeta)
		_ = mergo.Merge(&chk.ObjectMeta, from.ObjectMeta)
	case apiChi.MergeTypeOverrideByNonEmptyValues:
		_ = mergo.Merge(&chk.TypeMeta, from.TypeMeta, mergo.WithOverride)
		_ = mergo.Merge(&chk.ObjectMeta, from.ObjectMeta, mergo.WithOverride)
	}
	// Exclude skipped annotations
	chk.SetAnnotations(
		util.CopyMapFilter(
			chk.GetAnnotations(),
			nil,
			util.ListSkippedAnnotations(),
		),
	)

	// Do actual merge for Spec
	chk.GetSpec().MergeFrom(from.GetSpec(), _type)

	// Copy service attributes
	//chk.ensureRuntime().attributes = from.ensureRuntime().attributes

	chk.EnsureStatus().CopyFrom(from.Status, apiChi.CopyStatusOptions{
		InheritableFields: true,
	})
}

// HostsCount counts hosts
func (chk *ClickHouseKeeperInstallation) HostsCount() int {
	count := 0
	chk.WalkHosts(func(host *apiChi.Host) error {
		count++
		return nil
	})
	return count
}

// GetHostTemplate gets HostTemplate by name
func (chk *ClickHouseKeeperInstallation) GetHostTemplate(name string) (*apiChi.HostTemplate, bool) {
	if !chk.GetSpec().GetTemplates().GetHostTemplatesIndex().Has(name) {
		return nil, false
	}
	return chk.GetSpec().GetTemplates().GetHostTemplatesIndex().Get(name), true
}

// GetPodTemplate gets PodTemplate by name
func (chk *ClickHouseKeeperInstallation) GetPodTemplate(name string) (*apiChi.PodTemplate, bool) {
	if !chk.GetSpec().GetTemplates().GetPodTemplatesIndex().Has(name) {
		return nil, false
	}
	return chk.GetSpec().GetTemplates().GetPodTemplatesIndex().Get(name), true
}

// WalkPodTemplates walks over all PodTemplates
func (chk *ClickHouseKeeperInstallation) WalkPodTemplates(f func(template *apiChi.PodTemplate)) {
	chk.GetSpec().GetTemplates().GetPodTemplatesIndex().Walk(f)
}

// GetVolumeClaimTemplate gets VolumeClaimTemplate by name
func (chk *ClickHouseKeeperInstallation) GetVolumeClaimTemplate(name string) (*apiChi.VolumeClaimTemplate, bool) {
	if chk.GetSpec().GetTemplates().GetVolumeClaimTemplatesIndex().Has(name) {
		return chk.GetSpec().GetTemplates().GetVolumeClaimTemplatesIndex().Get(name), true
	}
	return nil, false
}

// WalkVolumeClaimTemplates walks over all VolumeClaimTemplates
func (chk *ClickHouseKeeperInstallation) WalkVolumeClaimTemplates(f func(template *apiChi.VolumeClaimTemplate)) {
	if chk == nil {
		return
	}
	chk.GetSpec().GetTemplates().GetVolumeClaimTemplatesIndex().Walk(f)
}

// GetServiceTemplate gets ServiceTemplate by name
func (chk *ClickHouseKeeperInstallation) GetServiceTemplate(name string) (*apiChi.ServiceTemplate, bool) {
	if !chk.GetSpec().GetTemplates().GetServiceTemplatesIndex().Has(name) {
		return nil, false
	}
	return chk.GetSpec().GetTemplates().GetServiceTemplatesIndex().Get(name), true
}

// GetRootServiceTemplate gets ServiceTemplate of a CHI
func (chk *ClickHouseKeeperInstallation) GetRootServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	if !chk.GetSpec().Defaults.Templates.HasServiceTemplate() {
		return nil, false
	}
	name := chk.GetSpec().Defaults.Templates.GetServiceTemplate()
	return chk.GetServiceTemplate(name)
}

func (chk *ClickHouseKeeperInstallation) GetName() string {
	if chk == nil {
		return ""
	}
	return chk.Name
}

func (chk *ClickHouseKeeperInstallation) GetNamespace() string {
	if chk == nil {
		return ""
	}
	return chk.Namespace
}

func (chk *ClickHouseKeeperInstallation) GetLabels() map[string]string {
	if chk == nil {
		return nil
	}
	return chk.Labels
}

func (chk *ClickHouseKeeperInstallation) GetAnnotations() map[string]string {
	if chk == nil {
		return nil
	}
	return chk.Annotations
}

// WalkHostsFullPath walks hosts with a function
func (chk *ClickHouseKeeperInstallation) WalkHostsFullPath(f apiChi.WalkHostsAddressFn) []error {
	return chk.WalkHostsFullPathAndScope(0, 0, f)
}

// WalkHostsFullPathAndScope walks hosts with full path
func (chk *ClickHouseKeeperInstallation) WalkHostsFullPathAndScope(
	crScopeCycleSize int,
	clusterScopeCycleSize int,
	f apiChi.WalkHostsAddressFn,
) (res []error) {
	if chk == nil {
		return nil
	}
	address := types.NewHostScopeAddress(crScopeCycleSize, clusterScopeCycleSize)
	for clusterIndex := range chk.GetSpec().Configuration.Clusters {
		cluster := chk.GetSpec().Configuration.Clusters[clusterIndex]
		address.ClusterScopeAddress.Init()
		for shardIndex := range cluster.Layout.Shards {
			shard := cluster.GetShard(shardIndex)
			for replicaIndex, host := range shard.Hosts {
				replica := cluster.GetReplica(replicaIndex)
				address.ClusterIndex = clusterIndex
				address.ShardIndex = shardIndex
				address.ReplicaIndex = replicaIndex
				res = append(res, f(chk, cluster, shard, replica, host, address))
				address.CRScopeAddress.Inc()
				address.ClusterScopeAddress.Inc()
			}
		}
	}
	return res
}

// WalkClusters walks clusters
func (chk *ClickHouseKeeperInstallation) WalkClusters(f func(i apiChi.ICluster) error) []error {
	if chk == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range chk.GetSpec().Configuration.Clusters {
		res = append(res, f(chk.GetSpec().Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkHosts walks hosts with a function
func (chk *ClickHouseKeeperInstallation) WalkHosts(f func(host *apiChi.Host) error) []error {
	if chk == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range chk.GetSpec().Configuration.Clusters {
		cluster := chk.GetSpec().Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := cluster.Layout.Shards[shardIndex]
			for replicaIndex := range shard.Hosts {
				host := shard.Hosts[replicaIndex]
				res = append(res, f(host))
			}
		}
	}

	return res
}
