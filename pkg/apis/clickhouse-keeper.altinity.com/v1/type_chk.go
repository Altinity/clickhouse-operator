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

	"github.com/imdario/mergo"
	"gopkg.in/yaml.v3"

	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (cr *ClickHouseKeeperInstallation) IsNonZero() bool {
	return cr != nil
}

func (cr *ClickHouseKeeperInstallation) GetSpec() apiChi.ICRSpec {
	return &cr.Spec
}

func (cr *ClickHouseKeeperInstallation) GetSpecT() *ChkSpec {
	return &cr.Spec
}

func (cr *ClickHouseKeeperInstallation) GetSpecA() any {
	return &cr.Spec
}

func (cr *ClickHouseKeeperInstallation) GetRuntime() apiChi.ICustomResourceRuntime {
	return cr.ensureRuntime()
}

func (cr *ClickHouseKeeperInstallation) ensureRuntime() *ClickHouseKeeperInstallationRuntime {
	if cr == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if cr.runtime != nil {
		return cr.runtime
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	cr.runtimeCreatorMutex.Lock()
	defer cr.runtimeCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if cr.runtime == nil {
		cr.runtime = newClickHouseKeeperInstallationRuntime()
	}
	return cr.runtime
}

func (cr *ClickHouseKeeperInstallation) IEnsureStatus() apiChi.IStatus {
	return any(cr.EnsureStatus()).(apiChi.IStatus)
}

// EnsureStatus ensures status
func (cr *ClickHouseKeeperInstallation) EnsureStatus() *Status {
	if cr == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if cr.Status != nil {
		return cr.Status
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	cr.statusCreatorMutex.Lock()
	defer cr.statusCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if cr.Status == nil {
		cr.Status = &Status{}
	}
	return cr.Status
}

// GetStatus gets Status
func (cr *ClickHouseKeeperInstallation) GetStatus() apiChi.IStatus {
	if cr == nil {
		return (*Status)(nil)
	}
	return cr.Status
}

// HasStatus checks whether CHI has Status
func (cr *ClickHouseKeeperInstallation) HasStatus() bool {
	if cr == nil {
		return false
	}
	return cr.Status != nil
}

// HasAncestor checks whether CR has an ancestor
func (cr *ClickHouseKeeperInstallation) HasAncestor() bool {
	if !cr.HasStatus() {
		return false
	}
	return cr.Status.HasNormalizedCRCompleted()
}

// GetAncestor gets ancestor of a CR
func (cr *ClickHouseKeeperInstallation) GetAncestor() apiChi.ICustomResource {
	if !cr.HasAncestor() {
		return (*ClickHouseKeeperInstallation)(nil)
	}
	return cr.Status.GetNormalizedCRCompleted()
}

// GetAncestorT gets ancestor of a CR
func (cr *ClickHouseKeeperInstallation) GetAncestorT() *ClickHouseKeeperInstallation {
	if !cr.HasAncestor() {
		return nil
	}
	return cr.Status.GetNormalizedCRCompleted()
}

// SetAncestor sets ancestor of a CR
func (cr *ClickHouseKeeperInstallation) SetAncestor(a *ClickHouseKeeperInstallation) {
	if cr == nil {
		return
	}
	cr.EnsureStatus().NormalizedCRCompleted = a
}

// HasTarget checks whether CR has a target
func (cr *ClickHouseKeeperInstallation) HasTarget() bool {
	if !cr.HasStatus() {
		return false
	}
	return cr.Status.HasNormalizedCR()
}

// GetTarget gets target of a CR
func (cr *ClickHouseKeeperInstallation) GetTarget() *ClickHouseKeeperInstallation {
	if !cr.HasTarget() {
		return nil
	}
	return cr.Status.GetNormalizedCR()
}

// SetTarget sets target of a CR
func (cr *ClickHouseKeeperInstallation) SetTarget(a *ClickHouseKeeperInstallation) {
	if cr == nil {
		return
	}
	cr.EnsureStatus().NormalizedCR = a
}

func (cr *ClickHouseKeeperInstallation) GetUsedTemplates() []*apiChi.TemplateRef {
	return nil
}

// FillStatus fills .Status
func (cr *ClickHouseKeeperInstallation) FillStatus(endpoint string, pods, fqdns []string, ip string) {
	cr.EnsureStatus().Fill(&FillStatusParams{
		CHOpIP:              ip,
		ClustersCount:       cr.ClustersCount(),
		ShardsCount:         cr.ShardsCount(),
		HostsCount:          cr.HostsCount(),
		TaskID:              "",
		HostsUpdatedCount:   0,
		HostsAddedCount:     0,
		HostsUnchangedCount: 0,
		HostsCompletedCount: 0,
		HostsDeleteCount:    0,
		HostsDeletedCount:   0,
		Pods:                pods,
		FQDNs:               fqdns,
		Endpoint:            endpoint,
		NormalizedCR: cr.Copy(types.CopyCROptions{
			SkipStatus:        true,
			SkipManagedFields: true,
		}),
	})
}

func (cr *ClickHouseKeeperInstallation) Fill() {
	apiChi.FillCR(cr)
}

// MergeFrom merges from CHI
func (cr *ClickHouseKeeperInstallation) MergeFrom(from *ClickHouseKeeperInstallation, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	// Merge Meta
	switch _type {
	case apiChi.MergeTypeFillEmptyValues:
		_ = mergo.Merge(&cr.TypeMeta, from.TypeMeta)
		_ = mergo.Merge(&cr.ObjectMeta, from.ObjectMeta)
	case apiChi.MergeTypeOverrideByNonEmptyValues:
		_ = mergo.Merge(&cr.TypeMeta, from.TypeMeta, mergo.WithOverride)
		_ = mergo.Merge(&cr.ObjectMeta, from.ObjectMeta, mergo.WithOverride)
	}
	// Exclude skipped annotations
	cr.SetAnnotations(
		util.CopyMapFilter(
			cr.GetAnnotations(),
			nil,
			util.ListSkippedAnnotations(),
		),
	)

	// Do actual merge for Spec
	cr.GetSpecT().MergeFrom(from.GetSpecT(), _type)

	// Copy service attributes
	//cr.ensureRuntime().attributes = from.ensureRuntime().attributes

	cr.EnsureStatus().CopyFrom(from.Status, types.CopyStatusOptions{
		InheritableFields: true,
	})
}

// FindCluster finds cluster by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cr *ClickHouseKeeperInstallation) FindCluster(needle interface{}) apiChi.ICluster {
	var resultCluster *ChkCluster
	cr.WalkClustersFullPath(func(chk *ClickHouseKeeperInstallation, clusterIndex int, cluster *ChkCluster) error {
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
func (cr *ClickHouseKeeperInstallation) FindShard(needleCluster interface{}, needleShard interface{}) apiChi.IShard {
	return cr.FindCluster(needleCluster).FindShard(needleShard)
}

// FindHost finds shard by name or index
// Expectations: name is expected to be a string, index is expected to be an int.
func (cr *ClickHouseKeeperInstallation) FindHost(needleCluster interface{}, needleShard interface{}, needleHost interface{}) *apiChi.Host {
	return cr.FindCluster(needleCluster).FindHost(needleShard, needleHost)
}

// ClustersCount counts clusters
func (cr *ClickHouseKeeperInstallation) ClustersCount() int {
	count := 0
	cr.WalkClusters(func(cluster apiChi.ICluster) error {
		count++
		return nil
	})
	return count
}

// ShardsCount counts shards
func (cr *ClickHouseKeeperInstallation) ShardsCount() int {
	count := 0
	cr.WalkShards(func(shard *ChkShard) error {
		count++
		return nil
	})
	return count
}

// HostsCount counts hosts
func (cr *ClickHouseKeeperInstallation) HostsCount() int {
	count := 0
	cr.WalkHosts(func(host *apiChi.Host) error {
		count++
		return nil
	})
	return count
}

// HostsCountAttributes counts hosts by attributes
func (cr *ClickHouseKeeperInstallation) HostsCountAttributes(a *apiChi.HostReconcileAttributes) int {
	count := 0
	cr.WalkHosts(func(host *apiChi.Host) error {
		if host.GetReconcileAttributes().Any(a) {
			count++
		}
		return nil
	})
	return count
}

// GetHostTemplate gets HostTemplate by name
func (cr *ClickHouseKeeperInstallation) GetHostTemplate(name string) (*apiChi.HostTemplate, bool) {
	if !cr.GetSpecT().GetTemplates().GetHostTemplatesIndex().Has(name) {
		return nil, false
	}
	return cr.GetSpecT().GetTemplates().GetHostTemplatesIndex().Get(name), true
}

// GetPodTemplate gets PodTemplate by name
func (cr *ClickHouseKeeperInstallation) GetPodTemplate(name string) (*apiChi.PodTemplate, bool) {
	if !cr.GetSpecT().GetTemplates().GetPodTemplatesIndex().Has(name) {
		return nil, false
	}
	return cr.GetSpecT().GetTemplates().GetPodTemplatesIndex().Get(name), true
}

// WalkPodTemplates walks over all PodTemplates
func (cr *ClickHouseKeeperInstallation) WalkPodTemplates(f func(template *apiChi.PodTemplate)) {
	cr.GetSpecT().GetTemplates().GetPodTemplatesIndex().Walk(f)
}

// GetVolumeClaimTemplate gets VolumeClaimTemplate by name
func (cr *ClickHouseKeeperInstallation) GetVolumeClaimTemplate(name string) (*apiChi.VolumeClaimTemplate, bool) {
	if cr.GetSpecT().GetTemplates().GetVolumeClaimTemplatesIndex().Has(name) {
		return cr.GetSpecT().GetTemplates().GetVolumeClaimTemplatesIndex().Get(name), true
	}
	return nil, false
}

// WalkVolumeClaimTemplates walks over all VolumeClaimTemplates
func (cr *ClickHouseKeeperInstallation) WalkVolumeClaimTemplates(f func(template *apiChi.VolumeClaimTemplate)) {
	if cr == nil {
		return
	}
	cr.GetSpecT().GetTemplates().GetVolumeClaimTemplatesIndex().Walk(f)
}

// GetServiceTemplate gets ServiceTemplate by name
func (cr *ClickHouseKeeperInstallation) GetServiceTemplate(name string) (*apiChi.ServiceTemplate, bool) {
	if !cr.GetSpecT().GetTemplates().GetServiceTemplatesIndex().Has(name) {
		return nil, false
	}
	return cr.GetSpecT().GetTemplates().GetServiceTemplatesIndex().Get(name), true
}

// GetRootServiceTemplate gets ServiceTemplate of a CHI
func (cr *ClickHouseKeeperInstallation) GetRootServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	if !cr.GetSpecT().GetDefaults().Templates.HasServiceTemplate() {
		return nil, false
	}
	name := cr.GetSpecT().GetDefaults().Templates.GetServiceTemplate()
	return cr.GetServiceTemplate(name)
}

// MatchNamespace matches namespace
func (cr *ClickHouseKeeperInstallation) MatchNamespace(namespace string) bool {
	if cr == nil {
		return false
	}
	return cr.Namespace == namespace
}

// MatchFullName matches full name
func (cr *ClickHouseKeeperInstallation) MatchFullName(namespace, name string) bool {
	if cr == nil {
		return false
	}
	return (cr.Namespace == namespace) && (cr.Name == name)
}

// FoundIn checks whether CHI can be found in haystack
func (cr *ClickHouseKeeperInstallation) FoundIn(haystack []*ClickHouseKeeperInstallation) bool {
	if cr == nil {
		return false
	}

	for _, candidate := range haystack {
		if candidate.MatchFullName(cr.Namespace, cr.Name) {
			return true
		}
	}

	return false
}

// IsAuto checks whether templating policy is auto
func (cr *ClickHouseKeeperInstallation) IsAuto() bool {
	return false
}

// IsStopped checks whether CHI is stopped
func (cr *ClickHouseKeeperInstallation) IsStopped() bool {
	return false
}

// IsRollingUpdate checks whether CHI should perform rolling update
func (cr *ClickHouseKeeperInstallation) IsRollingUpdate() bool {
	return false
}

// IsTroubleshoot checks whether CHI is in troubleshoot mode
func (cr *ClickHouseKeeperInstallation) IsTroubleshoot() bool {
	return false
}

// GetReconciling gets reconciling spec
func (cr *ClickHouseKeeperInstallation) GetReconciling() *apiChi.Reconciling {
	if cr == nil {
		return nil
	}
	return cr.GetSpecT().Reconciling
}

// Copy makes copy of a CHI, filtering fields according to specified CopyOptions
func (cr *ClickHouseKeeperInstallation) Copy(opts types.CopyCROptions) *ClickHouseKeeperInstallation {
	if cr == nil {
		return nil
	}
	jsonBytes, err := json.Marshal(cr)
	if err != nil {
		return nil
	}

	var chi2 *ClickHouseKeeperInstallation
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
func (cr *ClickHouseKeeperInstallation) JSON(opts types.CopyCROptions) string {
	if cr == nil {
		return ""
	}

	filtered := cr.Copy(opts)
	jsonBytes, err := json.MarshalIndent(filtered, "", "  ")
	if err != nil {
		return fmt.Sprintf("unable to parse. err: %v", err)
	}
	return string(jsonBytes)

}

// YAML return YAML string
func (cr *ClickHouseKeeperInstallation) YAML(opts types.CopyCROptions) string {
	if cr == nil {
		return ""
	}

	filtered := cr.Copy(opts)
	yamlBytes, err := yaml.Marshal(filtered)
	if err != nil {
		return fmt.Sprintf("unable to parse. err: %v", err)
	}
	return string(yamlBytes)
}

// FirstHost returns first host of the CHI
func (cr *ClickHouseKeeperInstallation) FirstHost() *apiChi.Host {
	var result *apiChi.Host
	cr.WalkHosts(func(host *apiChi.Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

func (cr *ClickHouseKeeperInstallation) GetName() string {
	if cr == nil {
		return ""
	}
	return cr.Name
}

func (cr *ClickHouseKeeperInstallation) GetNamespace() string {
	if cr == nil {
		return ""
	}
	return cr.Namespace
}

func (cr *ClickHouseKeeperInstallation) GetLabels() map[string]string {
	if cr == nil {
		return nil
	}
	return cr.Labels
}

func (cr *ClickHouseKeeperInstallation) GetAnnotations() map[string]string {
	if cr == nil {
		return nil
	}
	return cr.Annotations
}

// WalkClustersFullPath walks clusters with full path
func (cr *ClickHouseKeeperInstallation) WalkClustersFullPath(
	f func(chi *ClickHouseKeeperInstallation, clusterIndex int, cluster *ChkCluster) error,
) []error {
	if cr == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range cr.GetSpecT().Configuration.Clusters {
		res = append(res, f(cr, clusterIndex, cr.GetSpecT().Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkClusters walks clusters
func (cr *ClickHouseKeeperInstallation) WalkClusters(f func(i apiChi.ICluster) error) []error {
	if cr == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range cr.GetSpecT().Configuration.Clusters {
		res = append(res, f(cr.GetSpecT().Configuration.Clusters[clusterIndex]))
	}

	return res
}

// WalkShards walks shards
func (cr *ClickHouseKeeperInstallation) WalkShards(
	f func(
		shard *ChkShard,
	) error,
) []error {
	if cr == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range cr.GetSpecT().Configuration.Clusters {
		cluster := cr.GetSpecT().Configuration.Clusters[clusterIndex]
		for shardIndex := range cluster.Layout.Shards {
			shard := cluster.Layout.Shards[shardIndex]
			res = append(res, f(shard))
		}
	}

	return res
}

// WalkHostsFullPathAndScope walks hosts with full path
func (cr *ClickHouseKeeperInstallation) WalkHostsFullPathAndScope(
	crScopeCycleSize int,
	clusterScopeCycleSize int,
	f apiChi.WalkHostsAddressFn,
) (res []error) {
	if cr == nil {
		return nil
	}
	address := types.NewHostScopeAddress(crScopeCycleSize, clusterScopeCycleSize)
	for clusterIndex := range cr.GetSpecT().Configuration.Clusters {
		cluster := cr.GetSpecT().Configuration.Clusters[clusterIndex]
		address.ClusterScopeAddress.Init()
		for shardIndex := range cluster.Layout.Shards {
			shard := cluster.GetShard(shardIndex)
			for replicaIndex, host := range shard.Hosts {
				replica := cluster.GetReplica(replicaIndex)
				address.ClusterIndex = clusterIndex
				address.ShardIndex = shardIndex
				address.ReplicaIndex = replicaIndex
				res = append(res, f(cr, cluster, shard, replica, host, address))
				address.CRScopeAddress.Inc()
				address.ClusterScopeAddress.Inc()
			}
		}
	}
	return res
}

// WalkHostsFullPath walks hosts with a function
func (cr *ClickHouseKeeperInstallation) WalkHostsFullPath(f apiChi.WalkHostsAddressFn) []error {
	return cr.WalkHostsFullPathAndScope(0, 0, f)
}

// WalkHosts walks hosts with a function
func (cr *ClickHouseKeeperInstallation) WalkHosts(f func(host *apiChi.Host) error) []error {
	if cr == nil {
		return nil
	}
	res := make([]error, 0)

	for clusterIndex := range cr.GetSpecT().Configuration.Clusters {
		cluster := cr.GetSpecT().Configuration.Clusters[clusterIndex]
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

// WalkTillError walks hosts with a function until an error met
func (cr *ClickHouseKeeperInstallation) WalkTillError(
	ctx context.Context,
	fCRPreliminary func(ctx context.Context, chi *ClickHouseKeeperInstallation) error,
	fCluster func(ctx context.Context, cluster *ChkCluster) error,
	fShards func(ctx context.Context, shards []*ChkShard) error,
	fCRFinal func(ctx context.Context, chi *ClickHouseKeeperInstallation) error,
) error {
	if err := fCRPreliminary(ctx, cr); err != nil {
		return err
	}

	for clusterIndex := range cr.GetSpecT().Configuration.Clusters {
		cluster := cr.GetSpecT().Configuration.Clusters[clusterIndex]
		if err := fCluster(ctx, cluster); err != nil {
			return err
		}

		shards := make([]*ChkShard, 0, len(cluster.Layout.Shards))
		for shardIndex := range cluster.Layout.Shards {
			shards = append(shards, cluster.Layout.Shards[shardIndex])
		}
		if err := fShards(ctx, shards); err != nil {
			return err
		}
	}

	if err := fCRFinal(ctx, cr); err != nil {
		return err
	}

	return nil
}
