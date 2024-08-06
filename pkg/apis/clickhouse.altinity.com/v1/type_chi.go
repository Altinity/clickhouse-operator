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

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (cr *ClickHouseInstallation) IsNonZero() bool {
	return cr != nil
}

func (cr *ClickHouseInstallation) GetSpec() ICRSpec {
	return &cr.Spec
}

func (cr *ClickHouseInstallation) GetSpecT() *ChiSpec {
	return &cr.Spec
}

func (cr *ClickHouseInstallation) GetSpecA() any {
	return &cr.Spec
}

func (cr *ClickHouseInstallation) GetRuntime() ICustomResourceRuntime {
	return cr.ensureRuntime()
}

func (cr *ClickHouseInstallation) ensureRuntime() *ClickHouseInstallationRuntime {
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
		cr.runtime = newClickHouseInstallationRuntime()
	}
	return cr.runtime
}

func (cr *ClickHouseInstallation) IEnsureStatus() IStatus {
	return any(cr.EnsureStatus()).(IStatus)
}

// EnsureStatus ensures status
func (cr *ClickHouseInstallation) EnsureStatus() *Status {
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
func (cr *ClickHouseInstallation) GetStatus() IStatus {
	if cr == nil {
		return (*Status)(nil)
	}
	return cr.Status
}

// HasStatus checks whether CHI has Status
func (cr *ClickHouseInstallation) HasStatus() bool {
	if cr == nil {
		return false
	}
	return cr.Status != nil
}

// HasAncestor checks whether CR has an ancestor
func (cr *ClickHouseInstallation) HasAncestor() bool {
	if !cr.HasStatus() {
		return false
	}
	return cr.Status.HasNormalizedCRCompleted()
}

// GetAncestor gets ancestor of a CR
func (cr *ClickHouseInstallation) GetAncestor() ICustomResource {
	if !cr.HasAncestor() {
		return (*ClickHouseInstallation)(nil)
	}
	return cr.Status.GetNormalizedCRCompleted()
}

// GetAncestorT gets ancestor of a CR
func (cr *ClickHouseInstallation) GetAncestorT() *ClickHouseInstallation {
	if !cr.HasAncestor() {
		return nil
	}
	return cr.Status.GetNormalizedCRCompleted()
}

// SetAncestor sets ancestor of a CR
func (cr *ClickHouseInstallation) SetAncestor(a *ClickHouseInstallation) {
	if cr == nil {
		return
	}
	cr.EnsureStatus().NormalizedCRCompleted = a
}

// HasTarget checks whether CR has a target
func (cr *ClickHouseInstallation) HasTarget() bool {
	if !cr.HasStatus() {
		return false
	}
	return cr.Status.HasNormalizedCR()
}

// GetTarget gets target of a CR
func (cr *ClickHouseInstallation) GetTarget() *ClickHouseInstallation {
	if !cr.HasTarget() {
		return nil
	}
	return cr.Status.GetNormalizedCR()
}

// SetTarget sets target of a CR
func (cr *ClickHouseInstallation) SetTarget(a *ClickHouseInstallation) {
	if cr == nil {
		return
	}
	cr.EnsureStatus().NormalizedCR = a
}

func (cr *ClickHouseInstallation) GetUsedTemplates() []*TemplateRef {
	return cr.GetSpecT().UseTemplates
}

// FillStatus fills .Status
func (cr *ClickHouseInstallation) FillStatus(endpoint string, pods, fqdns []string, ip string) {
	cr.EnsureStatus().Fill(&FillStatusParams{
		CHOpIP:              ip,
		ClustersCount:       cr.ClustersCount(),
		ShardsCount:         cr.ShardsCount(),
		HostsCount:          cr.HostsCount(),
		TaskID:              cr.GetSpecT().GetTaskID(),
		HostsUpdatedCount:   0,
		HostsAddedCount:     0,
		HostsUnchangedCount: 0,
		HostsCompletedCount: 0,
		HostsDeleteCount:    0,
		HostsDeletedCount:   0,
		Pods:                pods,
		FQDNs:               fqdns,
		Endpoint:            endpoint,
		NormalizedCR: cr.Copy(CopyCROptions{
			SkipStatus:        true,
			SkipManagedFields: true,
		}),
	})
}

func (cr *ClickHouseInstallation) Fill() {
	FillCR(cr)
}

// MergeFrom merges from CHI
func (cr *ClickHouseInstallation) MergeFrom(from *ClickHouseInstallation, _type MergeType) {
	if from == nil {
		return
	}

	// Merge Meta
	switch _type {
	case MergeTypeFillEmptyValues:
		_ = mergo.Merge(&cr.TypeMeta, from.TypeMeta)
		_ = mergo.Merge(&cr.ObjectMeta, from.ObjectMeta)
	case MergeTypeOverrideByNonEmptyValues:
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
	cr.ensureRuntime().attributes = from.ensureRuntime().attributes

	cr.EnsureStatus().CopyFrom(from.Status, types.CopyStatusOptions{
		InheritableFields: true,
	})
}

// FindCluster finds cluster by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cr *ClickHouseInstallation) FindCluster(needle interface{}) ICluster {
	var resultCluster *ChiCluster
	cr.WalkClustersFullPath(func(chi *ClickHouseInstallation, clusterIndex int, cluster *ChiCluster) error {
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
func (cr *ClickHouseInstallation) FindShard(needleCluster interface{}, needleShard interface{}) IShard {
	return cr.FindCluster(needleCluster).FindShard(needleShard)
}

// FindHost finds shard by name or index
// Expectations: name is expected to be a string, index is expected to be an int.
func (cr *ClickHouseInstallation) FindHost(needleCluster interface{}, needleShard interface{}, needleHost interface{}) *Host {
	return cr.FindCluster(needleCluster).FindHost(needleShard, needleHost)
}

// ClustersCount counts clusters
func (cr *ClickHouseInstallation) ClustersCount() int {
	count := 0
	cr.WalkClusters(func(cluster ICluster) error {
		count++
		return nil
	})
	return count
}

// ShardsCount counts shards
func (cr *ClickHouseInstallation) ShardsCount() int {
	count := 0
	cr.WalkShards(func(shard *ChiShard) error {
		count++
		return nil
	})
	return count
}

// HostsCount counts hosts
func (cr *ClickHouseInstallation) HostsCount() int {
	count := 0
	cr.WalkHosts(func(host *Host) error {
		count++
		return nil
	})
	return count
}

// HostsCountAttributes counts hosts by attributes
func (cr *ClickHouseInstallation) HostsCountAttributes(a *HostReconcileAttributes) int {
	count := 0
	cr.WalkHosts(func(host *Host) error {
		if host.GetReconcileAttributes().Any(a) {
			count++
		}
		return nil
	})
	return count
}

// GetHostTemplate gets HostTemplate by name
func (cr *ClickHouseInstallation) GetHostTemplate(name string) (*HostTemplate, bool) {
	if !cr.GetSpecT().GetTemplates().GetHostTemplatesIndex().Has(name) {
		return nil, false
	}
	return cr.GetSpecT().GetTemplates().GetHostTemplatesIndex().Get(name), true
}

// GetPodTemplate gets PodTemplate by name
func (cr *ClickHouseInstallation) GetPodTemplate(name string) (*PodTemplate, bool) {
	if !cr.GetSpecT().GetTemplates().GetPodTemplatesIndex().Has(name) {
		return nil, false
	}
	return cr.GetSpecT().GetTemplates().GetPodTemplatesIndex().Get(name), true
}

// WalkPodTemplates walks over all PodTemplates
func (cr *ClickHouseInstallation) WalkPodTemplates(f func(template *PodTemplate)) {
	cr.GetSpecT().GetTemplates().GetPodTemplatesIndex().Walk(f)
}

// GetVolumeClaimTemplate gets VolumeClaimTemplate by name
func (cr *ClickHouseInstallation) GetVolumeClaimTemplate(name string) (*VolumeClaimTemplate, bool) {
	if cr.GetSpecT().GetTemplates().GetVolumeClaimTemplatesIndex().Has(name) {
		return cr.GetSpecT().GetTemplates().GetVolumeClaimTemplatesIndex().Get(name), true
	}
	return nil, false
}

// WalkVolumeClaimTemplates walks over all VolumeClaimTemplates
func (cr *ClickHouseInstallation) WalkVolumeClaimTemplates(f func(template *VolumeClaimTemplate)) {
	if cr == nil {
		return
	}
	cr.GetSpecT().GetTemplates().GetVolumeClaimTemplatesIndex().Walk(f)
}

// GetServiceTemplate gets ServiceTemplate by name
func (cr *ClickHouseInstallation) GetServiceTemplate(name string) (*ServiceTemplate, bool) {
	if !cr.GetSpecT().GetTemplates().GetServiceTemplatesIndex().Has(name) {
		return nil, false
	}
	return cr.GetSpecT().GetTemplates().GetServiceTemplatesIndex().Get(name), true
}

// GetRootServiceTemplate gets ServiceTemplate of a CHI
func (cr *ClickHouseInstallation) GetRootServiceTemplate() (*ServiceTemplate, bool) {
	if !cr.GetSpec().GetDefaults().Templates.HasServiceTemplate() {
		return nil, false
	}
	name := cr.GetSpec().GetDefaults().Templates.GetServiceTemplate()
	return cr.GetServiceTemplate(name)
}

// MatchNamespace matches namespace
func (cr *ClickHouseInstallation) MatchNamespace(namespace string) bool {
	if cr == nil {
		return false
	}
	return cr.Namespace == namespace
}

// MatchFullName matches full name
func (cr *ClickHouseInstallation) MatchFullName(namespace, name string) bool {
	if cr == nil {
		return false
	}
	return (cr.Namespace == namespace) && (cr.Name == name)
}

// FoundIn checks whether CHI can be found in haystack
func (cr *ClickHouseInstallation) FoundIn(haystack []*ClickHouseInstallation) bool {
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

// Possible templating policies
const (
	TemplatingPolicyAuto   = "auto"
	TemplatingPolicyManual = "manual"
)

// IsAuto checks whether templating policy is auto
func (cr *ClickHouseInstallation) IsAuto() bool {
	if cr == nil {
		return false
	}
	if (cr.Namespace == "") && (cr.Name == "") {
		return false
	}
	return cr.GetSpecT().GetTemplating().GetPolicy() == TemplatingPolicyAuto
}

// IsStopped checks whether CHI is stopped
func (cr *ClickHouseInstallation) IsStopped() bool {
	if cr == nil {
		return false
	}
	return cr.GetSpecT().GetStop().Value()
}

// Restart constants present available values for .spec.restart
// Controlling the operator's Clickhouse instances restart policy
const (
	// RestartRollingUpdate requires to roll over all hosts in the cluster and shutdown and reconcile each of it.
	// This restart policy means that all hosts in the cluster would pass through shutdown/reconcile cycle.
	RestartRollingUpdate = "RollingUpdate"
)

// IsRollingUpdate checks whether CHI should perform rolling update
func (cr *ClickHouseInstallation) IsRollingUpdate() bool {
	if cr == nil {
		return false
	}
	return cr.GetSpecT().GetRestart().Value() == RestartRollingUpdate
}

// IsTroubleshoot checks whether CHI is in troubleshoot mode
func (cr *ClickHouseInstallation) IsTroubleshoot() bool {
	if cr == nil {
		return false
	}
	return cr.GetSpecT().GetTroubleshoot().Value()
}

// GetReconciling gets reconciling spec
func (cr *ClickHouseInstallation) GetReconciling() *Reconciling {
	if cr == nil {
		return nil
	}
	return cr.GetSpecT().Reconciling
}

// Copy makes copy of a CHI, filtering fields according to specified CopyOptions
func (cr *ClickHouseInstallation) Copy(opts CopyCROptions) *ClickHouseInstallation {
	if cr == nil {
		return nil
	}
	jsonBytes, err := json.Marshal(cr)
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
func (cr *ClickHouseInstallation) JSON(opts CopyCROptions) string {
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
func (cr *ClickHouseInstallation) YAML(opts CopyCROptions) string {
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
func (cr *ClickHouseInstallation) FirstHost() *Host {
	var result *Host
	cr.WalkHosts(func(host *Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

func (cr *ClickHouseInstallation) GetName() string {
	if cr == nil {
		return ""
	}
	return cr.Name
}

func (cr *ClickHouseInstallation) GetNamespace() string {
	if cr == nil {
		return ""
	}
	return cr.Namespace
}

func (cr *ClickHouseInstallation) GetLabels() map[string]string {
	if cr == nil {
		return nil
	}
	return cr.Labels
}

func (cr *ClickHouseInstallation) GetAnnotations() map[string]string {
	if cr == nil {
		return nil
	}
	return cr.Annotations
}

// WalkClustersFullPath walks clusters with full path
func (cr *ClickHouseInstallation) WalkClustersFullPath(
	f func(chi *ClickHouseInstallation, clusterIndex int, cluster *ChiCluster) error,
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
func (cr *ClickHouseInstallation) WalkClusters(f func(i ICluster) error) []error {
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
func (cr *ClickHouseInstallation) WalkShards(
	f func(
		shard *ChiShard,
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
func (cr *ClickHouseInstallation) WalkHostsFullPathAndScope(
	crScopeCycleSize int,
	clusterScopeCycleSize int,
	f WalkHostsAddressFn,
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
func (cr *ClickHouseInstallation) WalkHostsFullPath(f WalkHostsAddressFn) []error {
	return cr.WalkHostsFullPathAndScope(0, 0, f)
}

// WalkHosts walks hosts with a function
func (cr *ClickHouseInstallation) WalkHosts(f func(host *Host) error) []error {
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
func (cr *ClickHouseInstallation) WalkTillError(
	ctx context.Context,
	fCRPreliminary func(ctx context.Context, chi *ClickHouseInstallation) error,
	fCluster func(ctx context.Context, cluster *ChiCluster) error,
	fShards func(ctx context.Context, shards []*ChiShard) error,
	fCRFinal func(ctx context.Context, chi *ClickHouseInstallation) error,
) error {
	if err := fCRPreliminary(ctx, cr); err != nil {
		return err
	}

	for clusterIndex := range cr.GetSpecT().Configuration.Clusters {
		cluster := cr.GetSpecT().Configuration.Clusters[clusterIndex]
		if err := fCluster(ctx, cluster); err != nil {
			return err
		}

		shards := make([]*ChiShard, 0, len(cluster.Layout.Shards))
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
