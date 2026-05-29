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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// Cluster defines item of a clusters section of .configuration
type Cluster struct {
	Name              string            `json:"name,omitempty"              yaml:"name,omitempty"`
	Zookeeper         *ZookeeperConfig  `json:"zookeeper,omitempty"         yaml:"zookeeper,omitempty"`
	Settings          *Settings         `json:"settings,omitempty"          yaml:"settings,omitempty"`
	Files             *Settings         `json:"files,omitempty"             yaml:"files,omitempty"`
	Templates         *TemplatesList    `json:"templates,omitempty"         yaml:"templates,omitempty"`
	SchemaPolicy      *SchemaPolicy     `json:"schemaPolicy,omitempty"      yaml:"schemaPolicy,omitempty"`
	Insecure          *types.StringBool `json:"insecure,omitempty"          yaml:"insecure,omitempty"`
	Secure            *types.StringBool `json:"secure,omitempty"            yaml:"secure,omitempty"`
	Secret            *ClusterSecret    `json:"secret,omitempty"            yaml:"secret,omitempty"`
	Security          *ClusterSecurity  `json:"security,omitempty"          yaml:"security,omitempty"`
	PDBManaged        *types.StringBool `json:"pdbManaged,omitempty"        yaml:"pdbManaged,omitempty"`
	PDBMaxUnavailable *types.Int32      `json:"pdbMaxUnavailable,omitempty" yaml:"pdbMaxUnavailable,omitempty"`
	Reconcile         *ClusterReconcile `json:"reconcile,omitempty"         yaml:"reconcile,omitempty"`
	Layout            *ChiClusterLayout `json:"layout,omitempty"            yaml:"layout,omitempty"`

	Runtime ChiClusterRuntime `json:"-" yaml:"-"`
}

type ChiClusterRuntime struct {
	Address ChiClusterAddress       `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r *ChiClusterRuntime) GetAddress() IClusterAddress {
	return &r.Address
}

func (r ChiClusterRuntime) GetCR() ICustomResource {
	return r.CHI
}

func (r *ChiClusterRuntime) SetCR(cr ICustomResource) {
	r.CHI = cr.(*ClickHouseInstallation)
}

// ChiClusterAddress defines address of a cluster within ClickHouseInstallation
type ChiClusterAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
}

func (a *ChiClusterAddress) GetNamespace() string {
	return a.Namespace
}

func (a *ChiClusterAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *ChiClusterAddress) GetCRName() string {
	return a.CHIName
}

func (a *ChiClusterAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *ChiClusterAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *ChiClusterAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *ChiClusterAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *ChiClusterAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (cluster *Cluster) GetName() string {
	if cluster == nil {
		return ""
	}
	return cluster.Name
}

// HasName checks whether cluster has a name
func (cluster *Cluster) HasName() bool {
	if cluster == nil {
		return false
	}
	return len(cluster.GetName()) > 0
}

func (c *Cluster) GetZookeeper() *ZookeeperConfig {
	if c == nil {
		return nil
	}
	return c.Zookeeper
}

func (c *Cluster) GetSchemaPolicy() *SchemaPolicy {
	return c.SchemaPolicy
}

// GetInsecure is a getter
func (cluster *Cluster) GetInsecure() *types.StringBool {
	if cluster == nil {
		return nil
	}
	return cluster.Insecure
}

// GetSecure is a getter
func (cluster *Cluster) GetSecure() *types.StringBool {
	if cluster == nil {
		return nil
	}
	return cluster.Secure
}

// GetSecret is a getter
func (c *Cluster) GetSecret() *ClusterSecret {
	if c == nil {
		return nil
	}
	return c.Secret
}

// GetSecurity returns the cluster-level Security block, nil-safe.
func (c *Cluster) GetSecurity() *ClusterSecurity {
	if c == nil {
		return nil
	}
	return c.Security
}

// GetPDBManaged is a getter
func (cluster *Cluster) GetPDBManaged() *types.StringBool {
	return cluster.PDBManaged
}

// GetPDBMaxUnavailable is a getter
func (cluster *Cluster) GetPDBMaxUnavailable() *types.Int32 {
	return cluster.PDBMaxUnavailable
}

// GetReconcile is a getter
func (cluster *Cluster) GetReconcile() *ClusterReconcile {
	cluster.Reconcile = cluster.Reconcile.Ensure()
	return cluster.Reconcile
}

// GetRuntime is a getter
func (cluster *Cluster) GetRuntime() IClusterRuntime {
	return &cluster.Runtime
}

// FillShardsReplicasExplicitlySpecified fills whether shard or replicas are explicitly specified
func (cluster *Cluster) FillShardsReplicasExplicitlySpecified() {
	if len(cluster.Layout.Shards) > 0 {
		cluster.Layout.ShardsExplicitlySpecified = true
	}
	if len(cluster.Layout.Replicas) > 0 {
		cluster.Layout.ReplicasExplicitlySpecified = true
	}
}

// isShardExplicitlySpecified checks whether shard is explicitly specified
func (cluster *Cluster) isShardExplicitlySpecified() bool {
	return cluster.Layout.ShardsExplicitlySpecified
}

// isReplicaExplicitlySpecified checks whether replica is explicitly specified
func (cluster *Cluster) isReplicaExplicitlySpecified() bool {
	return cluster.Layout.ReplicasExplicitlySpecified && !cluster.isShardExplicitlySpecified()
}

// IsShardSpecified checks whether shard is explicitly specified
func (cluster *Cluster) isShardToBeUsedToInheritSettingsFrom() bool {
	if !cluster.isShardExplicitlySpecified() && !cluster.isReplicaExplicitlySpecified() {
		return true
	}

	return cluster.isShardExplicitlySpecified()
}

func (cluster *Cluster) SelectSettingsSourceFrom(shard IShard, replica IReplica) any {
	if cluster.isShardToBeUsedToInheritSettingsFrom() {
		return shard
	}
	return replica
}

// InheritZookeeperFrom inherits zookeeper config from CHI
func (cluster *Cluster) InheritZookeeperFrom(chi *ClickHouseInstallation) {
	if !cluster.Zookeeper.IsEmpty() {
		// Has zk config explicitly specified already
		return
	}
	parentZk := chi.GetSpecT().GetConfiguration().GetZookeeper()
	if parentZk == nil {
		return
	}
	cluster.Zookeeper = cluster.Zookeeper.MergeFrom(parentZk, MergeTypeFillEmptyValues)
}

// InheritFilesFrom inherits files from CR
func (cluster *Cluster) InheritFilesFrom(chi *ClickHouseInstallation) {
	parentFiles := chi.GetSpecT().GetConfiguration().GetFiles()
	if parentFiles == nil {
		return
	}

	// Propagate host section only
	cluster.Files = cluster.Files.MergeFromCB(parentFiles, func(path string, _ *Setting) bool {
		if section, err := GetSectionFromPath(path); err == nil {
			if section.Equal(SectionHost) {
				return true
			}
		}

		return false
	})
}

// InheritClusterSecurityFrom inherits security knobs from the CHI spec into this
// cluster. Fills only empty fields, so cluster-level overrides win over CHI-level.
// Pattern mirrors InheritClusterReconcileFrom; the full 3-level inheritance
// (CHOP-config → CHI → cluster) is completed in normalizeClusterSecurity.
func (cluster *Cluster) InheritClusterSecurityFrom(chi *ClickHouseInstallation) {
	if chi == nil {
		return
	}
	specSecurity := chi.Spec.GetSecurity()
	if specSecurity == nil {
		return
	}
	if cluster.Security == nil {
		cluster.Security = &ClusterSecurity{}
	}
	cluster.Security.ClickHouse = cluster.Security.ClickHouse.MergeFrom(specSecurity.GetClickHouse(), MergeTypeFillEmptyValues)
	cluster.Security.Zookeeper = cluster.Security.Zookeeper.MergeFrom(specSecurity.GetZookeeper(), MergeTypeFillEmptyValues)
}

// InheritClusterReconcileFrom inherits reconcile runtime from CHI
func (cluster *Cluster) InheritClusterReconcileFrom(chi *ClickHouseInstallation) {
	if chi.Spec.Reconcile == nil {
		return
	}
	reconcile := cluster.GetReconcile()
	reconcile.Runtime = reconcile.Runtime.MergeFrom(chi.Spec.Reconcile.Runtime, MergeTypeFillEmptyValues)
	reconcile.StatefulSet = reconcile.StatefulSet.MergeFrom(chi.Spec.Reconcile.StatefulSet)
	reconcile.Host = reconcile.Host.MergeFrom(chi.Spec.Reconcile.Host)
	// Inherit CHI-level cluster hooks into this cluster's reconcile.hooks. The merge
	// is dedup'd (see mergeHookActions) so re-running normalization is idempotent.
	reconcile.Hooks = reconcile.Hooks.MergeFrom(chi.Spec.Reconcile.Cluster.GetHooks())
	cluster.Reconcile = reconcile
}

// InheritTemplatesFrom inherits templates from CHI
func (cluster *Cluster) InheritTemplatesFrom(chi *ClickHouseInstallation) {
	if chi.GetSpec().GetDefaults() == nil {
		return
	}
	if chi.GetSpec().GetDefaults().Templates == nil {
		return
	}
	cluster.Templates = cluster.Templates.MergeFrom(chi.GetSpec().GetDefaults().Templates, MergeTypeFillEmptyValues)
	cluster.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate returns service template, if exists
func (cluster *Cluster) GetServiceTemplate() (*ServiceTemplate, bool) {
	if !cluster.Templates.HasClusterServiceTemplate() {
		return nil, false
	}
	name := cluster.Templates.GetClusterServiceTemplate()
	return cluster.Runtime.CHI.GetServiceTemplate(name)
}

// GetCR gets parent CR
func (cluster *Cluster) GetCR() *ClickHouseInstallation {
	return cluster.Runtime.CHI
}

func (cluster *Cluster) GetAncestor() ICluster {
	return cluster.GetCR().GetAncestor().FindCluster(cluster.GetName())
}

// GetShard gets shard with specified index
func (cluster *Cluster) GetShard(shard int) *ChiShard {
	return cluster.Layout.Shards[shard]
}

// GetOrCreateHost gets or creates host on specified coordinates
func (cluster *Cluster) GetOrCreateHost(shard, replica int) *Host {
	return cluster.Layout.HostsField.GetOrCreate(shard, replica)
}

// GetReplica gets replica with specified index
func (cluster *Cluster) GetReplica(replica int) *ChiReplica {
	return cluster.Layout.Replicas[replica]
}

// FindShard finds shard by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *Cluster) FindShard(needle interface{}) IShard {
	var resultShard *ChiShard
	cluster.WalkShards(func(index int, shard IShard) error {
		switch v := needle.(type) {
		case string:
			if shard.GetName() == v {
				resultShard = shard.(*ChiShard)
			}
		case int:
			if index == v {
				resultShard = shard.(*ChiShard)
			}
		}
		return nil
	})
	return resultShard
}

// FindHost finds host by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *Cluster) FindHost(needleShard interface{}, needleHost interface{}) *Host {
	return cluster.FindShard(needleShard).FindHost(needleHost)
}

// FirstHost finds first host in the cluster
func (cluster *Cluster) FirstHost() *Host {
	var result *Host
	cluster.WalkHosts(func(host *Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

// HasRunningHosts reports whether the cluster has any hosts that existed in a previous
// reconcile (i.e. host has an ancestor). On first CHI creation, hosts have no ancestors —
// callers that need a live target (e.g. SQL hooks) use this to skip first-creation paths
// where pods don't exist yet.
//
// Approximated by checking only the first host's ancestor flag — sufficient because the
// reconciler advances all hosts together: either the cluster is brand new (no ancestors
// anywhere) or it has prior state (at least the first host has one).
func (cluster *Cluster) HasRunningHosts() bool {
	firstHost := cluster.FirstHost()
	if firstHost == nil {
		return false
	}
	return firstHost.HasAncestor()
}

// WalkShards walks shards
func (cluster *Cluster) WalkShards(f func(index int, shard IShard) error) []error {
	if cluster == nil {
		return nil
	}
	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := cluster.Layout.Shards[shardIndex]
		res = append(res, f(shardIndex, shard))
	}

	return res
}

// WalkReplicas walks replicas
func (cluster *Cluster) WalkReplicas(f func(index int, replica *ChiReplica) error) []error {
	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := cluster.Layout.Replicas[replicaIndex]
		res = append(res, f(replicaIndex, replica))
	}

	return res
}

// WalkHosts walks hosts
func (cluster *Cluster) WalkHosts(f func(host *Host) error) []error {
	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := cluster.Layout.Shards[shardIndex]
		for replicaIndex := range shard.Hosts {
			host := shard.Hosts[replicaIndex]
			res = append(res, f(host))
		}
	}

	return res
}

// WalkHostsByShards walks hosts by shards
func (cluster *Cluster) WalkHostsByShards(f func(shard, replica int, host *Host) error) []error {

	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := cluster.Layout.Shards[shardIndex]
		for replicaIndex := range shard.Hosts {
			host := shard.Hosts[replicaIndex]
			res = append(res, f(shardIndex, replicaIndex, host))
		}
	}

	return res
}

func (cluster *Cluster) GetLayout() *ChiClusterLayout {
	return cluster.Layout
}

// WalkHostsByReplicas walks hosts by replicas
func (cluster *Cluster) WalkHostsByReplicas(f func(shard, replica int, host *Host) error) []error {

	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := cluster.Layout.Replicas[replicaIndex]
		for shardIndex := range replica.Hosts {
			host := replica.Hosts[shardIndex]
			res = append(res, f(shardIndex, replicaIndex, host))
		}
	}

	return res
}

// HostsCount counts hosts
func (cluster *Cluster) HostsCount() int {
	count := 0
	cluster.WalkHosts(func(host *Host) error {
		count++
		return nil
	})
	return count
}

func (cluster *Cluster) IsZero() bool {
	return cluster == nil
}

func (cluster *Cluster) IsNonZero() bool {
	return cluster != nil
}

// IsStopped checks whether host is stopped
func (cluster *Cluster) IsStopped() bool {
	return cluster.GetCR().IsStopped()
}

func (cluster *Cluster) Ensure(create func() *Cluster) *Cluster {
	if cluster == nil {
		cluster = create()
	}
	return cluster
}

// ChiClusterLayout defines layout section of .spec.configuration.clusters
type ChiClusterLayout struct {
	ShardsCount   int `json:"shardsCount,omitempty"   yaml:"shardsCount,omitempty"`
	ReplicasCount int `json:"replicasCount,omitempty" yaml:"replicasCount,omitempty"`

	// TODO refactor into map[string]ChiShard
	Shards   []*ChiShard   `json:"shards,omitempty"   yaml:"shards,omitempty"`
	Replicas []*ChiReplica `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	// Internal data
	// Whether shards or replicas are explicitly specified as Shards []ChiShard or Replicas []ChiReplica
	ShardsExplicitlySpecified   bool        `json:"-" yaml:"-" testdiff:"ignore"`
	ReplicasExplicitlySpecified bool        `json:"-" yaml:"-" testdiff:"ignore"`
	HostsField                  *HostsField `json:"-" yaml:"-" testdiff:"ignore"`
}

// NewChiClusterLayout creates new cluster layout
func NewChiClusterLayout() *ChiClusterLayout {
	return new(ChiClusterLayout)
}

func (l *ChiClusterLayout) GetReplicasCount() int {
	return l.ReplicasCount
}

func (l *ChiClusterLayout) Ensure() *ChiClusterLayout {
	if l == nil {
		l = NewChiClusterLayout()
	}
	return l
}

// SchemaPolicy defines schema management policy - replica or shard-based
type SchemaPolicy struct {
	Replica string `json:"replica" yaml:"replica"`
	Shard   string `json:"shard"   yaml:"shard"`
}

// NewClusterSchemaPolicy creates new cluster layout
func NewClusterSchemaPolicy() *SchemaPolicy {
	return new(SchemaPolicy)
}
