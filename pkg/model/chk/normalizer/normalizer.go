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

package normalizer

import (
	"strings"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNamer "github.com/altinity/clickhouse-operator/pkg/model/common/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst_settings"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/templates"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// Normalizer specifies structures normalizer
type Normalizer struct {
	ctx   *Context
	namer interfaces.INameManager
}

// NewNormalizer creates new normalizer
func NewNormalizer() *Normalizer {
	return &Normalizer{
		namer: managers.NewNameManager(managers.NameManagerTypeKeeper),
	}
}

// CreateTemplated produces ready-to-use object
func (n *Normalizer) CreateTemplated(subj *apiChk.ClickHouseKeeperInstallation, options *normalizer.Options) (
	*apiChk.ClickHouseKeeperInstallation,
	error,
) {
	// Normalization starts with a new context
	n.buildContext(options)
	// Ensure normalization subject presence
	subj = n.ensureSubject(subj)
	// Build target from all templates and subject
	n.buildTargetFromTemplates(subj)
	// And launch normalization of the whole stack
	return n.normalizeTarget()
}

func (n *Normalizer) buildContext(options *normalizer.Options) {
	n.ctx = NewContext(options)
}

func (n *Normalizer) buildTargetFromTemplates(subj *apiChk.ClickHouseKeeperInstallation) {
	// Create new target that will be populated with data during normalization process
	n.ctx.SetTarget(n.createTarget())

	// At this moment we have target available - is either newly created or a system-wide template

	// Apply templates - both auto and explicitly requested - on top of target
	//n.applyTemplatesOnTarget(subj)

	// After all templates applied, place provided 'subject' on top of the whole stack (target)
	n.ctx.GetTarget().MergeFrom(subj, apiChi.MergeTypeOverrideByNonEmptyValues)
}

//func (n *Normalizer) applyTemplatesOnTarget(subj crTemplatesNormalizer.TemplateSubject) {
//	for _, template := range templatesNormalizer.ApplyTemplates(n.ctx.GetTarget(), subj) {
//		n.ctx.GetTarget().EnsureStatus().PushUsedTemplate(template)
//	}
//}

func (n *Normalizer) newSubject() *apiChk.ClickHouseKeeperInstallation {
	return managers.CreateCustomResource(managers.CustomResourceCHK).(*apiChk.ClickHouseKeeperInstallation)
}

func (n *Normalizer) shouldCreateDefaultCluster(subj *apiChk.ClickHouseKeeperInstallation) bool {
	if subj == nil {
		// No subject specified - meaning we are normalizing non-existing subject and it should have no clusters inside
		return false
	} else {
		// Subject specified - meaning we are normalizing existing subject and we need to ensure default cluster presence
		return true
	}
}

func (n *Normalizer) ensureSubject(subj *apiChk.ClickHouseKeeperInstallation) *apiChk.ClickHouseKeeperInstallation {
	n.ctx.Options().WithDefaultCluster = n.shouldCreateDefaultCluster(subj)

	if subj == nil {
		// Need to create subject
		return n.newSubject()
	} else {
		// Subject specified
		return subj
	}
}

func (n *Normalizer) GetTargetTemplate() *apiChk.ClickHouseKeeperInstallation {
	return nil // return chop.Config().Template.CHI.Runtime.Template
}

func (n *Normalizer) HasTargetTemplate() bool {
	return n.GetTargetTemplate() != nil
}

func (n *Normalizer) createTarget() *apiChk.ClickHouseKeeperInstallation {
	if n.HasTargetTemplate() {
		// Template specified - start with template
		return n.GetTargetTemplate().DeepCopy()
	} else {
		// No template specified - start with clear page
		return n.newSubject()
	}
}

// normalizeTarget normalizes target
func (n *Normalizer) normalizeTarget() (*apiChk.ClickHouseKeeperInstallation, error) {
	n.normalizeSpec()
	n.finalize()
	n.fillStatus()

	return n.ctx.GetTarget(), nil
}

func (n *Normalizer) normalizeSpec() {
	// Walk over Spec datatype fields
	n.ctx.GetTarget().GetSpec().NamespaceDomainPattern = n.normalizeNamespaceDomainPattern(n.ctx.GetTarget().GetSpec().NamespaceDomainPattern)
	n.ctx.GetTarget().GetSpec().Defaults = n.normalizeDefaults(n.ctx.GetTarget().GetSpec().Defaults)
	n.ctx.GetTarget().GetSpec().Configuration = n.normalizeConfiguration(n.ctx.GetTarget().GetSpec().Configuration)
	n.ctx.GetTarget().GetSpec().Templates = n.normalizeTemplates(n.ctx.GetTarget().GetSpec().Templates)
	// UseTemplates already done
}

// finalize performs some finalization tasks, which should be done after CHI is normalized
func (n *Normalizer) finalize() {
	n.ctx.GetTarget().Fill()
	n.ctx.GetTarget().WalkHosts(func(host *apiChi.Host) error {
		n.hostApplyHostTemplateSpecifiedOrDefault(host)
		return nil
	})
	n.fillCHIAddressInfo()
}

// fillCHIAddressInfo
func (n *Normalizer) fillCHIAddressInfo() {
	n.ctx.GetTarget().WalkHosts(func(host *apiChi.Host) error {
		host.Runtime.Address.StatefulSet = n.namer.Name(interfaces.NameStatefulSet, host)
		host.Runtime.Address.FQDN = n.namer.Name(interfaces.NameFQDN, host)
		return nil
	})
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	//endpoint := CreateCHIServiceFQDN(n.ctx.chi)
	//pods := make([]string, 0)
	//fqdns := make([]string, 0)
	//n.ctx.chi.WalkHosts(func(host *apiChi.Host) error {
	//	pods = append(pods, CreatePodName(host))
	//	fqdns = append(fqdns, CreateFQDN(host))
	//	return nil
	//})
	//ip, _ := chop.Get().ConfigManager.GetRuntimeParam(apiChi.OPERATOR_POD_IP)
	//n.ctx.chi.FillStatus(endpoint, pods, fqdns, ip)
}

func isNamespaceDomainPatternValid(namespaceDomainPattern *types.String) bool {
	if strings.Count(namespaceDomainPattern.Value(), "%s") > 1 {
		return false
	} else {
		return true
	}
}

// normalizeNamespaceDomainPattern normalizes .spec.namespaceDomainPattern
func (n *Normalizer) normalizeNamespaceDomainPattern(namespaceDomainPattern *types.String) *types.String {
	if isNamespaceDomainPatternValid(namespaceDomainPattern) {
		return namespaceDomainPattern
	}
	// In case namespaceDomainPattern is not valid - do not use it
	return nil
}

// normalizeDefaults normalizes .spec.defaults
func (n *Normalizer) normalizeDefaults(defaults *apiChi.ChiDefaults) *apiChi.ChiDefaults {
	if defaults == nil {
		defaults = apiChi.NewChiDefaults()
	}
	// Set defaults for CHI object properties
	defaults.ReplicasUseFQDN = defaults.ReplicasUseFQDN.Normalize(false)
	// Ensure field
	if defaults.DistributedDDL == nil {
		//defaults.DistributedDDL = api.NewDistributedDDL()
	}
	// Ensure field
	if defaults.StorageManagement == nil {
		defaults.StorageManagement = apiChi.NewStorageManagement()
	}
	// Ensure field
	if defaults.Templates == nil {
		//defaults.Templates = api.NewChiTemplateNames()
	}
	defaults.Templates.HandleDeprecatedFields()
	return defaults
}

// normalizeConfiguration normalizes .spec.configuration
func (n *Normalizer) normalizeConfiguration(conf *apiChk.Configuration) *apiChk.Configuration {
	if conf == nil {
		conf = apiChk.NewConfiguration()
	}

	n.normalizeConfigurationAllSettingsBasedSections(conf)
	conf.Clusters = n.normalizeClusters(conf.Clusters)
	return conf
}

// normalizeConfigurationAllSettingsBasedSections normalizes Settings-based configuration
func (n *Normalizer) normalizeConfigurationAllSettingsBasedSections(conf *apiChk.Configuration) {
	conf.Settings = n.normalizeConfigurationSettings(conf.Settings)
	conf.Files = n.normalizeConfigurationFiles(conf.Files)
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *apiChi.Templates) *apiChi.Templates {
	if templates == nil {
		return nil
	}

	n.normalizeHostTemplates(templates)
	n.normalizePodTemplates(templates)
	n.normalizeVolumeClaimTemplates(templates)
	n.normalizeServiceTemplates(templates)
	return templates
}

func (n *Normalizer) normalizeHostTemplates(templates *apiChi.Templates) {
	for i := range templates.HostTemplates {
		n.normalizeHostTemplate(&templates.HostTemplates[i])
	}
}

func (n *Normalizer) normalizePodTemplates(templates *apiChi.Templates) {
	for i := range templates.PodTemplates {
		n.normalizePodTemplate(&templates.PodTemplates[i])
	}
}

func (n *Normalizer) normalizeVolumeClaimTemplates(templates *apiChi.Templates) {
	for i := range templates.VolumeClaimTemplates {
		n.normalizeVolumeClaimTemplate(&templates.VolumeClaimTemplates[i])
	}
}

func (n *Normalizer) normalizeServiceTemplates(templates *apiChi.Templates) {
	for i := range templates.ServiceTemplates {
		n.normalizeServiceTemplate(&templates.ServiceTemplates[i])
	}
}

// normalizeHostTemplate normalizes .spec.templates.hostTemplates
func (n *Normalizer) normalizeHostTemplate(template *apiChi.HostTemplate) {
	templates.NormalizeHostTemplate(template)
	// Introduce HostTemplate into Index
	n.ctx.GetTarget().GetSpec().GetTemplates().EnsureHostTemplatesIndex().Set(template.Name, template)
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *apiChi.PodTemplate) {
	// TODO need to support multi-cluster
	replicasCount := 1
	if len(n.ctx.GetTarget().GetSpec().Configuration.Clusters) > 0 {
		replicasCount = n.ctx.GetTarget().GetSpec().Configuration.Clusters[0].Layout.ReplicasCount
	}
	templates.NormalizePodTemplate(replicasCount, template)
	// Introduce PodTemplate into Index
	n.ctx.GetTarget().GetSpec().GetTemplates().EnsurePodTemplatesIndex().Set(template.Name, template)
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *apiChi.VolumeClaimTemplate) {
	templates.NormalizeVolumeClaimTemplate(template)
	// Introduce VolumeClaimTemplate into Index
	n.ctx.GetTarget().GetSpec().GetTemplates().EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *apiChi.ServiceTemplate) {
	templates.NormalizeServiceTemplate(template)
	// Introduce ServiceClaimTemplate into Index
	n.ctx.GetTarget().GetSpec().GetTemplates().EnsureServiceTemplatesIndex().Set(template.Name, template)
}

// normalizeClusters normalizes clusters
func (n *Normalizer) normalizeClusters(clusters []*apiChk.ChkCluster) []*apiChk.ChkCluster {
	// We need to have at least one cluster available
	clusters = n.ensureClusters(clusters)
	// Normalize all clusters
	for i := range clusters {
		clusters[i] = n.normalizeCluster(clusters[i])
	}
	return clusters
}

// ensureClusters
func (n *Normalizer) ensureClusters(clusters []*apiChk.ChkCluster) []*apiChk.ChkCluster {
	// May be we have cluster(s) available
	if len(clusters) > 0 {
		return clusters
	}

	// In case no clusters available, we may want to create a default one
	if n.ctx.Options().WithDefaultCluster {
		return []*apiChk.ChkCluster{
			commonCreator.CreateCluster(interfaces.ClusterCHKDefault).(*apiChk.ChkCluster),
		}
	}

	// Nope, no clusters expected
	return nil
}

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *apiChi.Settings) *apiChi.Settings {
	return settings.
		Ensure().
		MergeFrom(config.DefaultSettings(n.ctx.GetTarget().GetSpec().GetPath())).
		Normalize()
}

// normalizeConfigurationFiles normalizes .spec.configuration.files
func (n *Normalizer) normalizeConfigurationFiles(files *apiChi.Settings) *apiChi.Settings {
	if files == nil {
		return nil
	}
	files.Normalize()

	files.WalkSafe(func(key string, setting *apiChi.Setting) {
		subst_settings.SubstSettingsFieldWithMountedFile(n.ctx, files, key)
	})

	return files
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *apiChk.ChkCluster) *apiChk.ChkCluster {
	// Ensure cluster
	if cluster == nil {
		cluster = commonCreator.CreateCluster(interfaces.ClusterCHKDefault).(*apiChk.ChkCluster)
	}

	// Runtime has to be prepared first
	cluster.GetRuntime().SetCR(n.ctx.GetTarget())

	// Then we need to inherit values from the parent

	// Inherit from .spec.configuration.files
	cluster.InheritFilesFrom(n.ctx.GetTarget())
	// Inherit from .spec.defaults
	cluster.InheritTemplatesFrom(n.ctx.GetTarget())

	cluster.Settings = n.normalizeConfigurationSettings(cluster.Settings)
	cluster.Files = n.normalizeConfigurationFiles(cluster.Files)

	// Ensure layout
	if cluster.Layout == nil {
		cluster.Layout = apiChk.NewChkClusterLayout()
	}
	cluster.FillShardReplicaSpecified()
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)
	n.ensureClusterLayoutShards(cluster.Layout)
	n.ensureClusterLayoutReplicas(cluster.Layout)

	createHostsField(cluster)
	//n.appendClusterSecretEnvVar(cluster)

	// Loop over all shards and replicas inside shards and fill structure
	cluster.WalkShards(func(index int, shard apiChi.IShard) error {
		n.normalizeShard(shard.(*apiChk.ChkShard), cluster, index)
		return nil
	})

	cluster.WalkReplicas(func(index int, replica *apiChk.ChkReplica) error {
		n.normalizeReplica(replica, cluster, index)
		return nil
	})

	cluster.Layout.HostsField.WalkHosts(func(shard, replica int, host *apiChi.Host) error {
		n.normalizeHost(host, cluster.GetShard(shard), cluster.GetReplica(replica), cluster, shard, replica)
		return nil
	})

	return cluster
}

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(clusterLayout *apiChk.ChkClusterLayout) *apiChk.ChkClusterLayout {
	// Ensure layout
	if clusterLayout == nil {
		clusterLayout = apiChk.NewChkClusterLayout()
	}

	// clusterLayout.ShardsCount
	// and
	// clusterLayout.ReplicasCount
	// must represent max number of shards and replicas requested respectively

	// Deal with unspecified ShardsCount
	if clusterLayout.ShardsCount == 0 {
		// We need to have at least one Shard
		clusterLayout.ShardsCount = 1
	}

	// Adjust layout.ShardsCount to max known count

	if len(clusterLayout.Shards) > clusterLayout.ShardsCount {
		// We have more explicitly specified shards than count specified.
		// Need to adjust.
		clusterLayout.ShardsCount = len(clusterLayout.Shards)
	}

	// Let's look for explicitly specified Shards in Layout.Replicas
	for i := range clusterLayout.Replicas {
		replica := &clusterLayout.Replicas[i]

		if replica.ShardsCount > clusterLayout.ShardsCount {
			// We have Shards number specified explicitly in this replica,
			// and this replica has more shards than specified in cluster.
			// Well, enlarge cluster shards count
			clusterLayout.ShardsCount = replica.ShardsCount
		}

		if len(replica.Hosts) > clusterLayout.ShardsCount {
			// We have more explicitly specified shards than count specified.
			// Well, enlarge cluster shards count
			clusterLayout.ShardsCount = len(replica.Hosts)
		}
	}

	// Deal with unspecified ReplicasCount
	if clusterLayout.ReplicasCount == 0 {
		// We need to have at least one Replica
		clusterLayout.ReplicasCount = 1
	}

	// Adjust layout.ReplicasCount to max known count

	if len(clusterLayout.Replicas) > clusterLayout.ReplicasCount {
		// We have more explicitly specified replicas than count specified.
		// Well, enlarge cluster replicas count
		clusterLayout.ReplicasCount = len(clusterLayout.Replicas)
	}

	// Let's look for explicitly specified Replicas in Layout.Shards
	for i := range clusterLayout.Shards {
		shard := &clusterLayout.Shards[i]

		if shard.ReplicasCount > clusterLayout.ReplicasCount {
			// We have Replicas number specified explicitly in this shard
			// Well, enlarge cluster replicas count
			clusterLayout.ReplicasCount = shard.ReplicasCount
		}

		if len(shard.Hosts) > clusterLayout.ReplicasCount {
			// We have more explicitly specified replicas than count specified.
			// Well, enlarge cluster replicas count
			clusterLayout.ReplicasCount = len(shard.Hosts)
		}
	}

	return clusterLayout
}

// ensureClusterLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureClusterLayoutShards(layout *apiChk.ChkClusterLayout) {
	// Disposition of shards in slice would be
	// [explicitly specified shards 0..N, N+1..layout.ShardsCount-1 empty slots for to-be-filled shards]

	// Some (may be all) shards specified, need to append assumed (unspecified, but expected to exist) shards
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Shards) < layout.ShardsCount {
		layout.Shards = append(layout.Shards, apiChk.ChkShard{})
	}
}

// ensureClusterLayoutReplicas ensures slice layout.Replicas is in place
func (n *Normalizer) ensureClusterLayoutReplicas(layout *apiChk.ChkClusterLayout) {
	// Disposition of replicas in slice would be
	// [explicitly specified replicas 0..N, N+1..layout.ReplicasCount-1 empty slots for to-be-filled replicas]

	// Some (may be all) replicas specified, need to append assumed (unspecified, but expected to exist) replicas
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Replicas) < layout.ReplicasCount {
		layout.Replicas = append(layout.Replicas, apiChk.ChkReplica{})
	}
}

// normalizeShard normalizes a shard - walks over all fields
func (n *Normalizer) normalizeShard(shard *apiChk.ChkShard, cluster *apiChk.ChkCluster, shardIndex int) {
	n.normalizeShardName(shard, shardIndex)
	n.normalizeShardWeight(shard)
	// For each shard of this normalized cluster inherit from cluster
	shard.InheritSettingsFrom(cluster)
	shard.Settings = n.normalizeConfigurationSettings(shard.Settings)
	shard.InheritFilesFrom(cluster)
	shard.Files = n.normalizeConfigurationFiles(shard.Files)
	shard.InheritTemplatesFrom(cluster)
	// Normalize Replicas
	n.normalizeShardReplicasCount(shard, cluster.Layout.ReplicasCount)
	n.normalizeShardHosts(shard, cluster, shardIndex)
	// Internal replication uses ReplicasCount thus it has to be normalized after shard ReplicaCount normalized
	//n.normalizeShardInternalReplication(shard)
}

// normalizeReplica normalizes a replica - walks over all fields
func (n *Normalizer) normalizeReplica(replica *apiChk.ChkReplica, cluster *apiChk.ChkCluster, replicaIndex int) {
	n.normalizeReplicaName(replica, replicaIndex)
	// For each replica of this normalized cluster inherit from cluster
	replica.InheritSettingsFrom(cluster)
	replica.Settings = n.normalizeConfigurationSettings(replica.Settings)
	replica.InheritFilesFrom(cluster)
	replica.Files = n.normalizeConfigurationFiles(replica.Files)
	replica.InheritTemplatesFrom(cluster)
	// Normalize Shards
	n.normalizeReplicaShardsCount(replica, cluster.Layout.ShardsCount)
	n.normalizeReplicaHosts(replica, cluster, replicaIndex)
}

// normalizeShardReplicasCount ensures shard.ReplicasCount filled properly
func (n *Normalizer) normalizeShardReplicasCount(shard *apiChk.ChkShard, layoutReplicasCount int) {
	if shard.ReplicasCount > 0 {
		// Shard has explicitly specified number of replicas
		return
	}

	// Here we have shard.ReplicasCount = 0,
	// meaning that shard does not have explicitly specified number of replicas.
	// We need to fill it.

	// Look for explicitly specified Replicas first
	if len(shard.Hosts) > 0 {
		// We have Replicas specified as a slice and no other replicas count provided,
		// this means we have explicitly specified replicas only and exact ReplicasCount is known
		shard.ReplicasCount = len(shard.Hosts)
		return
	}

	// No shard.ReplicasCount specified, no replicas explicitly provided,
	// so we have to use ReplicasCount from layout
	shard.ReplicasCount = layoutReplicasCount
}

// normalizeReplicaShardsCount ensures replica.ShardsCount filled properly
func (n *Normalizer) normalizeReplicaShardsCount(replica *apiChk.ChkReplica, layoutShardsCount int) {
	if replica.ShardsCount > 0 {
		// Replica has explicitly specified number of shards
		return
	}

	// Here we have replica.ShardsCount = 0, meaning that
	// replica does not have explicitly specified number of shards - need to fill it

	// Look for explicitly specified Shards first
	if len(replica.Hosts) > 0 {
		// We have Shards specified as a slice and no other shards count provided,
		// this means we have explicitly specified shards only and exact ShardsCount is known
		replica.ShardsCount = len(replica.Hosts)
		return
	}

	// No replica.ShardsCount specified, no shards explicitly provided, so we have to
	// use ShardsCount from layout
	replica.ShardsCount = layoutShardsCount
}

// normalizeShardName normalizes shard name
func (n *Normalizer) normalizeShardName(shard *apiChk.ChkShard, index int) {
	if (len(shard.Name) > 0) && !commonNamer.IsAutoGeneratedShardName(shard.Name, shard, index) {
		// Has explicitly specified name already
		return
	}

	shard.Name = n.namer.Name(interfaces.NameShard, shard, index)
}

// normalizeReplicaName normalizes replica name
func (n *Normalizer) normalizeReplicaName(replica *apiChk.ChkReplica, index int) {
	if (len(replica.Name) > 0) && !commonNamer.IsAutoGeneratedReplicaName(replica.Name, replica, index) {
		// Has explicitly specified name already
		return
	}

	replica.Name = n.namer.Name(interfaces.NameReplica, replica, index)
}

// normalizeShardName normalizes shard weight
func (n *Normalizer) normalizeShardWeight(shard *apiChk.ChkShard) {
}

// normalizeShardHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeShardHosts(shard *apiChk.ChkShard, cluster *apiChk.ChkCluster, shardIndex int) {
	// Use hosts from HostsField
	shard.Hosts = nil
	for len(shard.Hosts) < shard.ReplicasCount {
		// We still have some assumed hosts in this shard - let's add it as replicaIndex
		replicaIndex := len(shard.Hosts)
		// Check whether we have this host in HostsField
		host := cluster.GetOrCreateHost(shardIndex, replicaIndex)
		shard.Hosts = append(shard.Hosts, host)
	}
}

// normalizeReplicaHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeReplicaHosts(replica *apiChk.ChkReplica, cluster *apiChk.ChkCluster, replicaIndex int) {
	// Use hosts from HostsField
	replica.Hosts = nil
	for len(replica.Hosts) < replica.ShardsCount {
		// We still have some assumed hosts in this replica - let's add it as shardIndex
		shardIndex := len(replica.Hosts)
		// Check whether we have this host in HostsField
		host := cluster.GetOrCreateHost(shardIndex, replicaIndex)
		replica.Hosts = append(replica.Hosts, host)
	}
}
