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

package model

import (
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"regexp"
	"strconv"
	"strings"
)

type Normalizer struct {
	config             *chiv1.Config
	chi                *chiv1.ClickHouseInstallation
	withDefaultCluster bool
}

func NewNormalizer(config *chiv1.Config) *Normalizer {
	return &Normalizer{
		config: config,
	}
}

// CreateTemplatedChi produces ready-to-use CHI object
func (n *Normalizer) CreateTemplatedChi(chi *chiv1.ClickHouseInstallation, withDefaultCluster bool) (*chiv1.ClickHouseInstallation, error) {
	n.withDefaultCluster = withDefaultCluster
	if n.config.ChiTemplate == nil {
		// No template specified
		return n.DoChi(chi.DeepCopy())
	} else {
		// Template specified
		base := n.config.ChiTemplate.DeepCopy()
		base.MergeFrom(chi)
		return n.DoChi(base)
	}
}

// DoChi normalizes CHI.
// Returns normalized CHI
func (n *Normalizer) DoChi(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
	n.chi = chi

	// Walk over ChiSpec datatype fields
	n.doStop(&n.chi.Spec.Stop)
	n.doDefaults(&n.chi.Spec.Defaults)
	n.doConfiguration(&n.chi.Spec.Configuration)
	n.doTemplates(&n.chi.Spec.Templates)

	n.doStatus()

	return n.chi, nil
}

// doStatus prepares .status section
func (n *Normalizer) doStatus() {
	endpoint := CreateChiServiceFQDN(n.chi)
	pods := make([]string, 0)
	n.chi.WalkHosts(func(host *chiv1.ChiHost) error {
		pods = append(pods, CreatePodName(host))
		return nil
	})
	n.chi.StatusFill(endpoint, pods)
}

// doStop normalizes .spec.stop
func (n *Normalizer) doStop(stop *string) {
	// Set defaults for CHI object properties
	if !util.IsStringBool(*stop) {
		// In case it is unknown value - just use set it to false
		*stop = util.StringBoolFalse
	}
}

// doDefaults normalizes .spec.defaults
func (n *Normalizer) doDefaults(defaults *chiv1.ChiDefaults) {
	// Set defaults for CHI object properties
	n.doDefaultsReplicasUseFQDN(defaults)
	n.doDefaultsTemplates(defaults)
}

// doConfiguration normalizes .spec.configuration
func (n *Normalizer) doConfiguration(conf *chiv1.ChiConfiguration) {
	n.doConfigurationZookeeper(&conf.Zookeeper)
	n.doConfigurationUsers(&conf.Users)
	n.doConfigurationProfiles(&conf.Profiles)
	n.doConfigurationQuotas(&conf.Quotas)
	n.doConfigurationSettings(&conf.Settings)

	// ChiConfiguration.Clusters
	n.doClusters()
}

// doTemplates normalizes .spec.templates
func (n *Normalizer) doTemplates(templates *chiv1.ChiTemplates) {
	for i := range templates.PodTemplates {
		podTemplate := &templates.PodTemplates[i]
		n.doPodTemplate(podTemplate)
	}

	for i := range templates.VolumeClaimTemplates {
		vcTemplate := &templates.VolumeClaimTemplates[i]
		n.doVolumeClaimTemplate(vcTemplate)
	}

	for i := range templates.ServiceTemplates {
		serviceTemplate := &templates.ServiceTemplates[i]
		n.doServiceTemplate(serviceTemplate)
	}
}

// doPodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) doPodTemplate(template *chiv1.ChiPodTemplate) {
	// Name

	// Zone
	if len(template.Zone.Values) == 0 {
		// In case no values specified - no key is reasonable
		template.Zone.Key = ""
	} else if template.Zone.Key == "" {
		// We have values specified, but no key
		// Use default zone key in this case
		template.Zone.Key = "failure-domain.beta.kubernetes.io/zone"
	} else {
		// We have both key and value(s) specified explicitly
	}

	// Distribution
	if template.Distribution == podDistributionOnePerHost {
		// Known distribution, all is fine
	} else {
		// Default Pod Distribution
		template.Distribution = podDistributionUnspecified
	}

	// Spec
	template.Spec.Affinity = n.mergeAffinity(template.Spec.Affinity, n.newAffinity(template))

	// Introduce PodTemplate into Index
	// Ensure map is in place
	if n.chi.Spec.Templates.PodTemplatesIndex == nil {
		n.chi.Spec.Templates.PodTemplatesIndex = make(map[string]*chiv1.ChiPodTemplate)
	}

	n.chi.Spec.Templates.PodTemplatesIndex[template.Name] = template
}

func (n *Normalizer) newAffinity(template *chiv1.ChiPodTemplate) *v1.Affinity {
	nodeAffinity := n.newNodeAffinity(template)
	podAntiAffinity := n.newPodAntiAffinity(template)

	if (nodeAffinity == nil) && (podAntiAffinity == nil) {
		// Neither Affinity nor AntiAffinity specified
		return nil
	}

	return &v1.Affinity{
		NodeAffinity:    nodeAffinity,
		PodAffinity:     nil,
		PodAntiAffinity: podAntiAffinity,
	}
}

func (n *Normalizer) mergeAffinity(dst *v1.Affinity, src *v1.Affinity) *v1.Affinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.Affinity{
			NodeAffinity:    n.mergeNodeAffinity(nil, src.NodeAffinity),
			PodAffinity:     src.PodAffinity,
			PodAntiAffinity: n.mergePodAntiAffinity(nil, src.PodAntiAffinity),
		}
	}

	return dst
}

func (n *Normalizer) newNodeAffinity(template *chiv1.ChiPodTemplate) *v1.NodeAffinity {
	if template.Zone.Key == "" {
		return nil
	}

	return &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					// A list of node selector requirements by node's labels.
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      template.Zone.Key,
							Operator: v1.NodeSelectorOpIn,
							Values:   template.Zone.Values,
						},
					},
					// A list of node selector requirements by node's fields.
					//MatchFields: []v1.NodeSelectorRequirement{
					//	v1.NodeSelectorRequirement{},
					//},
				},
			},
		},

		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
	}
}

func (n *Normalizer) mergeNodeAffinity(dst *v1.NodeAffinity, src *v1.NodeAffinity) *v1.NodeAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	// Check NodeSelectors are available
	if src.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return dst
	}
	if len(src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
		}
	}

	// Copy NodeSelectors
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
			dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
			src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i],
		)
	}

	// Copy PreferredSchedulingTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
			dst.PreferredDuringSchedulingIgnoredDuringExecution,
			src.PreferredDuringSchedulingIgnoredDuringExecution[i],
		)
	}

	return dst
}

func (n *Normalizer) newPodAntiAffinity(template *chiv1.ChiPodTemplate) *v1.PodAntiAffinity {
	if template.Distribution == podDistributionOnePerHost {
		return &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: &v12.LabelSelector{
						// A list of node selector requirements by node's labels.
						MatchExpressions: []v12.LabelSelectorRequirement{
							{
								Key:      LabelApp,
								Operator: v12.LabelSelectorOpIn,
								Values: []string{
									LabelAppValue,
								},
							},
						},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},

			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{},
		}
	}

	return nil
}

func (n *Normalizer) mergePodAntiAffinity(dst *v1.PodAntiAffinity, src *v1.PodAntiAffinity) *v1.PodAntiAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if len(src.RequiredDuringSchedulingIgnoredDuringExecution) == 0 {
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:  []v1.PodAffinityTerm{},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{},
		}
	}

	// Copy PodAffinityTerm
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution {
		dst.RequiredDuringSchedulingIgnoredDuringExecution = append(
			dst.RequiredDuringSchedulingIgnoredDuringExecution,
			src.RequiredDuringSchedulingIgnoredDuringExecution[i],
		)
	}

	// Copy WeightedPodAffinityTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
			dst.PreferredDuringSchedulingIgnoredDuringExecution,
			src.PreferredDuringSchedulingIgnoredDuringExecution[i],
		)
	}

	return dst
}

// doVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) doVolumeClaimTemplate(template *chiv1.ChiVolumeClaimTemplate) {
	// Check name
	// Check PVCReclaimPolicy
	if !template.PVCReclaimPolicy.IsValid() {
		template.PVCReclaimPolicy = chiv1.PVCReclaimPolicyDelete
	}
	// Check Spec

	// Ensure map is in place
	if n.chi.Spec.Templates.VolumeClaimTemplatesIndex == nil {
		n.chi.Spec.Templates.VolumeClaimTemplatesIndex = make(map[string]*chiv1.ChiVolumeClaimTemplate)
	}
	n.chi.Spec.Templates.VolumeClaimTemplatesIndex[template.Name] = template
}

// doServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) doServiceTemplate(template *chiv1.ChiServiceTemplate) {
	// Check name
	// Check GenerateName
	// Check ObjectMeta
	// Check Spec

	// Ensure map is in place
	if n.chi.Spec.Templates.ServiceTemplatesIndex == nil {
		n.chi.Spec.Templates.ServiceTemplatesIndex = make(map[string]*chiv1.ChiServiceTemplate)
	}
	n.chi.Spec.Templates.ServiceTemplatesIndex[template.Name] = template
}

// doClusters normalizes clusters
func (n *Normalizer) doClusters() {

	// Introduce default cluster in case it is required
	if len(n.chi.Spec.Configuration.Clusters) == 0 {
		if n.withDefaultCluster {
			n.chi.Spec.Configuration.Clusters = []chiv1.ChiCluster{
				{
					Name: "cluster",
				},
			}
		} else {
			n.chi.Spec.Configuration.Clusters = []chiv1.ChiCluster{}
		}
	}

	// Normalize all clusters in this CHI
	n.chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		return n.doCluster(cluster)
	})
	n.chi.FillAddressInfo()
	n.chi.FillChiPointer()
	n.chi.WalkHosts(func(host *chiv1.ChiHost) error {
		return n.calcFingerprints(host)
	})
}

// calcFingerprints calculates fingerprints for ClickHouse configuration data
func (n *Normalizer) calcFingerprints(host *chiv1.ChiHost) error {
	host.Config.ZookeeperFingerprint = fingerprint(n.chi.Spec.Configuration.Zookeeper)
	host.Config.SettingsFingerprint = fingerprint(castToSliceOfStrings(n.chi.Spec.Configuration.Settings))

	return nil
}

// doConfigurationZookeeper normalizes .spec.configuration.zookeeper
func (n *Normalizer) doConfigurationZookeeper(zk *chiv1.ChiZookeeperConfig) {
	// In case no ZK port specified - assign default
	for i := range zk.Nodes {
		// Convenience wrapper
		node := &zk.Nodes[i]
		if node.Port == 0 {
			node.Port = zkDefaultPort
		}
	}

	// In case no ZK root specified - assign '/clickhouse/{namespace}/{chi name}'
	//if zk.Root == "" {
	//	zk.Root = fmt.Sprintf(zkDefaultRootTemplate, n.chi.Namespace, n.chi.Name)
	//}
}

// doConfigurationUsers normalizes .spec.configuration.users
func (n *Normalizer) doConfigurationUsers(users *map[string]interface{}) {
	normalizePaths(users)

	// Extract username from path
	usernameMap := make(map[string]bool)
	for path := range *users {
		// Split 'admin/password'
		tags := strings.Split(path, "/")

		// Basic sanity check - need to have at least "username/something" pair
		if len(tags) < 2 {
			// Skip incorrect entry
			continue
		}

		username := tags[0]
		usernameMap[username] = true
	}

	// Ensure "must have" sections are in place, which are
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/password OR user/password_sha256_hex
	for username := range usernameMap {
		if _, ok := (*users)[username+"/profile"]; !ok {
			// No 'user/profile' section
			(*users)[username+"/profile"] = n.config.ChConfigUserDefaultProfile
		}
		if _, ok := (*users)[username+"/quota"]; !ok {
			// No 'user/quota' section
			(*users)[username+"/quota"] = n.config.ChConfigUserDefaultQuota
		}
		if _, ok := (*users)[username+"/networks/ip"]; !ok {
			// No 'user/networks/ip' section
			(*users)[username+"/networks/ip"] = n.config.ChConfigUserDefaultNetworksIP
		}
		_, okPassword := (*users)[username+"/password"]
		_, okPasswordSHA256 := (*users)[username+"/password_sha256_hex"]
		if !okPassword && !okPasswordSHA256 {
			// Neither 'password' nor 'password_sha256_hex' are in place
			(*users)[username+"/password"] = n.config.ChConfigUserDefaultPassword
		}
	}
}

// doConfigurationProfiles normalizes .spec.configuration.profiles
func (n *Normalizer) doConfigurationProfiles(profiles *map[string]interface{}) {
	normalizePaths(profiles)
}

// doConfigurationQuotas normalizes .spec.configuration.quotas
func (n *Normalizer) doConfigurationQuotas(quotas *map[string]interface{}) {
	normalizePaths(quotas)
}

// doConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) doConfigurationSettings(settings *map[string]interface{}) {
	normalizePaths(settings)
}

// doCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) doCluster(cluster *chiv1.ChiCluster) error {
	// Use PodTemplate from .spec.defaults
	cluster.InheritTemplates(n.chi)

	// Convenience wrapper
	layout := &cluster.Layout

	n.doLayoutShardsCountAndReplicasCount(layout)

	// Loop over all shards and replicas inside shards and fill structure
	// .Layout.ShardsCount is provided
	n.ensureLayoutShards(layout)
	for shardIndex := range layout.Shards {
		// Convenience wrapper
		shard := &layout.Shards[shardIndex]

		// Normalize a shard - walk over all fields
		n.doShardName(shard, shardIndex)
		n.doShardWeight(shard)
		n.doShardInternalReplication(shard)
		// For each shard of this normalized cluster inherit cluster's PodTemplate
		shard.InheritTemplates(cluster)
		// Normalize Replicas
		n.doShardReplicasCount(shard, layout.ReplicasCount)
		n.doShardReplicas(shard)
	}

	return nil
}

// doLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) doLayoutShardsCountAndReplicasCount(layout *chiv1.ChiLayout) {
	if layout.ShardsCount == 0 {
		// We can look for explicitly specified Shards slice
		if len(layout.Shards) > 0 {
			// We have Shards specified as slice - ok, this means exact ShardsCount is known
			layout.ShardsCount = len(layout.Shards)
		} else {
			// Neither ShardsCount nor Shards are specified, assume 1 as default value
			layout.ShardsCount = 1
		}
	}

	// Here layout.ShardsCount is specified

	// layout.ReplicasCount is used in case Shard not opinionated how many replicas it (shard) needs
	if layout.ReplicasCount == 0 {
		// In case no ReplicasCount specified use 1 as a default value
		layout.ReplicasCount = 1
	}

	// Here layout.ReplicasCount is specified
}

// doShardReplicasCount ensures shard.ReplicasCount filled properly
func (n *Normalizer) doShardReplicasCount(shard *chiv1.ChiShard, layoutReplicasCount int) {
	if shard.ReplicasCount == 0 {
		// We can look for explicitly specified Replicas
		if len(shard.Replicas) > 0 {
			// We have Replicas specified as slice - ok, this means exact ReplicasCount is known
			shard.ReplicasCount = len(shard.Replicas)
		} else {
			// MergeFrom ReplicasCount from layout
			shard.ReplicasCount = layoutReplicasCount
		}
	}
}

// doShardName normalizes shard name
func (n *Normalizer) doShardName(shard *chiv1.ChiShard, index int) {
	if len(shard.Name) > 0 {
		// Already has a name
		return
	}

	shard.Name = strconv.Itoa(index)
}

// doShardName normalizes shard weight
func (n *Normalizer) doShardWeight(shard *chiv1.ChiShard) {
}

// ensureLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureLayoutShards(layout *chiv1.ChiLayout) {
	if layout.ShardsCount <= 0 {
		// May be need to do something like throw an exception
		return
	}

	// Disposition of shards in slice would be
	// [explicitly specified shards 0..N, N+1..layout.ShardsCount-1 empty slots for to-be-filled shards]

	if len(layout.Shards) == 0 {
		// No shards specified - just allocate required number
		layout.Shards = make([]chiv1.ChiShard, layout.ShardsCount, layout.ShardsCount)
	} else {
		// Some (may be all) shards specified, need to append space for unspecified shards
		// TODO may be there is better way to append N slots to slice
		for len(layout.Shards) < layout.ShardsCount {
			layout.Shards = append(layout.Shards, chiv1.ChiShard{})
		}
	}
}

// ensureShardReplicas ensures slice shard.Replicas is in place
func (n *Normalizer) ensureShardReplicas(shard *chiv1.ChiShard) {
	if shard.ReplicasCount <= 0 {
		// May be need to do something like throw an exception
		return
	}

	if shard.ReplicasCount == 0 {
		// No replicas specified - just allocate required number
		shard.Replicas = make([]chiv1.ChiHost, shard.ReplicasCount)
	} else {
		// Some (may be all) replicas specified, need to append space for unspecified replicas
		// TODO may be there is better way to append N slots to slice
		for len(shard.Replicas) < shard.ReplicasCount {
			shard.Replicas = append(shard.Replicas, chiv1.ChiHost{})
		}
	}
}

// doShardReplicas normalizes all replicas of specified shard
func (n *Normalizer) doShardReplicas(shard *chiv1.ChiShard) {
	// Fill each replica
	n.ensureShardReplicas(shard)
	for replicaIndex := range shard.Replicas {
		// Convenience wrapper
		host := &shard.Replicas[replicaIndex]

		// Normalize a host/replica
		n.doHostName(host, replicaIndex)
		n.doHostPort(host)
		// Use PodTemplate from shard
		host.InheritTemplates(shard)
	}
}

// doHostName normalizes host's name
func (n *Normalizer) doHostName(host *chiv1.ChiHost, index int) {
	if len(host.Name) > 0 {
		// Already has a name, do not change it
		return
	} else {
		// No name specified - name this host
		host.Name = strconv.Itoa(index)
	}
}

// doHostPort ensures chiv1.ChiReplica.Port is reasonable
func (n *Normalizer) doHostPort(host *chiv1.ChiHost) {
	if host.Port <= 0 {
		host.Port = chDefaultClientPortNumber
	}
}

// doShardInternalReplication ensures reasonable values in
// .spec.configuration.clusters.layout.shards.internalReplication
func (n *Normalizer) doShardInternalReplication(shard *chiv1.ChiShard) {
	// Default value set to true
	shard.InternalReplication = util.CastStringBoolToTrueFalse(shard.InternalReplication, true)
}

// doDefaultsReplicasUseFQDN ensures chiv1.ChiDefaults.ReplicasUseFQDN section has proper values
func (n *Normalizer) doDefaultsReplicasUseFQDN(d *chiv1.ChiDefaults) {
	// Default value set to false
	d.ReplicasUseFQDN = util.CastStringBoolToTrueFalse(d.ReplicasUseFQDN, false)
}

// doDefaultsTemplates ensures chiv1.ChiDefaults.Templates section has proper values
func (n *Normalizer) doDefaultsTemplates(d *chiv1.ChiDefaults) {
	d.Templates.HandleDeprecatedFields()
}

// normalizePath normalizes path in .spec.configuration.{users, profiles, quotas, settings} section
// Normalized path looks like 'a/b/c'
func normalizePath(path string) string {
	// Normalize multi-'/' values (like '//') to single-'/'
	re := regexp.MustCompile("//+")
	path = re.ReplaceAllString(path, "/")

	// Cut all leading and trailing '/', so the result would be 'a/b/c'
	return strings.Trim(path, "/")
}

// normalizePaths normalizes paths in whole conf section, like .spec.configuration.users
func normalizePaths(conf *map[string]interface{}) {
	pathsToNormalize := make([]string, 0, 0)

	// Find entries with paths to normalize
	for key := range *conf {
		path := normalizePath(key)
		if len(path) != len(key) {
			// Normalization worked. These paths have to be normalized
			pathsToNormalize = append(pathsToNormalize, key)
		}
	}

	// Add entries with normalized paths
	for _, key := range pathsToNormalize {
		path := normalizePath(key)
		(*conf)[path] = (*conf)[key]
	}

	// Delete entries with un-normalized paths
	for _, key := range pathsToNormalize {
		delete(*conf, key)
	}
}
