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

package models

import (
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"
	"regexp"
	"strings"
)

type Normalizer struct {
	config *config.Config
}

func NewNormalizer(config *config.Config) *Normalizer {
	return &Normalizer{
		config: config,
	}
}

// CreateTemplatedChi produces ready-to-use CHI object
func (n *Normalizer) CreateTemplatedChi(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
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
// Returns NamedNumber of deployments number required to satisfy clusters' infrastructure
func (n *Normalizer) DoChi(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
	// Set defaults for CHI object properties
	n.doDefaultsReplicasUseFQDN(&chi.Spec.Defaults)
	n.doConfiguration(&chi.Spec.Configuration)

	// Normalize all clusters in this CHI
	chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		return n.doCluster(chi, cluster)
	})
	chi.FillAddressInfo()
	chi.WalkReplicas(func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		replica.Config.ZkFingerprint = fingerprint(chi.Spec.Configuration.Zookeeper)
		return nil
	})
	chi.SetKnown()

	return chi, nil
}

// doConfiguration normalizes .spec.configuration
func (n *Normalizer) doConfiguration(conf *chiv1.ChiConfiguration) {
	// TODO normalize zookeeper
	n.doConfigurationUsers(&conf.Users)
	n.doConfigurationProfiles(&conf.Profiles)
	n.doConfigurationQuotas(&conf.Quotas)
	n.doConfigurationSettings(&conf.Settings)
}

// doConfigurationUsers normalizes .spec.configuration.users
func (n *Normalizer) doConfigurationUsers(users *map[string]interface{}) {
	normalizePaths(users)

	// Extract username from path
	usernameMap := make(map[string]bool)
	for path := range *users {
		// Split 'admin/password'
		tags := strings.Split(path, "/")
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
func (n *Normalizer) doCluster(
	chi *chiv1.ClickHouseInstallation,
	cluster *chiv1.ChiCluster,
) error {
	// Inherit PodTemplate from .spec.defaults
	cluster.InheritTemplates(chi)

	// Convenience wrapper
	layout := &cluster.Layout

	n.doLayoutShardsAndReplicasCount(layout)

	// Loop over all shards and replicas inside shards and fill structure
	// .Layout.ShardsCount is provided
	n.ensureLayoutShards(layout)
	for shardIndex := range layout.Shards {
		// Convenience wrapper
		shard := &layout.Shards[shardIndex]

		// For each shard of this normalized cluster inherit cluster's PodTemplate
		shard.InheritTemplates(cluster)

		// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
		// Default value set to "true"
		n.doShardInternalReplication(shard)
		// Normalize Replicas
		n.doShardReplicasCount(shard, layout.ReplicasCount)
		n.doShardReplicas(shard)
	}

	return nil
}

// doLayoutShardsAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) doLayoutShardsAndReplicasCount(layout *chiv1.ChiClusterLayout) {
	if len(layout.Shards) > 0 {
		// We have Shards specified - ok, this is to be exact ShardsCount
		layout.ShardsCount = len(layout.Shards)
	} else if layout.ShardsCount == 0 {
		// Neither ShardsCount nor Shards specified, assume 1 as default value
		layout.ShardsCount = 1
	} else {
		// ShardsCount specified explicitly, Just use it
	}

	if layout.ReplicasCount == 0 {
		// In case no ReplicasCount specified use 1 as a default value - it will be used in Standard layout only
		layout.ReplicasCount = 1
	}
}

// doShardReplicasCount ensures shard.ReplicasCount filled properly
func (n *Normalizer) doShardReplicasCount(shard *chiv1.ChiClusterLayoutShard, layoutReplicasCount int) {
	if len(shard.Replicas) > 0 {
		// .layout.type: Advanced + definitionType: Replicas
		// We have Replicas specified as slice
		shard.ReplicasCount = len(shard.Replicas)
	} else if shard.ReplicasCount == 0 {
		// We do not have Replicas slice specified
		// We do not have ReplicasCount specified
		// Inherit ReplicasCount from layout
		shard.ReplicasCount = layoutReplicasCount
	} else {
		// shard.ReplicasCount > 0 and is Already specified
		// .layout.type: Advanced + definitionType: ReplicasCount
		// We have Replicas specified as ReplicasCount
		// Nothing to do here
	}
}

// ensureLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureLayoutShards(layout *chiv1.ChiClusterLayout) {
	if (len(layout.Shards) == 0) && (layout.ShardsCount > 0) {
		layout.Shards = make([]chiv1.ChiClusterLayoutShard, layout.ShardsCount)
	}
}

// ensureShardReplicas ensures slice shard.Replicas is in place
func (n *Normalizer) ensureShardReplicas(shard *chiv1.ChiClusterLayoutShard) {
	if (len(shard.Replicas) == 0) && (shard.ReplicasCount > 0) {
		shard.Replicas = make([]chiv1.ChiClusterLayoutShardReplica, shard.ReplicasCount)
	}
}

// doShardReplicas normalizes all replicas of specified shard
func (n *Normalizer) doShardReplicas(shard *chiv1.ChiClusterLayoutShard) {
	// Fill each replica
	n.ensureShardReplicas(shard)
	for replicaIndex := range shard.Replicas {
		// Convenience wrapper
		replica := &shard.Replicas[replicaIndex]
		n.doReplicaPort(replica)
		// Inherit PodTemplate from shard
		replica.InheritTemplates(shard)
	}
}

// doReplicaPort ensures chiv1.ChiClusterLayoutShardReplica.Port is reasonable
func (n *Normalizer) doReplicaPort(replica *chiv1.ChiClusterLayoutShardReplica) {
	if replica.Port <= 0 {
		replica.Port = chDefaultClientPortNumber
	}
}

// doShardInternalReplication ensures reasonable values in
// .spec.configuration.clusters.layout.shards.internalReplication
func (n *Normalizer) doShardInternalReplication(shard *chiv1.ChiClusterLayoutShard) {
	// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
	// with default value set to "true"
	if shard.InternalReplication == shardInternalReplicationDisabled {
		shard.InternalReplication = stringFalse
	} else {
		shard.InternalReplication = stringTrue
	}
}

// doDefaultsReplicasUseFQDN ensures chiv1.ChiDefaults.ReplicasUseFQDN section has proper values
func (n *Normalizer) doDefaultsReplicasUseFQDN(d *chiv1.ChiDefaults) {
	// Acceptable values are 0 and 1
	// So if it is 1 - it is ok and assign 0 for any other values
	if d.ReplicasUseFQDN == 1 {
		// Acceptable value
		return
	}

	// Assign 0 for any other values (including 0 - rewrite is unimportant)
	d.ReplicasUseFQDN = 0
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
