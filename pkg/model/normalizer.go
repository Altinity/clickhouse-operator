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
	chopconfig "github.com/altinity/clickhouse-operator/pkg/config"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"regexp"
	"strconv"
	"strings"
)

type Normalizer struct {
	config *chopconfig.Config
	chi    *chiv1.ClickHouseInstallation
}

func NewNormalizer(config *chopconfig.Config) *Normalizer {
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
// Returns normalized CHI
func (n *Normalizer) DoChi(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
	n.chi = chi

	// Walk over ChiSpec datatype fields
	n.doDefaults(&n.chi.Spec.Defaults)
	n.doConfiguration(&n.chi.Spec.Configuration)
	// ChiSpec.Templates

	endpoint := CreateChiServiceFQDN(chi)
	pods := make([]string, 0)
	n.chi.WalkReplicas(func(replica *chiv1.ChiReplica) error {
		pods = append(pods, CreatePodName(replica))
		return nil
	})
	n.chi.StatusFill(endpoint, pods)

	return n.chi, nil
}

// doDefaults normalizes .spec.defaults
func (n *Normalizer) doDefaults(defaults *chiv1.ChiDefaults) {
	// Set defaults for CHI object properties
	n.doDefaultsReplicasUseFQDN(defaults)
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

// doClusters normalizes clusters
func (n *Normalizer) doClusters() {

	// Introduce default cluster
	if len(n.chi.Spec.Configuration.Clusters) == 0 {
		n.chi.Spec.Configuration.Clusters = []chiv1.ChiCluster{
			{
				Name: "cluster",
			},
		}
	}

	// Normalize all clusters in this CHI
	n.chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		return n.doCluster(cluster)
	})
	n.chi.FillAddressInfo()
	n.chi.WalkReplicas(func(replica *chiv1.ChiReplica) error {
		replica.Config.ZkFingerprint = fingerprint(n.chi.Spec.Configuration.Zookeeper)
		return nil
	})
}

// doConfigurationZookeeper normalizes .spec.configuration.zookeeper
func (n *Normalizer) doConfigurationZookeeper(zk *chiv1.ChiZookeeperConfig) {
	for i := range zk.Nodes {
		// Convenience wrapper
		node := &zk.Nodes[i]
		if node.Port == 0 {
			node.Port = 2181
		}
	}
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
func (n *Normalizer) doCluster(cluster *chiv1.ChiCluster) error {
	// Inherit PodTemplate from .spec.defaults
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
			// Inherit ReplicasCount from layout
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
		shard.Replicas = make([]chiv1.ChiReplica, shard.ReplicasCount)
	} else {
		// Some (may be all) replicas specified, need to append space for unspecified replicas
		// TODO may be there is better way to append N slots to slice
		for len(shard.Replicas) < shard.ReplicasCount {
			shard.Replicas = append(shard.Replicas, chiv1.ChiReplica{})
		}
	}
}

// doShardReplicas normalizes all replicas of specified shard
func (n *Normalizer) doShardReplicas(shard *chiv1.ChiShard) {
	// Fill each replica
	n.ensureShardReplicas(shard)
	for replicaIndex := range shard.Replicas {
		// Convenience wrapper
		replica := &shard.Replicas[replicaIndex]

		// Normalize a replica
		n.doReplicaName(replica, replicaIndex)
		n.doReplicaPort(replica)
		// Inherit PodTemplate from shard
		replica.InheritTemplates(shard)
	}
}

// doReplicaName normalizes replica name
func (n *Normalizer) doReplicaName(replica *chiv1.ChiReplica, index int) {
	if len(replica.Name) > 0 {
		// Already has a name
		return
	}

	replica.Name = strconv.Itoa(index)
}

// doReplicaPort ensures chiv1.ChiReplica.Port is reasonable
func (n *Normalizer) doReplicaPort(replica *chiv1.ChiReplica) {
	if replica.Port <= 0 {
		replica.Port = chDefaultClientPortNumber
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
