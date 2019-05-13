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
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"
	"regexp"
	"sort"
	"strings"
)

// ChiApplyTemplateAndNormalize produces ready-to-use CHI object
func ChiApplyTemplateAndNormalize(
	chi *chiv1.ClickHouseInstallation,
	config *config.Config,
) (*chiv1.ClickHouseInstallation, error) {
	if config.ChiTemplate == nil {
		// No template specified
		return ChiNormalize(chi.DeepCopy())
	} else {
		base := config.ChiTemplate.DeepCopy()
		base.MergeFrom(chi)
		return ChiNormalize(base)
	}
}

// ChiNormalize normalizes CHI.
// Returns NamedNumber of deployments number required to satisfy clusters' infrastructure
func ChiNormalize(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
	// Set defaults for CHI object properties
	defaultsNormalizeReplicasUseFQDN(&chi.Spec.Defaults)
	deploymentNormalizeScenario(&chi.Spec.Defaults.Deployment)
	templatesNormalizeVolumeClaimTemplatesNames(chi.Spec.Templates.VolumeClaimTemplates)
	configurationNormalize(&chi.Spec.Configuration)

	// Normalize all clusters in this CHI
	chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		return clusterNormalize(chi, cluster)
	})
	chi.FillAddressInfo()
	chi.WalkReplicas(func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		replica.Config.ZkFingerprint = fingerprint(chi.Spec.Configuration.Zookeeper)
		return nil
	})
	chi.SetKnown()

	return chi, nil
}

func configurationNormalize(conf *chiv1.ChiConfiguration) {
	configurationUsersNormalize(&conf.Users)
	configurationProfilesNormalize(&conf.Profiles)
	configurationQuotasNormalize(&conf.Quotas)
	configurationSettingsNormalize(&conf.Settings)
}

func normalizePath(path string) string {
	// Normalize '//' to '/'
	re := regexp.MustCompile("//+")
	path = re.ReplaceAllString(path, "/")
	// Cut all leading and trailing '/'
	return strings.Trim(path, "/")
}

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

func configurationUsersNormalize(conf *map[string]interface{}) {
	normalizePaths(conf)

	// Extract username from path
	usernameMap := make(map[string]bool)
	for path := range *conf {
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
		if _, ok := (*conf)[username+"/profile"]; !ok {
			// No 'user/profile' section
			(*conf)[username+"/profile"] = "default"
		}
		if _, ok := (*conf)[username+"/quota"]; !ok {
			// No 'user/quota' section
			(*conf)[username+"/quota"] = "default"
		}
		if _, ok := (*conf)[username+"/networks/ip"]; !ok {
			// No 'user/networks/ip' section
			(*conf)[username+"/networks/ip"] = []string{"::/0"}
		}
		_, okPassword := (*conf)[username+"/password"]
		_, okPasswordSHA256 := (*conf)[username+"/password_sha256_hex"]
		if !okPassword && !okPasswordSHA256 {
			// Neither 'password' nor 'password_sha256_hex' are in place
			(*conf)[username+"/password"] = "default"
		}
	}
}

func configurationProfilesNormalize(conf *map[string]interface{}) {
	normalizePaths(conf)
}

func configurationQuotasNormalize(conf *map[string]interface{}) {
	normalizePaths(conf)
}

func configurationSettingsNormalize(conf *map[string]interface{}) {
	normalizePaths(conf)
}

// clusterNormalize normalizes cluster and returns deployments usage counters for this cluster
func clusterNormalize(
	chi *chiv1.ClickHouseInstallation,
	cluster *chiv1.ChiCluster,
) error {
	// Apply default deployment for the whole cluster
	(&cluster.Deployment).MergeFrom(&chi.Spec.Defaults.Deployment)

	// Convenience wrapper
	layout := &cluster.Layout

	layoutNormalizeShardsAndReplicasCount(layout)
	layoutEnsureShards(layout)

	// Loop over all shards and replicas inside shards and fill structure
	// .Layout.ShardsCount is provided
	for shardIndex := range layout.Shards {
		// Convenience wrapper
		shard := &layout.Shards[shardIndex]

		// For each shard of this normalized cluster inherit cluster's Deployment
		(&shard.Deployment).MergeFrom(&cluster.Deployment)

		// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
		// Default value set to "true"
		shardNormalizeInternalReplication(shard)

		shardNormalizeReplicasCount(shard, layout.ReplicasCount)
		shardEnsureReplicas(shard)
		shardNormalizeReplicas(shard)
	}

	return nil
}

// layoutNormalizeShardsAndReplicasCount ensures at least 1 shard and 1 replica counters
func layoutNormalizeShardsAndReplicasCount(layout *chiv1.ChiClusterLayout) {
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

// shardNormalizeReplicasCount ensures shard.ReplicasCount filled properly
func shardNormalizeReplicasCount(shard *chiv1.ChiClusterLayoutShard, layoutReplicasCount int) {
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

// layoutEnsureShards ensures slice layout.Shards is in place
func layoutEnsureShards(layout *chiv1.ChiClusterLayout) {
	if (len(layout.Shards) == 0) && (layout.ShardsCount > 0) {
		layout.Shards = make([]chiv1.ChiClusterLayoutShard, layout.ShardsCount)
	}
}

// shardEnsureReplicas ensures slice shard.Replicas is in place
func shardEnsureReplicas(shard *chiv1.ChiClusterLayoutShard) {
	if (len(shard.Replicas) == 0) && (shard.ReplicasCount > 0) {
		shard.Replicas = make([]chiv1.ChiClusterLayoutShardReplica, shard.ReplicasCount)
	}
}

// shardNormalizeReplicas normalizes all replicas of specified shard
func shardNormalizeReplicas(shard *chiv1.ChiClusterLayoutShard) {
	// Fill each replica
	for replicaIndex := range shard.Replicas {
		// Convenience wrapper
		replica := &shard.Replicas[replicaIndex]

		replicaNormalizePort(replica)

		// Inherit deployment
		(&replica.Deployment).MergeFrom(&shard.Deployment)
		replica.Deployment.Fingerprint = deploymentGenerateFingerprint(replica, &replica.Deployment)
	}
}

// replicaNormalizePort ensures chiv1.ChiClusterLayoutShardReplica.Port is reasonable
func replicaNormalizePort(replica *chiv1.ChiClusterLayoutShardReplica) {
	if replica.Port <= 0 {
		replica.Port = chDefaultClientPortNumber
	}
}

// shardNormalizeInternalReplication ensures reasonable values in
// .spec.configuration.clusters.layout.shards.internalReplication
func shardNormalizeInternalReplication(shard *chiv1.ChiClusterLayoutShard) {
	// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
	// with default value set to "true"
	if shard.InternalReplication == shardInternalReplicationDisabled {
		shard.InternalReplication = stringFalse
	} else {
		shard.InternalReplication = stringTrue
	}
}

// deploymentGenerateString creates string representation
// of chiv1.ChiDeployment object of the following form:
// "PodTemplate::VolumeClaimTemplate::Scenario::Zone.MatchLabels.Key1=Zone.MatchLabels.Val1::Zone.MatchLabels.Key2=Zone.MatchLabels.Val2"
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object
// and they will have the same deployment string representation
func deploymentGenerateString(d *chiv1.ChiDeployment) string {
	zoneMatchLabelsNum := len(d.Zone.MatchLabels)

	// Labels should be sorted key keys
	keys := make([]string, 0, zoneMatchLabelsNum)
	for key := range d.Zone.MatchLabels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	a := make([]string, 0, zoneMatchLabelsNum+1)
	a = append(a, fmt.Sprintf("%s::%s::%s::", d.PodTemplate, d.VolumeClaimTemplate, d.Scenario))
	// Loop over sorted d.Zone.MatchLabels keys
	for _, key := range keys {
		// Append d.Zone.MatchLabels values
		a = append(a, fmt.Sprintf("%s=%s", key, d.Zone.MatchLabels[key]))
	}

	return strings.Join(a, "::")
}

// deploymentGenerateFingerprint creates fingerprint
// of chiv1.ChiDeployment object located inside chiv1.ClickHouseInstallation
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object
// and they will have the same fingerprint
func deploymentGenerateFingerprint(replica *chiv1.ChiClusterLayoutShardReplica, deployment *chiv1.ChiDeployment) string {
	hasher := sha1.New()
	hasher.Write([]byte(replica.Address.ChiName + deploymentGenerateString(deployment)))
	return hex.EncodeToString(hasher.Sum(nil))
}

// defaultsNormalizeReplicasUseFQDN ensures chiv1.ChiDefaults.ReplicasUseFQDN section has proper values
func defaultsNormalizeReplicasUseFQDN(d *chiv1.ChiDefaults) {
	// Acceptable values are 0 and 1
	// So if it is 1 - it is ok and assign 0 for any other values
	if d.ReplicasUseFQDN == 1 {
		// Acceptable value
		return
	}

	// Assign 0 for any other values (including 0 - rewrite is unimportant)
	d.ReplicasUseFQDN = 0
}

// deploymentNormalizeScenario normalizes chiv1.ChiDeployment.Scenario
func deploymentNormalizeScenario(d *chiv1.ChiDeployment) {
	if d.Scenario == "" {
		// Have no default deployment scenario specified - specify default one
		d.Scenario = deploymentScenarioDefault
	}
}

// templatesNormalizeVolumeClaimTemplatesNames normalizes naming of chiv1.ChiVolumeClaimTemplate
func templatesNormalizeVolumeClaimTemplatesNames(volumeClaimTemplates []chiv1.ChiVolumeClaimTemplate) {
	for i := range volumeClaimTemplates {
		// Convenience wrapper
		volumeClaimTemplate := &volumeClaimTemplates[i]
		//   templates:
		//    volumeClaimTemplates:
		//      - name: volumeclaim-template
		//        persistentVolumeClaim:
		//          meta:
		//            name: QWE
		// In case of .templates.volumeClaimTemplates.persistentVolumeClaim.meta.name (QWE in this case) is omitted
		// assign .templates.volumeClaimTemplates.persistentVolumeClaim.meta.name with .templates.volumeClaimTemplates.name
		if volumeClaimTemplate.PersistentVolumeClaim.Name == "" {
			volumeClaimTemplate.PersistentVolumeClaim.Name = volumeClaimTemplate.Name
		}
	}
}
