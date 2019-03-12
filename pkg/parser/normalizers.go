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

package parser

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"sort"
	"strings"
)

// NormalizedCHI normalizes CHI and all deployments usage counters
func NormalizeCHI(chi *chiv1.ClickHouseInstallation) chiDeploymentCountMap {
	// Set defaults for CHI object properties
	ensureSpecDefaultsReplicasUseFQDN(chi)
	ensureSpecDefaultsDeploymentScenario(chi)

	// deploymentCount maps deployment fingerprint to max among all clusters usage number of this deployment
	deploymentCount := make(chiDeploymentCountMap)

	// Normalize all clusters in this CHI
	for i := range chi.Spec.Configuration.Clusters {
		clusterDeploymentCount := normalizeSpecConfigurationClustersCluster(chi, &chi.Spec.Configuration.Clusters[i])

		// Accumulate deployments usage count. Find each deployment max usage number among all normalizedClusters
		deploymentCount.mergeInAndReplaceBiggerValues(clusterDeploymentCount)
	}

	return deploymentCount
}

// normalizeSpecConfigurationClustersCluster normalizes cluster and returns deployments usage counters for this cluster
func normalizeSpecConfigurationClustersCluster(
	chi *chiv1.ClickHouseInstallation,
	cluster *chiv1.ChiCluster,
) chiDeploymentCountMap {
	deploymentCount := make(chiDeploymentCountMap)

	// Apply default deployment for the whole cluster
	mergeDeployment(&cluster.Deployment, &chi.Spec.Defaults.Deployment)

	// Fill Layout field
	switch cluster.Layout.Type {
	case clusterLayoutTypeStandard:
		// Standard layout assumes to have 1 shard and 1 replica by default - in case not specified explicitly
		normalizeClusterStandardLayoutCounts(&cluster.Layout)

		// Handle .layout.shards
		// cluster of type "Standard" does not have shards specified.
		// So we need to build shards specification from the scratch
		cluster.Layout.Shards = make([]chiv1.ChiClusterLayoutShard, cluster.Layout.ShardsCount)
		// Loop over all shards and replicas inside shards and fill structure
		// .Layout.ShardsCount is provided
		for shardIndex := 0; shardIndex < cluster.Layout.ShardsCount; shardIndex++ {
			// Create replicas for each shard
			// .Layout.ReplicasCount is provided
			cluster.Layout.Shards[shardIndex].InternalReplication = stringTrue
			cluster.Layout.Shards[shardIndex].Replicas = make([]chiv1.ChiClusterLayoutShardReplica, cluster.Layout.ReplicasCount)

			// Fill each replica with data
			for replicaIndex := 0; replicaIndex < cluster.Layout.ReplicasCount; replicaIndex++ {
				// For each replica of this normalized cluster inherit cluster's Deployment
				mergeDeployment(&cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment, &cluster.Deployment)

				// And count how many times this deployment is used
				fingerprint := generateDeploymentFingerprint(chi, &cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment)
				cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment.Fingerprint = fingerprint
				deploymentCount[fingerprint]++
			}
		}

	case clusterLayoutTypeAdvanced:
		// Advanced layout assumes detailed shards definition

		// Loop over all shards and replicas inside shards and fill structure
		for shardIndex := range cluster.Layout.Shards {
			// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
			// with default value set to "true"
			normalizeClusterAdvancedLayoutShardsInternalReplication(&cluster.Layout.Shards[shardIndex])

			// For each shard of this normalized cluster inherit cluster's Deployment
			mergeDeployment(&cluster.Layout.Shards[shardIndex].Deployment, &cluster.Deployment)

			switch cluster.Layout.Shards[shardIndex].DefinitionType {
			case shardDefinitionTypeReplicasCount:
				// Define shards by replicas count:
				//      layout:
				//        type: Advanced
				//        shards:
				//
				//        - definitionType: ReplicasCount
				//          replicasCount: 2
				// This means no replicas provided explicitly, let's create replicas
				cluster.Layout.Shards[shardIndex].Replicas = make([]chiv1.ChiClusterLayoutShardReplica, cluster.Layout.Shards[shardIndex].ReplicasCount)

				// Fill each newly created replica
				for replicaIndex := 0; replicaIndex < cluster.Layout.Shards[shardIndex].ReplicasCount; replicaIndex++ {
					// Inherit deployment
					mergeDeployment(&cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment, &cluster.Layout.Shards[shardIndex].Deployment)

					// And count how many times this deployment is used
					fingerprint := generateDeploymentFingerprint(chi, &cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment)
					cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment.Fingerprint = fingerprint
					deploymentCount[fingerprint]++
				}

			default:
				// Define shards by replicas explicitly:
				//        - definitionType: Replicas
				//          replicas:
				//          - port: 9000
				//            deployment:
				//              scenario: Default
				//          - deployment:
				//              scenario: NodeMonopoly # 1 pod (CH server instance) per node (zone can be a set of n nodes) -> podAntiAffinity
				//              zone:
				//                matchLabels:
				//                  clickhouse.altinity.com/zone: zone4
				//                  clickhouse.altinity.com/kind: ssd
				//              podTemplate: clickhouse-v18.16.1
				// This means replicas provided explicitly, no need to create, just to normalize
				// Fill each replica
				for replicaIndex := range cluster.Layout.Shards[shardIndex].Replicas {
					// Inherit deployment
					mergeDeployment(&cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment, &cluster.Layout.Shards[shardIndex].Deployment)

					// And count how many times this deployment is used
					fingerprint := generateDeploymentFingerprint(chi, &cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment)
					cluster.Layout.Shards[shardIndex].Replicas[replicaIndex].Deployment.Fingerprint = fingerprint
					deploymentCount[fingerprint]++
				}
			}
		}
	}

	return deploymentCount
}

// normalizeClusterStandardLayoutCounts ensures at least 1 shard and 1 replica counters
func normalizeClusterStandardLayoutCounts(layout *chiv1.ChiClusterLayout) error {
	// Standard layout assumes to have 1 shard and 1 replica by default - in case not specified explicitly
	if layout.ShardsCount == 0 {
		layout.ShardsCount = 1
	}
	if layout.ReplicasCount == 0 {
		layout.ReplicasCount = 1
	}

	return nil
}

// normalizeClusterAdvancedLayoutShardsInternalReplication ensures reasonable values in
// .spec.configuration.clusters.layout.shards.internalReplication
func normalizeClusterAdvancedLayoutShardsInternalReplication(shard *chiv1.ChiClusterLayoutShard) error {
	// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
	// with default value set to "true"
	if shard.InternalReplication == shardInternalReplicationDisabled {
		shard.InternalReplication = stringFalse
	} else {
		shard.InternalReplication = stringTrue
	}

	return nil
}

// generateDeploymentString creates string representation
// of chiv1.ChiDeployment object of the following form:
// "PodTemplate::VolumeClaimTemplate::Scenario::Zone.MatchLabels.Key1=Zone.MatchLabels.Val1::Zone.MatchLabels.Key2=Zone.MatchLabels.Val2"
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object
// and they will have the same deployment string representation
func generateDeploymentString(d *chiv1.ChiDeployment) string {
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

// generateDeploymentFingerprint creates fingerprint
// of chiv1.ChiDeployment object located inside chiv1.ClickHouseInstallation
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object
// and they will have the same fingerprint
func generateDeploymentFingerprint(chi *chiv1.ClickHouseInstallation, d *chiv1.ChiDeployment) string {
	hasher := sha1.New()
	hasher.Write([]byte(chi.Namespace + chi.Name + generateDeploymentString(d)))
	return hex.EncodeToString(hasher.Sum(nil))
}

// generateDeploymentID generates short-printable deployment ID out of long deployment fingerprint
// Generally, fingerprint is perfectly OK - it is unique for each unique deployment inside ClickHouseInstallation object,
// but it is extremely long and thus can not be used in k8s resources names.
// So we need to produce another - much shorter - unique id for each unique deployment inside ClickHouseInstallation object.
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object and they will have the same
// deployment fingerprint and thus deployment id. This is addressed by FullDeploymentID, which is unique for each
// deployment inside ClickHouseInstallation object
func generateDeploymentID(fingerprint string) string {
	// Extract last 10 chars of fingerprint
	return fingerprint[len(fingerprint)-10:]
	//return randomString()
}

// generateFullDeploymentID generates full deployment ID out of deployment ID
// Full Deployment ID is unique for each deployment inside ClickHouseInstallation object and can be used for naming.
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object and they will have the same
// deployment fingerprint and thus deployment id. This is addressed by FullDeploymentID, which is unique for each
// deployment inside ClickHouseInstallation object
func generateFullDeploymentID(deploymentID string, index int) string {
	// 1eb454-2 (deployment id - sequential index of this deployment id)
	return fmt.Sprintf(fullDeploymentIDPattern, deploymentID, index)

}

// ensureSpecDefaultsReplicasUseFQDN ensures chi.Spec.Defaults.ReplicasUseFQDN section has proper values
func ensureSpecDefaultsReplicasUseFQDN(chi *chiv1.ClickHouseInstallation) {
	// Acceptable values are 0 and 1
	// So if it is 1 - it is ok and assign 0 for any other values
	if chi.Spec.Defaults.ReplicasUseFQDN == 1 {
		// Acceptable value
		return
	}

	// Assign 0 for any other values (including 0 - rewrite is unimportant)
	chi.Spec.Defaults.ReplicasUseFQDN = 0
}

// ensureSpecDefaultsDeploymentScenario ensures deployment has scenario specified
func ensureSpecDefaultsDeploymentScenario(chi *chiv1.ClickHouseInstallation) {
	if chi.Spec.Defaults.Deployment.Scenario == "" {
		// Have no default deployment scenario specified - specify default one
		chi.Spec.Defaults.Deployment.Scenario = deploymentScenarioDefault
	}
}

// mergeDeployment updates empty fields of chiv1.ChiDeployment with values from src deployment
func mergeDeployment(dst, src *chiv1.ChiDeployment) {
	if src == nil {
		return
	}

	// Have source to merge from - copy locally unassigned values

	if len(dst.Zone.MatchLabels) == 0 {
		zoneCopyFrom(&dst.Zone, &src.Zone)
	}

	if dst.PodTemplate == "" {
		(*dst).PodTemplate = src.PodTemplate
	}

	if dst.VolumeClaimTemplate == "" {
		(*dst).VolumeClaimTemplate = src.VolumeClaimTemplate
	}

	if dst.Scenario == "" {
		(*dst).Scenario = src.Scenario
	}
}

// zoneCopyFrom copies one chiv1.ChiDeploymentZone object into another
func zoneCopyFrom(dst, src *chiv1.ChiDeploymentZone) {
	tmp := make(map[string]string)
	for k, v := range src.MatchLabels {
		tmp[k] = v
	}
	(*dst).MatchLabels = tmp
}
