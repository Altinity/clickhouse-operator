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
	"sort"
	"strings"
)

func ChiCopyAndNormalize(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
	return ChiNormalize(chi.DeepCopy())
}

// ChiNormalize normalizes CHI.
// Returns NamedNumber of deployments number required to satisfy clusters' infrastructure
func ChiNormalize(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
	// Set defaults for CHI object properties
	defaultsNormalizeReplicasUseFQDN(&chi.Spec.Defaults)
	deploymentNormalizeScenario(&chi.Spec.Defaults.Deployment)

	// Normalize all clusters in this CHI
	chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		return clusterNormalize(chi, cluster)
	})
	chi.FillAddressInfo()
	chi.SetKnown()

	return chi, nil
}

// clusterNormalize normalizes cluster and returns deployments usage counters for this cluster
func clusterNormalize(
	chi *chiv1.ClickHouseInstallation,
	cluster *chiv1.ChiCluster,
) error {
	// Apply default deployment for the whole cluster
	deploymentMergeFrom(&cluster.Deployment, &chi.Spec.Defaults.Deployment)

	// Convenience wrapper
	layout := &cluster.Layout

	// Fill Layout field
	switch layout.Type {
	case clusterLayoutTypeStandard:
		// Standard layout assumes to have 1 shard and 1 replica by default - in case not specified explicitly
		layoutNormalizeCounts(layout)

		// Handle .layout.shards
		// cluster of type "Standard" does not have shards specified.
		// So we need to build shards specification from the scratch
		layout.Shards = make([]chiv1.ChiClusterLayoutShard, layout.ShardsCount)
		// Loop over all shards and replicas inside shards and fill structure
		// .Layout.ShardsCount is provided
		for shardIndex := 0; shardIndex < layout.ShardsCount; shardIndex++ {
			// Convenience wrapper
			shard := &layout.Shards[shardIndex]

			// Inherit ReplicasCount
			shard.ReplicasCount = layout.ReplicasCount
			// For .cluster.Layout.Type = Standard internal replication is turned on
			shard.InternalReplication = stringTrue

			// Create replicas for the shard
			// .Layout.ReplicasCount is provided
			shard.Replicas = make([]chiv1.ChiClusterLayoutShardReplica, shard.ReplicasCount)
			shardNormalizeReplicas(shard)
		}

	case clusterLayoutTypeAdvanced:
		// Advanced layout assumes detailed shards definition

		// Loop over all shards and replicas inside shards and fill structure
		for shardIndex := range layout.Shards {
			// Convenience wrapper
			shard := &layout.Shards[shardIndex]

			// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
			// with default value set to "true"
			shardNormalizeInternalReplication(shard)

			// For each shard of this normalized cluster inherit cluster's Deployment
			deploymentMergeFrom(&shard.Deployment, &cluster.Deployment)

			switch shard.DefinitionType {
			case shardDefinitionTypeReplicasCount:
				// Define shards by replicas count:
				//      layout:
				//        type: Advanced
				//        shards:
				//
				//        - definitionType: ReplicasCount
				//          replicasCount: 2
				// This means no replicas provided explicitly, let's create replicas
				// Create replicas for the shard
				// .Layout.ReplicasCount is provided
				shard.Replicas = make([]chiv1.ChiClusterLayoutShardReplica, shard.ReplicasCount)
				shardNormalizeReplicas(shard)

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

				shard.ReplicasCount = len(shard.Replicas)
				// Create replicas for the shard
				// .Layout.ReplicasCount is provided
				shardNormalizeReplicas(shard)
			}
		}
	}

	return nil
}

func shardNormalizeReplicas(shard *chiv1.ChiClusterLayoutShard) {
	// Fill each replica
	for replicaIndex := 0; replicaIndex < shard.ReplicasCount; replicaIndex++ {
		// Convenience wrapper
		replica := &shard.Replicas[replicaIndex]

		replicaNormalisePort(replica)

		// Inherit deployment
		deploymentMergeFrom(&replica.Deployment, &shard.Deployment)
		replica.Deployment.Fingerprint = deploymentGenerateFingerprint(replica, &replica.Deployment)
	}
}

// replicaNormalisePort ensures chiv1.ChiClusterLayoutShardReplica.Port is reasonable
func replicaNormalisePort(r *chiv1.ChiClusterLayoutShardReplica) {
	if r.Port <= 0 {
		r.Port = chDefaultClientPortNumber
	}
}

// layoutNormalizeCounts ensures at least 1 shard and 1 replica counters
func layoutNormalizeCounts(layout *chiv1.ChiClusterLayout) {
	// Standard layout assumes to have 1 shard and 1 replica by default - in case not specified explicitly
	if layout.ShardsCount == 0 {
		layout.ShardsCount = 1
	}
	if layout.ReplicasCount == 0 {
		layout.ReplicasCount = 1
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

// deploymentGenerateID generates short-printable deployment ID out of long deployment fingerprint
// Generally, fingerprint is perfectly OK - it is unique for each unique deployment inside ClickHouseInstallation object,
// but it is extremely long and thus can not be used in k8s resources names.
// So we need to produce another - much shorter - unique id for each unique deployment inside ClickHouseInstallation object.
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object and they will have the same
// deployment fingerprint and thus deployment id. This is addressed by FullDeploymentID, which is unique for each
// deployment inside ClickHouseInstallation object
func deploymentGenerateID(fingerprint string) string {
	// Extract last 10 chars of fingerprint
	return fingerprint[len(fingerprint)-10:]
	//return randomString()
}

// generateFullDeploymentID generates full deployment ID out of deployment ID
// Full Deployment ID is unique for each deployment inside ClickHouseInstallation object and can be used for naming.
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object and they will have the same
// deployment fingerprint and thus deployment id. This is addressed by FullDeploymentID, which is unique for each
// deployment inside ClickHouseInstallation object
func generateFullDeploymentID(replica *chiv1.ChiClusterLayoutShardReplica) string {
	deploymentID := deploymentGenerateID(replica.Deployment.Fingerprint)
	index := replica.Deployment.Index
	// 1eb454-2 (deployment id - sequential index of this deployment id)
	return fmt.Sprintf(fullDeploymentIDPattern, deploymentID, index)
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

// deploymentMergeFrom updates empty fields of chiv1.ChiDeployment with values from src deployment
func deploymentMergeFrom(dst, src *chiv1.ChiDeployment) {
	if src == nil {
		return
	}

	// Have source to merge from - copy locally unassigned values
	// Walk over all fields of ChiDeployment and assign from `src` in case unassigned

	if dst.PodTemplate == "" {
		(*dst).PodTemplate = src.PodTemplate
	}

	if dst.VolumeClaimTemplate == "" {
		(*dst).VolumeClaimTemplate = src.VolumeClaimTemplate
	}

	if len(dst.Zone.MatchLabels) == 0 {
		zoneCopyFrom(&dst.Zone, &src.Zone)
	}

	if dst.Scenario == "" {
		(*dst).Scenario = src.Scenario
	}
}

// zoneCopyFrom copies one chiv1.ChiDeploymentZone object into another
func zoneCopyFrom(dst, src *chiv1.ChiDeploymentZone) {
	if src == nil {
		return
	}

	tmp := make(map[string]string)
	for k, v := range src.MatchLabels {
		tmp[k] = v
	}
	(*dst).MatchLabels = tmp
}
