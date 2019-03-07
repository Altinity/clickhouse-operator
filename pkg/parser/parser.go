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
	"fmt"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"sort"
	"strings"
)

// NormalizedCHI returns list of "normalized" (converted to basic form) chiv1.ChiCluster objects
// with each cluster all deployments usage counters
func NormalizeCHI(chi *chiv1.ClickHouseInstallation) ([]*chiv1.ChiCluster, chiDeploymentCountMap) {
	// Set defaults for CHI object properties
	ensureSpecDefaultsReplicasUseFQDN(chi)
	ensureScenario(&chi.Spec.Defaults.Deployment)

	return getNormalizedClusters(chi)
}

// CreateCHIObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
// and slice of all full deployment ids
func CreateCHIObjects(chi *chiv1.ClickHouseInstallation, clusters []*chiv1.ChiCluster, deploymentCountMax chiDeploymentCountMap) (ObjectsMap, []string) {
	var options genOptions

	options.deploymentCountMax = deploymentCountMax

	// Allocate data structures
	options.fullDeploymentIDToFingerprint = make(map[string]string)
	options.ssDeployments = make(map[string]*chiv1.ChiDeployment)
	options.macrosData = make(map[string]macrosDataShardDescriptionList)
	options.includeConfigSection = make(map[string]bool)

	// configSections maps section name to section XML config such as "<yandex><macros>...</macros><yandex>"
	configSections := make(map[string]string)

	// Generate XMLs
	configSections[filenameRemoteServersXML] = generateRemoteServersConfig(chi, &options, clusters)
	options.includeConfigSection[filenameZookeeperXML] = includeIfNotEmpty(configSections, filenameZookeeperXML, genZookeeperConfig(chi))
	options.includeConfigSection[filenameUsersXML] = includeIfNotEmpty(configSections, filenameUsersXML, genUsersConfig(chi))
	options.includeConfigSection[filenameProfilesXML] = includeIfNotEmpty(configSections, filenameProfilesXML, genProfilesConfig(chi))
	options.includeConfigSection[filenameQuotasXML] = includeIfNotEmpty(configSections, filenameQuotasXML, genQuotasConfig(chi))
	options.includeConfigSection[filenameSettingsXML] = includeIfNotEmpty(configSections, filenameSettingsXML, genSettingsConfig(chi))

	// slice of full deployment ID's
	fullDeploymentIDs := make([]string, 0, len(options.fullDeploymentIDToFingerprint))
	for p := range options.fullDeploymentIDToFingerprint {
		fullDeploymentIDs = append(fullDeploymentIDs, p)
	}

	// Create k8s objects (data structures)
	return ObjectsMap{
		ObjectsConfigMaps:   createConfigMapObjects(chi, configSections, &options),
		ObjectsServices:     createServiceObjects(chi, &options),
		ObjectsStatefulSets: createStatefulSetObjects(chi, &options),
	}, fullDeploymentIDs
}

// getNormalizedClusters returns list of "normalized" (converted to basic form) chiv1.ChiCluster objects
// with each cluster all deployments usage counters
func getNormalizedClusters(chi *chiv1.ClickHouseInstallation) ([]*chiv1.ChiCluster, chiDeploymentCountMap) {
	// deploymentCount maps deployment fingerprint to max among all normalizedClusters usage number of this deployment
	deploymentCount := make(chiDeploymentCountMap)

	// Loop over all clusters and get normalized copy
	normalizedClusters := make([]*chiv1.ChiCluster, 0, len(chi.Spec.Configuration.Clusters))
	for _, cluster := range chi.Spec.Configuration.Clusters {
		normalizedCluster, normalizedClusterDeploymentCount := getNormalizedCluster(chi, cluster)
		normalizedClusters = append(normalizedClusters, normalizedCluster)

		// Accumulate deployments usage count. Find each deployment max usage number among all normalizedClusters
		deploymentCount.mergeInAndReplaceBiggerValues(normalizedClusterDeploymentCount)
	}

	return normalizedClusters, deploymentCount
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

// getNormalizedCluster returns chiv1.ChiCluster object after normalization procedures
func getNormalizedCluster(
	chi *chiv1.ClickHouseInstallation,
	cluster chiv1.ChiCluster,
) (*chiv1.ChiCluster, chiDeploymentCountMap) {
	deploymentCount := make(chiDeploymentCountMap)

	// Prepare partially normalized cluster - no Layout
	normalizedCluster := &chiv1.ChiCluster{
		Name: cluster.Name,
		// no Layout field filled here
		Deployment: cluster.Deployment,
	}
	mergeDeployment(&normalizedCluster.Deployment, &chi.Spec.Defaults.Deployment)

	// Fill Layout field
	switch cluster.Layout.Type {
	case clusterLayoutTypeStandard:
		// Standard layout assumes to have 1 shard and 1 replica by default - in case not specified explicitly
		normalizeClusterStandardLayoutCounts(&cluster.Layout)

		// Handle .layout.shards
		normalizedCluster.Layout.Shards = make([]chiv1.ChiClusterLayoutShard, cluster.Layout.ShardsCount)
		// Loop over all shards and replicas inside shards and fill structure
		for i := 0; i < cluster.Layout.ShardsCount; i++ {
			// Create replicas for each shard
			normalizedCluster.Layout.Shards[i].InternalReplication = stringTrue
			normalizedCluster.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica, cluster.Layout.ReplicasCount)

			// Fill each replica
			for j := 0; j < cluster.Layout.ReplicasCount; j++ {
				// For each replica of this normalized cluster inherit cluster's Deployment
				mergeDeployment(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment, &normalizedCluster.Deployment)

				// And count how many times this deployment is used
				fingerprint := generateDeploymentFingerprint(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment)
				normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.Fingerprint = fingerprint
				deploymentCount[fingerprint]++
			}
		}

	case clusterLayoutTypeAdvanced:
		// Advanced layout assumes detailed shards definition
		normalizedCluster.Layout.Shards = cluster.Layout.Shards

		// Loop over all shards and replicas inside shards and fill structure
		for i := range normalizedCluster.Layout.Shards {
			// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
			// with default value set to "true"
			normalizeClusterAdvancedLayoutShardsInternalReplication(&normalizedCluster.Layout.Shards[i])

			// For each shard of this normalized cluster inherit cluster's Deployment
			mergeDeployment(&normalizedCluster.Layout.Shards[i].Deployment, &normalizedCluster.Deployment)

			if normalizedCluster.Layout.Shards[i].DefinitionType == shardDefinitionTypeReplicasCount {
				// Define shards by replicas count:
				//      layout:
				//        type: Advanced
				//        shards:
				//
				//        - definitionType: ReplicasCount
				//          replicasCount: 2
				// This means no replicas provided explicitly, let's create replicas
				normalizedCluster.Layout.Shards[i].Replicas = make(
					[]chiv1.ChiClusterLayoutShardReplica,
					normalizedCluster.Layout.Shards[i].ReplicasCount,
				)

				// Fill each newly created replica
				for j := 0; j < normalizedCluster.Layout.Shards[i].ReplicasCount; j++ {
					// Inherit deployment
					mergeDeployment(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment, &normalizedCluster.Layout.Shards[i].Deployment)

					// And count how many times this deployment is used
					fingerprint := generateDeploymentFingerprint(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment)
					normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.Fingerprint = fingerprint
					deploymentCount[fingerprint]++
				}
			} else {
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
				//              podTemplateName: clickhouse-v18.16.1
				// This means replicas provided explicitly, no need to create, just to normalize
				// Fill each replica
				for j := range normalizedCluster.Layout.Shards[i].Replicas {
					// Inherit deployment
					mergeDeployment(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment, &normalizedCluster.Layout.Shards[i].Deployment)

					// And count how many times this deployment is used
					fingerprint := generateDeploymentFingerprint(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment)
					normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.Fingerprint = fingerprint
					deploymentCount[fingerprint]++
				}
			}
		}
	}

	return normalizedCluster, deploymentCount
}

// generateDeploymentFingerprint creates string representation
// of chiv1.ChiDeployment object of the following form:
// "PodTemplateName::VolumeClaimTemplate::Scenario::Zone.MatchLabels.Key1=Zone.MatchLabels.Val1::Zone.MatchLabels.Key2=Zone.MatchLabels.Val2"
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object
// and they will have the same deployment fingerprint
func generateDeploymentFingerprint(d *chiv1.ChiDeployment) string {
	zoneMatchLabelsNum := len(d.Zone.MatchLabels)

	// Labels should be sorted key keys
	keys := make([]string, 0, zoneMatchLabelsNum)
	for key := range d.Zone.MatchLabels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	a := make([]string, 0, zoneMatchLabelsNum+1)
	a = append(a, fmt.Sprintf("%s::%s::%s::", d.PodTemplateName, d.VolumeClaimTemplate, d.Scenario))
	// Loop over sorted d.Zone.MatchLabels keys
	for _, key := range keys {
		// Append d.Zone.MatchLabels values
		a = append(a, fmt.Sprintf("%s=%s", key, d.Zone.MatchLabels[key]))
	}

	return strings.Join(a, "::")
}

// generateDeploymentID generates short-printable deployment ID out of long deployment fingerprint
// Generally, fingerprint is perfectly OK - it is unique for each unique deployment inside ClickHouseInstallation object,
// but it is extremely long and thus can not be used in k8s resources names.
// So we need to produce another - much shorter - unique id for each unique deployment inside ClickHouseInstallation object.
// IMPORTANT there can be the same deployments inside ClickHouseInstallation object and they will have the same
// deployment fingerprint and thus deployment id. This is addressed by FullDeploymentID, which is unique for each
// deployment inside ClickHouseInstallation object
func generateDeploymentID(_ /* fingerprint */ string) string {
	return randomString()
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

// ensureScenario ensures deployment has scenario specified
func ensureScenario(d *chiv1.ChiDeployment) {
	if d.Scenario == "" {
		// Have no scenario specified - specify default one
		(*d).Scenario = deploymentScenarioDefault
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

	if dst.PodTemplateName == "" {
		(*dst).PodTemplateName = src.PodTemplateName
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

// mergeInAndReplaceBiggerValues combines chiDeploymentCountMap object with another one
// and replaces local values with another one's values in case another's value is bigger
func (d chiDeploymentCountMap) mergeInAndReplaceBiggerValues(another chiDeploymentCountMap) {

	// Loop over another struct and bring in new OR bigger values
	for key, value := range another {
		_, ok := d[key]

		if !ok {
			// No such key - new key/value pair just - include/merge it in
			d[key] = value
		} else if value > d[key] {
			// Have such key, but "another"'s value is bigger, overwrite local value
			d[key] = value
		}
	}
}
