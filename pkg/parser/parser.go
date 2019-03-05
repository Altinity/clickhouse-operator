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
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"sort"
)

// CreateChiObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
func CreateChiObjects(chi *chiv1.ClickHouseInstallation) (ObjectsMap, []string) {
	var (
		clusters []*chiv1.ChiCluster
		options  genOptions
	)

	// Set defaults for CHI object properties
	setSpecDefaultsReplicationFQDN(chi)
	inheritDeploymentAndSetDefaults(&chi.Spec.Defaults.Deployment, nil)
	clusters, options.deploymentCountMax = getNormalizedClusters(chi)

	// Allocate data structures
	options.ssNames = make(map[string]string)
	options.ssDeployments = make(map[string]*chiv1.ChiDeployment)
	options.macrosDataIndex = make(map[string]shardsIndex)
	options.includeConfigSection = make(map[string]bool)

	// configSections maps section name to section XML config <yandex><macros>...</macros><yandex>
	configSections := make(map[string]string)

	// Generate XMLs
	configSections[remoteServersXML] = genRemoteServersConfig(chi, &options, clusters)
	options.includeConfigSection[zookeeperXML] = includeIfNotEmpty(configSections, zookeeperXML, genZookeeperConfig(chi))
	options.includeConfigSection[usersXML] = includeIfNotEmpty(configSections, usersXML, genUsersConfig(chi))
	options.includeConfigSection[profilesXML] = includeIfNotEmpty(configSections, profilesXML, genProfilesConfig(chi))
	options.includeConfigSection[quotasXML] = includeIfNotEmpty(configSections, quotasXML, genQuotasConfig(chi))
	options.includeConfigSection[settingsXML] = includeIfNotEmpty(configSections, settingsXML, genSettingsConfig(chi))

	// Create objects index
	prefixes := make([]string, 0, len(options.ssNames))
	for p := range options.ssNames {
		prefixes = append(prefixes, p)
	}

	// Create k8s objects (data structures)
	return ObjectsMap{
		ObjectsConfigMaps:   createConfigMapObjects(chi, configSections, &options),
		ObjectsServices:     createServiceObjects(chi, &options),
		ObjectsStatefulSets: createStatefulSetObjects(chi, &options),
	}, prefixes
}

// getNormalizedClusters returns list of "normalized" (converted to basic form) chiv1.ChiCluster objects
// with each deployment usage counters
func getNormalizedClusters(chi *chiv1.ClickHouseInstallation) ([]*chiv1.ChiCluster, chiDeploymentCount) {
	link := make(chan *chiClusterAndDeploymentCount)
	// Loop over all clusters
	for _, cluster := range chi.Spec.Configuration.Clusters {
		go func(chin *chiv1.ClickHouseInstallation, c chiv1.ChiCluster, ch chan<- *chiClusterAndDeploymentCount) {
			// and normalize each cluster
			ch <- getNormalizedClusterLayoutStruct(chi, cluster)
		}(chi, cluster, link)
	}

	clusterCount := len(chi.Spec.Configuration.Clusters)
	clusterList := make([]*chiv1.ChiCluster, 0, clusterCount)
	// deploymentCount maps deployment fingerprint to max among all clusters usage number of this deployment
	deploymentCount := make(chiDeploymentCount)
	for i := 0; i < clusterCount; i++ {
		clusterAndDeploymentCount := <-link
		clusterList = append(clusterList, clusterAndDeploymentCount.cluster)
		// Accumulate deployments usage count. Find each deployment max usage number among all clusters
		deploymentCount.mergeInAndReplaceBiggerValues(clusterAndDeploymentCount.deploymentCount)
	}

	return clusterList, deploymentCount
}

// getNormalizedClusterLayoutStruct returns chiv1.ChiCluster object after normalization procedures
func getNormalizedClusterLayoutStruct(
	chi *chiv1.ClickHouseInstallation,
	originalCluster chiv1.ChiCluster,
) *chiClusterAndDeploymentCount {

	normalizedCluster := &chiv1.ChiCluster{
		Name:       originalCluster.Name,
		Deployment: originalCluster.Deployment,
	}
	inheritDeploymentAndSetDefaults(&normalizedCluster.Deployment, &chi.Spec.Defaults.Deployment)
	deploymentCount := make(chiDeploymentCount)

	switch originalCluster.Layout.Type {
	case clusterLayoutTypeStandard:
		// Standard layout assumes to have 1 shard and 1 replica by default - in case not specified explicitly
		if originalCluster.Layout.ShardsCount == 0 {
			originalCluster.Layout.ShardsCount = 1
		}
		if originalCluster.Layout.ReplicasCount == 0 {
			originalCluster.Layout.ReplicasCount = 1
		}

		// Handle .layout.shards
		normalizedCluster.Layout.Shards = make([]chiv1.ChiClusterLayoutShard, originalCluster.Layout.ShardsCount)
		// Loop over all shards and replicas inside shards and fill structure
		for i := 0; i < originalCluster.Layout.ShardsCount; i++ {
			// Create replicas for each shard
			normalizedCluster.Layout.Shards[i].InternalReplication = stringTrue
			normalizedCluster.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica, originalCluster.Layout.ReplicasCount)

			// Fill each replica
			for j := 0; j < originalCluster.Layout.ReplicasCount; j++ {
				// For each replica of this normalized cluster inherit cluster's Deployment
				inheritDeploymentAndSetDefaults(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment, &normalizedCluster.Deployment)
				// And count how many times this deployment is used
				fingerprint := deploymentToString(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment)
				normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.Fingerprint = fingerprint
				deploymentCount[fingerprint]++
			}
		}

	case clusterLayoutTypeAdvanced:
		// Advanced layout assumes detailed shards definition
		normalizedCluster.Layout.Shards = originalCluster.Layout.Shards

		// Loop over all shards and replicas inside shards and fill structure
		for i := range normalizedCluster.Layout.Shards {
			// For each shard of this normalized cluster inherit cluster's Deployment
			inheritDeploymentAndSetDefaults(&normalizedCluster.Layout.Shards[i].Deployment, &normalizedCluster.Deployment)

			// Advanced layout supports .spec.configuration.clusters.layout.shards.internalReplication
			// with default value set to "true"
			switch normalizedCluster.Layout.Shards[i].InternalReplication {
			case shardInternalReplicationDisabled:
				normalizedCluster.Layout.Shards[i].InternalReplication = stringFalse
			default:
				normalizedCluster.Layout.Shards[i].InternalReplication = stringTrue
			}

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
					inheritDeploymentAndSetDefaults(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment, &normalizedCluster.Layout.Shards[i].Deployment)
					// And count how many times this deployment is used
					fingerprint := deploymentToString(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment)
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
					inheritDeploymentAndSetDefaults(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment, &normalizedCluster.Layout.Shards[i].Deployment)
					// And count how many times this deployment is used
					fingerprint := deploymentToString(&normalizedCluster.Layout.Shards[i].Replicas[j].Deployment)
					normalizedCluster.Layout.Shards[i].Replicas[j].Deployment.Fingerprint = fingerprint
					deploymentCount[fingerprint]++
				}
			}
		}
	}

	return &chiClusterAndDeploymentCount{
		cluster:         normalizedCluster,
		deploymentCount: deploymentCount,
	}
}

// deploymentToString creates string representation of chiv1.ChiDeployment object
// of the following form:
// "PodTemplateName::VolumeClaimTemplate::Scenario::Zone.MatchLabels.Key1=Zone.MatchLabels.Val1::Zone.MatchLabels.Key2=Zone.MatchLabels.Val2"
func deploymentToString(d *chiv1.ChiDeployment) string {
	l := len(d.Zone.MatchLabels)

	keys := make([]string, 0, l)
	for key := range d.Zone.MatchLabels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	a := make([]string, 0, l+1)
	a = append(a, fmt.Sprintf("%s::%s::%s::", d.PodTemplateName, d.VolumeClaimTemplate, d.Scenario))
	// Loop over sorted d.Zone.MatchLabels keys
	for _, key := range keys {
		// Append d.Zone.MatchLabels values
		a = append(a, fmt.Sprintf("%s=%s", key, d.Zone.MatchLabels[key]))
	}

	return strings.Join(a, "::")
}

// setSpecDefaultsReplicationFQDN updates chi.Spec.Defaults section with default values
func setSpecDefaultsReplicationFQDN(chi *chiv1.ClickHouseInstallation) {
	if chi.Spec.Defaults.ReplicasUseFQDN != 1 {
		chi.Spec.Defaults.ReplicasUseFQDN = 0
	}
}

// inheritDeploymentAndSetDefaults updates chiv1.ChiDeployment with default values
func inheritDeploymentAndSetDefaults(d, parent *chiv1.ChiDeployment) {
	if parent != nil {
		// Have parent - copy (a.k.a inherit) unassigned values from parent
		if d.PodTemplateName == "" {
			(*d).PodTemplateName = parent.PodTemplateName
		}
		if d.VolumeClaimTemplate == "" {
			(*d).VolumeClaimTemplate = parent.VolumeClaimTemplate
		}
		if d.Scenario == "" {
			(*d).Scenario = parent.Scenario
		}
		if len(d.Zone.MatchLabels) == 0 {
			zoneCopyFrom(&d.Zone, &parent.Zone)
		}
	} else if d.Scenario == "" {
		// Have no parent, but have Scenario
		(*d).Scenario = deploymentScenarioDefault
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

// mergeInAndReplaceBiggerValues combines chiDeploymentCount object with another one
// and replaces local values with another one's values in case another's value is bigger
func (d chiDeploymentCount) mergeInAndReplaceBiggerValues(another chiDeploymentCount) {

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

// randomString generates random string
func randomString() string {
	b := make([]byte, 3)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(b)
	return hex.EncodeToString(b)
}

// includeIfNotEmpty inserts data into map object using specified key, if not empty value provided
func includeIfNotEmpty(dst map[string]string, key, src string) bool {
	// Do not include empty value
	if src == "" {
		return false
	}

	// Include value by specified key
	dst[key] = src

	return true
}
