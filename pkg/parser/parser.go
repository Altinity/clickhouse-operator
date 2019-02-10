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
	"sort"
	"strings"
	"time"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// CreateObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
func CreateObjects(chi *chiv1.ClickHouseInstallation) (ObjectsMap, []string) {
	var (
		clusters []*chiv1.ChiCluster
		options  genOptions
	)
	// Setting defaults for CHI object properties
	setDefaults(chi)
	setDeploymentDefaults(&chi.Spec.Defaults.Deployment, nil)
	clusters, options.dRefsMax = getNormalizedClusters(chi)
	// Allocating data structures
	options.ssNames = make(map[string]string)
	options.ssDeployments = make(map[string]*chiv1.ChiDeployment)
	options.macrosDataIndex = make(map[string]shardsIndex)
	options.includes = make(map[string]bool)
	cmData := make(map[string]string)
	// Generating XMLs
	cmData[remoteServersXML] = genRemoteServersConfig(chi, &options, clusters)
	options.includes[zookeeperXML] = includeIfNotEmpty(cmData, zookeeperXML, genZookeeperConfig(chi))
	options.includes[usersXML] = includeIfNotEmpty(cmData, usersXML, genUsersConfig(chi))
	options.includes[profilesXML] = includeIfNotEmpty(cmData, profilesXML, genProfilesConfig(chi))
	options.includes[quotasXML] = includeIfNotEmpty(cmData, quotasXML, genQuotasConfig(chi))
	options.includes[settingsXML] = includeIfNotEmpty(cmData, settingsXML, genSettingsConfig(chi))
	// Creating objects index
	prefixes := make([]string, 0, len(options.ssNames))
	for p := range options.ssNames {
		prefixes = append(prefixes, p)
	}
	// Creating k8s objects (data structures)
	return ObjectsMap{
		ObjectsConfigMaps:   createConfigMapObjects(chi, cmData, &options),
		ObjectsServices:     createServiceObjects(chi, &options),
		ObjectsStatefulSets: createStatefulSetObjects(chi, &options),
	}, prefixes
}

// getNormalizedClusters returns list of "normalized" (converted to basic form) chiv1.ChiCluster objects with additional data
func getNormalizedClusters(chi *chiv1.ClickHouseInstallation) ([]*chiv1.ChiCluster, chiDeploymentRefs) {
	link := make(chan *chiClusterDataLink)
	count := len(chi.Spec.Configuration.Clusters)
	for _, cluster := range chi.Spec.Configuration.Clusters {
		go func(chin *chiv1.ClickHouseInstallation, c chiv1.ChiCluster, ch chan<- *chiClusterDataLink) {
			ch <- getNormalizedClusterLayoutData(chin, c)
		}(chi, cluster, link)
	}
	cList := make([]*chiv1.ChiCluster, 0, count)
	dRefs := make(chiDeploymentRefs)
	for i := 0; i < count; i++ {
		data := <-link
		cList = append(cList, data.cluster)
		dRefs.mergeWith(data.deployments)
	}
	return cList, dRefs
}

// getNormalizedClusterLayoutData returns chiv1.ChiCluster object after normalization procedures
func getNormalizedClusterLayoutData(chi *chiv1.ClickHouseInstallation, c chiv1.ChiCluster) *chiClusterDataLink {
	n := &chiv1.ChiCluster{
		Name:       c.Name,
		Deployment: c.Deployment,
	}
	setDeploymentDefaults(&n.Deployment, &chi.Spec.Defaults.Deployment)
	d := make(chiDeploymentRefs)

	switch c.Layout.Type {
	case clusterLayoutTypeStandard:
		if c.Layout.ReplicasCount == 0 {
			c.Layout.ReplicasCount++
		}
		if c.Layout.ShardsCount == 0 {
			c.Layout.ShardsCount++
		}

		n.Layout.Shards = make([]chiv1.ChiClusterLayoutShard, c.Layout.ShardsCount)
		for i := 0; i < c.Layout.ShardsCount; i++ {
			n.Layout.Shards[i].InternalReplication = stringTrue
			n.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica, c.Layout.ReplicasCount)

			for j := 0; j < c.Layout.ReplicasCount; j++ {
				setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
				d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
			}
		}
	case clusterLayoutTypeAdvanced:
		n.Layout.Shards = c.Layout.Shards
		for i := range n.Layout.Shards {
			setDeploymentDefaults(&n.Layout.Shards[i].Deployment, &n.Deployment)

			switch n.Layout.Shards[i].InternalReplication {
			case shardInternalReplicationDisabled:
				n.Layout.Shards[i].InternalReplication = stringFalse
			default:
				n.Layout.Shards[i].InternalReplication = stringTrue
			}

			if n.Layout.Shards[i].DefinitionType == shardDefinitionTypeReplicasCount {
				n.Layout.Shards[i].Replicas = make([]chiv1.ChiClusterLayoutShardReplica,
					n.Layout.Shards[i].ReplicasCount)

				for j := 0; j < n.Layout.Shards[i].ReplicasCount; j++ {
					setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Layout.Shards[i].Deployment)
					n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
					d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
				}
				continue
			}

			for j := range n.Layout.Shards[i].Replicas {
				setDeploymentDefaults(&n.Layout.Shards[i].Replicas[j].Deployment, &n.Layout.Shards[i].Deployment)
				n.Layout.Shards[i].Replicas[j].Deployment.Key = deploymentToString(&n.Layout.Shards[i].Replicas[j].Deployment)
				d[n.Layout.Shards[i].Replicas[j].Deployment.Key]++
			}
		}
	}

	return &chiClusterDataLink{
		cluster:     n,
		deployments: d,
	}
}

// deploymentToString creates string representation of chiv1.ChiDeployment object
func deploymentToString(d *chiv1.ChiDeployment) string {
	l := len(d.Zone.MatchLabels)
	keys := make([]string, 0, l)
	a := make([]string, 0, l+1)
	a = append(a, fmt.Sprintf("%s::%s::%s::", d.Scenario, d.PodTemplateName, d.VolumeClaimTemplate))
	for k := range d.Zone.MatchLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		a = append(a, d.Zone.MatchLabels[k])
	}
	return strings.Join(a, "::")
}

// setDefaults updates chi.Spec.Defaults section with default values
func setDefaults(chi *chiv1.ClickHouseInstallation) {
	if chi.Spec.Defaults.ReplicasUseFQDN != 1 {
		chi.Spec.Defaults.ReplicasUseFQDN = 0
	}
}

// setDeploymentDefaults updates chiv1.ChiDeployment with default values
func setDeploymentDefaults(d, parent *chiv1.ChiDeployment) {
	if parent != nil {
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
		(*d).Scenario = deploymentScenarioDefault
	}
}

// zoneCopyFrom copies one chiv1.ChiDeploymentZone object into another
func zoneCopyFrom(z, source *chiv1.ChiDeploymentZone) {
	tmp := make(map[string]string)
	for k, v := range source.MatchLabels {
		tmp[k] = v
	}
	(*z).MatchLabels = tmp
}

// mergeWith combines chiDeploymentRefs object with another one
func (d chiDeploymentRefs) mergeWith(another chiDeploymentRefs) {
	for ak, av := range another {
		_, ok := d[ak]
		if !ok || av > d[ak] {
			d[ak] = av
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
func includeIfNotEmpty(dest map[string]string, key, src string) bool {
	if src == "" {
		return false
	}
	dest[key] = src
	return true
}
