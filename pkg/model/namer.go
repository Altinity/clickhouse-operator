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
	"fmt"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
	apps "k8s.io/api/apps/v1"
	"strconv"
	"strings"
)

const (
	namePartChiMaxLen     = 60
	namePartClusterMaxLen = 15
	namePartShardMaxLen   = 15
	namePartReplicaMaxLen = 15
)

const (
	// macrosChiName is a sanitized ClickHouseInstallation name
	macrosChiName = "{chi}"
	// macrosChiID is a sanitized ID made of original ClickHouseInstallation name
	macrosChiID = "{chiID}"

	// macrosClusterName is a sanitized cluster name
	macrosClusterName = "{cluster}"
	// macrosClusterID is a sanitized ID made of original cluster name
	macrosClusterID = "{clusterID}"
	// macrosClusterIndex is an index of the cluster in the CHI - integer number, converted into string
	macrosClusterIndex = "{clusterIndex}"

	// macrosShardName is a sanitized shard name
	macrosShardName = "{shard}"
	// macrosShardID is a sanitized ID made of original shard name
	macrosShardID = "{shardID}"
	// macrosShardIndex is an index of the shard in the cluster - integer number, converted into string
	macrosShardIndex = "{shardIndex}"

	// macrosReplicaName is a sanitized replica name
	macrosReplicaName = "{replica}"
	// macrosReplicaID is a sanitized ID made of original replica name
	macrosReplicaID = "{replicaID}"
	// macrosReplicaIndex is an index of the replica in the shard - integer number, converted into string
	macrosReplicaIndex = "{replicaIndex}"
)

const (
	// chiServiceNamePattern is a template of CHI Service name. "clickhouse-{chi}"
	chiServiceNamePattern = "clickhouse-" + macrosChiName

	// clusterServiceNamePattern is a template of cluster Service name. "cluster-{chi}-{cluster}"
	clusterServiceNamePattern = "cluster-" + macrosChiName + "-" + macrosClusterName

	// shardServiceNamePattern is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	shardServiceNamePattern = "shard-" + macrosChiName + "-" + macrosClusterName + "-" + macrosShardName

	// statefulSetNamePattern is a template of replica's StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{replica}"
	statefulSetNamePattern = "chi-" + macrosChiName + "-" + macrosClusterName + "-" + macrosShardName + "-" + macrosReplicaName

	// statefulSetServiceNamePattern is a template of replica's StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{replica}"
	statefulSetServiceNamePattern = "chi-" + macrosChiName + "-" + macrosClusterName + "-" + macrosShardName + "-" + macrosReplicaName

	// configMapCommonNamePattern is a template of common settings for the CHI ConfigMap. "chi-{chi}-common-configd"
	configMapCommonNamePattern = "chi-" + macrosChiName + "-common-configd"

	// configMapCommonUsersNamePattern is a template of common users settings for the CHI ConfigMap. "chi-{chi}-common-usersd"
	configMapCommonUsersNamePattern = "chi-" + macrosChiName + "-common-usersd"

	// configMapDeploymentNamePattern is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{replica}"
	configMapDeploymentNamePattern = "chi-" + macrosChiName + "-deploy-confd-" + macrosClusterName + "-" + macrosShardName + "-" + macrosReplicaName

	// namespaceDomainPattern presents Domain Name pattern of a namespace
	// In this pattern "%s" is substituted namespace name's value
	// Ex.: my-dev-namespace.svc.cluster.local
	namespaceDomainPattern = "%s.svc.cluster.local"

	// ServiceName.domain.name
	serviceFQDNPattern = "%s" + "." + namespaceDomainPattern

	// podFQDNPattern consists of 3 parts:
	// 1. nameless service of of stateful set
	// 2. namespace name
	// Hostname.domain.name
	podFQDNPattern = "%s" + "." + namespaceDomainPattern

	// podNamePattern is a name of a Pod as ServiceName-0
	podNamePattern = "%s-0"
)

// sanitize makes string fulfil kubernetes naming restrictions
// String can't end with '-', '_' and '.'
func sanitize(s string) string {
	return strings.Trim(s, "-_.")
}

func namePartChiName(name string) string {
	return sanitize(util.StringHead(name, namePartChiMaxLen))
}

func namePartChiNameID(name string) string {
	return util.CreateStringID(name, namePartChiMaxLen)
}

func namePartClusterName(name string) string {
	return sanitize(util.StringHead(name, namePartClusterMaxLen))
}

func namePartClusterNameID(name string) string {
	return util.CreateStringID(name, namePartClusterMaxLen)
}

func namePartShardName(name string) string {
	return sanitize(util.StringHead(name, namePartShardMaxLen))
}

func namePartShardNameID(name string) string {
	return util.CreateStringID(name, namePartShardMaxLen)
}

func namePartReplicaName(name string) string {
	return sanitize(util.StringHead(name, namePartReplicaMaxLen))
}

func namePartReplicaNameID(name string) string {
	return util.CreateStringID(name, namePartReplicaMaxLen)
}

func getNamePartChiName(obj interface{}) string {
	switch obj.(type) {
	case *chop.ClickHouseInstallation:
		chi := obj.(*chop.ClickHouseInstallation)
		return namePartChiName(chi.Name)
	case *chop.ChiCluster:
		cluster := obj.(*chop.ChiCluster)
		return namePartChiName(cluster.Address.ChiName)
	case *chop.ChiShard:
		shard := obj.(*chop.ChiShard)
		return namePartChiName(shard.Address.ChiName)
	case *chop.ChiHost:
		host := obj.(*chop.ChiHost)
		return namePartChiName(host.Address.ChiName)
	}

	return "ERROR"
}

func getNamePartClusterName(obj interface{}) string {
	switch obj.(type) {
	case *chop.ChiCluster:
		cluster := obj.(*chop.ChiCluster)
		return namePartClusterName(cluster.Address.ClusterName)
	case *chop.ChiShard:
		shard := obj.(*chop.ChiShard)
		return namePartClusterName(shard.Address.ClusterName)
	case *chop.ChiHost:
		host := obj.(*chop.ChiHost)
		return namePartClusterName(host.Address.ClusterName)
	}

	return "ERROR"
}

func getNamePartShardName(obj interface{}) string {
	switch obj.(type) {
	case *chop.ChiShard:
		shard := obj.(*chop.ChiShard)
		return namePartShardName(shard.Address.ShardName)
	case *chop.ChiHost:
		host := obj.(*chop.ChiHost)
		return namePartShardName(host.Address.ShardName)
	}

	return "ERROR"
}

func getNamePartReplicaName(host *chop.ChiHost) string {
	return namePartReplicaName(host.Address.ReplicaName)
}

func newNameReplacerChi(chi *chop.ClickHouseInstallation) *strings.Replacer {
	return strings.NewReplacer(
		macrosChiName, namePartChiName(chi.Name),
		macrosChiID, namePartChiNameID(chi.Name),
	)
}

func newNameReplacerCluster(cluster *chop.ChiCluster) *strings.Replacer {
	return strings.NewReplacer(
		macrosChiName, namePartChiName(cluster.Address.ChiName),
		macrosChiID, namePartChiNameID(cluster.Address.ChiName),
		macrosClusterName, namePartClusterName(cluster.Address.ClusterName),
		macrosClusterID, namePartClusterNameID(cluster.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(cluster.Address.ClusterIndex),
	)
}

func newNameReplacerShard(shard *chop.ChiShard) *strings.Replacer {
	return strings.NewReplacer(
		macrosChiName, namePartChiName(shard.Address.ChiName),
		macrosChiID, namePartChiNameID(shard.Address.ChiName),
		macrosClusterName, namePartClusterName(shard.Address.ClusterName),
		macrosClusterID, namePartClusterNameID(shard.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(shard.Address.ClusterIndex),
		macrosShardName, namePartShardName(shard.Address.ShardName),
		macrosShardID, namePartShardNameID(shard.Address.ShardName),
		macrosShardIndex, strconv.Itoa(shard.Address.ShardIndex),
	)
}

func newNameReplacerHost(host *chop.ChiHost) *strings.Replacer {
	return strings.NewReplacer(
		macrosChiName, namePartChiName(host.Address.ChiName),
		macrosChiID, namePartChiNameID(host.Address.ChiName),
		macrosClusterName, namePartClusterName(host.Address.ClusterName),
		macrosClusterID, namePartClusterNameID(host.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(host.Address.ClusterIndex),
		macrosShardName, namePartShardName(host.Address.ShardName),
		macrosShardID, namePartShardNameID(host.Address.ShardName),
		macrosShardIndex, strconv.Itoa(host.Address.ShardIndex),
		macrosReplicaName, namePartReplicaName(host.Address.ReplicaName),
		macrosReplicaID, namePartReplicaNameID(host.Address.ReplicaName),
		macrosReplicaIndex, strconv.Itoa(host.Address.ReplicaIndex),
	)
}

// CreateConfigMapPodName returns a name for a ConfigMap for ClickHouse pod
func CreateConfigMapPodName(host *chop.ChiHost) string {
	return newNameReplacerHost(host).Replace(configMapDeploymentNamePattern)
}

// CreateConfigMapCommonName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonName(chi *chop.ClickHouseInstallation) string {
	return newNameReplacerChi(chi).Replace(configMapCommonNamePattern)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonUsersName(chi *chop.ClickHouseInstallation) string {
	return newNameReplacerChi(chi).Replace(configMapCommonUsersNamePattern)
}

// CreateChiServiceName creates a name of a Installation Service resource
func CreateChiServiceName(chi *chop.ClickHouseInstallation) string {
	if template, ok := chi.GetOwnServiceTemplate(); ok {
		// Service template available
		if template.GenerateName != "" {
			// Service template has explicitly specified service name template
			return newNameReplacerChi(chi).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameReplacerChi(chi).Replace(chiServiceNamePattern)
}

// CreateChiServiceName creates a name of a Installation Service resource
func CreateChiServiceFQDN(chi *chop.ClickHouseInstallation) string {
	return fmt.Sprintf(
		serviceFQDNPattern,
		CreateChiServiceName(chi),
		chi.Namespace,
	)
}

// CreateClusterServiceName returns a name of a cluster's Service
func CreateClusterServiceName(cluster *chop.ChiCluster) string {
	if template, ok := cluster.GetServiceTemplate(); ok {
		// Service template available
		if template.GenerateName != "" {
			// Service template has explicitly specified service name template
			return newNameReplacerCluster(cluster).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameReplacerCluster(cluster).Replace(clusterServiceNamePattern)
}

// CreateShardServiceName returns a name of a shard's Service
func CreateShardServiceName(shard *chop.ChiShard) string {
	if template, ok := shard.GetServiceTemplate(); ok {
		// Service template available
		if template.GenerateName != "" {
			// Service template has explicitly specified service name template
			return newNameReplacerShard(shard).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameReplacerShard(shard).Replace(shardServiceNamePattern)
}

// CreateStatefulSetName creates a name of a StatefulSet for ClickHouse instance
func CreateStatefulSetName(host *chop.ChiHost) string {
	return newNameReplacerHost(host).Replace(statefulSetNamePattern)
}

// CreateStatefulSetServiceName returns a name of a StatefulSet-related Service for ClickHouse instance
func CreateStatefulSetServiceName(host *chop.ChiHost) string {
	if template, ok := host.GetServiceTemplate(); ok {
		// Service template available
		if template.GenerateName != "" {
			// Service template has explicitly specified service name template
			return newNameReplacerHost(host).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameReplacerHost(host).Replace(statefulSetServiceNamePattern)
}

// CreatePodHostname returns a name of a Pod of a ClickHouse instance
func CreatePodHostname(host *chop.ChiHost) string {
	// Pod has no own hostname - redirect to appropriate Service
	return CreateStatefulSetServiceName(host)
}

// CreatePodFQDN creates a fully qualified domain name of a pod
// ss-1eb454-2-0.my-dev-domain.svc.cluster.local
func CreatePodFQDN(host *chop.ChiHost) string {
	return fmt.Sprintf(
		podFQDNPattern,
		CreatePodHostname(host),
		host.Address.Namespace,
	)
}

// CreatePodFQDNsOfCluster creates fully qualified domain names of all pods in a cluster
func CreatePodFQDNsOfCluster(cluster *chop.ChiCluster) []string {
	fqdns := make([]string, 0)
	cluster.WalkHosts(func(host *chop.ChiHost) error {
		fqdns = append(fqdns, CreatePodFQDN(host))
		return nil
	})
	return fqdns
}

// CreatePodFQDNsOfShards creates fully qualified domain names of all pods in a shard
func CreatePodFQDNsOfShard(shard *chop.ChiShard) []string {
	fqdns := make([]string, 0)
	shard.WalkHosts(func(host *chop.ChiHost) error {
		fqdns = append(fqdns, CreatePodFQDN(host))
		return nil
	})
	return fqdns
}

// CreatePodFQDNsOfChi creates fully qualified domain names of all pods in a CHI
func CreatePodFQDNsOfChi(chi *chop.ClickHouseInstallation) []string {
	fqdns := make([]string, 0)
	chi.WalkHosts(func(host *chop.ChiHost) error {
		fqdns = append(fqdns, CreatePodFQDN(host))
		return nil
	})
	return fqdns
}

// CreatePodName create Pod name based on specified StatefulSet or Replica
func CreatePodName(obj interface{}) string {
	switch obj.(type) {
	case *apps.StatefulSet:
		statefulSet := obj.(*apps.StatefulSet)
		return fmt.Sprintf(podNamePattern, statefulSet.Name)
	case *chop.ChiHost:
		host := obj.(*chop.ChiHost)
		return fmt.Sprintf(podNamePattern, CreateStatefulSetName(host))
	}
	return "unknown-type"
}

// CreatePVCName create PVC name from PVC Template and host, to which PVC belongs to
func CreatePVCName(template *chop.ChiVolumeClaimTemplate, host *chop.ChiHost) string {
	return template.Name + "-" + CreatePodName(host)
}
