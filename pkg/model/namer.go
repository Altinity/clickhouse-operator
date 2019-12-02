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
	// Names context length
	namePartChiMaxLenNamesCtx     = 60
	namePartClusterMaxLenNamesCtx = 15
	namePartShardMaxLenNamesCtx   = 15
	namePartReplicaMaxLenNamesCtx = 15

	// Labels context length
	namePartChiMaxLenLabelsCtx     = 63
	namePartClusterMaxLenLabelsCtx = 63
	namePartShardMaxLenLabelsCtx   = 63
	namePartReplicaMaxLenLabelsCtx = 63
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

const (
	namerContextLabels = "labels"
	namerContextNames  = "names"
)

type namerContext string
type namer struct {
	ctx namerContext
}

func newNamer(ctx namerContext) *namer {
	return &namer{
		ctx: ctx,
	}
}

func (n *namer) namePartChiName(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartChiMaxLenLabelsCtx
	} else {
		len = namePartChiMaxLenNamesCtx
	}
	return sanitize(util.StringHead(name, len))
}

func (n *namer) namePartChiNameID(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartChiMaxLenLabelsCtx
	} else {
		len = namePartChiMaxLenNamesCtx
	}
	return util.CreateStringID(name, len)
}

func (n *namer) namePartClusterName(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartClusterMaxLenLabelsCtx
	} else {
		len = namePartClusterMaxLenNamesCtx
	}
	return sanitize(util.StringHead(name, len))
}

func (n *namer) namePartClusterNameID(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartClusterMaxLenLabelsCtx
	} else {
		len = namePartClusterMaxLenNamesCtx
	}
	return util.CreateStringID(name, len)
}

func (n *namer) namePartShardName(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartShardMaxLenLabelsCtx
	} else {
		len = namePartShardMaxLenNamesCtx
	}
	return sanitize(util.StringHead(name, len))
}

func (n *namer) namePartShardNameID(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartShardMaxLenLabelsCtx
	} else {
		len = namePartShardMaxLenNamesCtx
	}
	return util.CreateStringID(name, len)
}

func (n *namer) namePartReplicaName(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartReplicaMaxLenLabelsCtx
	} else {
		len = namePartReplicaMaxLenNamesCtx
	}
	return sanitize(util.StringHead(name, len))
}

func (n *namer) namePartReplicaNameID(name string) string {
	var len int
	if n.ctx == namerContextLabels {
		len = namePartReplicaMaxLenLabelsCtx
	} else {
		len = namePartReplicaMaxLenNamesCtx
	}
	return util.CreateStringID(name, len)
}

func (n *namer) getNamePartChiName(obj interface{}) string {
	switch obj.(type) {
	case *chop.ClickHouseInstallation:
		chi := obj.(*chop.ClickHouseInstallation)
		return n.namePartChiName(chi.Name)
	case *chop.ChiCluster:
		cluster := obj.(*chop.ChiCluster)
		return n.namePartChiName(cluster.Address.ChiName)
	case *chop.ChiShard:
		shard := obj.(*chop.ChiShard)
		return n.namePartChiName(shard.Address.ChiName)
	case *chop.ChiHost:
		host := obj.(*chop.ChiHost)
		return n.namePartChiName(host.Address.ChiName)
	}

	return "ERROR"
}

func (n *namer) getNamePartClusterName(obj interface{}) string {
	switch obj.(type) {
	case *chop.ChiCluster:
		cluster := obj.(*chop.ChiCluster)
		return n.namePartClusterName(cluster.Address.ClusterName)
	case *chop.ChiShard:
		shard := obj.(*chop.ChiShard)
		return n.namePartClusterName(shard.Address.ClusterName)
	case *chop.ChiHost:
		host := obj.(*chop.ChiHost)
		return n.namePartClusterName(host.Address.ClusterName)
	}

	return "ERROR"
}

func (n *namer) getNamePartShardName(obj interface{}) string {
	switch obj.(type) {
	case *chop.ChiShard:
		shard := obj.(*chop.ChiShard)
		return n.namePartShardName(shard.Address.ShardName)
	case *chop.ChiHost:
		host := obj.(*chop.ChiHost)
		return n.namePartShardName(host.Address.ShardName)
	}

	return "ERROR"
}

func (n *namer) getNamePartReplicaName(host *chop.ChiHost) string {
	return n.namePartReplicaName(host.Address.ReplicaName)
}

func newNameMacroReplacerChi(chi *chop.ClickHouseInstallation) *strings.Replacer {
	n := newNamer(namerContextNames)
	return strings.NewReplacer(
		macrosChiName, n.namePartChiName(chi.Name),
		macrosChiID, n.namePartChiNameID(chi.Name),
	)
}

func newNameMacroReplacerCluster(cluster *chop.ChiCluster) *strings.Replacer {
	n := newNamer(namerContextNames)
	return strings.NewReplacer(
		macrosChiName, n.namePartChiName(cluster.Address.ChiName),
		macrosChiID, n.namePartChiNameID(cluster.Address.ChiName),
		macrosClusterName, n.namePartClusterName(cluster.Address.ClusterName),
		macrosClusterID, n.namePartClusterNameID(cluster.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(cluster.Address.ClusterIndex),
	)
}

func newNameMacroReplacerShard(shard *chop.ChiShard) *strings.Replacer {
	n := newNamer(namerContextNames)
	return strings.NewReplacer(
		macrosChiName, n.namePartChiName(shard.Address.ChiName),
		macrosChiID, n.namePartChiNameID(shard.Address.ChiName),
		macrosClusterName, n.namePartClusterName(shard.Address.ClusterName),
		macrosClusterID, n.namePartClusterNameID(shard.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(shard.Address.ClusterIndex),
		macrosShardName, n.namePartShardName(shard.Address.ShardName),
		macrosShardID, n.namePartShardNameID(shard.Address.ShardName),
		macrosShardIndex, strconv.Itoa(shard.Address.ShardIndex),
	)
}

func newNameMacroReplacerHost(host *chop.ChiHost) *strings.Replacer {
	n := newNamer(namerContextNames)
	return strings.NewReplacer(
		macrosChiName, n.namePartChiName(host.Address.ChiName),
		macrosChiID, n.namePartChiNameID(host.Address.ChiName),
		macrosClusterName, n.namePartClusterName(host.Address.ClusterName),
		macrosClusterID, n.namePartClusterNameID(host.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(host.Address.ClusterIndex),
		macrosShardName, n.namePartShardName(host.Address.ShardName),
		macrosShardID, n.namePartShardNameID(host.Address.ShardName),
		macrosShardIndex, strconv.Itoa(host.Address.ShardIndex),
		macrosReplicaName, n.namePartReplicaName(host.Address.ReplicaName),
		macrosReplicaID, n.namePartReplicaNameID(host.Address.ReplicaName),
		macrosReplicaIndex, strconv.Itoa(host.Address.ReplicaIndex),
	)
}

// CreateConfigMapPodName returns a name for a ConfigMap for ClickHouse pod
func CreateConfigMapPodName(host *chop.ChiHost) string {
	return newNameMacroReplacerHost(host).Replace(configMapDeploymentNamePattern)
}

// CreateConfigMapCommonName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonName(chi *chop.ClickHouseInstallation) string {
	return newNameMacroReplacerChi(chi).Replace(configMapCommonNamePattern)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonUsersName(chi *chop.ClickHouseInstallation) string {
	return newNameMacroReplacerChi(chi).Replace(configMapCommonUsersNamePattern)
}

// CreateChiServiceName creates a name of a Installation Service resource
func CreateChiServiceName(chi *chop.ClickHouseInstallation) string {
	if template, ok := chi.GetChiServiceTemplate(); ok {
		// Service template available
		if template.GenerateName != "" {
			// Service template has explicitly specified service name template
			return newNameMacroReplacerChi(chi).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameMacroReplacerChi(chi).Replace(chiServiceNamePattern)
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
			return newNameMacroReplacerCluster(cluster).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameMacroReplacerCluster(cluster).Replace(clusterServiceNamePattern)
}

// CreateShardServiceName returns a name of a shard's Service
func CreateShardServiceName(shard *chop.ChiShard) string {
	if template, ok := shard.GetServiceTemplate(); ok {
		// Service template available
		if template.GenerateName != "" {
			// Service template has explicitly specified service name template
			return newNameMacroReplacerShard(shard).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameMacroReplacerShard(shard).Replace(shardServiceNamePattern)
}

// CreateStatefulSetName creates a name of a StatefulSet for ClickHouse instance
func CreateStatefulSetName(host *chop.ChiHost) string {
	return newNameMacroReplacerHost(host).Replace(statefulSetNamePattern)
}

// CreateStatefulSetServiceName returns a name of a StatefulSet-related Service for ClickHouse instance
func CreateStatefulSetServiceName(host *chop.ChiHost) string {
	if template, ok := host.GetServiceTemplate(); ok {
		// Service template available
		if template.GenerateName != "" {
			// Service template has explicitly specified service name template
			return newNameMacroReplacerHost(host).Replace(template.GenerateName)
		}
	}

	// Create Service name based on default Service Name template
	return newNameMacroReplacerHost(host).Replace(statefulSetServiceNamePattern)
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
