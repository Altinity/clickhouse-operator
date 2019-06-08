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
	namePartChiMaxLen     = 15
	namePartClusterMaxLen = 15
	namePartShardMaxLen   = 15
	namePartReplicaMaxLen = 15
)

const (
	// chiServiceNamePattern is a template of CHI Service name
	chiServiceNamePattern = "clickhouse-{chi}"

	// statefulSetNamePattern is a template of replica's StatefulSet's name
	statefulSetNamePattern = "chi-{chi}-{cluster}-{shard}-{replica}"

	// statefulSetServiceNamePattern is a template of replica's StatefulSet's Service name
	statefulSetServiceNamePattern = "chi-{chi}-{cluster}-{shard}-{replica}"

	// configMapCommonNamePattern is a template of common settings for the CHI ConfigMap
	configMapCommonNamePattern = "chi-{chi}-common-configd"

	// configMapCommonUsersNamePattern is a template of common users settings for the CHI ConfigMap
	configMapCommonUsersNamePattern = "chi-{chi}-common-usersd"

	// configMapDeploymentNamePattern is a template of macros ConfigMap
	configMapDeploymentNamePattern = "chi-{chi}-deploy-confd-{cluster}-{shard}-{replica}"

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

func namePartChiName(name string) string {
	return util.StringHead(name, namePartChiMaxLen)
}

func namePartChiNameID(name string) string {
	return util.CreateStringID(name, namePartChiMaxLen)
}

func namePartClusterName(name string) string {
	return util.StringHead(name, namePartClusterMaxLen)
}

func namePartClusterNameID(name string) string {
	return util.CreateStringID(name, namePartClusterMaxLen)
}

func namePartShardName(name string) string {
	return util.StringHead(name, namePartShardMaxLen)
}

func namePartShardNameID(name string) string {
	return util.CreateStringID(name, namePartShardMaxLen)
}

func namePartReplicaName(name string) string {
	return util.StringHead(name, namePartReplicaMaxLen)
}

func namePartReplicaNameID(name string) string {
	return util.CreateStringID(name, namePartReplicaMaxLen)
}

func getNamePartChiName(obj interface{}) string {
	switch obj.(type) {
	case *chop.ChiReplica:
		replica := obj.(*chop.ChiReplica)
		return namePartChiName(replica.Address.ChiName)
	case *chop.ClickHouseInstallation:
		chi := obj.(*chop.ClickHouseInstallation)
		return namePartChiName(chi.Name)
	}

	return "ERROR"
}

func getNamePartClusterName(replica *chop.ChiReplica) string {
	return namePartClusterName(replica.Address.ClusterName)
}

func getNamePartShardName(replica *chop.ChiReplica) string {
	return namePartShardName(replica.Address.ShardName)
}

func getNamePartReplicaName(replica *chop.ChiReplica) string {
	return namePartReplicaName(replica.Address.ReplicaName)
}

func newReplacerReplica(replica *chop.ChiReplica) *strings.Replacer {
	return strings.NewReplacer(
		"{chi}", namePartChiName(replica.Address.ChiName),
		"{chiID}", namePartChiNameID(replica.Address.ChiName),
		"{cluster}", namePartClusterName(replica.Address.ClusterName),
		"{clusterID}", namePartClusterNameID(replica.Address.ClusterName),
		"{clusterIndex}", strconv.Itoa(replica.Address.ClusterIndex),
		"{shard}", namePartShardName(replica.Address.ShardName),
		"{shardID}", namePartShardNameID(replica.Address.ShardName),
		"{shardIndex}", strconv.Itoa(replica.Address.ShardIndex),
		"{replica}", namePartReplicaName(replica.Address.ReplicaName),
		"{replicaID}", namePartReplicaNameID(replica.Address.ReplicaName),
		"{replicaIndex}", strconv.Itoa(replica.Address.ReplicaIndex),
	)
}

func newReplacerChi(chi *chop.ClickHouseInstallation) *strings.Replacer {
	return strings.NewReplacer(
		"{chi}", namePartChiName(chi.Name),
		"{chiID}", namePartChiNameID(chi.Name),
	)
}

// CreateConfigMapPodName returns a name for a ConfigMap for replica's pod
func CreateConfigMapPodName(replica *chop.ChiReplica) string {
	return newReplacerReplica(replica).Replace(configMapDeploymentNamePattern)
}

// CreateConfigMapCommonName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonName(chi *chop.ClickHouseInstallation) string {
	return newReplacerChi(chi).Replace(configMapCommonNamePattern)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonUsersName(chi *chop.ClickHouseInstallation) string {
	return newReplacerChi(chi).Replace(configMapCommonUsersNamePattern)
}

// CreateChiServiceName creates a name of a Installation Service resource
func CreateChiServiceName(chi *chop.ClickHouseInstallation) string {
	return newReplacerChi(chi).Replace(chiServiceNamePattern)
}

// CreateChiServiceName creates a name of a Installation Service resource
func CreateChiServiceFQDN(chi *chop.ClickHouseInstallation) string {
	return fmt.Sprintf(
		serviceFQDNPattern,
		CreateChiServiceName(chi),
		chi.Namespace,
	)
}

// CreateStatefulSetName creates a name of a StatefulSet for replica
func CreateStatefulSetName(replica *chop.ChiReplica) string {
	return newReplacerReplica(replica).Replace(statefulSetNamePattern)
}

// CreateStatefulSetServiceName returns a name of a StatefulSet-related Service for replica
func CreateStatefulSetServiceName(replica *chop.ChiReplica) string {
	return newReplacerReplica(replica).Replace(statefulSetServiceNamePattern)
}

// CreatePodHostname returns a name of a Pod resource for a replica
func CreatePodHostname(replica *chop.ChiReplica) string {
	// Pod has no own hostname - redirect to appropriate Service
	return CreateStatefulSetServiceName(replica)
}

// CreatePodFQDN creates a fully qualified domain name of a pod
// ss-1eb454-2-0.my-dev-domain.svc.cluster.local
func CreatePodFQDN(replica *chop.ChiReplica) string {
	return fmt.Sprintf(
		podFQDNPattern,
		CreatePodHostname(replica),
		replica.Address.Namespace,
	)
}

// CreatePodFQDNsOfCluster creates fully qualified domain names of all pods in a cluster
func CreatePodFQDNsOfCluster(cluster *chop.ChiCluster) []string {
	fqdns := make([]string, 0)
	cluster.WalkReplicas(func(replica *chop.ChiReplica) error {
		fqdns = append(fqdns, CreatePodFQDN(replica))
		return nil
	})
	return fqdns
}

// CreatePodFQDNsOfChi creates fully qualified domain names of all pods in a CHI
func CreatePodFQDNsOfChi(chi *chop.ClickHouseInstallation) []string {
	fqdns := make([]string, 0)
	chi.WalkReplicas(func(replica *chop.ChiReplica) error {
		fqdns = append(fqdns, CreatePodFQDN(replica))
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
	case *chop.ChiReplica:
		replica := obj.(*chop.ChiReplica)
		return fmt.Sprintf(podNamePattern, CreateStatefulSetName(replica))
	}
	return "unknown-type"
}
