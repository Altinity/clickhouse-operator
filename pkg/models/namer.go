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

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	apps "k8s.io/api/apps/v1"
)

func createStringID(str string, hashLen int) string {
	hasher := sha1.New()
	hasher.Write([]byte(str))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return hash[len(hash)-hashLen:]
}

func createChiNameID(name string) string {
	return createStringID(name, 6)
}

func createClusterNameID(name string) string {
	return createStringID(name, 4)
}

// CreateConfigMapPodName returns a name for a ConfigMap for replica's pod
func CreateConfigMapPodName(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		configMapDeploymentNamePattern,
		createChiNameID(replica.Address.ChiName),
		createClusterNameID(replica.Address.ClusterName),
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateConfigMapCommonName returns a name for a ConfigMap for replica's common config
func CreateConfigMapCommonName(chiName string) string {
	return fmt.Sprintf(
		configMapCommonNamePattern,
		createChiNameID(chiName),
	)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common config
func CreateConfigMapCommonUsersName(chiName string) string {
	return fmt.Sprintf(
		configMapCommonUsersNamePattern,
		createChiNameID(chiName),
	)
}

// CreateChiServiceName creates a name of a Installation Service resource
func CreateChiServiceName(chi *chop.ClickHouseInstallation) string {
	return fmt.Sprintf(
		chiServiceNamePattern,
		chi.Name,
	)
}

// CreateChiServiceName creates a name of a Installation Service resource
func CreateChiServiceFQDN(chi *chop.ClickHouseInstallation) string {
	return fmt.Sprintf(
		chiServiceFQDNPattern,
		chi.Name,
		chi.Namespace,
	)
}

// CreateStatefulSetName creates a name of a StatefulSet for replica
func CreateStatefulSetName(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		statefulSetNamePattern,
		createChiNameID(replica.Address.ChiName),
		createClusterNameID(replica.Address.ClusterName),
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateStatefulSetServiceName returns a name of a StatefulSet-related Service for replica
func CreateStatefulSetServiceName(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		statefulSetServiceNamePattern,
		createChiNameID(replica.Address.ChiName),
		createClusterNameID(replica.Address.ClusterName),
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreatePodHostname returns a name of a Pod resource for replica
// ss-1eb454-2-0
func CreatePodHostname(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		podHostnamePattern,
		createChiNameID(replica.Address.ChiName),
		createClusterNameID(replica.Address.ClusterName),
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateNamespaceDomainName creates domain name of a namespace
// .my-dev-namespace.svc.cluster.local
func CreateNamespaceDomainName(chiNamespace string) string {
	return fmt.Sprintf(namespaceDomainPattern, chiNamespace)
}

// CreatePodFQDN creates a fully qualified domain name of a pod
// ss-1eb454-2-0.my-dev-domain.svc.cluster.local
func CreatePodFQDN(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		podFQDNPattern,
		createChiNameID(replica.Address.ChiName),
		createClusterNameID(replica.Address.ClusterName),
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
		replica.Address.Namespace,
	)
}

// CreatePodFQDNsOfCluster creates fully qualified domain names of all pods in a cluster
func CreatePodFQDNsOfCluster(cluster *chop.ChiCluster) []string {
	fqdns := make([]string, 0)
	cluster.WalkReplicas(func(replica *chop.ChiClusterLayoutShardReplica) error {
		fqdns = append(fqdns, CreatePodFQDN(replica))
		return nil
	})
	return fqdns
}

// CreatePodFQDNsOfChi creates fully qualified domain names of all pods in a CHI
func CreatePodFQDNsOfChi(chi *chop.ClickHouseInstallation) []string {
	fqdns := make([]string, 0)
	chi.WalkReplicas(func(replica *chop.ChiClusterLayoutShardReplica) error {
		fqdns = append(fqdns, CreatePodFQDN(replica))
		return nil
	})
	return fqdns
}

// CreatePodName create Pod Name for a Service
func CreatePodName(statefulSet *apps.StatefulSet) string {
	return fmt.Sprintf(podNamePattern, statefulSet.Name)

}
