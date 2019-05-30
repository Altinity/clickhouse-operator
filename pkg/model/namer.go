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
)

func createChiNameID(name string) string {
	//return util.CreateStringID(name, 6)
	return util.StringHead(name, 15)
}

func createClusterNameID(name string) string {
	//return util.CreateStringID(name, 4)
	return util.StringHead(name, 15)
}

func createShardNameID(name string) string {
	return util.StringHead(name, 8)
}

func createReplicaNameID(name string) string {
	return util.StringHead(name, 8)
}

func nameSectionChi(obj interface{}) string {
	switch obj.(type) {
	case *chop.ChiReplica:
		replica := obj.(*chop.ChiReplica)
		return createChiNameID(replica.Address.ChiName)
	case *chop.ClickHouseInstallation:
		chi := obj.(*chop.ClickHouseInstallation)
		return createChiNameID(chi.Name)
	}

	return "ERROR"
}

func nameSectionCluster(replica *chop.ChiReplica) string {
	return createClusterNameID(replica.Address.ClusterName)
}

func nameSectionShard(replica *chop.ChiReplica) string {
	return createShardNameID(replica.Address.ShardName)
}

func nameSectionReplica(replica *chop.ChiReplica) string {
	return createReplicaNameID(replica.Address.ReplicaName)
}

// CreateConfigMapPodName returns a name for a ConfigMap for replica's pod
func CreateConfigMapPodName(replica *chop.ChiReplica) string {
	return fmt.Sprintf(
		configMapDeploymentNamePattern,
		nameSectionChi(replica),
		nameSectionCluster(replica),
		nameSectionShard(replica),
		nameSectionReplica(replica),
	)
}

// CreateConfigMapCommonName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonName(chi *chop.ClickHouseInstallation) string {
	return fmt.Sprintf(
		configMapCommonNamePattern,
		nameSectionChi(chi),
	)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common chopConfig
func CreateConfigMapCommonUsersName(chi *chop.ClickHouseInstallation) string {
	return fmt.Sprintf(
		configMapCommonUsersNamePattern,
		nameSectionChi(chi),
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
		CreateChiServiceName(chi),
		chi.Namespace,
	)
}

// CreateStatefulSetName creates a name of a StatefulSet for replica
func CreateStatefulSetName(replica *chop.ChiReplica) string {
	return fmt.Sprintf(
		statefulSetNamePattern,
		nameSectionChi(replica),
		nameSectionCluster(replica),
		nameSectionShard(replica),
		nameSectionReplica(replica),
	)
}

// CreateStatefulSetServiceName returns a name of a StatefulSet-related Service for replica
func CreateStatefulSetServiceName(replica *chop.ChiReplica) string {
	return fmt.Sprintf(
		statefulSetServiceNamePattern,
		nameSectionChi(replica),
		nameSectionCluster(replica),
		nameSectionShard(replica),
		nameSectionReplica(replica),
	)
}

// CreatePodHostname returns a name of a Pod resource for a replica
func CreatePodHostname(replica *chop.ChiReplica) string {
	// Pod has no own hostname - redirect to appropriate Service
	return CreateStatefulSetServiceName(replica)
}

// CreateNamespaceDomainName creates domain name of a namespace
// .my-dev-namespace.svc.cluster.local
func CreateNamespaceDomainName(chiNamespace string) string {
	return fmt.Sprintf(namespaceDomainPattern, chiNamespace)
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

// CreatePodName create Pod name based on specified StatefulSet name
func CreatePodName(statefulSet *apps.StatefulSet) string {
	return fmt.Sprintf(podNamePattern, statefulSet.Name)

}
