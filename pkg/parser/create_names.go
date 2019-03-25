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

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// CreateConfigMapDeploymentName returns a name for a ConfigMap for replica's deployment
func CreateConfigMapDeploymentName(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		configMapDeploymentNamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateConfigMapCommonName returns a name for a ConfigMap for replica's common config
func CreateConfigMapCommonName(chiName string) string {
	return fmt.Sprintf(configMapCommonNamePattern, chiName)
}

// CreateConfigMapCommonUsersName returns a name for a ConfigMap for replica's common config
func CreateConfigMapCommonUsersName(chiName string) string {
	return fmt.Sprintf(configMapCommonUsersNamePattern, chiName)
}

// CreateCHIServiceName creates a name of a Installation Service resource
func CreateCHIServiceName(prefix string) string {
	return fmt.Sprintf(chiServiceNamePattern, prefix)
}

// CreateStatefulSetName creates a name of a StatefulSet for replica
func CreateStatefulSetName(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		statefulSetNamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreateStatefulSetServiceName returns a name of a StatefulSet-related Service for replica
func CreateStatefulSetServiceName(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		statefulSetServiceNamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
		replica.Address.ShardIndex,
		replica.Address.ReplicaIndex,
	)
}

// CreatePodHostname returns a name of a Pod resource for replica
// ss-1eb454-2-0
func CreatePodHostname(replica *chop.ChiClusterLayoutShardReplica) string {
	return fmt.Sprintf(
		podHostnamePattern,
		replica.Address.CHIName,
		replica.Address.ClusterIndex,
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
// prefix is a fullDeploymentID
// ss-1eb454-2-0.my-dev-domain.svc.cluster.local
func CreatePodFQDN(chiNamespace, prefix string) string {
	return fmt.Sprintf(
		podFQDNPattern,
		prefix,
		chiNamespace,
	)
}
