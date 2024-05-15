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

package namer

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
	// chiServiceNamePattern is a template of CHI Service name. "clickhouse-{chi}"
	chiServiceNamePattern = "clickhouse-" + MacrosChiName

	// clusterServiceNamePattern is a template of cluster Service name. "cluster-{chi}-{cluster}"
	clusterServiceNamePattern = "cluster-" + MacrosChiName + "-" + MacrosClusterName

	// shardServiceNamePattern is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	shardServiceNamePattern = "shard-" + MacrosChiName + "-" + MacrosClusterName + "-" + MacrosShardName

	// replicaServiceNamePattern is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	replicaServiceNamePattern = "shard-" + MacrosChiName + "-" + MacrosClusterName + "-" + MacrosReplicaName

	// statefulSetNamePattern is a template of hosts's StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	statefulSetNamePattern = "chi-" + MacrosChiName + "-" + MacrosClusterName + "-" + MacrosHostName

	// statefulSetServiceNamePattern is a template of hosts's StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	statefulSetServiceNamePattern = "chi-" + MacrosChiName + "-" + MacrosClusterName + "-" + MacrosHostName

	// configMapCommonNamePattern is a template of common settings for the CHI ConfigMap. "chi-{chi}-common-configd"
	configMapCommonNamePattern = "chi-" + MacrosChiName + "-common-configd"

	// configMapCommonUsersNamePattern is a template of common users settings for the CHI ConfigMap. "chi-{chi}-common-usersd"
	configMapCommonUsersNamePattern = "chi-" + MacrosChiName + "-common-usersd"

	// configMapHostNamePattern is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	configMapHostNamePattern = "chi-" + MacrosChiName + "-deploy-confd-" + MacrosClusterName + "-" + MacrosHostName

	// configMapHostMigrationNamePattern is a template of macros ConfigMap. "chi-{chi}-migration-{cluster}-{shard}-{host}"
	//configMapHostMigrationNamePattern = "chi-" + MacrosChiName + "-migration-" + MacrosClusterName + "-" + MacrosHostName

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

	// podNamePattern is a name of a Pod within StatefulSet. In our setup each StatefulSet has only 1 pod,
	// so all pods would have '-0' suffix after StatefulSet name
	// Ex.: StatefulSetName-0
	podNamePattern = "%s-0"
)

const (
	NamerContextLabels = "labels"
	NamerContextNames  = "names"
)
