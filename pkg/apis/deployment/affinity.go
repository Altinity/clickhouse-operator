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

package deployment

import "github.com/altinity/clickhouse-operator/pkg/util"

// Possible pod distributions
const (
	PodDistributionUnspecified = "Unspecified"
	// AntiAffinity section
	PodDistributionClickHouseAntiAffinity                    = "ClickHouseAntiAffinity"
	PodDistributionShardAntiAffinity                         = "ShardAntiAffinity"
	PodDistributionReplicaAntiAffinity                       = "ReplicaAntiAffinity"
	PodDistributionAnotherNamespaceAntiAffinity              = "AnotherNamespaceAntiAffinity"
	PodDistributionAnotherClickHouseInstallationAntiAffinity = "AnotherClickHouseInstallationAntiAffinity"
	PodDistributionAnotherClusterAntiAffinity                = "AnotherClusterAntiAffinity"
	// Affinity section
	PodDistributionNamespaceAffinity              = "NamespaceAffinity"
	PodDistributionClickHouseInstallationAffinity = "ClickHouseInstallationAffinity"
	PodDistributionClusterAffinity                = "ClusterAffinity"
	PodDistributionShardAffinity                  = "ShardAffinity"
	PodDistributionReplicaAffinity                = "ReplicaAffinity"
	PodDistributionPreviousTailAffinity           = "PreviousTailAffinity"
	// Misc section
	PodDistributionMaxNumberPerNode                    = "MaxNumberPerNode"
	PodDistributionMaxNumberPerNodeEqualsReplicasCount = 2000000000
	// Shortcuts section
	PodDistributionCircularReplication = "CircularReplication"

	PodDistributionScopeUnspecified = "Unspecified"
	// Pods from different ClickHouseInstallation.Cluster.Shard can co-exist on one node
	PodDistributionScopeShard = "Shard"
	// Pods from different ClickHouseInstallation.Cluster.Replica can co-exist on one node
	PodDistributionScopeReplica = "Replica"
	// Pods from different ClickHouseInstallation.Cluster can co-exist on one node
	PodDistributionScopeCluster = "Cluster"
	// Pods from different ClickHouseInstallations can co-exist on one node
	PodDistributionScopeClickHouseInstallation = "ClickHouseInstallation"
	// Pods from different Namespaces can co-exist on one node
	PodDistributionScopeNamespace = "Namespace"
	// No Pods can co-exist on one node
	PodDistributionScopeGlobal = "Global"

	// Deprecated value
	PodDistributionOnePerHost = "OnePerHost"
)

// Possible port distributions
const (
	PortDistributionUnspecified       = "Unspecified"
	PortDistributionClusterScopeIndex = "ClusterScopeIndex"
)

// podDistributionTypes enumerates every recognized PodDistribution.Type value in canonical (humped) form.
var podDistributionTypes = []string{
	PodDistributionUnspecified,
	PodDistributionClickHouseAntiAffinity,
	PodDistributionShardAntiAffinity,
	PodDistributionReplicaAntiAffinity,
	PodDistributionAnotherNamespaceAntiAffinity,
	PodDistributionAnotherClickHouseInstallationAntiAffinity,
	PodDistributionAnotherClusterAntiAffinity,
	PodDistributionNamespaceAffinity,
	PodDistributionClickHouseInstallationAffinity,
	PodDistributionClusterAffinity,
	PodDistributionShardAffinity,
	PodDistributionReplicaAffinity,
	PodDistributionPreviousTailAffinity,
	PodDistributionMaxNumberPerNode,
	PodDistributionCircularReplication,
	PodDistributionOnePerHost,
}

// podDistributionScopes enumerates every recognized PodDistribution.Scope value in canonical (humped) form.
var podDistributionScopes = []string{
	PodDistributionScopeUnspecified,
	PodDistributionScopeShard,
	PodDistributionScopeReplica,
	PodDistributionScopeCluster,
	PodDistributionScopeClickHouseInstallation,
	PodDistributionScopeNamespace,
	PodDistributionScopeGlobal,
}

// portDistributionTypes enumerates every recognized PortDistribution.Type value in canonical (humped) form.
var portDistributionTypes = []string{
	PortDistributionUnspecified,
	PortDistributionClusterScopeIndex,
}

// NormalizePodDistributionType folds any accepted casing of a PodDistribution type to its canonical const.
func NormalizePodDistributionType(value string) string {
	return util.FoldEnum(value, podDistributionTypes...)
}

// NormalizePodDistributionScope folds any accepted casing of a PodDistribution scope to its canonical const.
func NormalizePodDistributionScope(value string) string {
	return util.FoldEnum(value, podDistributionScopes...)
}

// NormalizePortDistributionType folds any accepted casing of a PortDistribution type to its canonical const.
func NormalizePortDistributionType(value string) string {
	return util.FoldEnum(value, portDistributionTypes...)
}
