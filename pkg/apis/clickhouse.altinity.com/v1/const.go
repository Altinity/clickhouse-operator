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

// +k8s:deepcopy-gen=package,register
// +groupName=clickhouse.altinity.com

// Package v1 defines version 1 of the API used with ClickHouse Installation Custom Resources.
package v1

// Possible CHI statuses
const (
	StatusInProgress  = "InProgress"
	StatusCompleted   = "Completed"
	StatusTerminating = "Terminating"
)

// Possible kinds of CRDs
const (
	ClickHouseInstallationCRDResourceKind         = "ClickHouseInstallation"
	ClickHouseInstallationTemplateCRDResourceKind = "ClickHouseInstallationTemplate"
	ClickHouseOperatorCRDResourceKind             = "ClickHouseOperator"
)

// used in type OperatorConfig struct
const (
	// What to do in case StatefulSet can't reach new Generation - abort CHI reconcile
	OnStatefulSetCreateFailureActionAbort = "abort"

	// What to do in case StatefulSet can't reach new Generation - delete newly created problematic StatefulSet
	OnStatefulSetCreateFailureActionDelete = "delete"

	// What to do in case StatefulSet can't reach new Generation - do nothing, keep StatefulSet broken and move to the next
	OnStatefulSetCreateFailureActionIgnore = "ignore"
)

// used in type OperatorConfig struct
const (
	// What to do in case StatefulSet can't reach new Generation - abort CHI reconcile
	OnStatefulSetUpdateFailureActionAbort = "abort"

	// What to do in case StatefulSet can't reach new Generation - delete Pod and rollback StatefulSet to previous Generation
	// Pod would be recreated by StatefulSet based on rollback-ed configuration
	OnStatefulSetUpdateFailureActionRollback = "rollback"

	// What to do in case StatefulSet can't reach new Generation - do nothing, keep StatefulSet broken and move to the next
	OnStatefulSetUpdateFailureActionIgnore = "ignore"
)

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

// Username/password replacers
const (
	UsernameReplacer = "***"
	PasswordReplacer = "***"
)
