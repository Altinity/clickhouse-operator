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

const (
	StatusInProgress = "InProgress"
	StatusCompleted  = "Completed"
)

const (
	// ClickHouseInstallationCRDResourceKind defines kind of CRD resource
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

const (
	PodDistributionUnspecified                               = "Unspecified"
	PodDistributionClickHouseAntiAffinity                    = "ClickHouseAntiAffinity"
	PodDistributionShardAntiAffinity                         = "ShardAntiAffinity"
	PodDistributionReplicaAntiAffinity                       = "ReplicaAntiAffinity"
	PodDistributionAnotherNamespaceAntiAffinity              = "AnotherNamespaceAntiAffinity"
	PodDistributionAnotherClickHouseInstallationAntiAffinity = "AnotherClickHouseInstallationAntiAffinity"
	PodDistributionAnotherClusterAntiAffinity                = "AnotherClusterAntiAffinity"
	PodDistributionMaxNumberPerNode                          = "MaxNumberPerNode"
	PodDistributionNamespaceAffinity                         = "NamespaceAffinity"
	PodDistributionClickHouseInstallationAffinity            = "ClickHouseInstallationAffinity"
	PodDistributionClusterAffinity                           = "ClusterAffinity"
	PodDistributionShardAffinity                             = "ShardAffinity"
	PodDistributionReplicaAffinity                           = "ReplicaAffinity"
	PodDistributionPreviousTailAffinity                      = "PreviousTailAffinity"
	PodDistributionCircularReplication                       = "CircularReplication"

	// Deprecated value
	PodDistributionOnePerHost = "OnePerHost"
)

const (
	PortDistributionUnspecified       = "Unspecified"
	PortDistributionClusterScopeIndex = "ClusterScopeIndex"
)
