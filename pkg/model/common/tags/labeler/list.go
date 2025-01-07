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

package labeler

// Set of kubernetes labels used by the operator
const (
	// Main labels

	LabelReadyName                   = "APIGroupName" + "/" + "ready"
	LabelReadyValueReady             = "yes"
	LabelReadyValueNotReady          = "no"
	LabelAppName                     = "APIGroupName" + "/" + "app"
	LabelAppValue                    = "chop"
	LabelCHOP                        = "APIGroupName" + "/" + "chop"
	LabelCHOPCommit                  = "APIGroupName" + "/" + "chop-commit"
	LabelCHOPDate                    = "APIGroupName" + "/" + "chop-date"
	LabelNamespace                   = "APIGroupName" + "/" + "namespace"
	LabelCRName                      = "APIGroupName" + "/" + "chi or chk"
	LabelClusterName                 = "APIGroupName" + "/" + "cluster"
	LabelShardName                   = "APIGroupName" + "/" + "shard"
	LabelReplicaName                 = "APIGroupName" + "/" + "replica"
	LabelConfigMap                   = "APIGroupName" + "/" + "ConfigMap"
	LabelConfigMapValueCRCommon      = "CRCommon"
	LabelConfigMapValueCRStorage     = "CRStorage"
	LabelConfigMapValueCRCommonUsers = "CRCommonUsers"
	LabelConfigMapValueHost          = "Host"
	LabelService                     = "APIGroupName" + "/" + "Service"
	LabelServiceValueCR              = "chi or chk"
	LabelServiceValueCluster         = "cluster"
	LabelServiceValueShard           = "shard"
	LabelServiceValueHost            = "host"
	LabelPVCReclaimPolicyName        = "APIGroupName" + "/" + "reclaimPolicy"

	// Supplementary service labels - used to cooperate with k8s

	LabelZookeeperConfigVersion = "APIGroupName" + "/" + "zookeeper-version"
	LabelSettingsConfigVersion  = "APIGroupName" + "/" + "settings-version"
	LabelObjectVersion          = "APIGroupName" + "/" + "object-version"

	// Optional labels

	LabelShardScopeIndex         = "APIGroupName" + "/" + "shardScopeIndex"
	LabelReplicaScopeIndex       = "APIGroupName" + "/" + "replicaScopeIndex"
	LabelCRScopeIndex            = "APIGroupName" + "/" + "cr ScopeIndex"
	LabelCRScopeCycleSize        = "APIGroupName" + "/" + "cr ScopeCycleSize"
	LabelCRScopeCycleIndex       = "APIGroupName" + "/" + "cr ScopeCycleIndex"
	LabelCRScopeCycleOffset      = "APIGroupName" + "/" + "cr ScopeCycleOffset"
	LabelClusterScopeIndex       = "APIGroupName" + "/" + "clusterScopeIndex"
	LabelClusterScopeCycleSize   = "APIGroupName" + "/" + "clusterScopeCycleSize"
	LabelClusterScopeCycleIndex  = "APIGroupName" + "/" + "clusterScopeCycleIndex"
	LabelClusterScopeCycleOffset = "APIGroupName" + "/" + "clusterScopeCycleOffset"
)
