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

import "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"

// Set of kubernetes labels used by the operator
const (
	// Main labels

	LabelReadyName                    = clickhouse_altinity_com.APIGroupName + "/" + "ready"
	LabelReadyValueReady              = "yes"
	LabelReadyValueNotReady           = "no"
	LabelAppName                      = clickhouse_altinity_com.APIGroupName + "/" + "app"
	LabelAppValue                     = "chop"
	LabelCHOP                         = clickhouse_altinity_com.APIGroupName + "/" + "chop"
	LabelCHOPCommit                   = clickhouse_altinity_com.APIGroupName + "/" + "chop-commit"
	LabelCHOPDate                     = clickhouse_altinity_com.APIGroupName + "/" + "chop-date"
	LabelNamespace                    = clickhouse_altinity_com.APIGroupName + "/" + "namespace"
	LabelCHIName                      = clickhouse_altinity_com.APIGroupName + "/" + "chi"
	LabelClusterName                  = clickhouse_altinity_com.APIGroupName + "/" + "cluster"
	LabelShardName                    = clickhouse_altinity_com.APIGroupName + "/" + "shard"
	LabelReplicaName                  = clickhouse_altinity_com.APIGroupName + "/" + "replica"
	LabelConfigMap                    = clickhouse_altinity_com.APIGroupName + "/" + "ConfigMap"
	labelConfigMapValueCHICommon      = "ChiCommon"
	labelConfigMapValueCHICommonUsers = "ChiCommonUsers"
	labelConfigMapValueHost           = "Host"
	LabelService                      = clickhouse_altinity_com.APIGroupName + "/" + "Service"
	labelServiceValueCHI              = "chi"
	labelServiceValueCluster          = "cluster"
	labelServiceValueShard            = "shard"
	labelServiceValueHost             = "host"
	LabelPVCReclaimPolicyName         = clickhouse_altinity_com.APIGroupName + "/" + "reclaimPolicy"

	// Supplementary service labels - used to cooperate with k8s

	LabelZookeeperConfigVersion = clickhouse_altinity_com.APIGroupName + "/" + "zookeeper-version"
	LabelSettingsConfigVersion  = clickhouse_altinity_com.APIGroupName + "/" + "settings-version"
	LabelObjectVersion          = clickhouse_altinity_com.APIGroupName + "/" + "object-version"

	// Optional labels

	LabelShardScopeIndex         = clickhouse_altinity_com.APIGroupName + "/" + "shardScopeIndex"
	LabelReplicaScopeIndex       = clickhouse_altinity_com.APIGroupName + "/" + "replicaScopeIndex"
	LabelCHIScopeIndex           = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeIndex"
	LabelCHIScopeCycleSize       = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleSize"
	LabelCHIScopeCycleIndex      = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleIndex"
	LabelCHIScopeCycleOffset     = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleOffset"
	LabelClusterScopeIndex       = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeIndex"
	LabelClusterScopeCycleSize   = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleSize"
	LabelClusterScopeCycleIndex  = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleIndex"
	LabelClusterScopeCycleOffset = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleOffset"
)
