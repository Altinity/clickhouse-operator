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

import (
	clickhouse_keeper_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

// Set of kubernetes labels used by the operator
var list = types.List{
	// Main labels

	labeler.LabelReadyName:                   clickhouse_keeper_altinity_com.APIGroupName + "/" + "ready",
	labeler.LabelReadyValueReady:             "yes",
	labeler.LabelReadyValueNotReady:          "no",
	labeler.LabelAppName:                     clickhouse_keeper_altinity_com.APIGroupName + "/" + "app",
	labeler.LabelAppValue:                    "chop",
	labeler.LabelCHOP:                        clickhouse_keeper_altinity_com.APIGroupName + "/" + "chop",
	labeler.LabelCHOPCommit:                  clickhouse_keeper_altinity_com.APIGroupName + "/" + "chop-commit",
	labeler.LabelCHOPDate:                    clickhouse_keeper_altinity_com.APIGroupName + "/" + "chop-date",
	labeler.LabelNamespace:                   clickhouse_keeper_altinity_com.APIGroupName + "/" + "namespace",
	labeler.LabelCRName:                      clickhouse_keeper_altinity_com.APIGroupName + "/" + "chk",
	labeler.LabelClusterName:                 clickhouse_keeper_altinity_com.APIGroupName + "/" + "cluster",
	labeler.LabelShardName:                   clickhouse_keeper_altinity_com.APIGroupName + "/" + "shard",
	labeler.LabelReplicaName:                 clickhouse_keeper_altinity_com.APIGroupName + "/" + "replica",
	labeler.LabelConfigMap:                   clickhouse_keeper_altinity_com.APIGroupName + "/" + "ConfigMap",
	labeler.LabelConfigMapValueCRCommon:      "ChkCommon",
	labeler.LabelConfigMapValueCRCommonUsers: "ChkCommonUsers",
	labeler.LabelConfigMapValueHost:          "Host",
	labeler.LabelService:                     clickhouse_keeper_altinity_com.APIGroupName + "/" + "Service",
	labeler.LabelServiceValueCR:              "chk",
	labeler.LabelServiceValueCluster:         "cluster",
	labeler.LabelServiceValueShard:           "shard",
	labeler.LabelServiceValueHost:            "host",
	labeler.LabelPVCReclaimPolicyName:        clickhouse_keeper_altinity_com.APIGroupName + "/" + "reclaimPolicy",

	// Supplementary service labels - used to cooperate with k8s

	labeler.LabelZookeeperConfigVersion: clickhouse_keeper_altinity_com.APIGroupName + "/" + "zookeeper-version",
	labeler.LabelSettingsConfigVersion:  clickhouse_keeper_altinity_com.APIGroupName + "/" + "settings-version",
	labeler.LabelObjectVersion:          clickhouse_keeper_altinity_com.APIGroupName + "/" + "object-version",

	// Optional labels

	labeler.LabelShardScopeIndex:         clickhouse_keeper_altinity_com.APIGroupName + "/" + "shardScopeIndex",
	labeler.LabelReplicaScopeIndex:       clickhouse_keeper_altinity_com.APIGroupName + "/" + "replicaScopeIndex",
	labeler.LabelCRScopeIndex:            clickhouse_keeper_altinity_com.APIGroupName + "/" + "chkScopeIndex",
	labeler.LabelCRScopeCycleSize:        clickhouse_keeper_altinity_com.APIGroupName + "/" + "chkScopeCycleSize",
	labeler.LabelCRScopeCycleIndex:       clickhouse_keeper_altinity_com.APIGroupName + "/" + "chkScopeCycleIndex",
	labeler.LabelCRScopeCycleOffset:      clickhouse_keeper_altinity_com.APIGroupName + "/" + "chkScopeCycleOffset",
	labeler.LabelClusterScopeIndex:       clickhouse_keeper_altinity_com.APIGroupName + "/" + "clusterScopeIndex",
	labeler.LabelClusterScopeCycleSize:   clickhouse_keeper_altinity_com.APIGroupName + "/" + "clusterScopeCycleSize",
	labeler.LabelClusterScopeCycleIndex:  clickhouse_keeper_altinity_com.APIGroupName + "/" + "clusterScopeCycleIndex",
	labeler.LabelClusterScopeCycleOffset: clickhouse_keeper_altinity_com.APIGroupName + "/" + "clusterScopeCycleOffset",
}
