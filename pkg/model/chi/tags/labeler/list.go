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
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

// Set of kubernetes labels used by the operator
var list = types.List{
	// Main labels

	labeler.LabelReadyName:                   clickhouse_altinity_com.APIGroupName + "/" + "ready",
	labeler.LabelReadyValueReady:             "yes",
	labeler.LabelReadyValueNotReady:          "no",
	labeler.LabelAppName:                     clickhouse_altinity_com.APIGroupName + "/" + "app",
	labeler.LabelAppValue:                    "chop",
	labeler.LabelCHOP:                        clickhouse_altinity_com.APIGroupName + "/" + "chop",
	labeler.LabelCHOPCommit:                  clickhouse_altinity_com.APIGroupName + "/" + "chop-commit",
	labeler.LabelCHOPDate:                    clickhouse_altinity_com.APIGroupName + "/" + "chop-date",
	labeler.LabelNamespace:                   clickhouse_altinity_com.APIGroupName + "/" + "namespace",
	labeler.LabelCRName:                      clickhouse_altinity_com.APIGroupName + "/" + "chi",
	labeler.LabelClusterName:                 clickhouse_altinity_com.APIGroupName + "/" + "cluster",
	labeler.LabelShardName:                   clickhouse_altinity_com.APIGroupName + "/" + "shard",
	labeler.LabelReplicaName:                 clickhouse_altinity_com.APIGroupName + "/" + "replica",
	labeler.LabelConfigMap:                   clickhouse_altinity_com.APIGroupName + "/" + "ConfigMap",
	labeler.LabelConfigMapValueCRCommon:      "ChiCommon",
	labeler.LabelConfigMapValueCRStorage:      "ChiStorage",
	labeler.LabelConfigMapValueCRCommonUsers: "ChiCommonUsers",
	labeler.LabelConfigMapValueHost:          "Host",
	labeler.LabelService:                     clickhouse_altinity_com.APIGroupName + "/" + "Service",
	labeler.LabelServiceValueCR:              "chi",
	labeler.LabelServiceValueCluster:         "cluster",
	labeler.LabelServiceValueShard:           "shard",
	labeler.LabelServiceValueHost:            "host",
	labeler.LabelPVCReclaimPolicyName:        clickhouse_altinity_com.APIGroupName + "/" + "reclaimPolicy",

	// Supplementary service labels - used to cooperate with k8s

	labeler.LabelZookeeperConfigVersion: clickhouse_altinity_com.APIGroupName + "/" + "zookeeper-version",
	labeler.LabelSettingsConfigVersion:  clickhouse_altinity_com.APIGroupName + "/" + "settings-version",
	labeler.LabelObjectVersion:          clickhouse_altinity_com.APIGroupName + "/" + "object-version",

	// Optional labels

	labeler.LabelShardScopeIndex:         clickhouse_altinity_com.APIGroupName + "/" + "shardScopeIndex",
	labeler.LabelReplicaScopeIndex:       clickhouse_altinity_com.APIGroupName + "/" + "replicaScopeIndex",
	labeler.LabelCRScopeIndex:            clickhouse_altinity_com.APIGroupName + "/" + "chiScopeIndex",
	labeler.LabelCRScopeCycleSize:        clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleSize",
	labeler.LabelCRScopeCycleIndex:       clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleIndex",
	labeler.LabelCRScopeCycleOffset:      clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleOffset",
	labeler.LabelClusterScopeIndex:       clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeIndex",
	labeler.LabelClusterScopeCycleSize:   clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleSize",
	labeler.LabelClusterScopeCycleIndex:  clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleIndex",
	labeler.LabelClusterScopeCycleOffset: clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleOffset",
}
