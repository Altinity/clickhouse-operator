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

package macro

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/common/macro"
)

var List = types.List{
	// MacrosNamespace is a sanitized namespace name where ClickHouseInstallation runs
	macro.MacrosNamespace: "{namespace}",

	// MacrosCRName is a sanitized Custom Resource name
	macro.MacrosCRName: "{chi}",

	// MacrosClusterName is a sanitized cluster name
	macro.MacrosClusterName: "{cluster}",
	// MacrosClusterIndex is an index of the cluster in the CHI - integer number, converted into string
	macro.MacrosClusterIndex: "{clusterIndex}",

	// MacrosShardName is a sanitized shard name
	macro.MacrosShardName: "{shard}",
	// MacrosShardIndex is an index of the shard in the cluster - integer number, converted into string
	macro.MacrosShardIndex: "{shardIndex}",

	// MacrosReplicaName is a sanitized replica name
	macro.MacrosReplicaName: "{replica}",
	// MacrosReplicaIndex is an index of the replica in the cluster - integer number, converted into string
	macro.MacrosReplicaIndex: "{replicaIndex}",

	// MacrosHostName is a sanitized host name
	macro.MacrosHostName: "{host}",
	// MacrosCRScopeIndex is an index of the host on the CHI-scope
	macro.MacrosCRScopeIndex: "{chiScopeIndex}",
	// MacrosCRScopeCycleIndex is an index of the host in the CHI-scope cycle - integer number, converted into string
	macro.MacrosCRScopeCycleIndex: "{chiScopeCycleIndex}",
	// MacrosCRScopeCycleOffset is an offset of the host in the CHI-scope cycle - integer number, converted into string
	macro.MacrosCRScopeCycleOffset: "{chiScopeCycleOffset}",
	// MacrosClusterScopeIndex is an index of the host on the cluster-scope
	macro.MacrosClusterScopeIndex: "{clusterScopeIndex}",
	// MacrosClusterScopeCycleIndex is an index of the host in the Cluster-scope cycle - integer number, converted into string
	macro.MacrosClusterScopeCycleIndex: "{clusterScopeCycleIndex}",
	// MacrosClusterScopeCycleOffset is an offset of the host in the Cluster-scope cycle - integer number, converted into string
	macro.MacrosClusterScopeCycleOffset: "{clusterScopeCycleOffset}",
	// MacrosShardScopeIndex is an index of the host on the shard-scope
	macro.MacrosShardScopeIndex: "{shardScopeIndex}",
	// MacrosReplicaScopeIndex is an index of the host on the replica-scope
	macro.MacrosReplicaScopeIndex: "{replicaScopeIndex}",
	// MacrosClusterScopeCycleHeadPointsToPreviousCycleTail is {clusterScopeIndex} of previous Cycle Tail
	macro.MacrosClusterScopeCycleHeadPointsToPreviousCycleTail: "{clusterScopeCycleHeadPointsToPreviousCycleTail}",
}
