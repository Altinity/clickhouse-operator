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
	// MacrosNamespace is a sanitized namespace name where ClickHouseInstallation runs
	MacrosNamespace = "{namespace}"

	// MacrosChiName is a sanitized ClickHouseInstallation name
	MacrosChiName = "{chi}"
	// MacrosChiID is a sanitized ID made of original ClickHouseInstallation name
	MacrosChiID = "{chiID}"

	// MacrosClusterName is a sanitized cluster name
	MacrosClusterName = "{cluster}"
	// MacrosClusterID is a sanitized ID made of original cluster name
	MacrosClusterID = "{clusterID}"
	// MacrosClusterIndex is an index of the cluster in the CHI - integer number, converted into string
	MacrosClusterIndex = "{clusterIndex}"

	// MacrosShardName is a sanitized shard name
	MacrosShardName = "{shard}"
	// MacrosShardID is a sanitized ID made of original shard name
	MacrosShardID = "{shardID}"
	// MacrosShardIndex is an index of the shard in the cluster - integer number, converted into string
	MacrosShardIndex = "{shardIndex}"

	// MacrosReplicaName is a sanitized replica name
	MacrosReplicaName = "{replica}"
	// MacrosReplicaID is a sanitized ID made of original replica name
	MacrosReplicaID = "{replicaID}"
	// MacrosReplicaIndex is an index of the replica in the cluster - integer number, converted into string
	MacrosReplicaIndex = "{replicaIndex}"

	// MacrosHostName is a sanitized host name
	MacrosHostName = "{host}"
	// MacrosHostID is a sanitized ID made of original host name
	MacrosHostID = "{hostID}"
	// MacrosChiScopeIndex is an index of the host on the CHI-scope
	MacrosChiScopeIndex = "{chiScopeIndex}"
	// MacrosChiScopeCycleIndex is an index of the host in the CHI-scope cycle - integer number, converted into string
	MacrosChiScopeCycleIndex = "{chiScopeCycleIndex}"
	// MacrosChiScopeCycleOffset is an offset of the host in the CHI-scope cycle - integer number, converted into string
	MacrosChiScopeCycleOffset = "{chiScopeCycleOffset}"
	// MacrosClusterScopeIndex is an index of the host on the cluster-scope
	MacrosClusterScopeIndex = "{clusterScopeIndex}"
	// MacrosClusterScopeCycleIndex is an index of the host in the Cluster-scope cycle - integer number, converted into string
	MacrosClusterScopeCycleIndex = "{clusterScopeCycleIndex}"
	// MacrosClusterScopeCycleOffset is an offset of the host in the Cluster-scope cycle - integer number, converted into string
	MacrosClusterScopeCycleOffset = "{clusterScopeCycleOffset}"
	// MacrosShardScopeIndex is an index of the host on the shard-scope
	MacrosShardScopeIndex = "{shardScopeIndex}"
	// MacrosReplicaScopeIndex is an index of the host on the replica-scope
	MacrosReplicaScopeIndex = "{replicaScopeIndex}"
	// MacrosClusterScopeCycleHeadPointsToPreviousCycleTail is {clusterScopeIndex} of previous Cycle Tail
	MacrosClusterScopeCycleHeadPointsToPreviousCycleTail = "{clusterScopeCycleHeadPointsToPreviousCycleTail}"
)
