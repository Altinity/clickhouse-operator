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

package short

const (
	// Names context length
	namePartCRMaxLenNamesCtx      = 60
	namePartClusterMaxLenNamesCtx = 15
	namePartShardMaxLenNamesCtx   = 15
	namePartReplicaMaxLenNamesCtx = 15

	// Labels context length
	namePartCRMaxLenLabelsCtx      = 63
	namePartClusterMaxLenLabelsCtx = 63
	namePartShardMaxLenLabelsCtx   = 63
	namePartReplicaMaxLenLabelsCtx = 63
)

type NameType string

const (
	Namespace               NameType = "NamePartNamespace"
	CRName                  NameType = "NamePartCHIName"
	ClusterName             NameType = "NamePartClusterName"
	ShardName               NameType = "NamePartShardName"
	ReplicaName             NameType = "NamePartReplicaName"
	HostName                NameType = "NamePartHostName"
	CHIScopeCycleSize       NameType = "NamePartCHIScopeCycleSize"
	CHIScopeCycleIndex      NameType = "NamePartCHIScopeCycleIndex"
	CHIScopeCycleOffset     NameType = "NamePartCHIScopeCycleOffset"
	ClusterScopeCycleSize   NameType = "NamePartClusterScopeCycleSize"
	ClusterScopeCycleIndex  NameType = "NamePartClusterScopeCycleIndex"
	ClusterScopeCycleOffset NameType = "NamePartClusterScopeCycleOffset"
	CHIScopeIndex           NameType = "NamePartCHIScopeIndex"
	ClusterScopeIndex       NameType = "NamePartClusterScopeIndex"
	ShardScopeIndex         NameType = "NamePartShardScopeIndex"
	ReplicaScopeIndex       NameType = "NamePartReplicaScopeIndex"
)

const (
	TargetLabels = "labels"
	TargetNames  = "names"
)
