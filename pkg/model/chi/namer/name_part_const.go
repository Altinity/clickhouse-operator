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
	// Names context length
	namePartChiMaxLenNamesCtx     = 60
	namePartClusterMaxLenNamesCtx = 15
	namePartShardMaxLenNamesCtx   = 15
	namePartReplicaMaxLenNamesCtx = 15

	// Labels context length
	namePartChiMaxLenLabelsCtx     = 63
	namePartClusterMaxLenLabelsCtx = 63
	namePartShardMaxLenLabelsCtx   = 63
	namePartReplicaMaxLenLabelsCtx = 63
)

type NamePartType string

const (
	NamePartNamespace               NamePartType = "NamePartNamespace"
	NamePartCHIName                 NamePartType = "NamePartCHIName"
	NamePartClusterName             NamePartType = "NamePartClusterName"
	NamePartShardName               NamePartType = "NamePartShardName"
	NamePartReplicaName             NamePartType = "NamePartReplicaName"
	NamePartHostName                NamePartType = "NamePartHostName"
	NamePartCHIScopeCycleSize       NamePartType = "NamePartCHIScopeCycleSize"
	NamePartCHIScopeCycleIndex      NamePartType = "NamePartCHIScopeCycleIndex"
	NamePartCHIScopeCycleOffset     NamePartType = "NamePartCHIScopeCycleOffset"
	NamePartClusterScopeCycleSize   NamePartType = "NamePartClusterScopeCycleSize"
	NamePartClusterScopeCycleIndex  NamePartType = "NamePartClusterScopeCycleIndex"
	NamePartClusterScopeCycleOffset NamePartType = "NamePartClusterScopeCycleOffset"
	NamePartCHIScopeIndex           NamePartType = "NamePartCHIScopeIndex"
	NamePartClusterScopeIndex       NamePartType = "NamePartClusterScopeIndex"
	NamePartShardScopeIndex         NamePartType = "NamePartShardScopeIndex"
	NamePartReplicaScopeIndex       NamePartType = "NamePartReplicaScopeIndex"
)
