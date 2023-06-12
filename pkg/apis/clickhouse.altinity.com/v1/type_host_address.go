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

package v1

import "fmt"

// ChiHostAddress defines address of a host within ClickHouseInstallation
type ChiHostAddress struct {
	Namespace               string `json:"namespace"               yaml:"namespace"`
	StatefulSet             string `json:"statefulSet"             yaml:"statefulSet"`
	CHIName                 string `json:"chiName"                 yaml:"chiName"`
	ClusterName             string `json:"clusterName"             yaml:"clusterName"`
	ClusterIndex            int    `json:"clusterIndex"            yaml:"clusterIndex"`
	ShardName               string `json:"shardName"               yaml:"shardName"`
	ShardIndex              int    `json:"shardIndex"              yaml:"shardIndex"`
	ShardScopeIndex         int    `json:"shardScopeIndex"         yaml:"shardScopeIndex"`
	ReplicaName             string `json:"replicaName"             yaml:"replicaName"`
	ReplicaIndex            int    `json:"replicaIndex"            yaml:"replicaIndex"`
	ReplicaScopeIndex       int    `json:"replicaScopeIndex"       yaml:"replicaScopeIndex"`
	HostName                string `json:"hostName"                yaml:"hostName"`
	CHIScopeIndex           int    `json:"chiScopeIndex"           yaml:"chiScopeIndex"`
	CHIScopeCycleSize       int    `json:"chiScopeCycleSize"       yaml:"chiScopeCycleSize"`
	CHIScopeCycleIndex      int    `json:"chiScopeCycleIndex"      yaml:"chiScopeCycleIndex"`
	CHIScopeCycleOffset     int    `json:"chiScopeCycleOffset"     yaml:"chiScopeCycleOffset"`
	ClusterScopeIndex       int    `json:"clusterScopeIndex"       yaml:"clusterScopeIndex"`
	ClusterScopeCycleSize   int    `json:"clusterScopeCycleSize"   yaml:"clusterScopeCycleSize"`
	ClusterScopeCycleIndex  int    `json:"clusterScopeCycleIndex"  yaml:"clusterScopeCycleIndex"`
	ClusterScopeCycleOffset int    `json:"clusterScopeCycleOffset" yaml:"clusterScopeCycleOffset"`
}

// CompactString creates compact string representation
func (a ChiHostAddress) CompactString() string {
	return fmt.Sprintf("ns:%s|chi:%s|clu:%s|sha:%s|rep:%s|host:%s", a.Namespace, a.CHIName, a.ClusterName, a.ShardName, a.ReplicaName, a.HostName)
}

// ClusterNameString creates cluster+host pair
func (a ChiHostAddress) ClusterNameString() string {
	return fmt.Sprintf("%s/%s", a.ClusterName, a.HostName)
}

// NamespaceNameString creates namespace+name pair
func (a ChiHostAddress) NamespaceNameString() string {
	return fmt.Sprintf("%s/%s", a.Namespace, a.HostName)
}

// NamespaceCHINameString creates namespace+CHI pair
func (a ChiHostAddress) NamespaceCHINameString() string {
	return fmt.Sprintf("%s/%s", a.Namespace, a.CHIName)
}
