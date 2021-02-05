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
	Namespace               string `json:"namespace"`
	StatefulSet             string `json:"statefulSet"`
	CHIName                 string `json:"chiName"`
	ClusterName             string `json:"clusterName"`
	ClusterIndex            int    `json:"clusterIndex"`
	ShardName               string `json:"shardName,omitempty"`
	ShardIndex              int    `json:"shardIndex"`
	ShardScopeIndex         int    `json:"shardScopeIndex"`
	ReplicaName             string `json:"replicaName,omitempty"`
	ReplicaIndex            int    `json:"replicaIndex"`
	ReplicaScopeIndex       int    `json:"replicaScopeIndex"`
	HostName                string `json:"hostName,omitempty"`
	CHIScopeIndex           int    `json:"chiScopeIndex"`
	CHIScopeCycleSize       int    `json:"chiScopeCycleSize"`
	CHIScopeCycleIndex      int    `json:"chiScopeCycleIndex"`
	CHIScopeCycleOffset     int    `json:"chiScopeCycleOffset"`
	ClusterScopeIndex       int    `json:"clusterScopeIndex"`
	ClusterScopeCycleSize   int    `json:"clusterScopeCycleSize"`
	ClusterScopeCycleIndex  int    `json:"clusterScopeCycleIndex"`
	ClusterScopeCycleOffset int    `json:"clusterScopeCycleOffset"`
}

func (a ChiHostAddress) CompactString() string {
	return fmt.Sprintf("ns:%s|chi:%s|clu:%s|sha:%s|rep:%s|host:%s", a.Namespace, a.CHIName, a.ClusterName, a.ShardName, a.ReplicaName, a.HostName)
}

func (a ChiHostAddress) ClusterNameString() string {
	return fmt.Sprintf("%s/%s", a.ClusterName, a.HostName)
}

func (a ChiHostAddress) NamespaceNameString() string {
	return fmt.Sprintf("%s/%s", a.Namespace, a.HostName)
}

func (a ChiHostAddress) NamespaceCHINameString() string {
	return fmt.Sprintf("%s/%s", a.Namespace, a.CHIName)
}
