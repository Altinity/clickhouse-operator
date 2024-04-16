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

// HostAddress defines address of a host within ClickHouseInstallation
type HostAddress struct {
	Namespace               string `json:"namespace,omitempty"               yaml:"namespace,omitempty"`
	StatefulSet             string `json:"statefulSet,omitempty"             yaml:"statefulSet,omitempty"`
	FQDN                    string `json:"fqdn,omitempty"                    yaml:"fqdn,omitempty"`
	CHIName                 string `json:"chiName,omitempty"                 yaml:"chiName,omitempty"`
	ClusterName             string `json:"clusterName,omitempty"             yaml:"clusterName,omitempty"`
	ClusterIndex            int    `json:"clusterIndex,omitempty"            yaml:"clusterIndex,omitempty"`
	ShardName               string `json:"shardName,omitempty"               yaml:"shardName,omitempty"`
	ShardIndex              int    `json:"shardIndex,omitempty"              yaml:"shardIndex,omitempty"`
	ShardScopeIndex         int    `json:"shardScopeIndex,omitempty"         yaml:"shardScopeIndex,omitempty"`
	ReplicaName             string `json:"replicaName,omitempty"             yaml:"replicaName,omitempty"`
	ReplicaIndex            int    `json:"replicaIndex,omitempty"            yaml:"replicaIndex,omitempty"`
	ReplicaScopeIndex       int    `json:"replicaScopeIndex,omitempty"       yaml:"replicaScopeIndex,omitempty"`
	HostName                string `json:"hostName,omitempty"                yaml:"hostName,omitempty"`
	CHIScopeIndex           int    `json:"chiScopeIndex,omitempty"           yaml:"chiScopeIndex,omitempty"`
	CHIScopeCycleSize       int    `json:"chiScopeCycleSize,omitempty"       yaml:"chiScopeCycleSize,omitempty"`
	CHIScopeCycleIndex      int    `json:"chiScopeCycleIndex,omitempty"      yaml:"chiScopeCycleIndex,omitempty"`
	CHIScopeCycleOffset     int    `json:"chiScopeCycleOffset,omitempty"     yaml:"chiScopeCycleOffset,omitempty"`
	ClusterScopeIndex       int    `json:"clusterScopeIndex,omitempty"       yaml:"clusterScopeIndex,omitempty"`
	ClusterScopeCycleSize   int    `json:"clusterScopeCycleSize,omitempty"   yaml:"clusterScopeCycleSize,omitempty"`
	ClusterScopeCycleIndex  int    `json:"clusterScopeCycleIndex,omitempty"  yaml:"clusterScopeCycleIndex,omitempty"`
	ClusterScopeCycleOffset int    `json:"clusterScopeCycleOffset,omitempty" yaml:"clusterScopeCycleOffset,omitempty"`
}

// CompactString creates compact string representation
func (a HostAddress) CompactString() string {
	return fmt.Sprintf("ns:%s|chi:%s|clu:%s|sha:%s|rep:%s|host:%s",
		a.Namespace, a.CHIName, a.ClusterName, a.ShardName, a.ReplicaName, a.HostName)
}

// ClusterNameString creates cluster+host pair
func (a HostAddress) ClusterNameString() string {
	return fmt.Sprintf("%s/%s", a.ClusterName, a.HostName)
}

// NamespaceNameString creates namespace+name pair
func (a HostAddress) NamespaceNameString() string {
	return fmt.Sprintf("%s/%s", a.Namespace, a.HostName)
}
