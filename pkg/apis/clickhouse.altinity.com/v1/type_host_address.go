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

func (a *HostAddress) GetNamespace() string {
	return a.Namespace
}

func (a *HostAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *HostAddress) GetStatefulSet() string {
	return a.StatefulSet
}

func (a *HostAddress) GetFQDN() string {
	return a.FQDN
}

func (a *HostAddress) GetCRName() string {
	return a.CHIName
}

func (a *HostAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *HostAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *HostAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *HostAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *HostAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (a *HostAddress) GetShardName() string {
	return a.ShardName
}

func (a *HostAddress) SetShardName(name string) {
	a.ShardName = name
}

func (a *HostAddress) GetShardIndex() int {
	return a.ShardIndex
}

func (a *HostAddress) SetShardIndex(index int) {
	a.ShardIndex = index
}

func (a *HostAddress) GetShardScopeIndex() int {
	return a.ShardScopeIndex
}

func (a *HostAddress) SetShardScopeIndex(index int) {
	a.ShardScopeIndex = index
}

func (a *HostAddress) GetReplicaName() string {
	return a.ReplicaName
}

func (a *HostAddress) SetReplicaName(name string) {
	a.ReplicaName = name
}

func (a *HostAddress) GetReplicaIndex() int {
	return a.ReplicaIndex
}

func (a *HostAddress) SetReplicaIndex(index int) {
	a.ReplicaIndex = index
}

func (a *HostAddress) GetReplicaScopeIndex() int {
	return a.ReplicaScopeIndex
}

func (a *HostAddress) SetReplicaScopeIndex(index int) {
	a.ReplicaScopeIndex = index
}

func (a *HostAddress) GetHostName() string {
	return a.HostName
}

func (a *HostAddress) SetHostName(name string) {
	a.HostName = name
}

func (a *HostAddress) GetCRScopeIndex() int {
	return a.CHIScopeIndex
}

func (a *HostAddress) SetCRScopeIndex(index int) {
	a.CHIScopeIndex = index
}

func (a *HostAddress) GetCRScopeCycleSize() int {
	return a.CHIScopeCycleSize
}

func (a *HostAddress) SetCRScopeCycleSize(size int) {
	a.CHIScopeCycleSize = size
}

func (a *HostAddress) GetCRScopeCycleIndex() int {
	return a.CHIScopeCycleIndex
}

func (a *HostAddress) SetCRScopeCycleIndex(index int) {
	a.CHIScopeCycleIndex = index
}

func (a *HostAddress) GetCRScopeCycleOffset() int {
	return a.CHIScopeCycleOffset
}

func (a *HostAddress) SetCRScopeCycleOffset(offset int) {
	a.CHIScopeCycleOffset = offset
}

func (a *HostAddress) GetClusterScopeIndex() int {
	return a.ClusterScopeIndex
}

func (a *HostAddress) SetClusterScopeIndex(index int) {
	a.ClusterScopeIndex = index
}

func (a *HostAddress) GetClusterScopeCycleSize() int {
	return a.ClusterScopeCycleSize
}

func (a *HostAddress) SetClusterScopeCycleSize(size int) {
	a.ClusterScopeCycleSize = size
}

func (a *HostAddress) GetClusterScopeCycleIndex() int {
	return a.ClusterScopeCycleIndex
}

func (a *HostAddress) SetClusterScopeCycleIndex(index int) {
	a.ClusterScopeCycleIndex = index
}

func (a *HostAddress) GetClusterScopeCycleOffset() int {
	return a.ClusterScopeCycleOffset
}

func (a *HostAddress) SetClusterScopeCycleOffset(offset int) {
	a.ClusterScopeCycleOffset = offset
}

// CompactString creates compact string representation
func (a HostAddress) CompactString() string {
	return fmt.Sprintf("ns:%s|chi:%s|clu:%s|sha:%s|rep:%s|host:%s",
		a.GetNamespace(),
		a.GetCRName(),
		a.GetClusterName(),
		a.GetShardName(),
		a.GetReplicaName(),
		a.GetHostName())
}

// ClusterNameString creates cluster+host pair
func (a HostAddress) ClusterNameString() string {
	return fmt.Sprintf("%s/%s", a.GetClusterName(), a.GetHostName())
}

// NamespaceNameString creates namespace+name pair
func (a HostAddress) NamespaceNameString() string {
	return fmt.Sprintf("%s/%s", a.GetNamespace(), a.GetHostName())
}
