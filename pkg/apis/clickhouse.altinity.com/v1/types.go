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

import (
	"k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MergeType string

const (
	MergeTypeFillEmptyValues          = "fillempty"
	MergeTypeOverrideByNonEmptyValues = "override"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallation defines the Installation of a ClickHouse Database Cluster
type ClickHouseInstallation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata"`
	Spec              ChiSpec   `json:"spec"               yaml:"spec"`
	Status            ChiStatus `json:"status"`
}

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseInstallationTemplate ClickHouseInstallation

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseOperatorConfiguration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              OperatorConfig `json:"spec"`
	Status            string         `json:"status"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	Stop                   string           `json:"stop,omitempty"                   yaml:"stop"`
	NamespaceDomainPattern string           `json:"namespaceDomainPattern,omitempty" yaml:"namespaceDomainPattern"`
	Defaults               ChiDefaults      `json:"defaults,omitempty"               yaml:"defaults"`
	Configuration          Configuration    `json:"configuration"                    yaml:"configuration"`
	Templates              ChiTemplates     `json:"templates,omitempty"              yaml:"templates"`
	UseTemplates           []ChiUseTemplate `json:"useTemplates,omitempty"           yaml:"useTemplates"`
}

// ChiUseTemplates defines UseTemplates section of ClickHouseInstallation resource
type ChiUseTemplate struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`
	UseType   string `json:"useType"   yaml:"useType"`
}

// ChiDefaults defines defaults section of .spec
type ChiDefaults struct {
	ReplicasUseFQDN string            `json:"replicasUseFQDN,omitempty" yaml:"replicasUseFQDN"`
	DistributedDDL  ChiDistributedDDL `json:"distributedDDL,omitempty"  yaml:"distributedDDL"`
	Templates       ChiTemplateNames  `json:"templates,omitempty"       yaml:"templates"`
}

// ChiTemplateNames defines references to .spec.templates to be used on current level of cluster
type ChiTemplateNames struct {
	HostTemplate            string `json:"hostTemplate,omitempty"            yaml:"hostTemplate"`
	PodTemplate             string `json:"podTemplate,omitempty"             yaml:"podTemplate"`
	DataVolumeClaimTemplate string `json:"dataVolumeClaimTemplate,omitempty" yaml:"dataVolumeClaimTemplate"`
	LogVolumeClaimTemplate  string `json:"logVolumeClaimTemplate,omitempty"  yaml:"logVolumeClaimTemplate"`
	// DEPRECATED!!!  VolumeClaimTemplate is deprecated in favor of DataVolumeClaimTemplate and LogVolumeClaimTemplate
	VolumeClaimTemplate    string `json:"volumeClaimTemplate,omitempty"     yaml:"volumeClaimTemplate"`
	ServiceTemplate        string `json:"serviceTemplate,omitempty"         yaml:"serviceTemplate"`
	ClusterServiceTemplate string `json:"clusterServiceTemplate,omitempty"  yaml:"clusterServiceTemplate"`
	ShardServiceTemplate   string `json:"shardServiceTemplate,omitempty"    yaml:"shardServiceTemplate"`
	ReplicaServiceTemplate string `json:"replicaServiceTemplate,omitempty"  yaml:"replicaServiceTemplate"`
}

// ChiShard defines item of a shard section of .spec.configuration.clusters[n].shards
// TODO unify with ChiReplica based on HostsSet
type ChiShard struct {
	// DEPRECATED - to be removed soon
	DefinitionType string `json:"definitionType"`

	Name                string           `json:"name,omitempty"`
	Weight              int              `json:"weight,omitempty"`
	InternalReplication string           `json:"internalReplication,omitempty"`
	Settings            Settings         `json:"settings,omitempty"`
	Files               Settings         `json:"files,omitempty"`
	Templates           ChiTemplateNames `json:"templates,omitempty"`
	ReplicasCount       int              `json:"replicasCount,omitempty"`
	// TODO refactor into map[string]ChiHost
	Hosts []*ChiHost `json:"replicas,omitempty"`

	// Internal data
	Address ChiShardAddress         `json:"address"`
	CHI     *ClickHouseInstallation `json:"-" testdiff:"ignore"`
}

// ChiReplica defines item of a replica section of .spec.configuration.clusters[n].replicas
// TODO unify with ChiShard based on HostsSet
type ChiReplica struct {
	Name        string           `json:"name,omitempty"`
	Settings    Settings         `json:"settings,omitempty"`
	Files       Settings         `json:"files,omitempty"`
	Templates   ChiTemplateNames `json:"templates,omitempty"`
	ShardsCount int              `json:"shardsCount,omitempty"`
	// TODO refactor into map[string]ChiHost
	Hosts []*ChiHost `json:"shards,omitempty"`

	// Internal data
	Address ChiReplicaAddress       `json:"address"`
	CHI     *ClickHouseInstallation `json:"-" testdiff:"ignore"`
}

// ChiShardAddress defines address of a shard within ClickHouseInstallation
type ChiShardAddress struct {
	Namespace    string `json:"namespace"`
	CHIName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ShardName    string `json:"shardName,omitempty"`
	ShardIndex   int    `json:"shardIndex"`
}

// ChiReplicaAddress defines address of a replica within ClickHouseInstallation
type ChiReplicaAddress struct {
	Namespace    string `json:"namespace"`
	CHIName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ReplicaName  string `json:"replicaName,omitempty"`
	ReplicaIndex int    `json:"replicaIndex"`
}

// ChiHost defines host (a data replica within a shard) of .spec.configuration.clusters[n].shards[m]
type ChiHost struct {
	Name string `json:"name,omitempty"`
	// DEPRECATED - to be removed soon
	Port                int32            `json:"port,omitempty"`
	TCPPort             int32            `json:"tcpPort,omitempty"`
	HTTPPort            int32            `json:"httpPort,omitempty"`
	InterserverHTTPPort int32            `json:"interserverHTTPPort,omitempty"`
	Settings            Settings         `json:"settings,omitempty"`
	Files               Settings         `json:"files,omitempty"`
	Templates           ChiTemplateNames `json:"templates,omitempty"`

	// Internal data
	Address     ChiHostAddress          `json:"-"`
	Config      ChiHostConfig           `json:"-"`
	StatefulSet *v1.StatefulSet         `json:"-" testdiff:"ignore"`
	CHI         *ClickHouseInstallation `json:"-" testdiff:"ignore"`
}

// ChiHostTemplate defines full Host Template
type ChiHostTemplate struct {
	Name             string                `json:"name"                       yaml:"name"`
	PortDistribution []ChiPortDistribution `json:"portDistribution,omitempty" yaml:"portDistribution"`
	Spec             ChiHost               `json:"spec,omitempty"             yaml:"spec"`
}

type ChiPortDistribution struct {
	Type string `json:"type,omitempty"   yaml:"type"`
}

// ChiHostAddress defines address of a host within ClickHouseInstallation
type ChiHostAddress struct {
	Namespace               string `json:"namespace"`
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

// ChiHostConfig defines additional data related to a host
type ChiHostConfig struct {
	ZookeeperFingerprint string `json:"zookeeperfingerprint"`
	SettingsFingerprint  string `json:"settingsfingerprint"`
	FilesFingerprint     string `json:"filesfingerprint"`
}

// CHITemplates defines templates section of .spec
type ChiTemplates struct {
	// Templates
	HostTemplates        []ChiHostTemplate        `json:"hostTemplates,omitempty"        yaml:"hostTemplates"`
	PodTemplates         []ChiPodTemplate         `json:"podTemplates,omitempty"         yaml:"podTemplates"`
	VolumeClaimTemplates []ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates"`
	ServiceTemplates     []ChiServiceTemplate     `json:"serviceTemplates,omitempty"     yaml:"serviceTemplates"`

	// Index maps template name to template itself
	HostTemplatesIndex        map[string]*ChiHostTemplate        `json:",omitempty" testdiff:"ignore"`
	PodTemplatesIndex         map[string]*ChiPodTemplate         `json:",omitempty" testdiff:"ignore"`
	VolumeClaimTemplatesIndex map[string]*ChiVolumeClaimTemplate `json:",omitempty" testdiff:"ignore"`
	ServiceTemplatesIndex     map[string]*ChiServiceTemplate     `json:",omitempty" testdiff:"ignore"`
}

// ChiPodTemplate defines full Pod Template, directly used by StatefulSet
type ChiPodTemplate struct {
	Name         string             `json:"name"                    yaml:"name"`
	GenerateName string             `json:"generateName,omitempty"  yaml:"generateName"`
	Zone         ChiPodTemplateZone `json:"zone"                    yaml:"zone"`
	// DEPRECATED - to be removed soon
	Distribution    string               `json:"distribution"    yaml:"distribution"`
	PodDistribution []ChiPodDistribution `json:"podDistribution" yaml:"podDistribution"`
	Spec            corev1.PodSpec       `json:"spec"            yaml:"spec"`
}

type ChiPodTemplateZone struct {
	Key    string   `json:"key"    yaml:"key"`
	Values []string `json:"values" yaml:"values"`
}

type ChiPodDistribution struct {
	Type   string `json:"type,omitempty"   yaml:"type"`
	Scope  string `json:"scope,omitempty"  yaml:"scope"`
	Number int    `json:"number,omitempty" yaml:"number"`
}

// ChiVolumeClaimTemplate defines PersistentVolumeClaim Template, directly used by StatefulSet
type ChiVolumeClaimTemplate struct {
	Name             string                           `json:"name"          yaml:"name"`
	PVCReclaimPolicy PVCReclaimPolicy                 `json:"reclaimPolicy" yaml:"reclaimPolicy"`
	Spec             corev1.PersistentVolumeClaimSpec `json:"spec"          yaml:"spec"`
}

type PVCReclaimPolicy string

const (
	PVCReclaimPolicyRetain PVCReclaimPolicy = "Retain"
	PVCReclaimPolicyDelete PVCReclaimPolicy = "Delete"
)

// isValid checks whether PVCReclaimPolicy is valid
func (v PVCReclaimPolicy) IsValid() bool {
	switch v {
	case PVCReclaimPolicyRetain:
		return true
	case PVCReclaimPolicyDelete:
		return true
	}
	return false
}

type ChiServiceTemplate struct {
	Name         string             `json:"name"         yaml:"name"`
	GenerateName string             `json:"generateName" yaml:"generateName"`
	ObjectMeta   metav1.ObjectMeta  `json:"metadata"     yaml:"metadata"`
	Spec         corev1.ServiceSpec `json:"spec"         yaml:"spec"`
}

// ChiDistributedDDL defines distributedDDL section of .spec.defaults
type ChiDistributedDDL struct {
	Profile string `json:"profile,omitempty" yaml:"profile"`
}

// ChiZookeeperConfig defines zookeeper section of .spec.configuration
// Refers to
// https://clickhouse.yandex/docs/en/single/index.html?#server-settings_zookeeper
type ChiZookeeperConfig struct {
	Nodes              []ChiZookeeperNode `json:"nodes,omitempty"                yaml:"nodes"`
	SessionTimeoutMs   int                `json:"session_timeout_ms,omitempty"   yaml:"session_timeout_ms"`
	OperationTimeoutMs int                `json:"operation_timeout_ms,omitempty" yaml:"operation_timeout_ms"`
	Root               string             `json:"root,omitempty"                 yaml:"root"`
	Identity           string             `json:"identity,omitempty"             yaml:"identity"`
}

// ChiZookeeperNode defines item of nodes section of .spec.configuration.zookeeper
type ChiZookeeperNode struct {
	Host string `json:"host,omitempty" yaml:"host"`
	Port int32  `json:"port,omitempty" yaml:"port"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallationList defines a list of ClickHouseInstallation resources
type ClickHouseInstallationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ClickHouseInstallation `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseInstallationTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ClickHouseInstallationTemplate `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseOperatorConfigurationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ClickHouseOperatorConfiguration `json:"items"`
}
