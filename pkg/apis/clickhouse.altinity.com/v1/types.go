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
	metav1.TypeMeta   `json:",inline"            yaml:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Spec              ChiSpec   `json:"spec"     yaml:"spec"`
	Status            ChiStatus `json:"status"   yaml:"status"`
}

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseInstallationTemplate ClickHouseInstallation

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseOperatorConfiguration struct {
	metav1.TypeMeta   `json:",inline"               yaml:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"    yaml:"metadata,omitempty"`
	Spec              OperatorConfig `json:"spec"   yaml:"spec"`
	Status            string         `json:"status" yaml:"status"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	TaskID                 *string          `json:"taskID,omitempty"                 yaml:"taskID,omitempty"`
	Stop                   string           `json:"stop,omitempty"                   yaml:"stop,omitempty"`
	NamespaceDomainPattern string           `json:"namespaceDomainPattern,omitempty" yaml:"namespaceDomainPattern,omitempty"`
	Templating             *ChiTemplating   `json:"templating,omitempty"             yaml:"templating,omitempty"`
	Reconciling            *ChiReconciling  `json:"reconciling,omitempty"            yaml:"reconciling,omitempty"`
	Defaults               *ChiDefaults     `json:"defaults,omitempty"               yaml:"defaults,omitempty"`
	Configuration          *Configuration   `json:"configuration,omitempty"          yaml:"configuration,omitempty"`
	Templates              *ChiTemplates    `json:"templates,omitempty"              yaml:"templates,omitempty"`
	UseTemplates           []ChiUseTemplate `json:"useTemplates,omitempty"           yaml:"useTemplates,omitempty"`
}

// ChiUseTemplate defines UseTemplate section of ClickHouseInstallation resource
type ChiUseTemplate struct {
	Name      string `json:"name,omitempty"      yaml:"name,omitempty"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	UseType   string `json:"useType,omitempty"   yaml:"useType,omitempty"`
}

// ChiTemplating
type ChiTemplating struct {
	Policy string `json:"policy,omitempty" yaml:"policy,omitempty"`
}

// NewChiTemplating
func NewChiTemplating() *ChiTemplating {
	return new(ChiTemplating)
}

// GetPolicy
func (t *ChiTemplating) GetPolicy() string {
	if t == nil {
		return ""
	}
	return t.Policy
}

// SetPolicy
func (t *ChiTemplating) SetPolicy(p string) {
	if t == nil {
		return
	}
	t.Policy = p
}

// ChiReconciling
type ChiReconciling struct {
	Policy string `json:"policy,omitempty" yaml:"policy,omitempty"`
}

func NewChiReconciling() *ChiReconciling {
	return new(ChiReconciling)
}

// GetPolicy
func (t *ChiReconciling) GetPolicy() string {
	if t == nil {
		return ""
	}
	return t.Policy
}

// SetPolicy
func (t *ChiReconciling) SetPolicy(p string) {
	if t == nil {
		return
	}
	t.Policy = p
}

// ChiDefaults defines defaults section of .spec
type ChiDefaults struct {
	ReplicasUseFQDN string             `json:"replicasUseFQDN,omitempty" yaml:"replicasUseFQDN,omitempty"`
	DistributedDDL  *ChiDistributedDDL `json:"distributedDDL,omitempty"  yaml:"distributedDDL,omitempty"`
	Templates       *ChiTemplateNames  `json:"templates,omitempty"       yaml:"templates,omitempty"`
}

// ChiTemplateNames defines references to .spec.templates to be used on current level of cluster
type ChiTemplateNames struct {
	HostTemplate            string `json:"hostTemplate,omitempty"            yaml:"hostTemplate,omitempty"`
	PodTemplate             string `json:"podTemplate,omitempty"             yaml:"podTemplate,omitempty"`
	DataVolumeClaimTemplate string `json:"dataVolumeClaimTemplate,omitempty" yaml:"dataVolumeClaimTemplate,omitempty"`
	LogVolumeClaimTemplate  string `json:"logVolumeClaimTemplate,omitempty"  yaml:"logVolumeClaimTemplate,omitempty"`
	ServiceTemplate         string `json:"serviceTemplate,omitempty"         yaml:"serviceTemplate,omitempty"`
	ClusterServiceTemplate  string `json:"clusterServiceTemplate,omitempty"  yaml:"clusterServiceTemplate,omitempty"`
	ShardServiceTemplate    string `json:"shardServiceTemplate,omitempty"    yaml:"shardServiceTemplate,omitempty"`
	ReplicaServiceTemplate  string `json:"replicaServiceTemplate,omitempty"  yaml:"replicaServiceTemplate,omitempty"`

	// DEPRECATED!!!  VolumeClaimTemplate is deprecated in favor of DataVolumeClaimTemplate and LogVolumeClaimTemplate
	VolumeClaimTemplate string `json:"volumeClaimTemplate,omitempty"     yaml:"volumeClaimTemplate,omitempty"`
}

// ChiShard defines item of a shard section of .spec.configuration.clusters[n].shards
// TODO unify with ChiReplica based on HostsSet
type ChiShard struct {
	Name                string            `json:"name,omitempty"                yaml:"name,omitempty"`
	Weight              int               `json:"weight,omitempty"              yaml:"weight,omitempty"`
	InternalReplication string            `json:"internalReplication,omitempty" yaml:"internalReplication,omitempty"`
	Settings            *Settings         `json:"settings,omitempty"            yaml:"settings,omitempty"`
	Files               *Settings         `json:"files,omitempty"               yaml:"files,omitempty"`
	Templates           *ChiTemplateNames `json:"templates,omitempty"           yaml:"templates,omitempty"`
	ReplicasCount       int               `json:"replicasCount,omitempty"       yaml:"replicasCount,omitempty"`
	// TODO refactor into map[string]ChiHost
	Hosts []*ChiHost `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	// Internal data
	Address ChiShardAddress         `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`

	// DEPRECATED - to be removed soon
	DefinitionType string `json:"definitionType,omitempty" yaml:"definitionType,omitempty"`
}

// ChiReplica defines item of a replica section of .spec.configuration.clusters[n].replicas
// TODO unify with ChiShard based on HostsSet
type ChiReplica struct {
	Name        string            `json:"name,omitempty"        yaml:"name,omitempty"`
	Settings    *Settings         `json:"settings,omitempty"    yaml:"settings,omitempty"`
	Files       *Settings         `json:"files,omitempty"       yaml:"files,omitempty"`
	Templates   *ChiTemplateNames `json:"templates,omitempty"   yaml:"templates,omitempty"`
	ShardsCount int               `json:"shardsCount,omitempty" yaml:"shardsCount,omitempty"`
	// TODO refactor into map[string]ChiHost
	Hosts []*ChiHost `json:"shards,omitempty" yaml:"shards,omitempty"`

	// Internal data
	Address ChiReplicaAddress       `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

// ChiShardAddress defines address of a shard within ClickHouseInstallation
type ChiShardAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
	ShardName    string `json:"shardName,omitempty"    yaml:"shardName,omitempty"`
	ShardIndex   int    `json:"shardIndex,omitempty"   yaml:"shardIndex,omitempty"`
}

// ChiReplicaAddress defines address of a replica within ClickHouseInstallation
type ChiReplicaAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
	ReplicaName  string `json:"replicaName,omitempty"  yaml:"replicaName,omitempty"`
	ReplicaIndex int    `json:"replicaIndex,omitempty" yaml:"replicaIndex,omitempty"`
}

// ChiHostTemplate defines full Host Template
type ChiHostTemplate struct {
	Name             string                `json:"name,omitempty"             yaml:"name,omitempty"`
	PortDistribution []ChiPortDistribution `json:"portDistribution,omitempty" yaml:"portDistribution,omitempty"`
	Spec             ChiHost               `json:"spec,omitempty"             yaml:"spec,omitempty"`
}

type ChiPortDistribution struct {
	Type string `json:"type,omitempty"   yaml:"type,omitempty"`
}

// ChiHostConfig defines additional data related to a host
type ChiHostConfig struct {
	ZookeeperFingerprint string `json:"zookeeperfingerprint" yaml:"zookeeperfingerprint"`
	SettingsFingerprint  string `json:"settingsfingerprint"  yaml:"settingsfingerprint"`
	FilesFingerprint     string `json:"filesfingerprint"     yaml:"filesfingerprint"`
}

type StatefulSetStatus string

const (
	StatefulSetStatusModified StatefulSetStatus = "modified"
	StatefulSetStatusNew      StatefulSetStatus = "new"
	StatefulSetStatusSame     StatefulSetStatus = "same"
	StatefulSetStatusUnknown  StatefulSetStatus = "unknown"
)

// ChiHostReconcileAttributes defines host reconcile status
type ChiHostReconcileAttributes struct {
	status  StatefulSetStatus
	add     bool
	remove  bool
	modify  bool
	unclear bool
}

func NewChiHostReconcileAttributes() *ChiHostReconcileAttributes {
	return &ChiHostReconcileAttributes{}
}

func (s *ChiHostReconcileAttributes) Equal(to ChiHostReconcileAttributes) bool {
	if s == nil {
		return false
	}
	return (s.add == to.add) && (s.remove == to.remove) && (s.modify == to.modify) && (s.unclear == to.unclear)
}

func (s *ChiHostReconcileAttributes) Any(to ChiHostReconcileAttributes) bool {
	if s == nil {
		return false
	}
	return (s.add && to.add) || (s.remove && to.remove) || (s.modify && to.modify) || (s.unclear && to.unclear)
}

func (s *ChiHostReconcileAttributes) SetStatus(status StatefulSetStatus) *ChiHostReconcileAttributes {
	s.status = status
	return s
}

func (s *ChiHostReconcileAttributes) GetStatus() StatefulSetStatus {
	return s.status
}

func (s *ChiHostReconcileAttributes) SetAdd() *ChiHostReconcileAttributes {
	s.add = true
	return s
}

func (s *ChiHostReconcileAttributes) UnsetAdd() *ChiHostReconcileAttributes {
	s.add = false
	return s
}

func (s *ChiHostReconcileAttributes) SetRemove() *ChiHostReconcileAttributes {
	s.remove = true
	return s
}

func (s *ChiHostReconcileAttributes) SetModify() *ChiHostReconcileAttributes {
	s.modify = true
	return s
}

func (s *ChiHostReconcileAttributes) SetUnclear() *ChiHostReconcileAttributes {
	s.unclear = true
	return s
}

func (s *ChiHostReconcileAttributes) IsAdd() bool {
	return s.add
}

func (s *ChiHostReconcileAttributes) IsRemove() bool {
	return s.remove
}

func (s *ChiHostReconcileAttributes) IsModify() bool {
	return s.modify
}

func (s *ChiHostReconcileAttributes) IsUnclear() bool {
	return s.unclear
}

// CHITemplates defines templates section of .spec
type ChiTemplates struct {
	// Templates
	HostTemplates        []ChiHostTemplate        `json:"hostTemplates,omitempty"        yaml:"hostTemplates,omitempty"`
	PodTemplates         []ChiPodTemplate         `json:"podTemplates,omitempty"         yaml:"podTemplates,omitempty"`
	VolumeClaimTemplates []ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates,omitempty"`
	ServiceTemplates     []ChiServiceTemplate     `json:"serviceTemplates,omitempty"     yaml:"serviceTemplates,omitempty"`

	// Index maps template name to template itself
	HostTemplatesIndex        *HostTemplatesIndex        `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	PodTemplatesIndex         *PodTemplatesIndex         `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	VolumeClaimTemplatesIndex *VolumeClaimTemplatesIndex `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	ServiceTemplatesIndex     *ServiceTemplatesIndex     `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// ChiPodTemplate defines full Pod Template, directly used by StatefulSet
type ChiPodTemplate struct {
	Name         string             `json:"name"                    yaml:"name"`
	GenerateName string             `json:"generateName,omitempty"  yaml:"generateName,omitempty"`
	Zone         ChiPodTemplateZone `json:"zone,omitempty"          yaml:"zone,omitempty"`
	// DEPRECATED - to be removed soon
	Distribution    string               `json:"distribution"              yaml:"distribution"`
	PodDistribution []ChiPodDistribution `json:"podDistribution,omitempty" yaml:"podDistribution,omitempty"`
	ObjectMeta      metav1.ObjectMeta    `json:"metadata,omitempty"        yaml:"metadata,omitempty"`
	Spec            corev1.PodSpec       `json:"spec,omitempty"            yaml:"spec,omitempty"`
}

type ChiPodTemplateZone struct {
	Key    string   `json:"key,omitempty"    yaml:"key,omitempty"`
	Values []string `json:"values,omitempty" yaml:"values,omitempty"`
}

type ChiPodDistribution struct {
	Type   string `json:"type,omitempty"   yaml:"type,omitempty"`
	Scope  string `json:"scope,omitempty"  yaml:"scope,omitempty"`
	Number int    `json:"number,omitempty" yaml:"number,omitempty"`
}

// ChiVolumeClaimTemplate defines PersistentVolumeClaim Template, directly used by StatefulSet
type ChiVolumeClaimTemplate struct {
	Name             string                           `json:"name"                    yaml:"name"`
	PVCReclaimPolicy PVCReclaimPolicy                 `json:"reclaimPolicy,omitempty" yaml:"reclaimPolicy,omitempty"`
	ObjectMeta       metav1.ObjectMeta                `json:"metadata,omitempty"      yaml:"metadata,omitempty"`
	Spec             corev1.PersistentVolumeClaimSpec `json:"spec,omitempty"          yaml:"spec,omitempty"`
}

type PVCReclaimPolicy string

const (
	PVCReclaimPolicyRetain PVCReclaimPolicy = "Retain"
	PVCReclaimPolicyDelete PVCReclaimPolicy = "Delete"
)

// NewPVCReclaimPolicyFromString creates new PVCReclaimPolicy from string
func NewPVCReclaimPolicyFromString(s string) PVCReclaimPolicy {
	return PVCReclaimPolicy(s)
}

// IsValid checks whether PVCReclaimPolicy is valid
func (v PVCReclaimPolicy) IsValid() bool {
	switch v {
	case PVCReclaimPolicyRetain:
		return true
	case PVCReclaimPolicyDelete:
		return true
	}
	return false
}

// String returns string value for PVCReclaimPolicy
func (v PVCReclaimPolicy) String() string {
	return string(v)
}

type ChiServiceTemplate struct {
	Name         string             `json:"name"                   yaml:"name"`
	GenerateName string             `json:"generateName,omitempty" yaml:"generateName,omitempty"`
	ObjectMeta   metav1.ObjectMeta  `json:"metadata,omitempty"     yaml:"metadata,omitempty"`
	Spec         corev1.ServiceSpec `json:"spec,omitempty"         yaml:"spec,omitempty"`
}

// ChiDistributedDDL defines distributedDDL section of .spec.defaults
type ChiDistributedDDL struct {
	Profile string `json:"profile,omitempty" yaml:"profile"`
}

// ChiZookeeperConfig defines zookeeper section of .spec.configuration
// Refers to
// https://clickhouse.yandex/docs/en/single/index.html?#server-settings_zookeeper
type ChiZookeeperConfig struct {
	Nodes              []ChiZookeeperNode `json:"nodes,omitempty"                yaml:"nodes,omitempty"`
	SessionTimeoutMs   int                `json:"session_timeout_ms,omitempty"   yaml:"session_timeout_ms,omitempty"`
	OperationTimeoutMs int                `json:"operation_timeout_ms,omitempty" yaml:"operation_timeout_ms,omitempty"`
	Root               string             `json:"root,omitempty"                 yaml:"root,omitempty"`
	Identity           string             `json:"identity,omitempty"             yaml:"identity,omitempty"`
}

// ChiZookeeperNode defines item of nodes section of .spec.configuration.zookeeper
type ChiZookeeperNode struct {
	Host string `json:"host,omitempty" yaml:"host,omitempty"`
	Port int32  `json:"port,omitempty" yaml:"port,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallationList defines a list of ClickHouseInstallation resources
type ClickHouseInstallationList struct {
	metav1.TypeMeta `json:",inline"  yaml:",inline"`
	metav1.ListMeta `json:"metadata" yaml:"metadata"`
	Items           []ClickHouseInstallation `json:"items" yaml:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseInstallationTemplateList struct {
	metav1.TypeMeta `json:",inline"  yaml:",inline"`
	metav1.ListMeta `json:"metadata" yaml:"metadata"`
	Items           []ClickHouseInstallationTemplate `json:"items" yaml:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClickHouseOperatorConfigurationList struct {
	metav1.TypeMeta `json:",inline"  yaml:",inline"`
	metav1.ListMeta `json:"metadata" yaml:"metadata"`
	Items           []ClickHouseOperatorConfiguration `json:"items" yaml:"items"`
}
