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

const (
	StatusInProgress = "InProgress"
	StatusCompleted  = "Completed"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallation defines the Installation of a ClickHouse Database Cluster
type ClickHouseInstallation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata"`
	Spec              ChiSpec   `json:"spec"     yaml:"spec"`
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
	Spec              Config `json:"spec"`
	Status            string `json:"status"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	Stop          string           `json:"stop,omitempty"      yaml:"stop"`
	Defaults      ChiDefaults      `json:"defaults,omitempty"  yaml:"defaults"`
	Configuration ChiConfiguration `json:"configuration"       yaml:"configuration"`
	Templates     ChiTemplates     `json:"templates,omitempty" yaml:"templates"`
}

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	Version           string   `json:"version"`
	ClustersCount     int      `json:"clusters"`
	ShardsCount       int      `json:"shards"`
	HostsCount        int      `json:"hosts"`
	Status            string   `json:"status"`
	UpdatedHostsCount int      `json:"updated"`
	AddedHostsCount   int      `json:"added"`
	DeletedHostsCount int      `json:"deleted"`
	DeleteHostsCount  int      `json:"delete"`
	Pods              []string `json:"pods"`
	Endpoint          string   `json:"endpoint"`
}

// ChiDefaults defines defaults section of .spec
type ChiDefaults struct {
	ReplicasUseFQDN string            `json:"replicasUseFQDN,omitempty" yaml:"replicasUseFQDN"`
	DistributedDDL  ChiDistributedDDL `json:"distributedDDL,omitempty"  yaml:"distributedDDL"`
	Templates       ChiTemplateNames  `json:"templates,omitempty"       yaml:"templates"`
}

// ChiTemplateNames defines references to .spec.templates to be used on current level of cluster
type ChiTemplateNames struct {
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

func (templates *ChiTemplateNames) HandleDeprecatedFields() {
	if templates.DataVolumeClaimTemplate == "" {
		templates.DataVolumeClaimTemplate = templates.VolumeClaimTemplate
	}
}

func (templates *ChiTemplateNames) MergeFrom(from *ChiTemplateNames) {
	if templates.PodTemplate == "" {
		templates.PodTemplate = from.PodTemplate
	}
	if templates.DataVolumeClaimTemplate == "" {
		templates.DataVolumeClaimTemplate = from.DataVolumeClaimTemplate
	}
	if templates.LogVolumeClaimTemplate == "" {
		templates.LogVolumeClaimTemplate = from.LogVolumeClaimTemplate
	}
	if templates.VolumeClaimTemplate == "" {
		templates.VolumeClaimTemplate = from.VolumeClaimTemplate
	}
	if templates.ServiceTemplate == "" {
		templates.ServiceTemplate = from.ServiceTemplate
	}
	if templates.ClusterServiceTemplate == "" {
		templates.ClusterServiceTemplate = from.ClusterServiceTemplate
	}
	if templates.ShardServiceTemplate == "" {
		templates.ShardServiceTemplate = from.ShardServiceTemplate
	}
	if templates.ReplicaServiceTemplate == "" {
		templates.ReplicaServiceTemplate = from.ReplicaServiceTemplate
	}
}

// ChiConfiguration defines configuration section of .spec
type ChiConfiguration struct {
	Zookeeper ChiZookeeperConfig     `json:"zookeeper,omitempty" yaml:"zookeeper"`
	Users     map[string]interface{} `json:"users,omitempty"     yaml:"users"`
	Profiles  map[string]interface{} `json:"profiles,omitempty"  yaml:"profiles"`
	Quotas    map[string]interface{} `json:"quotas,omitempty"    yaml:"quotas"`
	Settings  map[string]interface{} `json:"settings,omitempty"  yaml:"settings"`
	Files     map[string]string      `json:"files,omitempty"     yaml:"files"`
	// TODO refactor into map[string]ChiCluster
	Clusters []ChiCluster `json:"clusters,omitempty"`
}

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name      string           `json:"name"`
	Layout    ChiLayout        `json:"layout"`
	Templates ChiTemplateNames `json:"templates,omitempty"`

	// Internal data
	Address ChiClusterAddress       `json:"address"`
	Chi     *ClickHouseInstallation `json:"-" testdiff:"ignore"`
}

// ChiClusterAddress defines address of a cluster within ClickHouseInstallation
type ChiClusterAddress struct {
	Namespace    string `json:"namespace"`
	ChiName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
}

// ChiLayout defines layout section of .spec.configuration.clusters
type ChiLayout struct {
	// DEPRECATED - to be removed soon
	Type          string `json:"type"`
	ShardsCount   int    `json:"shardsCount,omitempty"`
	ReplicasCount int    `json:"replicasCount,omitempty"`
	// TODO refactor into map[string]ChiShard
	Shards []ChiShard `json:"shards,omitempty"`
}

// ChiShard defines item of a shard section of .spec.configuration.clusters[n].shards
type ChiShard struct {
	// DEPRECATED - to be removed soon
	DefinitionType      string           `json:"definitionType"`
	Name                string           `json:"name,omitempty"`
	Weight              int              `json:"weight,omitempty"`
	InternalReplication string           `json:"internalReplication,omitempty"`
	Templates           ChiTemplateNames `json:"templates,omitempty"`
	ReplicasCount       int              `json:"replicasCount,omitempty"`
	// TODO refactor into map[string]ChiReplica
	Replicas []ChiHost `json:"replicas,omitempty"`

	// Internal data
	Address ChiShardAddress         `json:"address"`
	Chi     *ClickHouseInstallation `json:"-" testdiff:"ignore"`
}

// ChiShardAddress defines address of a shard within ClickHouseInstallation
type ChiShardAddress struct {
	Namespace    string `json:"namespace"`
	ChiName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ShardName    string `json:"shardName,omitempty"`
	ShardIndex   int    `json:"shardIndex"`
}

// ChiHost defines host (a data replica within a shard) of .spec.configuration.clusters[n].shards[m]
type ChiHost struct {
	Name      string           `json:"name,omitempty"`
	Port      int32            `json:"port,omitempty"`
	Templates ChiTemplateNames `json:"templates,omitempty"`

	// Internal data
	Address ChiHostAddress          `json:"address"`
	Config  ChiHostConfig           `json:"config"`
	Chi     *ClickHouseInstallation `json:"-" testdiff:"ignore"`
}

// ChiHostAddress defines address of a host within ClickHouseInstallation
type ChiHostAddress struct {
	Namespace    string `json:"namespace"`
	ChiName      string `json:"chiName"`
	ClusterName  string `json:"clusterName"`
	ClusterIndex int    `json:"clusterIndex"`
	ShardName    string `json:"shardName,omitempty"`
	ShardIndex   int    `json:"shardIndex"`
	ReplicaName  string `json:"replicaName,omitempty"`
	ReplicaIndex int    `json:"replicaIndex"`
	HostIndex    int    `json:"hostIndex"`
}

// ChiHostConfig defines additional data related to a host
type ChiHostConfig struct {
	ZookeeperFingerprint string `json:"zookeeperfingerprint"`
	SettingsFingerprint  string `json:"settingsfingerprint"`
}

// ChiTemplates defines templates section of .spec
type ChiTemplates struct {
	// Templates
	PodTemplates         []ChiPodTemplate         `json:"podTemplates,omitempty"         yaml:"podTemplates"`
	VolumeClaimTemplates []ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates"`
	ServiceTemplates     []ChiServiceTemplate     `json:"serviceTemplates,omitempty"     yaml:"serviceTemplates"`

	// Index maps template name to template itself
	PodTemplatesIndex         map[string]*ChiPodTemplate         `testdiff:"ignore"`
	VolumeClaimTemplatesIndex map[string]*ChiVolumeClaimTemplate `testdiff:"ignore"`
	ServiceTemplatesIndex     map[string]*ChiServiceTemplate     `testdiff:"ignore"`
}

// ChiPodTemplate defines full Pod Template, directly used by StatefulSet
type ChiPodTemplate struct {
	Name         string             `json:"name"         yaml:"name"`
	Zone         ChiPodTemplateZone `json:"zone"         yaml:"zone""`
	Distribution string             `json:"distribution" yaml:"distribution"`
	Spec         corev1.PodSpec     `json:"spec"         yaml:"spec"`
}

type ChiPodTemplateZone struct {
	Key    string   `json:"key" yaml:"key"`
	Values []string `json:"values" yaml:"values"`
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
	Host string `json:"host" yaml:"host"`
	Port int32  `json:"port" yaml:"port"`
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

const (
	// ClickHouseInstallationCRDResourceKind defines kind of CRD resource
	ClickHouseInstallationCRDResourceKind         = "ClickHouseInstallation"
	ClickHouseInstallationTemplateCRDResourceKind = "ClickHouseInstallationTemplate"
	ClickHouseOperatorCRDResourceKind             = "ClickHouseOperator"
)

// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// Do not forget to update func (config *Config) String()
// Do not forget to update CRD spec
type Config struct {
	// Full path to the config file and folder where this Config originates from
	ConfigFilePath   string
	ConfigFolderPath string

	// WatchNamespaces where operator watches for events
	WatchNamespaces []string `json:"watchNamespaces" yaml:"watchNamespaces"`

	// Paths where to look for additional ClickHouse config .xml files to be mounted into Pod
	// config.d
	// conf.d
	// users.d
	// respectively
	ChCommonConfigsPath string `json:"chCommonConfigsPath" yaml:"chCommonConfigsPath"`
	ChHostConfigsPath   string `json:"chHostConfigsPath"   yaml:"chHostConfigsPath"`
	ChUsersConfigsPath  string `json:"chUsersConfigsPath"  yaml:"chUsersConfigsPath"`
	// Config files fetched from these paths. Maps "file name->file content"
	ChCommonConfigs map[string]string
	ChHostConfigs   map[string]string
	ChUsersConfigs  map[string]string

	// Path where to look for ClickHouseInstallation templates .yaml files
	ChiTemplatesPath string `json:"chiTemplatesPath" yaml:"chiTemplatesPath"`
	// Chi template files fetched from this path. Maps "file name->file content"
	ChiTemplateFiles map[string]string
	// Chi template objects unmarshalled from ChiTemplateFiles. Maps "metadata.name->object"
	ChiTemplates map[string]*ClickHouseInstallation
	// ClickHouseInstallation template
	ChiTemplate *ClickHouseInstallation

	// Create/Update StatefulSet behavior - for how long to wait for StatefulSet to reach new Generation
	StatefulSetUpdateTimeout uint64 `json:"statefulSetUpdateTimeout" yaml:"statefulSetUpdateTimeout"`
	// Create/Update StatefulSet behavior - for how long to sleep while polling StatefulSet to reach new Generation
	StatefulSetUpdatePollPeriod uint64 `json:"statefulSetUpdatePollPeriod" yaml:"statefulSetUpdatePollPeriod"`

	// Rolling Create/Update behavior
	// StatefulSet create behavior - what to do in case StatefulSet can't reach new Generation
	OnStatefulSetCreateFailureAction string `json:"onStatefulSetCreateFailureAction" yaml:"onStatefulSetCreateFailureAction"`
	// StatefulSet update behavior - what to do in case StatefulSet can't reach new Generation
	OnStatefulSetUpdateFailureAction string `json:"onStatefulSetUpdateFailureAction" yaml:"onStatefulSetUpdateFailureAction"`

	// Default values for ClickHouse user configuration
	// 1. user/profile - string
	// 2. user/quota - string
	// 3. user/networks/ip - multiple strings
	// 4. user/password - string
	ChConfigUserDefaultProfile    string   `json:"chConfigUserDefaultProfile"    yaml:"chConfigUserDefaultProfile"`
	ChConfigUserDefaultQuota      string   `json:"chConfigUserDefaultQuota"      yaml:"chConfigUserDefaultQuota"`
	ChConfigUserDefaultNetworksIP []string `json:"chConfigUserDefaultNetworksIP" yaml:"chConfigUserDefaultNetworksIP"`
	ChConfigUserDefaultPassword   string   `json:"chConfigUserDefaultPassword"   yaml:"chConfigUserDefaultPassword"`

	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	ChUsername string `json:"chUsername" yaml:"chUsername"`
	ChPassword string `json:"chPassword" yaml:"chPassword"`
	ChPort     int    `json:"chPort"     yaml:"chPort""`
}

const (
	// What to do in case StatefulSet can't reach new Generation - abort rolling create
	OnStatefulSetCreateFailureActionAbort = "abort"

	// What to do in case StatefulSet can't reach new Generation - delete newly created problematic StatefulSet
	OnStatefulSetCreateFailureActionDelete = "delete"
)

const (
	// What to do in case StatefulSet can't reach new Generation - abort rolling update
	OnStatefulSetUpdateFailureActionAbort = "abort"

	// What to do in case StatefulSet can't reach new Generation - delete Pod and rollback StatefulSet to previous Generation
	// Pod would be recreated by StatefulSet based on rollback-ed configuration
	OnStatefulSetUpdateFailureActionRollback = "rollback"
)
