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
	"sync"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// MergeType specifies merge types type
type MergeType string

// Possible merge types
const (
	MergeTypeFillEmptyValues          MergeType = "fillempty"
	MergeTypeOverrideByNonEmptyValues MergeType = "override"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallation defines the Installation of a ClickHouse Database Cluster
type ClickHouseInstallation struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ChiSpec `json:"spec"               yaml:"spec"`
	Status *Status `json:"status,omitempty"   yaml:"status,omitempty"`

	runtime             *ClickHouseInstallationRuntime `json:"-" yaml:"-"`
	statusCreatorMutex  sync.Mutex                     `json:"-" yaml:"-"`
	runtimeCreatorMutex sync.Mutex                     `json:"-" yaml:"-"`
}

type ClickHouseInstallationRuntime struct {
	attributes        *ComparableAttributes `json:"-" yaml:"-"`
	commonConfigMutex sync.Mutex            `json:"-" yaml:"-"`
}

func newClickHouseInstallationRuntime() *ClickHouseInstallationRuntime {
	return &ClickHouseInstallationRuntime{
		attributes: &ComparableAttributes{},
	}
}

func (runtime *ClickHouseInstallationRuntime) GetAttributes() *ComparableAttributes {
	return runtime.attributes
}

func (runtime *ClickHouseInstallationRuntime) LockCommonConfig() {
	runtime.commonConfigMutex.Lock()
}

func (runtime *ClickHouseInstallationRuntime) UnlockCommonConfig() {
	runtime.commonConfigMutex.Unlock()
}

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallationTemplate defines ClickHouseInstallation template
type ClickHouseInstallationTemplate ClickHouseInstallation

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseOperatorConfiguration defines CHOp config
type ClickHouseOperatorConfiguration struct {
	meta.TypeMeta   `json:",inline"               yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"    yaml:"metadata,omitempty"`
	Spec            OperatorConfig `json:"spec"   yaml:"spec"`
	Status          string         `json:"status" yaml:"status"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	TaskID                 *types.String     `json:"taskID,omitempty"                 yaml:"taskID,omitempty"`
	Stop                   *types.StringBool `json:"stop,omitempty"                   yaml:"stop,omitempty"`
	Restart                *types.String     `json:"restart,omitempty"                yaml:"restart,omitempty"`
	Troubleshoot           *types.StringBool `json:"troubleshoot,omitempty"           yaml:"troubleshoot,omitempty"`
	NamespaceDomainPattern *types.String     `json:"namespaceDomainPattern,omitempty" yaml:"namespaceDomainPattern,omitempty"`
	Templating             *ChiTemplating    `json:"templating,omitempty"             yaml:"templating,omitempty"`
	Reconciling            *Reconciling      `json:"reconciling,omitempty"            yaml:"reconciling,omitempty"`
	Defaults               *ChiDefaults      `json:"defaults,omitempty"               yaml:"defaults,omitempty"`
	Configuration          *Configuration    `json:"configuration,omitempty"          yaml:"configuration,omitempty"`
	Templates              *Templates        `json:"templates,omitempty"              yaml:"templates,omitempty"`
	UseTemplates           []*TemplateRef    `json:"useTemplates,omitempty"           yaml:"useTemplates,omitempty"`
}

// TemplateRef defines UseTemplate section of ClickHouseInstallation resource
type TemplateRef struct {
	Name      string `json:"name,omitempty"      yaml:"name,omitempty"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	UseType   string `json:"useType,omitempty"   yaml:"useType,omitempty"`
}

// ChiTemplating defines templating policy struct
type ChiTemplating struct {
	Policy      string         `json:"policy,omitempty"      yaml:"policy,omitempty"`
	CHISelector TargetSelector `json:"chiSelector,omitempty" yaml:"chiSelector,omitempty"`
}

// NewChiTemplating creates new templating
func NewChiTemplating() *ChiTemplating {
	return new(ChiTemplating)
}

// GetPolicy gets policy
func (t *ChiTemplating) GetPolicy() string {
	if t == nil {
		return ""
	}
	return t.Policy
}

// SetPolicy sets policy
func (t *ChiTemplating) SetPolicy(p string) {
	if t == nil {
		return
	}
	t.Policy = p
}

// GetSelector gets CHI selector
func (t *ChiTemplating) GetSelector() TargetSelector {
	if t == nil {
		return nil
	}
	return t.CHISelector
}

// MergeFrom merges from specified templating
func (t *ChiTemplating) MergeFrom(from *ChiTemplating, _type MergeType) *ChiTemplating {
	if from == nil {
		return t
	}

	if t == nil {
		t = NewChiTemplating()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if t.Policy == "" {
			t.Policy = from.Policy
		}
		if t.CHISelector == nil {
			t.CHISelector = from.CHISelector
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.Policy != "" {
			// Override by non-empty values only
			t.Policy = from.Policy
		}
		if from.CHISelector != nil {
			// Override by non-empty values only
			t.CHISelector = from.CHISelector
		}
	}

	return t
}

// TargetSelector specifies target selector based on labels
type TargetSelector map[string]string

// Matches checks whether TargetSelector matches provided set of labels
func (s TargetSelector) Matches(labels map[string]string) bool {
	if s == nil {
		// Empty selector matches all labels
		return true
	}

	// Walk over selector keys
	for key, selectorValue := range s {
		if labelValue, ok := labels[key]; !ok {
			// Labels have no key specified in selector.
			// Selector does not match the labels
			return false
		} else if selectorValue != labelValue {
			// Labels have the key specified in selector, but selector value is not the same as labels value
			// Selector does not match the labels
			return false
		} else {
			// Selector value and label value are equal
			// So far label matches selector
			// Continue iteration to next value
		}
	}

	// All keys are in place with the same values
	// Selector matches the labels

	return true
}

// TemplatesList defines references to .spec.templates to be used
type TemplatesList struct {
	HostTemplate            string `json:"hostTemplate,omitempty"            yaml:"hostTemplate,omitempty"`
	PodTemplate             string `json:"podTemplate,omitempty"             yaml:"podTemplate,omitempty"`
	DataVolumeClaimTemplate string `json:"dataVolumeClaimTemplate,omitempty" yaml:"dataVolumeClaimTemplate,omitempty"`
	LogVolumeClaimTemplate  string `json:"logVolumeClaimTemplate,omitempty"  yaml:"logVolumeClaimTemplate,omitempty"`
	ServiceTemplate         string `json:"serviceTemplate,omitempty"         yaml:"serviceTemplate,omitempty"`
	ClusterServiceTemplate  string `json:"clusterServiceTemplate,omitempty"  yaml:"clusterServiceTemplate,omitempty"`
	ShardServiceTemplate    string `json:"shardServiceTemplate,omitempty"    yaml:"shardServiceTemplate,omitempty"`
	ReplicaServiceTemplate  string `json:"replicaServiceTemplate,omitempty"  yaml:"replicaServiceTemplate,omitempty"`

	// VolumeClaimTemplate is deprecated in favor of DataVolumeClaimTemplate and LogVolumeClaimTemplate
	// !!! DEPRECATED !!!
	VolumeClaimTemplate string `json:"volumeClaimTemplate,omitempty"     yaml:"volumeClaimTemplate,omitempty"`
}

// HostTemplate defines full Host Template
type HostTemplate struct {
	Name             string             `json:"name,omitempty"             yaml:"name,omitempty"`
	PortDistribution []PortDistribution `json:"portDistribution,omitempty" yaml:"portDistribution,omitempty"`
	Spec             Host               `json:"spec,omitempty"             yaml:"spec,omitempty"`
}

// PortDistribution defines port distribution
type PortDistribution struct {
	Type string `json:"type,omitempty"   yaml:"type,omitempty"`
}

// ChiHostConfig defines additional data related to a host
type ChiHostConfig struct {
	ZookeeperFingerprint string `json:"zookeeperfingerprint" yaml:"zookeeperfingerprint"`
	SettingsFingerprint  string `json:"settingsfingerprint"  yaml:"settingsfingerprint"`
	FilesFingerprint     string `json:"filesfingerprint"     yaml:"filesfingerprint"`
}

// Templates defines templates section of .spec
type Templates struct {
	// Templates
	HostTemplates        []HostTemplate        `json:"hostTemplates,omitempty"        yaml:"hostTemplates,omitempty"`
	PodTemplates         []PodTemplate         `json:"podTemplates,omitempty"         yaml:"podTemplates,omitempty"`
	VolumeClaimTemplates []VolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates,omitempty"`
	ServiceTemplates     []ServiceTemplate     `json:"serviceTemplates,omitempty"     yaml:"serviceTemplates,omitempty"`

	// Index maps template name to template itself
	HostTemplatesIndex        *HostTemplatesIndex        `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	PodTemplatesIndex         *PodTemplatesIndex         `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	VolumeClaimTemplatesIndex *VolumeClaimTemplatesIndex `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	ServiceTemplatesIndex     *ServiceTemplatesIndex     `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// PodTemplate defines full Pod Template, directly used by StatefulSet
type PodTemplate struct {
	Name            string            `json:"name"                      yaml:"name"`
	GenerateName    string            `json:"generateName,omitempty"    yaml:"generateName,omitempty"`
	Zone            PodTemplateZone   `json:"zone,omitempty"            yaml:"zone,omitempty"`
	PodDistribution []PodDistribution `json:"podDistribution,omitempty" yaml:"podDistribution,omitempty"`
	ObjectMeta      meta.ObjectMeta   `json:"metadata,omitempty"        yaml:"metadata,omitempty"`
	Spec            core.PodSpec      `json:"spec,omitempty"            yaml:"spec,omitempty"`
}

// PodTemplateZone defines pod template zone
type PodTemplateZone struct {
	Key    string   `json:"key,omitempty"    yaml:"key,omitempty"`
	Values []string `json:"values,omitempty" yaml:"values,omitempty"`
}

// PodDistribution defines pod distribution
type PodDistribution struct {
	Type        string `json:"type,omitempty"        yaml:"type,omitempty"`
	Scope       string `json:"scope,omitempty"       yaml:"scope,omitempty"`
	Number      int    `json:"number,omitempty"      yaml:"number,omitempty"`
	TopologyKey string `json:"topologyKey,omitempty" yaml:"topologyKey,omitempty"`
}

// ServiceTemplate defines CHI service template
type ServiceTemplate struct {
	Name         string           `json:"name"                   yaml:"name"`
	GenerateName string           `json:"generateName,omitempty" yaml:"generateName,omitempty"`
	ObjectMeta   meta.ObjectMeta  `json:"metadata,omitempty"     yaml:"metadata,omitempty"`
	Spec         core.ServiceSpec `json:"spec,omitempty"         yaml:"spec,omitempty"`
}

// DistributedDDL defines distributedDDL section of .spec.defaults
type DistributedDDL struct {
	Profile string `json:"profile,omitempty" yaml:"profile"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallationList defines a list of ClickHouseInstallation resources
type ClickHouseInstallationList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseInstallation `json:"items" yaml:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseInstallationTemplateList defines CHI template list
type ClickHouseInstallationTemplateList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseInstallationTemplate `json:"items" yaml:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseOperatorConfigurationList defines CHI operator config list
type ClickHouseOperatorConfigurationList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseOperatorConfiguration `json:"items" yaml:"items"`
}

// Secured interface for nodes and hosts
type Secured interface {
	IsSecure() bool
}
