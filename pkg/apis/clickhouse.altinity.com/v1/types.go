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
	"strings"
	"time"
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
	metav1.TypeMeta   `json:",inline"            yaml:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Spec              ChiSpec   `json:"spec"     yaml:"spec"`
	Status            ChiStatus `json:"status"   yaml:"status"`

	Attributes ComparableAttributes `json:"-" yaml:"-"`
}

// ComparableAttributes specifies CHI attributes that are comparable
type ComparableAttributes struct {
	ExchangeEnv  []corev1.EnvVar `json:"-" yaml:"-"`
	SkipOwnerRef bool            `json:"-" yaml:"-"`
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
	metav1.TypeMeta   `json:",inline"               yaml:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"    yaml:"metadata,omitempty"`
	Spec              OperatorConfig `json:"spec"   yaml:"spec"`
	Status            string         `json:"status" yaml:"status"`
}

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	TaskID                 *string          `json:"taskID,omitempty"                 yaml:"taskID,omitempty"`
	Stop                   string           `json:"stop,omitempty"                   yaml:"stop,omitempty"`
	Restart                string           `json:"restart,omitempty"                yaml:"restart,omitempty"`
	Troubleshoot           string           `json:"troubleshoot,omitempty"           yaml:"troubleshoot,omitempty"`
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

// ChiTemplating defines templating policy struct
type ChiTemplating struct {
	Policy string `json:"policy,omitempty" yaml:"policy,omitempty"`
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
	case MergeTypeOverrideByNonEmptyValues:
		if from.Policy != "" {
			// Override by non-empty values only
			t.Policy = from.Policy
		}
	}

	return t
}

// Possible objects cleanup options
const (
	ObjectsCleanupUnspecified = "Unspecified"
	ObjectsCleanupRetain      = "Retain"
	ObjectsCleanupDelete      = "Delete"
)

// ChiObjectsCleanup specifies object cleanup struct
type ChiObjectsCleanup struct {
	StatefulSet string `json:"statefulSet,omitempty" yaml:"statefulSet,omitempty"`
	PVC         string `json:"pvc,omitempty"         yaml:"pvc,omitempty"`
	ConfigMap   string `json:"configMap,omitempty"   yaml:"configMap,omitempty"`
	Service     string `json:"service,omitempty"     yaml:"service,omitempty"`
}

// NewChiObjectsCleanup creates new object cleanup
func NewChiObjectsCleanup() *ChiObjectsCleanup {
	return new(ChiObjectsCleanup)
}

// MergeFrom merges from specified cleanup
func (c *ChiObjectsCleanup) MergeFrom(from *ChiObjectsCleanup, _type MergeType) *ChiObjectsCleanup {
	if from == nil {
		return c
	}

	if c == nil {
		c = NewChiObjectsCleanup()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if c.StatefulSet == "" {
			c.StatefulSet = from.StatefulSet
		}
		if c.PVC == "" {
			c.PVC = from.PVC
		}
		if c.ConfigMap == "" {
			c.ConfigMap = from.ConfigMap
		}
		if c.Service == "" {
			c.Service = from.Service
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.StatefulSet != "" {
			// Override by non-empty values only
			c.StatefulSet = from.StatefulSet
		}
		if from.PVC != "" {
			// Override by non-empty values only
			c.PVC = from.PVC
		}
		if from.ConfigMap != "" {
			// Override by non-empty values only
			c.ConfigMap = from.ConfigMap
		}
		if from.Service != "" {
			// Override by non-empty values only
			c.Service = from.Service
		}
	}

	return c
}

// GetStatefulSet gets stateful set
func (c *ChiObjectsCleanup) GetStatefulSet() string {
	if c == nil {
		return ""
	}
	return c.StatefulSet
}

// SetStatefulSet sets stateful set
func (c *ChiObjectsCleanup) SetStatefulSet(v string) *ChiObjectsCleanup {
	if c == nil {
		return nil
	}
	c.StatefulSet = v
	return c
}

// GetPVC gets PVC
func (c *ChiObjectsCleanup) GetPVC() string {
	if c == nil {
		return ""
	}
	return c.PVC
}

// SetPVC sets PVC
func (c *ChiObjectsCleanup) SetPVC(v string) *ChiObjectsCleanup {
	if c == nil {
		return nil
	}
	c.PVC = v
	return c
}

// GetConfigMap gets config map
func (c *ChiObjectsCleanup) GetConfigMap() string {
	if c == nil {
		return ""
	}
	return c.ConfigMap
}

// SetConfigMap sets config map
func (c *ChiObjectsCleanup) SetConfigMap(v string) *ChiObjectsCleanup {
	if c == nil {
		return nil
	}
	c.ConfigMap = v
	return c
}

// GetService gets service
func (c *ChiObjectsCleanup) GetService() string {
	if c == nil {
		return ""
	}
	return c.Service
}

// SetService sets service
func (c *ChiObjectsCleanup) SetService(v string) *ChiObjectsCleanup {
	if c == nil {
		return nil
	}
	c.Service = v
	return c
}

// ChiCleanup defines cleanup
type ChiCleanup struct {
	// UnknownObjects specifies cleanup of unknown objects
	UnknownObjects *ChiObjectsCleanup `json:"unknownObjects,omitempty" yaml:"unknownObjects,omitempty"`
	// ReconcileFailedObjects specifies cleanup of failed objects
	ReconcileFailedObjects *ChiObjectsCleanup `json:"reconcileFailedObjects,omitempty" yaml:"reconcileFailedObjects,omitempty"`
}

// NewChiCleanup creates new cleanup
func NewChiCleanup() *ChiCleanup {
	return new(ChiCleanup)
}

// MergeFrom merges from specified cleanup
func (t *ChiCleanup) MergeFrom(from *ChiCleanup, _type MergeType) *ChiCleanup {
	if from == nil {
		return t
	}

	if t == nil {
		t = NewChiCleanup()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
	case MergeTypeOverrideByNonEmptyValues:
	}

	t.UnknownObjects = t.UnknownObjects.MergeFrom(from.UnknownObjects, _type)
	t.ReconcileFailedObjects = t.ReconcileFailedObjects.MergeFrom(from.ReconcileFailedObjects, _type)

	return t
}

// GetUnknownObjects gets unknown objects cleanup
func (t *ChiCleanup) GetUnknownObjects() *ChiObjectsCleanup {
	if t == nil {
		return nil
	}
	return t.UnknownObjects
}

// DefaultUnknownObjects makes default cleanup for known objects
func (t *ChiCleanup) DefaultUnknownObjects() *ChiObjectsCleanup {
	return NewChiObjectsCleanup().
		SetStatefulSet(ObjectsCleanupDelete).
		SetPVC(ObjectsCleanupDelete).
		SetConfigMap(ObjectsCleanupDelete).
		SetService(ObjectsCleanupDelete)
}

// GetReconcileFailedObjects gets failed objects cleanup
func (t *ChiCleanup) GetReconcileFailedObjects() *ChiObjectsCleanup {
	if t == nil {
		return nil
	}
	return t.ReconcileFailedObjects
}

// DefaultReconcileFailedObjects makes default cleanup for failed objects
func (t *ChiCleanup) DefaultReconcileFailedObjects() *ChiObjectsCleanup {
	return NewChiObjectsCleanup().
		SetStatefulSet(ObjectsCleanupRetain).
		SetPVC(ObjectsCleanupRetain).
		SetConfigMap(ObjectsCleanupRetain).
		SetService(ObjectsCleanupRetain)
}

// SetDefaults set defaults for cleanup
func (t *ChiCleanup) SetDefaults() *ChiCleanup {
	if t == nil {
		return nil
	}
	t.UnknownObjects = t.DefaultUnknownObjects()
	t.ReconcileFailedObjects = t.DefaultReconcileFailedObjects()
	return t
}

// ChiReconciling defines CHI reconciling struct
type ChiReconciling struct {
	// About to be DEPRECATED
	Policy string `json:"policy,omitempty" yaml:"policy,omitempty"`
	// ConfigMapPropagationTimeout specifies timeout for ConfigMap to propagate
	ConfigMapPropagationTimeout int `json:"configMapPropagationTimeout,omitempty" yaml:"configMapPropagationTimeout,omitempty"`
	// Cleanup specifies cleanup behavior
	Cleanup *ChiCleanup `json:"cleanup,omitempty" yaml:"cleanup,omitempty"`
}

// NewChiReconciling creates new reconciling
func NewChiReconciling() *ChiReconciling {
	return new(ChiReconciling)
}

// MergeFrom merges from specified reconciling
func (t *ChiReconciling) MergeFrom(from *ChiReconciling, _type MergeType) *ChiReconciling {
	if from == nil {
		return t
	}

	if t == nil {
		t = NewChiReconciling()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if t.Policy == "" {
			t.Policy = from.Policy
		}
		if t.ConfigMapPropagationTimeout == 0 {
			t.ConfigMapPropagationTimeout = from.ConfigMapPropagationTimeout
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.Policy != "" {
			// Override by non-empty values only
			t.Policy = from.Policy
		}
		if from.ConfigMapPropagationTimeout != 0 {
			// Override by non-empty values only
			t.ConfigMapPropagationTimeout = from.ConfigMapPropagationTimeout
		}
	}

	t.Cleanup = t.Cleanup.MergeFrom(from.Cleanup, _type)

	return t
}

// SetDefaults set default values for reconciling
func (t *ChiReconciling) SetDefaults() *ChiReconciling {
	if t == nil {
		return nil
	}
	t.Policy = ReconcilingPolicyUnspecified
	t.ConfigMapPropagationTimeout = 60
	t.Cleanup = NewChiCleanup().SetDefaults()
	return t
}

// GetPolicy gets policy
func (t *ChiReconciling) GetPolicy() string {
	if t == nil {
		return ""
	}
	return t.Policy
}

// SetPolicy sets policy
func (t *ChiReconciling) SetPolicy(p string) {
	if t == nil {
		return
	}
	t.Policy = p
}

// GetConfigMapPropagationTimeout gets config map propagation timeout
func (t *ChiReconciling) GetConfigMapPropagationTimeout() int {
	if t == nil {
		return 0
	}
	return t.ConfigMapPropagationTimeout
}

// SetConfigMapPropagationTimeout sets config map propagation timeout
func (t *ChiReconciling) SetConfigMapPropagationTimeout(timeout int) {
	if t == nil {
		return
	}
	t.ConfigMapPropagationTimeout = timeout
}

// GetConfigMapPropagationTimeoutDuration gets config map propagation timeout duration
func (t *ChiReconciling) GetConfigMapPropagationTimeoutDuration() time.Duration {
	if t == nil {
		return 0
	}
	return time.Duration(t.GetConfigMapPropagationTimeout()) * time.Second
}

// Possible reconcile policy values
const (
	ReconcilingPolicyUnspecified = "unspecified"
	ReconcilingPolicyWait        = "wait"
	ReconcilingPolicyNoWait      = "nowait"
)

// IsReconcilingPolicyWait checks whether reconcile policy is "wait"
func (t *ChiReconciling) IsReconcilingPolicyWait() bool {
	return strings.ToLower(t.GetPolicy()) == ReconcilingPolicyWait
}

// IsReconcilingPolicyNoWait checks whether reconcile policy is "no wait"
func (t *ChiReconciling) IsReconcilingPolicyNoWait() bool {
	return strings.ToLower(t.GetPolicy()) == ReconcilingPolicyNoWait
}

// GetCleanup gets cleanup
func (t *ChiReconciling) GetCleanup() *ChiCleanup {
	if t == nil {
		return nil
	}
	return t.Cleanup
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

	// VolumeClaimTemplate is deprecated in favor of DataVolumeClaimTemplate and LogVolumeClaimTemplate
	// !!! DEPRECATED !!!
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

	// DefinitionType is DEPRECATED - to be removed soon
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

// ChiPortDistribution defines port distribution
type ChiPortDistribution struct {
	Type string `json:"type,omitempty"   yaml:"type,omitempty"`
}

// ChiHostConfig defines additional data related to a host
type ChiHostConfig struct {
	ZookeeperFingerprint string `json:"zookeeperfingerprint" yaml:"zookeeperfingerprint"`
	SettingsFingerprint  string `json:"settingsfingerprint"  yaml:"settingsfingerprint"`
	FilesFingerprint     string `json:"filesfingerprint"     yaml:"filesfingerprint"`
}

// StatefulSetStatus specifies StatefulSet status
type StatefulSetStatus string

// Possible values for StatefulSet status
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

// NewChiHostReconcileAttributes creates new reconcile attributes
func NewChiHostReconcileAttributes() *ChiHostReconcileAttributes {
	return &ChiHostReconcileAttributes{}
}

// Equal checks whether reconcile attributes are equal
func (s *ChiHostReconcileAttributes) Equal(to ChiHostReconcileAttributes) bool {
	if s == nil {
		return false
	}
	return (s.add == to.add) && (s.remove == to.remove) && (s.modify == to.modify) && (s.unclear == to.unclear)
}

// Any checks whether any of the attributes is set
func (s *ChiHostReconcileAttributes) Any(to ChiHostReconcileAttributes) bool {
	if s == nil {
		return false
	}
	return (s.add && to.add) || (s.remove && to.remove) || (s.modify && to.modify) || (s.unclear && to.unclear)
}

// SetStatus sets status
func (s *ChiHostReconcileAttributes) SetStatus(status StatefulSetStatus) *ChiHostReconcileAttributes {
	s.status = status
	return s
}

// GetStatus gets status
func (s *ChiHostReconcileAttributes) GetStatus() StatefulSetStatus {
	return s.status
}

// SetAdd sets 'add' attribute
func (s *ChiHostReconcileAttributes) SetAdd() *ChiHostReconcileAttributes {
	s.add = true
	return s
}

// UnsetAdd unsets 'add' attribute
func (s *ChiHostReconcileAttributes) UnsetAdd() *ChiHostReconcileAttributes {
	s.add = false
	return s
}

// SetRemove sets 'remove' attribute
func (s *ChiHostReconcileAttributes) SetRemove() *ChiHostReconcileAttributes {
	s.remove = true
	return s
}

// SetModify sets 'modify' attribute
func (s *ChiHostReconcileAttributes) SetModify() *ChiHostReconcileAttributes {
	s.modify = true
	return s
}

// SetUnclear sets 'unclear' attribute
func (s *ChiHostReconcileAttributes) SetUnclear() *ChiHostReconcileAttributes {
	s.unclear = true
	return s
}

// IsAdd checks whether 'add' attribute is set
func (s *ChiHostReconcileAttributes) IsAdd() bool {
	return s.add
}

// IsRemove checks whether 'remove' attribute is set
func (s *ChiHostReconcileAttributes) IsRemove() bool {
	return s.remove
}

// IsModify checks whether 'modify' attribute is set
func (s *ChiHostReconcileAttributes) IsModify() bool {
	return s.modify
}

// IsUnclear checks whether 'unclear' attribute is set
func (s *ChiHostReconcileAttributes) IsUnclear() bool {
	return s.unclear
}

// ChiTemplates defines templates section of .spec
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
	Name            string               `json:"name"                      yaml:"name"`
	GenerateName    string               `json:"generateName,omitempty"    yaml:"generateName,omitempty"`
	Zone            ChiPodTemplateZone   `json:"zone,omitempty"            yaml:"zone,omitempty"`
	PodDistribution []ChiPodDistribution `json:"podDistribution,omitempty" yaml:"podDistribution,omitempty"`
	ObjectMeta      metav1.ObjectMeta    `json:"metadata,omitempty"        yaml:"metadata,omitempty"`
	Spec            corev1.PodSpec       `json:"spec,omitempty"            yaml:"spec,omitempty"`
}

// ChiPodTemplateZone defines pod template zone
type ChiPodTemplateZone struct {
	Key    string   `json:"key,omitempty"    yaml:"key,omitempty"`
	Values []string `json:"values,omitempty" yaml:"values,omitempty"`
}

// ChiPodDistribution defines pod distribution
type ChiPodDistribution struct {
	Type        string `json:"type,omitempty"        yaml:"type,omitempty"`
	Scope       string `json:"scope,omitempty"       yaml:"scope,omitempty"`
	Number      int    `json:"number,omitempty"      yaml:"number,omitempty"`
	TopologyKey string `json:"topologyKey,omitempty" yaml:"topologyKey,omitempty"`
}

// ChiVolumeClaimTemplate defines PersistentVolumeClaim Template, directly used by StatefulSet
type ChiVolumeClaimTemplate struct {
	Name             string                           `json:"name"                    yaml:"name"`
	PVCReclaimPolicy PVCReclaimPolicy                 `json:"reclaimPolicy,omitempty" yaml:"reclaimPolicy,omitempty"`
	ObjectMeta       metav1.ObjectMeta                `json:"metadata,omitempty"      yaml:"metadata,omitempty"`
	Spec             corev1.PersistentVolumeClaimSpec `json:"spec,omitempty"          yaml:"spec,omitempty"`
}

// PVCReclaimPolicy defines PVC reclaim policy
type PVCReclaimPolicy string

// Possible values of PVC reclaim policy
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

// ChiServiceTemplate defines CHI service template
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

// ClickHouseInstallationTemplateList defines CHI template list
type ClickHouseInstallationTemplateList struct {
	metav1.TypeMeta `json:",inline"  yaml:",inline"`
	metav1.ListMeta `json:"metadata" yaml:"metadata"`
	Items           []ClickHouseInstallationTemplate `json:"items" yaml:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseOperatorConfigurationList defines CHI operator config list
type ClickHouseOperatorConfigurationList struct {
	metav1.TypeMeta `json:",inline"  yaml:",inline"`
	metav1.ListMeta `json:"metadata" yaml:"metadata"`
	Items           []ClickHouseOperatorConfiguration `json:"items" yaml:"items"`
}
