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
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/imdario/mergo"
	"sync"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseKeeperInstallation defines a ClickHouse Keeper ChkCluster
type ClickHouseKeeperInstallation struct {
	meta.TypeMeta      `json:",inline"                     yaml:",inline"`
	meta.ObjectMeta    `json:"metadata,omitempty"          yaml:"metadata,omitempty"`
	Spec               ChkSpec    `json:"spec"             yaml:"spec"`
	Status             *ChkStatus `json:"status,omitempty" yaml:"status,omitempty"`
	statusCreatorMutex sync.Mutex
}

// EnsureStatus ensures status
func (chk *ClickHouseKeeperInstallation) EnsureStatus() *ChkStatus {
	if chk == nil {
		return nil
	}

	// Assume that most of the time, we'll see a non-nil value.
	if chk.Status != nil {
		return chk.Status
	}

	// Otherwise, we need to acquire a lock to initialize the field.
	chk.statusCreatorMutex.Lock()
	defer chk.statusCreatorMutex.Unlock()
	// Note that we have to check this property again to avoid a TOCTOU bug.
	if chk.Status == nil {
		chk.Status = &ChkStatus{}
	}
	return chk.Status
}

// GetStatus gets Status
func (chk *ClickHouseKeeperInstallation) GetStatus() *ChkStatus {
	if chk == nil {
		return nil
	}
	return chk.Status
}

// HasStatus checks whether CHI has Status
func (chk *ClickHouseKeeperInstallation) HasStatus() bool {
	if chk == nil {
		return false
	}
	return chk.Status != nil
}

// HasAncestor checks whether CHI has an ancestor
func (chk *ClickHouseKeeperInstallation) HasAncestor() bool {
	if !chk.HasStatus() {
		return false
	}
	return chk.Status.HasNormalizedCHKCompleted()
}

// GetAncestor gets ancestor of a CHI
func (chk *ClickHouseKeeperInstallation) GetAncestor() *ClickHouseKeeperInstallation {
	if !chk.HasAncestor() {
		return nil
	}
	return chk.Status.GetNormalizedCHKCompleted()
}

// SetAncestor sets ancestor of a CHI
func (chk *ClickHouseKeeperInstallation) SetAncestor(a *ClickHouseKeeperInstallation) {
	if chk == nil {
		return
	}
	chk.EnsureStatus().NormalizedCHKCompleted = a
}

// HasTarget checks whether CHI has a target
func (chk *ClickHouseKeeperInstallation) HasTarget() bool {
	if !chk.HasStatus() {
		return false
	}
	return chk.Status.HasNormalizedCHK()
}

// GetTarget gets target of a CHI
func (chk *ClickHouseKeeperInstallation) GetTarget() *ClickHouseKeeperInstallation {
	if !chk.HasTarget() {
		return nil
	}
	return chk.Status.GetNormalizedCHK()
}

// SetTarget sets target of a CHI
func (chk *ClickHouseKeeperInstallation) SetTarget(a *ClickHouseKeeperInstallation) {
	if chk == nil {
		return
	}
	chk.EnsureStatus().NormalizedCHK = a
}

// MergeFrom merges from CHI
func (chk *ClickHouseKeeperInstallation) MergeFrom(from *ClickHouseKeeperInstallation, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	// Merge Meta
	switch _type {
	case apiChi.MergeTypeFillEmptyValues:
		_ = mergo.Merge(&chk.TypeMeta, from.TypeMeta)
		_ = mergo.Merge(&chk.ObjectMeta, from.ObjectMeta)
	case apiChi.MergeTypeOverrideByNonEmptyValues:
		_ = mergo.Merge(&chk.TypeMeta, from.TypeMeta, mergo.WithOverride)
		_ = mergo.Merge(&chk.ObjectMeta, from.ObjectMeta, mergo.WithOverride)
	}
	// Exclude skipped annotations
	chk.Annotations = util.CopyMapFilter(
		chk.Annotations,
		nil,
		util.ListSkippedAnnotations(),
	)

	// Do actual merge for Spec
	(&chk.Spec).MergeFrom(&from.Spec, _type)

	chk.EnsureStatus().CopyFrom(from.Status, apiChi.CopyCHIStatusOptions{
		InheritableFields: true,
	})
}

// ChkSpec defines spec section of ClickHouseKeeper resource
type ChkSpec struct {
	Configuration *ChkConfiguration `json:"configuration,omitempty"          yaml:"configuration,omitempty"`
	Templates     *ChkTemplates     `json:"templates,omitempty"              yaml:"templates,omitempty"`
}

func (spec ChkSpec) GetConfiguration() *ChkConfiguration {
	return spec.Configuration
}

func (spec ChkSpec) EnsureConfiguration() *ChkConfiguration {
	if spec.GetConfiguration() == nil {
		spec.Configuration = new(ChkConfiguration)
	}
	return spec.Configuration
}

func (spec ChkSpec) GetTemplates() *ChkTemplates {
	return spec.Templates
}

// MergeFrom merges from spec
func (spec *ChkSpec) MergeFrom(from *ChkSpec, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	spec.Configuration = spec.Configuration.MergeFrom(from.Configuration, _type)
	spec.Templates = spec.Templates.MergeFrom(from.Templates, _type)
}

// ChkConfiguration defines configuration section of .spec
type ChkConfiguration struct {
	Settings *apiChi.Settings `json:"settings,omitempty"  yaml:"settings,omitempty"`
	Clusters []*ChkCluster    `json:"clusters,omitempty"  yaml:"clusters,omitempty"`
}

// NewConfiguration creates new ChkConfiguration objects
func NewConfiguration() *ChkConfiguration {
	return new(ChkConfiguration)
}

func (c *ChkConfiguration) GetSettings() *apiChi.Settings {
	if c == nil {
		return nil
	}

	return c.Settings
}

func (c *ChkConfiguration) GetClusters() []*ChkCluster {
	if c == nil {
		return nil
	}

	return c.Clusters
}

func (c *ChkConfiguration) GetCluster(i int) *ChkCluster {
	clusters := c.GetClusters()
	if clusters == nil {
		return nil
	}
	if i >= len(clusters) {
		return nil
	}
	return clusters[i]
}

// MergeFrom merges from specified source
func (configuration *ChkConfiguration) MergeFrom(from *ChkConfiguration, _type apiChi.MergeType) *ChkConfiguration {
	if from == nil {
		return configuration
	}

	if configuration == nil {
		configuration = NewConfiguration()
	}

	configuration.Settings = configuration.Settings.MergeFrom(from.Settings)

	// TODO merge clusters
	// Copy Clusters for now
	configuration.Clusters = from.Clusters

	return configuration
}

// ChkCluster defines item of a clusters section of .configuration
type ChkCluster struct {
	Name   string            `json:"name,omitempty"         yaml:"name,omitempty"`
	Layout *ChkClusterLayout `json:"layout,omitempty"       yaml:"layout,omitempty"`
}

func (c *ChkCluster) GetLayout() *ChkClusterLayout {
	if c == nil {
		return nil
	}
	return c.Layout
}

// ChkClusterLayout defines layout section of .spec.configuration.clusters
type ChkClusterLayout struct {
	// The valid range of size is from 1 to 7.
	ReplicasCount int `json:"replicasCount,omitempty" yaml:"replicasCount,omitempty"`
}

// NewChkClusterLayout creates new cluster layout
func NewChkClusterLayout() *ChkClusterLayout {
	return new(ChkClusterLayout)
}

func (c *ChkClusterLayout) GetReplicasCount() int {
	if c == nil {
		return 0
	}
	return c.ReplicasCount
}

// ChkTemplates defines templates section of .spec
type ChkTemplates struct {
	PodTemplates         []apiChi.ChiPodTemplate         `json:"podTemplates,omitempty"         yaml:"podTemplates,omitempty"`
	VolumeClaimTemplates []apiChi.ChiVolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates,omitempty"`
	ServiceTemplates     []apiChi.ChiServiceTemplate     `json:"serviceTemplates,omitempty"     yaml:"serviceTemplates,omitempty"`

	// Index maps template name to template itself
	PodTemplatesIndex         *apiChi.PodTemplatesIndex         `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	VolumeClaimTemplatesIndex *apiChi.VolumeClaimTemplatesIndex `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	ServiceTemplatesIndex     *apiChi.ServiceTemplatesIndex     `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// NewChkTemplates creates new ChkTemplates object
func NewChkTemplates() *ChkTemplates {
	return new(ChkTemplates)
}

// Len returns accumulated len of all templates
func (templates *ChkTemplates) Len() int {
	if templates == nil {
		return 0
	}

	return 0 +
		len(templates.PodTemplates) +
		len(templates.VolumeClaimTemplates) +
		len(templates.ServiceTemplates)
}

// MergeFrom merges from specified object
func (templates *ChkTemplates) MergeFrom(from *ChkTemplates, _type apiChi.MergeType) *ChkTemplates {
	if from.Len() == 0 {
		return templates
	}

	if templates == nil {
		templates = NewChkTemplates()
	}

	// Merge sections

	templates.mergePodTemplates(from)
	templates.mergeVolumeClaimTemplates(from)
	templates.mergeServiceTemplates(from)

	return templates
}

// mergePodTemplates merges pod templates section
func (templates *ChkTemplates) mergePodTemplates(from *ChkTemplates) {
	if len(from.PodTemplates) == 0 {
		return
	}

	// We have templates to merge from
	// Loop over all 'from' templates and either copy it in case no such template in receiver or merge it
	for fromIndex := range from.PodTemplates {
		fromTemplate := &from.PodTemplates[fromIndex]

		// Try to find entry with the same name among local templates in receiver
		sameNameFound := false
		for toIndex := range templates.PodTemplates {
			toTemplate := &templates.PodTemplates[toIndex]
			if toTemplate.Name == fromTemplate.Name {
				// Receiver already have such a template
				sameNameFound = true

				//toSpec := &toTemplate.Spec
				//fromSpec := &fromTemplate.Spec
				//_ = mergo.Merge(toSpec, *fromSpec, mergo.WithGrowSlice, mergo.WithOverride, mergo.WithOverrideEmptySlice)

				// Merge `to` template with `from` template
				_ = mergo.Merge(toTemplate, *fromTemplate, mergo.WithSliceDeepMerge)
				// Receiver `to` template is processed
				break
			}
		}

		if !sameNameFound {
			// Receiver does not have template with such a name
			// Append template from `from`
			templates.PodTemplates = append(templates.PodTemplates, *fromTemplate.DeepCopy())
		}
	}
}

// mergeVolumeClaimTemplates merges volume claim templates section
func (templates *ChkTemplates) mergeVolumeClaimTemplates(from *ChkTemplates) {
	if len(from.VolumeClaimTemplates) == 0 {
		return
	}

	// We have templates to merge from
	// Loop over all 'from' templates and either copy it in case no such template in receiver or merge it
	for fromIndex := range from.VolumeClaimTemplates {
		fromTemplate := &from.VolumeClaimTemplates[fromIndex]

		// Try to find entry with the same name among local templates in receiver
		sameNameFound := false
		for toIndex := range templates.VolumeClaimTemplates {
			toTemplate := &templates.VolumeClaimTemplates[toIndex]
			if toTemplate.Name == fromTemplate.Name {
				// Receiver already have such a template
				sameNameFound = true
				// Merge `to` template with `from` template
				_ = mergo.Merge(toTemplate, *fromTemplate, mergo.WithSliceDeepMerge)
				// Receiver `to` template is processed
				break
			}
		}

		if !sameNameFound {
			// Receiver does not have template with such a name
			// Append template from `from`
			templates.VolumeClaimTemplates = append(templates.VolumeClaimTemplates, *fromTemplate.DeepCopy())
		}
	}
}

// mergeServiceTemplates merges service templates section
func (templates *ChkTemplates) mergeServiceTemplates(from *ChkTemplates) {
	if len(from.ServiceTemplates) == 0 {
		return
	}

	// We have templates to merge from
	// Loop over all 'from' templates and either copy it in case no such template in receiver or merge it
	for fromIndex := range from.ServiceTemplates {
		fromTemplate := &from.ServiceTemplates[fromIndex]

		// Try to find entry with the same name among local templates in receiver
		sameNameFound := false
		for toIndex := range templates.ServiceTemplates {
			toTemplate := &templates.ServiceTemplates[toIndex]
			if toTemplate.Name == fromTemplate.Name {
				// Receiver already have such a template
				sameNameFound = true
				// Merge `to` template with `from` template
				_ = mergo.Merge(toTemplate, *fromTemplate, mergo.WithSliceDeepCopy)
				// Receiver `to` template is processed
				break
			}
		}

		if !sameNameFound {
			// Receiver does not have template with such a name
			// Append template from `from`
			templates.ServiceTemplates = append(templates.ServiceTemplates, *fromTemplate.DeepCopy())
		}
	}
}

// GetPodTemplatesIndex returns index of pod templates
func (templates *ChkTemplates) GetPodTemplatesIndex() *apiChi.PodTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.PodTemplatesIndex
}

// EnsurePodTemplatesIndex ensures index exists
func (templates *ChkTemplates) EnsurePodTemplatesIndex() *apiChi.PodTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.PodTemplatesIndex != nil {
		return templates.PodTemplatesIndex
	}
	templates.PodTemplatesIndex = apiChi.NewPodTemplatesIndex()
	return templates.PodTemplatesIndex
}

// GetVolumeClaimTemplatesIndex returns index of VolumeClaim templates
func (templates *ChkTemplates) GetVolumeClaimTemplatesIndex() *apiChi.VolumeClaimTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.VolumeClaimTemplatesIndex
}

// EnsureVolumeClaimTemplatesIndex ensures index exists
func (templates *ChkTemplates) EnsureVolumeClaimTemplatesIndex() *apiChi.VolumeClaimTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.VolumeClaimTemplatesIndex != nil {
		return templates.VolumeClaimTemplatesIndex
	}
	templates.VolumeClaimTemplatesIndex = apiChi.NewVolumeClaimTemplatesIndex()
	return templates.VolumeClaimTemplatesIndex
}

// GetServiceTemplatesIndex returns index of Service templates
func (templates *ChkTemplates) GetServiceTemplatesIndex() *apiChi.ServiceTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.ServiceTemplatesIndex
}

// EnsureServiceTemplatesIndex ensures index exists
func (templates *ChkTemplates) EnsureServiceTemplatesIndex() *apiChi.ServiceTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.ServiceTemplatesIndex != nil {
		return templates.ServiceTemplatesIndex
	}
	templates.ServiceTemplatesIndex = apiChi.NewServiceTemplatesIndex()
	return templates.ServiceTemplatesIndex
}

func (t *ChkTemplates) GetPodTemplates() []apiChi.ChiPodTemplate {
	if t == nil {
		return nil
	}

	return t.PodTemplates
}

func (t *ChkTemplates) GetVolumeClaimTemplates() []apiChi.ChiVolumeClaimTemplate {
	if t == nil {
		return nil
	}

	return t.VolumeClaimTemplates
}

func (t *ChkTemplates) GetServiceTemplates() []apiChi.ChiServiceTemplate {
	if t == nil {
		return nil
	}

	return t.ServiceTemplates
}

func (spec *ChkSpec) GetPath() string {
	switch {
	case spec.GetConfiguration().GetSettings().Has("keeper_server/storage_path"):
		return spec.GetConfiguration().GetSettings().Get("keeper_server/storage_path").String()

	case spec.GetConfiguration().GetSettings().Has("keeper_server/path"):
		return spec.GetConfiguration().GetSettings().Get("keeper_server/path").String()

	default:
		return "/var/lib/clickhouse_keeper"
	}
}

func (spec *ChkSpec) GetPort(name string, defaultValue int) int {
	// Has no setting - use default value
	if !spec.GetConfiguration().GetSettings().Has(name) {
		return defaultValue
	}

	// Port name is specified
	return spec.GetConfiguration().GetSettings().Get(name).ScalarInt()
}

func (spec *ChkSpec) GetClientPort() int {
	return spec.GetPort("keeper_server/tcp_port", 9181)
}

func (spec *ChkSpec) GetRaftPort() int {
	return spec.GetPort("keeper_server/raft_configuration/server/port", 9234)
}

func (spec *ChkSpec) GetPrometheusPort() int {
	return spec.GetPort("prometheus/port", -1)
}

// ChkStatus defines status section of ClickHouseKeeper resource
type ChkStatus struct {
	CHOpVersion string `json:"chop-version,omitempty"           yaml:"chop-version,omitempty"`
	CHOpCommit  string `json:"chop-commit,omitempty"            yaml:"chop-commit,omitempty"`
	CHOpDate    string `json:"chop-date,omitempty"              yaml:"chop-date,omitempty"`
	CHOpIP      string `json:"chop-ip,omitempty"                yaml:"chop-ip,omitempty"`

	Status string `json:"status,omitempty"                 yaml:"status,omitempty"`

	// Replicas is the number of number of desired replicas in the cluster
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of number of ready replicas in the cluster
	ReadyReplicas []apiChi.ChiZookeeperNode `json:"readyReplicas,omitempty"`

	Pods                   []string                      `json:"pods,omitempty"                   yaml:"pods,omitempty"`
	PodIPs                 []string                      `json:"pod-ips,omitempty"                yaml:"pod-ips,omitempty"`
	FQDNs                  []string                      `json:"fqdns,omitempty"                  yaml:"fqdns,omitempty"`
	NormalizedCHK          *ClickHouseKeeperInstallation `json:"normalized,omitempty"             yaml:"normalized,omitempty"`
	NormalizedCHKCompleted *ClickHouseKeeperInstallation `json:"normalizedCompleted,omitempty"    yaml:"normalizedCompleted,omitempty"`
}

// CopyFrom copies the state of a given ChiStatus f into the receiver ChiStatus of the call.
func (s *ChkStatus) CopyFrom(from *ChkStatus, opts apiChi.CopyCHIStatusOptions) {
	if s == nil || from == nil {
		return
	}

	if opts.InheritableFields {
	}

	if opts.MainFields {
		s.CHOpVersion = from.CHOpVersion
		s.CHOpCommit = from.CHOpCommit
		s.CHOpDate = from.CHOpDate
		s.CHOpIP = from.CHOpIP
		s.Status = from.Status
		s.Replicas = from.Replicas
		s.ReadyReplicas = from.ReadyReplicas
		s.Pods = from.Pods
		s.PodIPs = from.PodIPs
		s.FQDNs = from.FQDNs
		s.NormalizedCHK = from.NormalizedCHK
	}

	if opts.Normalized {
		s.NormalizedCHK = from.NormalizedCHK
	}

	if opts.WholeStatus {
		s.CHOpVersion = from.CHOpVersion
		s.CHOpCommit = from.CHOpCommit
		s.CHOpDate = from.CHOpDate
		s.CHOpIP = from.CHOpIP
		s.Status = from.Status
		s.Replicas = from.Replicas
		s.ReadyReplicas = from.ReadyReplicas
		s.Pods = from.Pods
		s.PodIPs = from.PodIPs
		s.FQDNs = from.FQDNs
		s.NormalizedCHK = from.NormalizedCHK
		s.NormalizedCHKCompleted = from.NormalizedCHKCompleted
	}
}

// HasNormalizedCHKCompleted is a checker
func (s *ChkStatus) HasNormalizedCHKCompleted() bool {
	return s.GetNormalizedCHKCompleted() != nil
}

// HasNormalizedCHK is a checker
func (s *ChkStatus) HasNormalizedCHK() bool {
	return s.GetNormalizedCHK() != nil
}

// ClearNormalizedCHK clears normalized CHK in status
func (s *ChkStatus) ClearNormalizedCHK() {
	s.NormalizedCHK = nil
}

// GetNormalizedCHK gets target CHK
func (s *ChkStatus) GetNormalizedCHK() *ClickHouseKeeperInstallation {
	return s.NormalizedCHK
}

// GetNormalizedCHKCompleted gets completed CHI
func (s *ChkStatus) GetNormalizedCHKCompleted() *ClickHouseKeeperInstallation {
	return s.NormalizedCHKCompleted
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseKeeperList defines a list of ClickHouseKeeper resources
type ClickHouseKeeperInstallationList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseKeeperInstallation `json:"items" yaml:"items"`
}
