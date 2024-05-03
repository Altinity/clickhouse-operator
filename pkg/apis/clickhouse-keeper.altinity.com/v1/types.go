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

	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/imdario/mergo"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseKeeperInstallation defines a ClickHouse Keeper ChkCluster
type ClickHouseKeeperInstallation struct {
	meta.TypeMeta   `json:",inline"                     yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"          yaml:"metadata,omitempty"`

	Spec   ChkSpec    `json:"spec"             yaml:"spec"`
	Status *ChkStatus `json:"status,omitempty" yaml:"status,omitempty"`

	Runtime ClickHouseKeeperInstallationRuntime `json:"-" yaml:"-"`
}

type ClickHouseKeeperInstallationRuntime struct {
	statusCreatorMutex sync.Mutex `json:"-" yaml:"-"`
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
	chk.Runtime.statusCreatorMutex.Lock()
	defer chk.Runtime.statusCreatorMutex.Unlock()
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
	Templates     *apiChi.Templates `json:"templates,omitempty"              yaml:"templates,omitempty"`
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

func (spec ChkSpec) GetTemplates() *apiChi.Templates {
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
	Name   string                `json:"name,omitempty"         yaml:"name,omitempty"`
	Layout *apiChi.ClusterLayout `json:"layout,omitempty"       yaml:"layout,omitempty"`
}

func (c *ChkCluster) GetLayout() *apiChi.ClusterLayout {
	if c == nil {
		return nil
	}
	return c.Layout
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

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseKeeperList defines a list of ClickHouseKeeper resources
type ClickHouseKeeperInstallationList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseKeeperInstallation `json:"items" yaml:"items"`
}
