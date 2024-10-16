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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
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
