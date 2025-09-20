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
	"strings"
	"time"
)

// ChiReconcile defines reconcile specification
type ChiReconcile struct {
	// About to be DEPRECATED
	Policy string `json:"policy,omitempty" yaml:"policy,omitempty"`

	// ConfigMapPropagationTimeout specifies timeout for ConfigMap to propagate
	ConfigMapPropagationTimeout int `json:"configMapPropagationTimeout,omitempty" yaml:"configMapPropagationTimeout,omitempty"`
	// Cleanup specifies cleanup behavior
	Cleanup *Cleanup `json:"cleanup,omitempty" yaml:"cleanup,omitempty"`
	// Macros specifies macros application rules
	Macros ReconcileMacros `json:"macros,omitempty" yaml:"macros,omitempty"`

	// Runtime specifies runtime settings
	Runtime ReconcileRuntime `json:"runtime,omitempty" yaml:"runtime,omitempty"`
	// Host specifies host-lever reconcile settings
	Host ReconcileHost `json:"host" yaml:"host"`
}

type ClusterReconcile struct {
	// Runtime specifies runtime settings
	Runtime ReconcileRuntime `json:"runtime" yaml:"runtime"`
	// Host specifies host-lever reconcile settings
	Host ReconcileHost `json:"host" yaml:"host"`
}

// NewChiReconcile creates new reconcile
func NewChiReconcile() *ChiReconcile {
	return new(ChiReconcile)
}

// MergeFrom merges from specified reconcile
func (r *ChiReconcile) MergeFrom(from *ChiReconcile, _type MergeType) *ChiReconcile {
	if from == nil {
		return r
	}

	if r == nil {
		r = NewChiReconcile()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if r.Policy == "" {
			r.Policy = from.Policy
		}
		if r.ConfigMapPropagationTimeout == 0 {
			r.ConfigMapPropagationTimeout = from.ConfigMapPropagationTimeout
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.Policy != "" {
			// Override by non-empty values only
			r.Policy = from.Policy
		}
		if from.ConfigMapPropagationTimeout != 0 {
			// Override by non-empty values only
			r.ConfigMapPropagationTimeout = from.ConfigMapPropagationTimeout
		}
	}

	r.Cleanup = r.Cleanup.MergeFrom(from.Cleanup, _type)
	r.Runtime = r.Runtime.MergeFrom(from.Runtime, _type)
	r.Macros = r.Macros.MergeFrom(from.Macros, _type)

	return r
}

// SetDefaults set default values for reconcile
func (r *ChiReconcile) SetDefaults() *ChiReconcile {
	if r == nil {
		return nil
	}
	r.Policy = ReconcilingPolicyUnspecified
	r.ConfigMapPropagationTimeout = 10
	r.Cleanup = NewCleanup().SetDefaults()
	return r
}

// GetPolicy gets policy
func (r *ChiReconcile) GetPolicy() string {
	if r == nil {
		return ""
	}
	return r.Policy
}

// SetPolicy sets policy
func (r *ChiReconcile) SetPolicy(p string) {
	if r == nil {
		return
	}
	r.Policy = p
}

func (r *ChiReconcile) HasConfigMapPropagationTimeout() bool {
	return r.GetConfigMapPropagationTimeout() > 0
}

// GetConfigMapPropagationTimeout gets config map propagation timeout
func (r *ChiReconcile) GetConfigMapPropagationTimeout() int {
	if r == nil {
		return 0
	}
	return r.ConfigMapPropagationTimeout
}

// SetConfigMapPropagationTimeout sets config map propagation timeout
func (r *ChiReconcile) SetConfigMapPropagationTimeout(timeout int) {
	if r == nil {
		return
	}
	r.ConfigMapPropagationTimeout = timeout
}

// GetConfigMapPropagationTimeoutDuration gets config map propagation timeout duration
func (r *ChiReconcile) GetConfigMapPropagationTimeoutDuration() time.Duration {
	if r == nil {
		return 0
	}
	return time.Duration(r.GetConfigMapPropagationTimeout()) * time.Second
}

// Possible reconcile policy values
const (
	ReconcilingPolicyUnspecified = "unspecified"
	ReconcilingPolicyWait        = "wait"
	ReconcilingPolicyNoWait      = "nowait"
)

// IsReconcilingPolicyWait checks whether reconcile policy is "wait"
func (r *ChiReconcile) IsReconcilingPolicyWait() bool {
	return strings.ToLower(r.GetPolicy()) == ReconcilingPolicyWait
}

// IsReconcilingPolicyNoWait checks whether reconcile policy is "no wait"
func (r *ChiReconcile) IsReconcilingPolicyNoWait() bool {
	return strings.ToLower(r.GetPolicy()) == ReconcilingPolicyNoWait
}

// GetCleanup gets cleanup
func (r *ChiReconcile) GetCleanup() *Cleanup {
	if r == nil {
		return nil
	}
	return r.Cleanup
}

// GetCleanup gets cleanup
func (r *ChiReconcile) SetCleanup(cleanup *Cleanup) {
	if r == nil {
		return
	}
	r.Cleanup = cleanup
}

func (r *ChiReconcile) InheritRuntimeFrom(from OperatorConfigReconcileRuntime) {
	if r == nil {
		return
	}

	if r.Runtime.ReconcileShardsThreadsNumber == 0 {
		r.Runtime.ReconcileShardsThreadsNumber = from.ReconcileShardsThreadsNumber
	}
	if r.Runtime.ReconcileShardsMaxConcurrencyPercent == 0 {
		r.Runtime.ReconcileShardsMaxConcurrencyPercent = from.ReconcileShardsMaxConcurrencyPercent
	}
}

func (r *ChiReconcile) InheritHostFrom(from ReconcileHost) {
	r.Host = r.Host.MergeFrom(from)
}
