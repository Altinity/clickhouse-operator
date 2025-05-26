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

// Reconciling defines reconciling specification
type Reconciling struct {
	// About to be DEPRECATED
	Policy string `json:"policy,omitempty" yaml:"policy,omitempty"`
	// ConfigMapPropagationTimeout specifies timeout for ConfigMap to propagate
	ConfigMapPropagationTimeout int `json:"configMapPropagationTimeout,omitempty" yaml:"configMapPropagationTimeout,omitempty"`
	// Cleanup specifies cleanup behavior
	Cleanup *Cleanup `json:"cleanup,omitempty" yaml:"cleanup,omitempty"`
	// Runtime specifies runtime settings
	Runtime ReconcileRuntime `json:"runtime" yaml:"runtime"`
}

// NewReconciling creates new reconciling
func NewReconciling() *Reconciling {
	return new(Reconciling)
}

// MergeFrom merges from specified reconciling
func (t *Reconciling) MergeFrom(from *Reconciling, _type MergeType) *Reconciling {
	if from == nil {
		return t
	}

	if t == nil {
		t = NewReconciling()
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
	t.Runtime = t.Runtime.MergeFrom(from.Runtime, _type)

	return t
}

// SetDefaults set default values for reconciling
func (t *Reconciling) SetDefaults() *Reconciling {
	if t == nil {
		return nil
	}
	t.Policy = ReconcilingPolicyUnspecified
	t.ConfigMapPropagationTimeout = 10
	t.Cleanup = NewCleanup().SetDefaults()
	return t
}

// GetPolicy gets policy
func (t *Reconciling) GetPolicy() string {
	if t == nil {
		return ""
	}
	return t.Policy
}

// SetPolicy sets policy
func (t *Reconciling) SetPolicy(p string) {
	if t == nil {
		return
	}
	t.Policy = p
}

func (t *Reconciling) HasConfigMapPropagationTimeout() bool {
	return t.GetConfigMapPropagationTimeout() > 0
}

// GetConfigMapPropagationTimeout gets config map propagation timeout
func (t *Reconciling) GetConfigMapPropagationTimeout() int {
	if t == nil {
		return 0
	}
	return t.ConfigMapPropagationTimeout
}

// SetConfigMapPropagationTimeout sets config map propagation timeout
func (t *Reconciling) SetConfigMapPropagationTimeout(timeout int) {
	if t == nil {
		return
	}
	t.ConfigMapPropagationTimeout = timeout
}

// GetConfigMapPropagationTimeoutDuration gets config map propagation timeout duration
func (t *Reconciling) GetConfigMapPropagationTimeoutDuration() time.Duration {
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
func (t *Reconciling) IsReconcilingPolicyWait() bool {
	return strings.ToLower(t.GetPolicy()) == ReconcilingPolicyWait
}

// IsReconcilingPolicyNoWait checks whether reconcile policy is "no wait"
func (t *Reconciling) IsReconcilingPolicyNoWait() bool {
	return strings.ToLower(t.GetPolicy()) == ReconcilingPolicyNoWait
}

// GetCleanup gets cleanup
func (t *Reconciling) GetCleanup() *Cleanup {
	if t == nil {
		return nil
	}
	return t.Cleanup
}

// GetCleanup gets cleanup
func (t *Reconciling) SetCleanup(cleanup *Cleanup) {
	if t == nil {
		return
	}
	t.Cleanup = cleanup
}
