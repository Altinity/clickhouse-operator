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
