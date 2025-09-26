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

type ReconcileRuntime struct {
	ReconcileShardsThreadsNumber         int `json:"reconcileShardsThreadsNumber,omitempty"         yaml:"reconcileShardsThreadsNumber,omitempty"`
	ReconcileShardsMaxConcurrencyPercent int `json:"reconcileShardsMaxConcurrencyPercent,omitempty" yaml:"reconcileShardsMaxConcurrencyPercent,omitempty"`
}

func (r ReconcileRuntime) MergeFrom(from ReconcileRuntime, _type MergeType) ReconcileRuntime {
	switch _type {
	case MergeTypeFillEmptyValues:
		if r.ReconcileShardsThreadsNumber == 0 {
			r.ReconcileShardsThreadsNumber = from.ReconcileShardsThreadsNumber
		}
		if r.ReconcileShardsMaxConcurrencyPercent == 0 {
			r.ReconcileShardsMaxConcurrencyPercent = from.ReconcileShardsMaxConcurrencyPercent
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.ReconcileShardsThreadsNumber != 0 {
			// Override by non-empty values only
			r.ReconcileShardsThreadsNumber = from.ReconcileShardsThreadsNumber
		}
		if from.ReconcileShardsMaxConcurrencyPercent != 0 {
			// Override by non-empty values only
			r.ReconcileShardsMaxConcurrencyPercent = from.ReconcileShardsMaxConcurrencyPercent
		}
	}
	return r
}
