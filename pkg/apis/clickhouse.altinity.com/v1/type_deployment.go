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

// MergeFrom updates empty fields of chiv1.ChiDeployment with values from `from` deployment
func (deployment *ChiDeployment) MergeFrom(from *ChiDeployment) {
	if from == nil {
		return
	}

	// Have source to merge from - copy locally unassigned values
	// Walk over all fields of ChiDeployment and assign from `from` in case unassigned

	if deployment.PodTemplate == "" {
		(*deployment).PodTemplate = from.PodTemplate
	}

	if deployment.VolumeClaimTemplate == "" {
		(*deployment).VolumeClaimTemplate = from.VolumeClaimTemplate
	}

	if len(deployment.Zone.MatchLabels) == 0 {
		(&deployment.Zone).CopyFrom(&from.Zone)
	}

	if deployment.Scenario == "" {
		(*deployment).Scenario = from.Scenario
	}
}
