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

func (defaults *ChiDefaults) MergeFrom(from *ChiDefaults) {
	if from == nil {
		return
	}

	if from.ReplicasUseFQDN == "" {
		defaults.ReplicasUseFQDN = from.ReplicasUseFQDN
	}
	(&defaults.DistributedDDL).MergeFrom(&from.DistributedDDL)

	if defaults.Templates.PodTemplate == "" {
		defaults.Templates.PodTemplate = from.Templates.PodTemplate
	}

	if defaults.Templates.VolumeClaimTemplate == "" {
		defaults.Templates.VolumeClaimTemplate = from.Templates.VolumeClaimTemplate
	}
}
