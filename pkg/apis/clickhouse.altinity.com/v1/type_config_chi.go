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

func (configuration *ChiConfiguration) MergeFrom(from *ChiConfiguration, _type MergeType) {
	if from == nil {
		return
	}

	(&configuration.Zookeeper).MergeFrom(&from.Zookeeper, _type)
	(&configuration.Users).MergeFrom(from.Users)
	(&configuration.Profiles).MergeFrom(from.Profiles)
	(&configuration.Quotas).MergeFrom(from.Quotas)
	(&configuration.Settings).MergeFrom(from.Settings)
	(&configuration.Files).MergeFrom(from.Files)

	// TODO merge clusters
	// Copy Clusters for now
	configuration.Clusters = from.Clusters
}
