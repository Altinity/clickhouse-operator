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

func (configuration *ChiConfiguration) MergeFrom(from *ChiConfiguration) {
	if from == nil {
		return
	}

	(&configuration.Zookeeper).MergeFrom(&from.Zookeeper)
	mapStringInterfaceMergeFrom(&configuration.Users, &from.Users)
	mapStringInterfaceMergeFrom(&configuration.Profiles, &from.Profiles)
	mapStringInterfaceMergeFrom(&configuration.Quotas, &from.Quotas)
	mapStringInterfaceMergeFrom(&configuration.Settings, &from.Settings)
	mapStringStringMergeFrom(&configuration.Files, &from.Files)

	// TODO merge clusters
	// Copy Clusters for now
	configuration.Clusters = from.Clusters
}

// mapStringInterfaceMergeFrom merges into `dst` non-empty new-key-values from `src` in case no such `key` already in `src`
func mapStringInterfaceMergeFrom(dst, src *map[string]interface{}) {
	if (src == nil) || (*src == nil) {
		return
	}

	if *dst == nil {
		*dst = make(map[string]interface{})
	}

	for key, value := range *src {
		if _, ok := (*dst)[key]; ok {
			// Such key already exists in dst
			continue
		}

		// No such a key in dst
		(*dst)[key] = value
	}
}

// mapStringStringMergeFrom merges into `dst` non-empty new-key-values from `src` in case no such `key` already in `src`
func mapStringStringMergeFrom(dst, src *map[string]string) {
	if (src == nil) || (*src == nil) {
		return
	}

	if *dst == nil {
		*dst = make(map[string]string)
	}

	for key, value := range *src {
		if _, ok := (*dst)[key]; ok {
			// Such key already exists in dst
			continue
		}

		// No such a key in dst
		(*dst)[key] = value
	}
}
