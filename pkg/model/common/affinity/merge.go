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

package affinity

import (
	core "k8s.io/api/core/v1"
)

// Merge merges from src into dst and returns dst
func Merge(dst *core.Affinity, src *core.Affinity) *core.Affinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	created := false
	if dst == nil {
		// No receiver specified, allocate a new one
		dst = &core.Affinity{}
		created = true
	}

	dst.NodeAffinity = mergeNodeAffinity(dst.NodeAffinity, src.NodeAffinity)
	dst.PodAffinity = mergePodAffinity(dst.PodAffinity, src.PodAffinity)
	dst.PodAntiAffinity = mergePodAntiAffinity(dst.PodAntiAffinity, src.PodAntiAffinity)

	empty := (dst.NodeAffinity == nil) && (dst.PodAffinity == nil) && (dst.PodAntiAffinity == nil)
	if created && empty {
		// Do not return empty and internally created dst
		return nil
	}

	return dst
}
