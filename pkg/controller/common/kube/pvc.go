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

package kube

import (
	"strings"

	core "k8s.io/api/core/v1"
)

// FilterPVCsOfPod keeps in the list PVCs of the specified pod only.
// PVCs of a host are listed by the host scope selector, which carries the shard and replica names,
// and a replica name of a host defined in layout.shards[].replicas is its position in the list.
// When hosts shift in the list - one is removed or inserted not at the tail - PVCs of another host
// carry the same labels until they are reconciled, so the selector alone may return PVCs of two hosts.
// A PVC of a StatefulSet pod is named "<volume claim template name>-<pod name>", the pod name is unique for a host.
func FilterPVCsOfPod(list *core.PersistentVolumeClaimList, podName string) *core.PersistentVolumeClaimList {
	if list == nil {
		return nil
	}
	items := list.Items[:0]
	for _, pvc := range list.Items {
		if strings.HasSuffix(pvc.GetName(), "-"+podName) {
			items = append(items, pvc)
		}
	}
	list.Items = items
	return list
}
