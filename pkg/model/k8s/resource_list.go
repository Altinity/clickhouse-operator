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

package k8s

import (
	"github.com/juliangruber/go-intersect"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ResourcesListApply
func ResourcesListApply(curResourceList core.ResourceList, desiredResourceList core.ResourceList) bool {
	// Prepare lists of current resource names
	var curResourceNames []core.ResourceName
	for resourceName := range curResourceList {
		curResourceNames = append(curResourceNames, resourceName)
	}
	// Prepare lists of desired resource names
	var desiredResourceNames []core.ResourceName
	for resourceName := range desiredResourceList {
		desiredResourceNames = append(desiredResourceNames, resourceName)
	}

	// Prepare list of resources which needs to be applied-updated-replaced
	resourceNamesToApply := intersect.Simple(curResourceNames, desiredResourceNames).([]interface{})
	updated := false
	for _, resourceName := range resourceNamesToApply {
		updated = updated || ResourceListReplaceResourceQuantity(curResourceList, desiredResourceList, resourceName.(core.ResourceName))
	}
	return updated
}

// ResourceListReplaceResource
func ResourceListReplaceResourceQuantity(
	curResourceList core.ResourceList,
	desiredResourceList core.ResourceList,
	resourceName core.ResourceName,
) bool {
	if (curResourceList == nil) || (desiredResourceList == nil) {
		// Nowhere or nothing to apply
		return false
	}

	var (
		found                   bool
		curResourceQuantity     resource.Quantity
		desiredResourceQuantity resource.Quantity
	)

	if curResourceQuantity, found = curResourceList[resourceName]; !found {
		// No such resource in target list
		return false
	}

	if desiredResourceQuantity, found = desiredResourceList[resourceName]; !found {
		// No such resource in source list
		return false
	}

	if curResourceQuantity.Equal(desiredResourceQuantity) {
		// No need to apply
		return false
	}

	// Replace resource
	curResourceList[resourceName] = desiredResourceList[resourceName]
	return true
}
