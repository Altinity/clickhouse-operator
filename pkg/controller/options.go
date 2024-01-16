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

package controller

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// NewListOptions returns filled meta.ListOptions
func NewListOptions(labelsMaps ...map[string]string) meta.ListOptions {
	if len(labelsMaps) == 0 {
		return meta.ListOptions{}
	}

	labelMap := labelsMaps[0]
	labelSelector := labels.SelectorFromSet(labelMap)
	return meta.ListOptions{
		LabelSelector: labelSelector.String(),
	}
}

// NewGetOptions returns filled metav1.GetOptions
func NewGetOptions() meta.GetOptions {
	return meta.GetOptions{}
}

// NewCreateOptions returns filled metav1.CreateOptions
func NewCreateOptions() meta.CreateOptions {
	return meta.CreateOptions{}
}

// NewUpdateOptions returns filled metav1.UpdateOptions
func NewUpdateOptions() meta.UpdateOptions {
	return meta.UpdateOptions{}
}

// NewPatchOptions returns filled metav1.PatchOptions
func NewPatchOptions() meta.PatchOptions {
	return meta.PatchOptions{}
}

// NewDeleteOptions returns filled *metav1.DeleteOptions
func NewDeleteOptions() meta.DeleteOptions {
	gracePeriodSeconds := int64(0)
	propagationPolicy := meta.DeletePropagationForeground
	return meta.DeleteOptions{
		GracePeriodSeconds: &gracePeriodSeconds,
		PropagationPolicy:  &propagationPolicy,
	}
}
