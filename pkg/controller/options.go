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
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// NewListOptions returns filled metav1.ListOptions
func NewListOptions(labelsMap map[string]string) metaV1.ListOptions {
	labelSelector := labels.SelectorFromSet(labelsMap)
	return metaV1.ListOptions{
		LabelSelector: labelSelector.String(),
	}
}

// NewGetOptions returns filled metav1.GetOptions
func NewGetOptions() metaV1.GetOptions {
	return metaV1.GetOptions{}
}

// NewCreateOptions returns filled metav1.CreateOptions
func NewCreateOptions() metaV1.CreateOptions {
	return metaV1.CreateOptions{}
}

// NewUpdateOptions returns filled metav1.UpdateOptions
func NewUpdateOptions() metaV1.UpdateOptions {
	return metaV1.UpdateOptions{}
}

// NewPatchOptions returns filled metav1.PatchOptions
func NewPatchOptions() metaV1.PatchOptions {
	return metaV1.PatchOptions{}
}

// NewDeleteOptions returns filled *metav1.DeleteOptions
func NewDeleteOptions() metaV1.DeleteOptions {
	gracePeriodSeconds := int64(0)
	propagationPolicy := metaV1.DeletePropagationForeground
	return metaV1.DeleteOptions{
		GracePeriodSeconds: &gracePeriodSeconds,
		PropagationPolicy:  &propagationPolicy,
	}
}
