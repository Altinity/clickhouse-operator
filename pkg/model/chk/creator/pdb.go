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

package creator

import (
	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// CreatePodDisruptionBudget returns a pdb for the clickhouse keeper cluster
func CreatePodDisruptionBudget(cr api.ICustomResource) *policy.PodDisruptionBudget {
	pdbCount := intstr.FromInt(1)
	return &policy.PodDisruptionBudget{
		TypeMeta: meta.TypeMeta{
			Kind:       "PodDisruptionBudget",
			APIVersion: "policy/v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      cr.GetName(),
			Namespace: cr.GetNamespace(),
		},
		Spec: policy.PodDisruptionBudgetSpec{
			MaxUnavailable: &pdbCount,
			Selector: &meta.LabelSelector{
				MatchLabels: map[string]string{
					"app": cr.GetName(),
				},
			},
		},
	}
}
