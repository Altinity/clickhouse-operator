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
	"fmt"

	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// CreatePodDisruptionBudget creates new PodDisruptionBudget
func (c *Creator) CreatePodDisruptionBudget(cluster api.ICluster) *policy.PodDisruptionBudget {
	return &policy.PodDisruptionBudget{
		TypeMeta: meta.TypeMeta{
			Kind:       "PodDisruptionBudget",
			APIVersion: "policy/v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:            fmt.Sprintf("%s-%s", cluster.GetRuntime().GetAddress().GetCRName(), cluster.GetRuntime().GetAddress().GetClusterName()),
			Namespace:       c.cr.GetNamespace(),
			Labels:          c.macro.Scope(c.cr).Map(c.tagger.Label(interfaces.LabelPDB, cluster)),
			Annotations:     c.macro.Scope(c.cr).Map(c.tagger.Annotate(interfaces.AnnotatePDB, cluster)),
			OwnerReferences: c.or.CreateOwnerReferences(c.cr),
		},
		Spec: policy.PodDisruptionBudgetSpec{
			Selector: &meta.LabelSelector{
				MatchLabels: c.tagger.Selector(interfaces.SelectorClusterScope, cluster),
			},
			MaxUnavailable: &intstr.IntOrString{
				Type:   intstr.Int,
				IntVal: cluster.GetPDBMaxUnavailable().Value(),
			},
		},
	}
}
