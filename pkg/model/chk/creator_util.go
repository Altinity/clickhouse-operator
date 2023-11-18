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

package chk

import (
	"fmt"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func getPodAnnotations(chk *v1alpha1.ClickHouseKeeper) map[string]string {
	var annotations map[string]string
	if chk.Spec.PodTemplate != nil && chk.Spec.PodTemplate.ObjectMeta.Annotations != nil {
		annotations = chk.Spec.PodTemplate.ObjectMeta.Annotations
	}
	if port := chk.Spec.GetPrometheusPort(); port != -1 {
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations["prometheus.io/port"] = fmt.Sprintf("%d", port)
		annotations["prometheus.io/scrape"] = "true"
	}
	return annotations
}

func GetPodLabels(chk *v1alpha1.ClickHouseKeeper) map[string]string {
	var labels map[string]string
	if chk.Spec.PodTemplate != nil && chk.Spec.PodTemplate.ObjectMeta.Labels != nil {
		labels = chk.Spec.PodTemplate.ObjectMeta.Labels
	} else {
		labels = map[string]string{
			"app": chk.GetName(),
			"uid": string(chk.UID),
		}
	}
	return labels
}

func setupPort(container *corev1.Container, port int, containerPort corev1.ContainerPort) {
	found := false
	for _, p := range container.Ports {
		if p.ContainerPort == int32(port) {
			found = true
		}
	}
	if !found {
		container.Ports = append(container.Ports, containerPort)
	}
}
