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
	core "k8s.io/api/core/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func getPodTemplate(chk *apiChk.ClickHouseKeeperInstallation) apiChi.PodTemplate {
	if len(chk.Spec.GetTemplates().GetPodTemplates()) < 1 {
		return apiChi.PodTemplate{}
	}
	return chk.Spec.GetTemplates().GetPodTemplates()[0]
}

func getPodTemplateAnnotations(chk *apiChk.ClickHouseKeeperInstallation) map[string]string {
	if len(chk.Spec.GetTemplates().GetPodTemplates()) < 1 {
		return nil
	}

	return getPodTemplate(chk).ObjectMeta.Annotations
}

func getPodTemplateLabels(chk *apiChk.ClickHouseKeeperInstallation) map[string]string {
	if len(chk.Spec.GetTemplates().GetPodTemplates()) < 1 {
		return nil
	}

	return getPodTemplate(chk).ObjectMeta.Labels
}

func getVolumeClaimTemplates(chk *apiChk.ClickHouseKeeperInstallation) (claims []core.PersistentVolumeClaim) {
	for _, template := range chk.Spec.GetTemplates().GetVolumeClaimTemplates() {
		pvc := core.PersistentVolumeClaim{
			ObjectMeta: template.ObjectMeta,
			Spec:       template.Spec,
		}
		if pvc.Name == "" {
			pvc.Name = template.Name
		}
		claims = append(claims, pvc)
	}
	return claims
}
