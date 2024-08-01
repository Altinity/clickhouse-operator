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
	chk2 "github.com/altinity/clickhouse-operator/pkg/model/chk"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateStatefulSet return a clickhouse keeper stateful set from the chk spec
func CreateStatefulSet(chk *api.ClickHouseKeeperInstallation) *apps.StatefulSet {
	labels := labeler.GetPodLabels(chk)
	//	annotations := chk2.getPodAnnotations(chk)
	replicas := int32(chk2.GetReplicasCount(chk))

	return &apps.StatefulSet{
		TypeMeta: meta.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      chk.GetName(),
			Namespace: chk.Namespace,
			Labels:    labels,
		},
		Spec: apps.StatefulSetSpec{
			Replicas: &replicas,
			//			ServiceName: chk2.getHeadlessServiceName(chk),
			Selector: &meta.LabelSelector{
				MatchLabels: labels,
			},

			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{
					GenerateName: chk.GetName(),
					Labels:       labels,
					//					Annotations:  annotations,
				},
				Spec: createPodTemplateSpec(chk),
			},
			//			VolumeClaimTemplates: chk2.getVolumeClaimTemplates(chk),

			PodManagementPolicy: apps.OrderedReadyPodManagement,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
			RevisionHistoryLimit: chop.Config().GetRevisionHistoryLimit(),
		},
	}
}

func createPodTemplateSpec(chk *api.ClickHouseKeeperInstallation) core.PodSpec {
	//podSpec := chk2.getPodTemplate(chk).Spec
	//
	//if len(podSpec.Volumes) == 0 {
	//	podSpec.Volumes = createVolumes(chk)
	//}
	//podSpec.InitContainers = createInitContainers(chk)
	//podSpec.Containers = createContainers(chk)
	//
	//return podSpec
	return core.PodSpec{}
}

func createVolumes(chk *api.ClickHouseKeeperInstallation) []core.Volume {
	var volumes []core.Volume

	//switch length := len(chk2.getVolumeClaimTemplates(chk)); length {
	//case 0:
	//	volumes = append(volumes, createEphemeralVolume("log-storage-path"))
	//	volumes = append(volumes, createEphemeralVolume("snapshot-storage-path"))
	//case 1:
	//	volumes = append(volumes, createPVCVolume("both-paths"))
	//case 2:
	//	volumes = append(volumes, createPVCVolume("log-storage-path"))
	//	volumes = append(volumes, createPVCVolume("snapshot-storage-path"))
	//}
	//if path := chk.Spec.GetPath(); path != "" {
	//	volumes = append(volumes, createEphemeralVolume("working-dir"))
	//}
	//
	//volumes = append(volumes, createEphemeralVolume("etc-clickhouse-keeper"))
	//volumes = append(volumes, createConfigMapVolume("keeper-config", chk.Name, "keeper_config.xml", "keeper_config.xml"))

	return volumes
}

func createEphemeralVolume(name string) core.Volume {
	return core.Volume{
		Name: name,
		VolumeSource: core.VolumeSource{
			EmptyDir: &core.EmptyDirVolumeSource{
				Medium:    core.StorageMediumDefault,
				SizeLimit: nil,
			},
		},
	}
}

func createPVCVolume(name string) core.Volume {
	return core.Volume{
		Name: name,
		VolumeSource: core.VolumeSource{
			PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
				ClaimName: name,
			},
		},
	}
}

func createConfigMapVolume(volumeName string, configMapName string, key string, path string) core.Volume {
	return core.Volume{
		Name: volumeName,
		VolumeSource: core.VolumeSource{
			ConfigMap: &core.ConfigMapVolumeSource{
				LocalObjectReference: core.LocalObjectReference{
					Name: configMapName,
				},
				Items: []core.KeyToPath{
					{
						Key:  key,
						Path: path,
					},
				},
			},
		},
	}
}

func mountVolumes(chk *api.ClickHouseKeeperInstallation) []core.VolumeMount {
	path := chk.Spec.GetPath()
	return []core.VolumeMount{
		{
			Name:      "working-dir",
			MountPath: path,
		},
		{
			Name:      "log-storage-path",
			MountPath: fmt.Sprintf("%s/coordination/logs", path),
		},
		{
			Name:      "snapshot-storage-path",
			MountPath: fmt.Sprintf("%s/coordination/snapshots", path),
		},
	}
}

func mountSharedVolume(chk *api.ClickHouseKeeperInstallation) []core.VolumeMount {
	path := chk.Spec.GetPath()
	return []core.VolumeMount{
		{
			Name:      "working-dir",
			MountPath: path,
		},
		{
			Name:      "both-paths",
			MountPath: fmt.Sprintf("%s/coordination/logs", path),
			SubPath:   "logs",
		},
		{
			Name:      "both-paths",
			MountPath: fmt.Sprintf("%s/coordination/snapshots", path),
			SubPath:   "snapshots",
		},
	}
}
