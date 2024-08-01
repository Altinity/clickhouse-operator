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
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type ContainerManagerKeeper struct {
	probe *ProbeManagerKeeper
}

func NewContainerManagerKeeper(probe *ProbeManagerKeeper) *ContainerManagerKeeper {
	return &ContainerManagerKeeper{
		probe: probe,
	}
}

func (cm *ContainerManagerKeeper) NewDefaultAppContainer(host *apiChi.Host) core.Container {
	return cm.newDefaultContainerKeeper(host)
}

func (cm *ContainerManagerKeeper) GetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return cm.getContainerKeeper(statefulSet)
}

func (cm *ContainerManagerKeeper) EnsureAppContainer(statefulSet *apps.StatefulSet, host *apiChi.Host) {
	cm.ensureContainerSpecifiedKeeper(statefulSet, host)
}

func (cm *ContainerManagerKeeper) EnsureLogContainer(statefulSet *apps.StatefulSet) {
}

func (cm *ContainerManagerKeeper) getContainerKeeper(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.KeeperContainerName)
}

func (cm *ContainerManagerKeeper) ensureContainerSpecifiedKeeper(statefulSet *apps.StatefulSet, host *apiChi.Host) {
	_, ok := cm.getContainerKeeper(statefulSet)
	if ok {
		return
	}

	// No ClickHouse container available, let's add one
	k8s.PodSpecAddContainer(
		&statefulSet.Spec.Template.Spec,
		cm.newDefaultContainerKeeper(host),
	)
}

func (cm *ContainerManagerKeeper) createInitContainers(chk *apiChk.ClickHouseKeeperInstallation) []core.Container {
	return []core.Container{
		cm.newDefaultContainerServerIDInjector(),
	}

}

func (cm *ContainerManagerKeeper) newDefaultContainerServerIDInjector() core.Container {
	container := core.Container{
		Name:  config.KeeperServerIDInjectorContainerName,
		Image: config.DefaultServerIDInjectorDockerImage,
		Command: []string{
			`bash`,
			`-xc`,
			// Build keeper config
			`export KEEPER_ID=${HOSTNAME##*-};` +
				`sed "s/KEEPER_ID/${KEEPER_ID}/g" /tmp/clickhouse-keeper/keeper_config.xml > ` + config.DirPathConfigFilename + `;` +
				`cat ` + config.DirPathConfigFilename,
		},
	}
	k8s.ContainerAppendVolumeMounts(
		&container,
		core.VolumeMount{
			Name:      "keeper-config",
			MountPath: "/tmp/clickhouse-keeper",
		},
		core.VolumeMount{
			Name:      "etc-clickhouse-keeper",
			MountPath: "/etc/clickhouse-keeper",
		},
	)
	return container
}

func (cm *ContainerManagerKeeper) createContainers(chk *apiChk.ClickHouseKeeperInstallation) []core.Container {
	containers := []core.Container{
		cm.newDefaultContainerKeeper(nil),
	}
	clientPort := chk.Spec.GetClientPort()
	setupPort(
		&containers[0],
		clientPort,
		core.ContainerPort{
			Name:          "client",
			ContainerPort: int32(clientPort),
		})
	raftPort := chk.Spec.GetRaftPort()
	setupPort(
		&containers[0],
		raftPort,
		core.ContainerPort{
			Name:          "raft",
			ContainerPort: int32(raftPort),
		})
	prometheusPort := chk.Spec.GetPrometheusPort()
	if prometheusPort != -1 {
		setupPort(
			&containers[0],
			prometheusPort,
			core.ContainerPort{
				Name:          "prometheus",
				ContainerPort: int32(prometheusPort),
			})
	}

	//switch length := len(chk2.getVolumeClaimTemplates(chk)); length {
	//case 0:
	//	containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountVolumes(chk)...)
	//case 1:
	//	containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountSharedVolume(chk)...)
	//case 2:
	//	containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountVolumes(chk)...)
	//}
	//containers[0].VolumeMounts = append(containers[0].VolumeMounts,
	//	core.VolumeMount{
	//		Name:      "etc-clickhouse-keeper",
	//		MountPath: "/etc/clickhouse-keeper",
	//	})

	return containers
}

func (cm *ContainerManagerKeeper) newDefaultContainerKeeper(host *apiChi.Host) core.Container {
	container := core.Container{
		Name:          config.KeeperContainerName,
		Image:         config.DefaultKeeperDockerImage,
		LivenessProbe: cm.probe.createDefaultKeeperLivenessProbe(host),
	}
	k8s.ContainerAppendVolumeMounts(
		&container,
		core.VolumeMount{
			Name:      "keeper-config",
			MountPath: "/tmp/clickhouse-keeper",
		},
		core.VolumeMount{
			Name:      "etc-clickhouse-keeper",
			MountPath: "/etc/clickhouse-keeper",
		},
	)
	return container

}

func setupPort(container *core.Container, port int, containerPort core.ContainerPort) {
	// Check whether such a port already specified in the container
	for _, p := range container.Ports {
		if p.ContainerPort == int32(port) {
			// Yes, such a port already specified in the container, nothing to do here
			return
		}
	}

	// Port is not specified in the container, let's specify it
	container.Ports = append(container.Ports, containerPort)
}
