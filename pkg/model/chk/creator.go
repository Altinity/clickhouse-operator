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

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// CreateConfigMap returns a config map containing ClickHouse Keeper config XML
func CreateConfigMap(chk *api.ClickHouseKeeperInstallation) *core.ConfigMap {
	chk.Spec.Settings = util.MergeStringMapsPreserve(chk.Spec.Settings, defaultKeeperSettings(chk.Spec.GetPath()))

	return &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      chk.Name,
			Namespace: chk.Namespace,
		},
		Data: map[string]string{
			"keeper_config.xml": generateXMLConfig(chk.Spec.Settings, chk),
		},
	}
}

// CreateStatefulSet return a clickhouse keeper stateful set from the chk spec
func CreateStatefulSet(chk *api.ClickHouseKeeperInstallation) *apps.StatefulSet {
	labels := GetPodLabels(chk)
	annotations := getPodAnnotations(chk)
	replicas := chk.Spec.GetReplicas()

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
			ServiceName: getHeadlessServiceName(chk),
			Replicas:    &replicas,
			Selector: &meta.LabelSelector{
				MatchLabels: labels,
			},
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: apps.OrderedReadyPodManagement,
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{
					GenerateName: chk.GetName(),
					Labels:       labels,
					Annotations:  annotations,
				},
				Spec: createPodTemplateSpec(chk),
			},
			VolumeClaimTemplates: chk.Spec.VolumeClaimTemplates,
		},
	}
}

func createPodTemplateSpec(chk *api.ClickHouseKeeperInstallation) core.PodSpec {
	if chk.Spec.PodTemplate == nil {
		chk.Spec.PodTemplate = &api.ChkPodTemplate{}
	}
	podSpec := chk.Spec.PodTemplate.Spec

	if len(podSpec.Volumes) == 0 {
		podSpec.Volumes = createVolumes(chk)
	}
	podSpec.InitContainers = createInitContainers(chk)
	podSpec.Containers = createContainers(chk)

	return podSpec
}

func createVolumes(chk *api.ClickHouseKeeperInstallation) []core.Volume {
	var volumes []core.Volume

	switch length := len(chk.Spec.VolumeClaimTemplates); length {
	case 0:
		volumes = append(volumes, createEphemeralVolume("log-storage-path"))
		volumes = append(volumes, createEphemeralVolume("snapshot-storage-path"))
	case 1:
		volumes = append(volumes, createPVCVolume("both-paths"))
	case 2:
		volumes = append(volumes, createPVCVolume("log-storage-path"))
		volumes = append(volumes, createPVCVolume("snapshot-storage-path"))
	}
	if path := chk.Spec.GetPath(); path != "" {
		volumes = append(volumes, createEphemeralVolume("working-dir"))
	}

	volumes = append(volumes, createEphemeralVolume("etc-clickhouse-keeper"))
	volumes = append(volumes, createConfigMapVolume("keeper-config", chk.Name, "keeper_config.xml", "keeper_config.xml"))

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

func createConfigMapVolume(name string, chkName string, key string, path string) core.Volume {
	return core.Volume{
		Name: name,
		VolumeSource: core.VolumeSource{
			ConfigMap: &core.ConfigMapVolumeSource{
				LocalObjectReference: core.LocalObjectReference{
					Name: chkName,
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

func createInitContainers(chk *api.ClickHouseKeeperInstallation) []core.Container {
	var initContainers []core.Container

	if len(chk.Spec.PodTemplate.Spec.InitContainers) == 0 {
		initContainers = []core.Container{{}}
	} else {
		initContainers = chk.Spec.PodTemplate.Spec.InitContainers
	}
	if initContainers[0].Name == "" {
		initContainers[0].Name = "server-id-injector"
	}
	if initContainers[0].Image == "" {
		initContainers[0].Image = "bash"
	}
	if len(initContainers[0].Command) == 0 {
		initContainers[0].Command = []string{
			"bash",
			"-xc",
			"export KEEPER_ID=${HOSTNAME##*-}; sed \"s/KEEPER_ID/$KEEPER_ID/g\" /tmp/clickhouse-keeper/keeper_config.xml > /etc/clickhouse-keeper/keeper_config.xml; cat /etc/clickhouse-keeper/keeper_config.xml",
		}
	}
	initContainers[0].VolumeMounts = append(initContainers[0].VolumeMounts,
		core.VolumeMount{
			Name:      "keeper-config",
			MountPath: "/tmp/clickhouse-keeper",
		},
	)
	initContainers[0].VolumeMounts = append(initContainers[0].VolumeMounts,
		core.VolumeMount{
			Name:      "etc-clickhouse-keeper",
			MountPath: "/etc/clickhouse-keeper",
		},
	)

	return initContainers
}

func createContainers(chk *api.ClickHouseKeeperInstallation) []core.Container {
	var containers []core.Container
	if len(chk.Spec.PodTemplate.Spec.Containers) == 0 {
		containers = []core.Container{{}}
	} else {
		containers = chk.Spec.PodTemplate.Spec.Containers
	}

	if containers[0].Name == "" {
		containers[0].Name = "clickhouse-keeper"
	}
	if containers[0].Image == "" {
		containers[0].Image = "clickhouse/clickhouse-keeper:head-alpine"
	}
	if containers[0].LivenessProbe == nil {
		probeScript := fmt.Sprintf(
			"date && OK=$(exec 3<>/dev/tcp/127.0.0.1/%d ; printf 'ruok' >&3 ; IFS=; tee <&3; exec 3<&- ;); if [[ \"$OK\" == \"imok\" ]]; then exit 0; else exit 1; fi",
			chk.Spec.GetClientPort())
		containers[0].LivenessProbe = &core.Probe{
			ProbeHandler: core.ProbeHandler{
				Exec: &core.ExecAction{
					Command: []string{
						"bash",
						"-xc",
						probeScript,
					},
				},
			},
			InitialDelaySeconds: 60,
			PeriodSeconds:       3,
			FailureThreshold:    10,
		}
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
	promethuesPort := chk.Spec.GetPrometheusPort()
	if promethuesPort != -1 {
		setupPort(
			&containers[0],
			promethuesPort,
			core.ContainerPort{
				Name:          "prometheus",
				ContainerPort: int32(promethuesPort),
			})
	}

	switch length := len(chk.Spec.VolumeClaimTemplates); length {
	case 0:
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountVolumes(chk)...)
	case 1:
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountSharedVolume(chk)...)
	case 2:
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountVolumes(chk)...)
	}
	containers[0].VolumeMounts = append(containers[0].VolumeMounts,
		core.VolumeMount{
			Name:      "etc-clickhouse-keeper",
			MountPath: "/etc/clickhouse-keeper",
		})

	return containers
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

// CreateClientService returns a client service resource for the clickhouse keeper cluster
func CreateClientService(chk *api.ClickHouseKeeperInstallation) *core.Service {
	svcPorts := []core.ServicePort{
		core.ServicePort{
			Name: "client",
			Port: int32(chk.Spec.GetClientPort()),
		},
	}

	prometheusPort := chk.Spec.GetPrometheusPort()
	if prometheusPort != -1 {
		svcPorts = append(svcPorts,
			core.ServicePort{
				Name: "prometheus",
				Port: int32(prometheusPort),
			},
		)
	}

	return createService(chk.Name, chk, svcPorts, true)
}

// CreateHeadlessService returns an internal headless-service for the chk stateful-set
func CreateHeadlessService(chk *api.ClickHouseKeeperInstallation) *core.Service {
	svcPorts := []core.ServicePort{
		{
			Name: "raft",
			Port: int32(chk.Spec.GetRaftPort()),
		},
	}
	return createService(getHeadlessServiceName(chk), chk, svcPorts, false)
}

func createService(name string, chk *api.ClickHouseKeeperInstallation, ports []core.ServicePort, clusterIP bool) *core.Service {
	service := core.Service{
		TypeMeta: meta.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: chk.Namespace,
		},
		Spec: core.ServiceSpec{
			Ports:    ports,
			Selector: GetPodLabels(chk),
		},
	}
	if !clusterIP {
		service.Spec.ClusterIP = core.ClusterIPNone
	}
	return &service
}

// CreatePodDisruptionBudget returns a pdb for the clickhouse keeper cluster
func CreatePodDisruptionBudget(chk *api.ClickHouseKeeperInstallation) *policy.PodDisruptionBudget {
	pdbCount := intstr.FromInt(1)
	return &policy.PodDisruptionBudget{
		TypeMeta: meta.TypeMeta{
			Kind:       "PodDisruptionBudget",
			APIVersion: "policy/v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      chk.GetName(),
			Namespace: chk.Namespace,
		},
		Spec: policy.PodDisruptionBudgetSpec{
			MaxUnavailable: &pdbCount,
			Selector: &meta.LabelSelector{
				MatchLabels: map[string]string{
					"app": chk.GetName(),
				},
			},
		},
	}
}

func setupPort(container *core.Container, port int, containerPort core.ContainerPort) {
	// Check whether such a port already specified
	for _, p := range container.Ports {
		if p.ContainerPort == int32(port) {
			// Yes, such a port already specified, nothing to do here
			return
		}
	}

	// Port is not specified, let's specify it
	container.Ports = append(container.Ports, containerPort)
}
