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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// createConfigMap returns a config map containing ClickHouse Keeper config XML
func createConfigMap(chk *v1alpha1.ClickHouseKeeper) *corev1.ConfigMap {
	chk.Spec.Settings = util.MergeStringMapsPreserve(chk.Spec.Settings, defaultKeeperSettings(chk.Spec.GetPath()))

	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      chk.Name,
			Namespace: chk.Namespace,
		},
		Data: map[string]string{
			"keeper_config.xml": generateXMLConfig(chk.Spec.Settings, chk),
		},
	}
}

// createStatefulSet return a clickhouse keeper stateful set from the chk spec
func createStatefulSet(chk *v1alpha1.ClickHouseKeeper) *appsv1.StatefulSet {
	labels := getPodLabels(chk)
	annotations := getPodAnnotations(chk)
	replicas := chk.Spec.GetReplicas()

	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      chk.GetName(),
			Namespace: chk.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: getHeadlessServiceName(chk),
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
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

func createPodTemplateSpec(chk *v1alpha1.ClickHouseKeeper) corev1.PodSpec {
	if chk.Spec.PodTemplate == nil {
		chk.Spec.PodTemplate = &v1alpha1.ChkPodTemplate{}
	}
	podSpec := chk.Spec.PodTemplate.Spec

	if len(podSpec.Volumes) == 0 {
		podSpec.Volumes = createVolumes(chk)
	}
	podSpec.InitContainers = createInitContainers(chk)
	podSpec.Containers = createContainers(chk)

	return podSpec
}

func createVolumes(chk *v1alpha1.ClickHouseKeeper) []corev1.Volume {
	var volumes []corev1.Volume

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

func createEphemeralVolume(name string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumDefault,
				SizeLimit: nil,
			},
		},
	}
}

func createPVCVolume(name string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: name,
			},
		},
	}
}

func createConfigMapVolume(name string, chkName string, key string, path string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: chkName,
				},
				Items: []corev1.KeyToPath{
					{Key: key, Path: path},
				},
			},
		},
	}
}

func createInitContainers(chk *v1alpha1.ClickHouseKeeper) []corev1.Container {
	var initContainers []corev1.Container

	if len(chk.Spec.PodTemplate.Spec.InitContainers) == 0 {
		initContainers = []corev1.Container{{}}
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
	initContainers[0].VolumeMounts = append(initContainers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "keeper-config",
		MountPath: "/tmp/clickhouse-keeper",
	})
	initContainers[0].VolumeMounts = append(initContainers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "etc-clickhouse-keeper",
		MountPath: "/etc/clickhouse-keeper",
	})

	return initContainers
}

func createContainers(chk *v1alpha1.ClickHouseKeeper) []corev1.Container {
	var containers []corev1.Container
	if len(chk.Spec.PodTemplate.Spec.Containers) == 0 {
		containers = []corev1.Container{{}}
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
		containers[0].LivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{
					"bash",
					"-xc",
					probeScript,
				}},
			},
			InitialDelaySeconds: 60,
			PeriodSeconds:       3,
			FailureThreshold:    10,
		}
	}
	clientPort := chk.Spec.GetClientPort()
	setupPort(&containers[0], clientPort, corev1.ContainerPort{Name: "client", ContainerPort: int32(clientPort)})
	raftPort := chk.Spec.GetRaftPort()
	setupPort(&containers[0], raftPort, corev1.ContainerPort{Name: "raft", ContainerPort: int32(raftPort)})
	promethuesPort := chk.Spec.GetPrometheusPort()
	if promethuesPort != -1 {
		setupPort(&containers[0], promethuesPort, corev1.ContainerPort{Name: "prometheus", ContainerPort: int32(promethuesPort)})
	}

	switch length := len(chk.Spec.VolumeClaimTemplates); length {
	case 0:
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountVolumes(chk)...)
	case 1:
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountSharedVolume(chk)...)
	case 2:
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, mountVolumes(chk)...)
	}
	containers[0].VolumeMounts = append(containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "etc-clickhouse-keeper",
		MountPath: "/etc/clickhouse-keeper",
	})

	return containers
}

func mountVolumes(chk *v1alpha1.ClickHouseKeeper) []corev1.VolumeMount {
	path := chk.Spec.GetPath()
	return []corev1.VolumeMount{
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

func mountSharedVolume(chk *v1alpha1.ClickHouseKeeper) []corev1.VolumeMount {
	path := chk.Spec.GetPath()
	return []corev1.VolumeMount{
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

// createClientService returns a client service resource for the clickhouse keeper cluster
func createClientService(chk *v1alpha1.ClickHouseKeeper) *corev1.Service {
	svcPorts := []corev1.ServicePort{
		corev1.ServicePort{
			Name: "client",
			Port: int32(chk.Spec.GetClientPort()),
		},
	}

	prometheusPort := chk.Spec.GetPrometheusPort()
	if prometheusPort != -1 {
		svcPorts = append(svcPorts,
			corev1.ServicePort{
				Name: "prometheus",
				Port: int32(prometheusPort),
			},
		)
	}

	return createService(chk.Name, chk, svcPorts, true)
}

// createHeadlessService returns an internal headless-service for the chk stateful-set
func createHeadlessService(chk *v1alpha1.ClickHouseKeeper) *corev1.Service {
	svcPorts := []corev1.ServicePort{
		{
			Name: "raft",
			Port: int32(chk.Spec.GetRaftPort()),
		},
	}
	return createService(headlessSvcName(chk), chk, svcPorts, false)
}

func createService(name string, chk *v1alpha1.ClickHouseKeeper, ports []corev1.ServicePort, clusterIP bool) *corev1.Service {
	service := corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: chk.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports:    ports,
			Selector: getPodLabels(chk),
		},
	}
	if !clusterIP {
		service.Spec.ClusterIP = corev1.ClusterIPNone
	}
	return &service
}

// createPodDisruptionBudget returns a pdb for the clickhouse keeper cluster
func createPodDisruptionBudget(chk *v1alpha1.ClickHouseKeeper) *policyv1.PodDisruptionBudget {
	pdbCount := intstr.FromInt(1)
	return &policyv1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodDisruptionBudget",
			APIVersion: "policy/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      chk.GetName(),
			Namespace: chk.Namespace,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &pdbCount,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": chk.GetName(),
				},
			},
		},
	}
}
