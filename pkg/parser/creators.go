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

package parser

import (
	"fmt"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// createConfigMapObjects returns a list of corev1.ConfigMap objects
func createConfigMapObjects(
	chi *chiv1.ClickHouseInstallation,
	configSections map[string]string,
	options *genOptions,
) ConfigMapList {

	deploymentsCount := len(options.fullDeploymentIDToFingerprint)
	configMapList := make(ConfigMapList, 1, deploymentsCount+1)
	configMapList[0] = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapName(chi.Name),
			Namespace: chi.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: chi.Name,
			},
		},
		Data: configSections,
	}

	// Each deployment has to have macros.xml config file
	for fullDeploymentID := range options.fullDeploymentIDToFingerprint {
		// Add corev1.ConfigMap object to the list
		configMapList = append(configMapList, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CreateConfigMapMacrosName(chi.Name, fullDeploymentID),
				Namespace: chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: chi.Name,
				},
			},
			Data: map[string]string{
				macrosXMLFilename: generateHostMacros(chi.Name, fullDeploymentID, options.macrosData[fullDeploymentID]),
			},
		})
	}

	return configMapList
}

// createServiceObjects returns a list of corev1.Service objects
func createServiceObjects(chi *chiv1.ClickHouseInstallation, options *genOptions) ServiceList {
	serviceList := make(ServiceList, 0, len(options.fullDeploymentIDToFingerprint))

	for fullDeploymentID := range options.fullDeploymentIDToFingerprint {
		statefulSetName := CreateStatefulSetName(fullDeploymentID)
		serviceName := CreateServiceName(fullDeploymentID)

		// Add corev1.Service object to the list
		serviceList = append(serviceList, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: chi.Name,
				},
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: templateDefaultsServiceClusterIP,
				Selector: map[string]string{
					chDefaultAppLabel: statefulSetName,
				},
				Ports: []corev1.ServicePort{
					{
						Name: chDefaultRPCPortName,
						Port: chDefaultRPCPortNumber,
					},
					{
						Name: chDefaultInterServerPortName,
						Port: chDefaultInterServerPortNumber,
					},
					{
						Name: chDefaultRestPortName,
						Port: chDefaultRestPortNumber,
					},
				},
			},
		})
	}

	return serviceList
}

// createStatefulSetObjects returns a list of apps.StatefulSet objects
func createStatefulSetObjects(chi *chiv1.ClickHouseInstallation, options *genOptions) StatefulSetList {
	replicasNum := int32(1)
	configMapName := CreateConfigMapName(chi.Name)
	statefulSetList := make(StatefulSetList, 0, len(options.fullDeploymentIDToFingerprint))

	// Populate templates indexes
	volumeClaimTemplatesIndex := createVolumeClaimTemplatesIndex(chi)
	podTemplatesIndex := createPodTemplatesIndex(chi)

	// Defining list of shared volume mounts
	includes := includesObjects{
		{
			filename: zookeeperXMLFilename,
			fullpath: fullPathZookeeperXML,
		},
		{
			filename: usersXMLFilename,
			fullpath: fullPathUsersXML,
		},
		{
			filename: profilesXMLFilename,
			fullpath: fullPathProfilesXML,
		},
		{
			filename: quotasXMLFilename,
			fullpath: fullPathQuotasXML,
		},
		{
			filename: settingsXMLFilename,
			fullpath: fullPathSettingsXML,
		},
	}

	// Populate corev1.VolumeMount list with actual values
	sharedVolumeMounts := make([]corev1.VolumeMount, 1)
	sharedVolumeMounts[0] = corev1.VolumeMount{
		Name:      configMapName,
		MountPath: fullPathRemoteServersXML,
		SubPath:   remoteServersXMLFilename,
	}

	for i := range includes {
		if options.includeConfigSection[includes[i].filename] {
			sharedVolumeMounts = append(
				sharedVolumeMounts, corev1.VolumeMount{
					Name:      configMapName,
					MountPath: includes[i].fullpath,
					SubPath:   includes[i].filename,
				},
			)
		}
	}

	// Create list of apps.StatefulSet objects
	for fullDeploymentID, fingerprint := range options.fullDeploymentIDToFingerprint {
		statefulSetName := CreateStatefulSetName(fullDeploymentID)
		cmMacros := CreateConfigMapMacrosName(chi.Name, fullDeploymentID)
		vcTemplate := options.ssDeployments[fingerprint].VolumeClaimTemplate
		podTemplate := options.ssDeployments[fingerprint].PodTemplateName
		serviceName := CreateServiceName(fullDeploymentID)

		// Copy list of shared corev1.VolumeMount objects into new slice
		l := len(sharedVolumeMounts)
		currentVolumeMounts := make([]corev1.VolumeMount, l, l+1)
		copy(currentVolumeMounts, sharedVolumeMounts)
		// Appending "macros.xml" section
		currentVolumeMounts = append(currentVolumeMounts, corev1.VolumeMount{
			Name:      cmMacros,
			MountPath: fullPathMacrosXML,
			SubPath:   macrosXMLFilename,
		})

		// Create apps.StatefulSet object
		statefulSetObject := &apps.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      statefulSetName,
				Namespace: chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: chi.Name,
				},
			},
			Spec: apps.StatefulSetSpec{
				Replicas:    &replicasNum,
				ServiceName: serviceName,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						chDefaultAppLabel: statefulSetName,
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name: statefulSetName,
						Labels: map[string]string{
							chDefaultAppLabel:  statefulSetName,
							ChopGeneratedLabel: chi.Name,
						},
					},
					Spec: corev1.PodSpec{
						Volumes:    []corev1.Volume{},
						Containers: []corev1.Container{},
					},
				},
			},
		}

		// Checking that custom pod templates has been defined
		if data, ok := podTemplatesIndex[podTemplate]; ok {
			statefulSetObject.Spec.Template.Spec.Containers = make([]corev1.Container, len(data.containers))
			copy(statefulSetObject.Spec.Template.Spec.Containers, data.containers)
			statefulSetObject.Spec.Template.Spec.Volumes = make([]corev1.Volume, len(data.volumes))
			copy(statefulSetObject.Spec.Template.Spec.Volumes, data.volumes)
			// Appending current VolumeMounts set to custom containers
			for i := range statefulSetObject.Spec.Template.Spec.Containers {
				for j := range currentVolumeMounts {
					statefulSetObject.Spec.Template.Spec.Containers[i].VolumeMounts = append(
						statefulSetObject.Spec.Template.Spec.Containers[i].VolumeMounts,
						currentVolumeMounts[j])
				}
			}
		} else {
			// Adding default container template
			statefulSetObject.Spec.Template.Spec.Containers = append(
				statefulSetObject.Spec.Template.Spec.Containers,
				createDefaultContainerTemplate(chi, statefulSetName, currentVolumeMounts))
		}

		// Adding default configMaps as Pod's volumes
		statefulSetObject.Spec.Template.Spec.Volumes = append(
			statefulSetObject.Spec.Template.Spec.Volumes, createVolume(configMapName))
		statefulSetObject.Spec.Template.Spec.Volumes = append(
			statefulSetObject.Spec.Template.Spec.Volumes, createVolume(cmMacros))
		// Checking that corev1.PersistentVolumeClaim template has been defined
		if data, ok := volumeClaimTemplatesIndex[vcTemplate]; ok {
			statefulSetObject.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
				*volumeClaimTemplatesIndex[vcTemplate].template,
			}
			// Adding default corev1.VolumeMount section for ClickHouse data
			if data.useDefaultName {
				statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts = append(
					statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts,
					corev1.VolumeMount{
						Name:      chDefaultVolumeMountNameData,
						MountPath: fullPathClickHouseData,
					})
			}
		}
		// Appending apps.StatefulSet to the resulting list
		statefulSetList = append(statefulSetList, statefulSetObject)
	}

	return statefulSetList
}

// createVolume returns corev1.Volume object with defined name
func createVolume(name string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: name,
				},
			},
		}}
}

// createDefaultContainerTemplate returns default corev1.Container object
func createDefaultContainerTemplate(chi *chiv1.ClickHouseInstallation, n string, vm []corev1.VolumeMount) corev1.Container {
	return corev1.Container{
		Name:  n,
		Image: chDefaultDockerImage,
		Ports: []corev1.ContainerPort{
			{
				Name:          chDefaultRPCPortName,
				ContainerPort: chDefaultRPCPortNumber,
			},
			{
				Name:          chDefaultInterServerPortName,
				ContainerPort: chDefaultInterServerPortNumber,
			},
			{
				Name:          chDefaultRestPortName,
				ContainerPort: chDefaultRestPortNumber,
			},
		},
		VolumeMounts: vm,
	}
}

// createVolumeClaimTemplatesIndex returns a map of vcTemplatesIndexData used as a reference storage for VolumeClaimTemplates
func createVolumeClaimTemplatesIndex(chi *chiv1.ClickHouseInstallation) vcTemplatesIndex {
	index := make(vcTemplatesIndex)
	for i := range chi.Spec.Templates.VolumeClaimTemplates {
		flag := false
		if chi.Spec.Templates.VolumeClaimTemplates[i].Template.Name == useDefaultNamePlaceholder {
			chi.Spec.Templates.VolumeClaimTemplates[i].Template.Name = chDefaultVolumeMountNameData
			flag = true
		}
		index[chi.Spec.Templates.VolumeClaimTemplates[i].Name] = &vcTemplatesIndexData{
			template:       &chi.Spec.Templates.VolumeClaimTemplates[i].Template,
			useDefaultName: flag,
		}
	}
	return index
}

// createPodTemplatesIndex returns a map of podTemplatesIndexData used as a reference storage for PodTemplates
func createPodTemplatesIndex(chi *chiv1.ClickHouseInstallation) podTemplatesIndex {
	index := make(podTemplatesIndex)
	for i := range chi.Spec.Templates.PodTemplates {
		index[chi.Spec.Templates.PodTemplates[i].Name] = &podTemplatesIndexData{
			containers: chi.Spec.Templates.PodTemplates[i].Containers,
			volumes:    chi.Spec.Templates.PodTemplates[i].Volumes,
		}
	}
	return index
}

// CreateConfigMapMacrosName returns a name for a ConfigMap (CH macros) resource based on predefined pattern
func CreateConfigMapMacrosName(chiName, prefix string) string {
	return fmt.Sprintf(configMapMacrosNamePattern, chiName, prefix)
}

// CreateConfigMapName returns a name for a ConfigMap resource based on predefined pattern
func CreateConfigMapName(chiName string) string {
	return fmt.Sprintf(configMapNamePattern, chiName)
}

// CreateServiceName creates a name of a Service resource
// prefix is a fullDeploymentID
func CreateServiceName(prefix string) string {
	return fmt.Sprintf(serviceNamePattern, prefix)
}

// CreateStatefulSetName creates a name of a StatefulSet resource
// prefix is a fullDeploymentID
func CreateStatefulSetName(prefix string) string {
	return fmt.Sprintf(statefulSetNamePattern, prefix)
}

// CreatePodHostname creates a name of a Pod resource
// prefix is a fullDeploymentID
// ss-1eb454-2-0
func CreatePodHostname(prefix string) string {
	return fmt.Sprintf(hostnamePattern, prefix)
}

// CreatePodHostnamePlusService creates a name of a Pod resource within namespace
// prefix is a fullDeploymentID
// ss-1eb454-2-0.svc-1eb454-2
func CreatePodHostnamePlusService(prefix string) string {
	return fmt.Sprintf(hostnamePlusServicePattern, prefix, prefix)
}

// CreateNamespaceDomainName creates domain name of a namespace
// .my-dev-namespace.svc.cluster.local
func CreateNamespaceDomainName(chiNamespace string) string {
	return fmt.Sprintf(namespaceDomainPattern, chiNamespace)
}

// CreatePodFQDN creates a fully qualified domain name of a pod
// prefix is a fullDeploymentID
// ss-1eb454-2-0.svc-1eb454-2.my-dev-domain.svc.cluster.local
func CreatePodFQDN(chiNamespace, prefix string) string {
	return fmt.Sprintf(
		podFQDNPattern,
		prefix,
		prefix,
		chiNamespace,
	)
}
