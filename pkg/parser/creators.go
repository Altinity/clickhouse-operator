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

// CreateCHIObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
// and slice of all full deployment ids
func CreateCHIObjects(chi *chiv1.ClickHouseInstallation, deploymentCountMax chiDeploymentCountMap) (ObjectsMap, []string) {
	var options genOptions

	options.deploymentCountMax = deploymentCountMax

	// Allocate data structures
	options.fullDeploymentIDToFingerprint = make(map[string]string)
	options.ssDeployments = make(map[string]*chiv1.ChiDeployment)
	options.macrosData = make(map[string]macrosDataShardDescriptionList)
	options.configSection = make(map[string]bool)

	// commonConfigSections maps section name to section XML config such as "<yandex><macros>...</macros><yandex>"
	commonConfigSections := make(map[string]string)

	// Generate XMLs
	commonConfigSections[filenameRemoteServersXML] = generateRemoteServersConfig(chi, &options)

	options.configSection[filenameZookeeperXML] = includeIfNotEmpty(commonConfigSections, filenameZookeeperXML, genZookeeperConfig(chi))
	options.configSection[filenameUsersXML] = includeIfNotEmpty(commonConfigSections, filenameUsersXML, genUsersConfig(chi))
	options.configSection[filenameProfilesXML] = includeIfNotEmpty(commonConfigSections, filenameProfilesXML, genProfilesConfig(chi))
	options.configSection[filenameQuotasXML] = includeIfNotEmpty(commonConfigSections, filenameQuotasXML, genQuotasConfig(chi))
	options.configSection[filenameSettingsXML] = includeIfNotEmpty(commonConfigSections, filenameSettingsXML, genSettingsConfig(chi))

	// slice of full deployment ID's
	fullDeploymentIDs := make([]string, 0, len(options.fullDeploymentIDToFingerprint))
	for p := range options.fullDeploymentIDToFingerprint {
		fullDeploymentIDs = append(fullDeploymentIDs, p)
	}

	// Create k8s objects (data structures)
	return ObjectsMap{
		ObjectsConfigMaps:   createConfigMapObjects(chi, commonConfigSections, &options),
		ObjectsServices:     createServiceObjects(chi, &options),
		ObjectsStatefulSets: createStatefulSetObjects(chi, &options),
	}, fullDeploymentIDs
}

// createConfigMapObjects returns a list of corev1.ConfigMap objects
func createConfigMapObjects(
	chi *chiv1.ClickHouseInstallation,
	commonConfigSections map[string]string,
	options *genOptions,
) ConfigMapList {

	deploymentsCount := len(options.fullDeploymentIDToFingerprint)

	// There are two types of ConfigMaps:
	// 1. Common configs - for all resources in the CHI
	// 2. Personal configs
	// configMapList contains all ConfigMaps
	configMapList := make(ConfigMapList, 1, deploymentsCount+1) // deploymentsCount for personal configs and +1 for common

	// ConfigMap common for all resources in CHI
	configMapList[0] = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonName(chi.Name),
			Namespace: chi.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: chi.Name,
			},
		},
		Data: commonConfigSections,
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
				filenameMacrosXML: generateHostMacros(chi.Name, fullDeploymentID, options.macrosData[fullDeploymentID]),
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
	configMapCommonName := CreateConfigMapCommonName(chi.Name)
	statefulSetList := make(StatefulSetList, 0, len(options.fullDeploymentIDToFingerprint))

	// Populate templates indexes
	volumeClaimTemplatesIndex := createVolumeClaimTemplatesIndex(chi)
	podTemplatesIndex := createPodTemplatesIndex(chi)

	// Defining list of shared volume mounts
	includes := includesObjects{
		{
			filename: filenameZookeeperXML,
			fullpath: fullPathZookeeperXML,
		},
		{
			filename: filenameUsersXML,
			fullpath: fullPathUsersXML,
		},
		{
			filename: filenameProfilesXML,
			fullpath: fullPathProfilesXML,
		},
		{
			filename: filenameQuotasXML,
			fullpath: fullPathQuotasXML,
		},
		{
			filename: filenameSettingsXML,
			fullpath: fullPathSettingsXML,
		},
	}

	// Populate corev1.VolumeMount list with actual values
	commonVolumeMounts := make([]corev1.VolumeMount, 1)
	commonVolumeMounts[0] = corev1.VolumeMount{
		Name:      configMapCommonName,
		MountPath: fullPathRemoteServersXML,
		SubPath:   filenameRemoteServersXML,
	}

	for i := range includes {
		if options.configSection[includes[i].filename] {
			commonVolumeMounts = append(
				commonVolumeMounts, corev1.VolumeMount{
					Name:      configMapCommonName,
					MountPath: includes[i].fullpath,
					SubPath:   includes[i].filename,
				},
			)
		}
	}

	// Create list of apps.StatefulSet objects
	for fullDeploymentID, fingerprint := range options.fullDeploymentIDToFingerprint {

		serviceName := CreateServiceName(fullDeploymentID)
		statefulSetName := CreateStatefulSetName(fullDeploymentID)
		configMapMacrosName := CreateConfigMapMacrosName(chi.Name, fullDeploymentID)
		volumeClaimTemplate := options.ssDeployments[fingerprint].VolumeClaimTemplate
		podTemplate := options.ssDeployments[fingerprint].PodTemplate

		// Copy list of shared corev1.VolumeMount objects into new slice
		l := len(commonVolumeMounts)
		currentVolumeMounts := make([]corev1.VolumeMount, l, l+1)
		// Common volume mounts
		copy(currentVolumeMounts, commonVolumeMounts)
		// Personal volume mounts
		// Appending "macros.xml" section
		currentVolumeMounts = append(currentVolumeMounts, corev1.VolumeMount{
			Name:      configMapMacrosName,
			MountPath: fullPathMacrosXML,
			SubPath:   filenameMacrosXML,
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

		// Check custom pod templates has been defined
		if data, ok := podTemplatesIndex[podTemplate]; ok {
			statefulSetObject.Spec.Template.Spec.Containers = make([]corev1.Container, len(data.containers))
			copy(statefulSetObject.Spec.Template.Spec.Containers, data.containers)

			statefulSetObject.Spec.Template.Spec.Volumes = make([]corev1.Volume, len(data.volumes))
			copy(statefulSetObject.Spec.Template.Spec.Volumes, data.volumes)

			// Append current VolumeMounts set to custom containers
			for i := range statefulSetObject.Spec.Template.Spec.Containers {
				for j := range currentVolumeMounts {
					statefulSetObject.Spec.Template.Spec.Containers[i].VolumeMounts = append(
						statefulSetObject.Spec.Template.Spec.Containers[i].VolumeMounts,
						currentVolumeMounts[j])
				}
			}
		} else {
			// Add default container template
			statefulSetObject.Spec.Template.Spec.Containers = append(
				statefulSetObject.Spec.Template.Spec.Containers,
				createDefaultContainerTemplate(chi, statefulSetName, currentVolumeMounts),
			)
		}

		// Adding default configMaps as Pod's volumes
		statefulSetObject.Spec.Template.Spec.Volumes = append(
			statefulSetObject.Spec.Template.Spec.Volumes, createVolume(configMapCommonName))
		statefulSetObject.Spec.Template.Spec.Volumes = append(
			statefulSetObject.Spec.Template.Spec.Volumes, createVolume(configMapMacrosName))
		// Checking that corev1.PersistentVolumeClaim template has been defined
		if data, ok := volumeClaimTemplatesIndex[volumeClaimTemplate]; ok {
			statefulSetObject.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
				*volumeClaimTemplatesIndex[volumeClaimTemplate].template,
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
		// Convenience wrapper
		podTemplate := &chi.Spec.Templates.PodTemplates[i]
		index[podTemplate.Name] = &podTemplatesIndexData{
			containers: podTemplate.Containers,
			volumes:    podTemplate.Volumes,
		}
	}

	return index
}

// CreateConfigMapMacrosName returns a name for a ConfigMap (CH macros) resource based on predefined pattern
func CreateConfigMapMacrosName(chiName, prefix string) string {
	return fmt.Sprintf(configMapMacrosNamePattern, chiName, prefix)
}

// CreateConfigMapCommonName returns a name for a ConfigMap resource based on predefined pattern
func CreateConfigMapCommonName(chiName string) string {
	return fmt.Sprintf(configMapCommonNamePattern, chiName)
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
