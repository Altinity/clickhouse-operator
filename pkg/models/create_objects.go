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

package models

import (
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"

	"github.com/golang/glog"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ChiCreateObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
func ChiCreateObjects(chi *chiv1.ClickHouseInstallation, config *config.Config) []interface{} {
	list := make([]interface{}, 0)
	list = append(list, createServiceObjects(chi))
	list = append(list, createConfigMapObjects(chi, config))
	list = append(list, createStatefulSetObjects(chi))

	return list
}

// createConfigMapObjects returns a list of corev1.ConfigMap objects
func createConfigMapObjects(chi *chiv1.ClickHouseInstallation, config *config.Config) ConfigMapList {
	configMapList := make(ConfigMapList, 0)
	configMapList = append(
		configMapList,
		createConfigMapObjectsCommon(chi, config)...,
	)
	configMapList = append(
		configMapList,
		createConfigMapObjectsDeployment(chi, config)...,
	)

	return configMapList
}

func createConfigMapObjectsCommon(chi *chiv1.ClickHouseInstallation, config *config.Config) ConfigMapList {
	var configs configSections

	// commonConfigSections maps section name to section XML config of the following sections:
	// 1. remote servers
	// 2. zookeeper
	// 3. settings
	// 4. listen
	configs.commonConfigSections = make(map[string]string)
	includeNonEmpty(configs.commonConfigSections, filenameRemoteServersXML, generateRemoteServersConfig(chi))
	includeNonEmpty(configs.commonConfigSections, filenameZookeeperXML, generateZookeeperConfig(chi))
	includeNonEmpty(configs.commonConfigSections, filenameSettingsXML, generateSettingsConfig(chi))
	includeNonEmpty(configs.commonConfigSections, filenameListenXML, generateListenConfig(chi))
	// Extra user-specified configs
	for filename, content := range config.CommonConfigs {
		includeNonEmpty(configs.commonConfigSections, filename, content)
	}

	// commonConfigSections maps section name to section XML config of the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	configs.commonUsersConfigSections = make(map[string]string)
	includeNonEmpty(configs.commonUsersConfigSections, filenameUsersXML, generateUsersConfig(chi))
	includeNonEmpty(configs.commonUsersConfigSections, filenameQuotasXML, generateQuotasConfig(chi))
	includeNonEmpty(configs.commonUsersConfigSections, filenameProfilesXML, generateProfilesConfig(chi))
	// Extra user-specified configs
	for filename, content := range config.UsersConfigs {
		includeNonEmpty(configs.commonUsersConfigSections, filename, content)
	}

	// There are two types of configs, kept in ConfigMaps:
	// 1. Common configs - for all resources in the CHI (remote servers, zookeeper setup, etc)
	//    consists of common configs and common users configs
	// 2. Personal configs - macros config
	// configMapList contains all configs so we need deploymentsNum+2 ConfigMap objects
	// personal config for each deployment and +2 for common config + common user config
	configMapList := make(ConfigMapList, 0)

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated config files,
	// such as remote servers, zookeeper setup, etc
	configMapList = append(
		configMapList,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CreateConfigMapCommonName(chi.Name),
				Namespace: chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: chi.Name,
					ChiGeneratedLabel:  chi.Name,
				},
			},
			// Data contains several sections which are to be several xml configs
			Data: configs.commonConfigSections,
		},
	)

	// ConfigMap common for all users resources in CHI
	configMapList = append(
		configMapList,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CreateConfigMapCommonUsersName(chi.Name),
				Namespace: chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: chi.Name,
					ChiGeneratedLabel:  chi.Name,
				},
			},
			// Data contains several sections which are to be several xml configs
			Data: configs.commonUsersConfigSections,
		},
	)

	return configMapList
}

func createConfigMapObjectsDeployment(chi *chiv1.ClickHouseInstallation, config *config.Config) ConfigMapList {
	configMapList := make(ConfigMapList, 0)
	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		// Prepare for this replica deployment config files map as filename->content
		deploymentConfigSections := make(map[string]string)
		includeNonEmpty(deploymentConfigSections, filenameMacrosXML, generateHostMacros(replica))
		// Extra user-specified configs
		for filename, content := range config.DeploymentConfigs {
			includeNonEmpty(deploymentConfigSections, filename, content)
		}

		// Add corev1.ConfigMap object to the list
		configMapList = append(
			configMapList,
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      CreateConfigMapDeploymentName(replica),
					Namespace: replica.Address.Namespace,
					Labels: map[string]string{
						ChopGeneratedLabel: replica.Address.ChiName,
						ChiGeneratedLabel:  replica.Address.ChiName,
					},
				},
				Data: deploymentConfigSections,
			},
		)

		return nil
	}
	chi.WalkReplicas(replicaProcessor)

	return configMapList
}

// createServiceObjects returns a list of corev1.Service objects
func createServiceObjects(chi *chiv1.ClickHouseInstallation) ServiceList {
	// We'd like to create "number of deployments" + 1 kubernetes services in order to provide access
	// to each deployment separately and one common predictably-named access point - common service
	serviceList := make(ServiceList, 0)
	serviceList = append(
		serviceList,
		createServiceObjectsCommon(chi)...,
	)
	serviceList = append(
		serviceList,
		createServiceObjectsDeployment(chi)...,
	)

	return serviceList
}

func createServiceObjectsCommon(chi *chiv1.ClickHouseInstallation) ServiceList {
	// Create one predictably-named service to access the whole installation
	// NAME                             TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
	// service/clickhouse-replcluster   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   1h
	return ServiceList{
		createServiceObjectChi(chi, CreateChiServiceName(chi)),
	}
}

func createServiceObjectsDeployment(chi *chiv1.ClickHouseInstallation) ServiceList {
	// Create "number of deployments" service - one service for each stateful set
	// Each replica has its stateful set and each stateful set has it service
	// NAME                             TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
	// service/chi-01a1ce7dce-2         ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   1h
	serviceList := make(ServiceList, 0)

	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		// Add corev1.Service object to the list
		serviceList = append(
			serviceList,
			createServiceObjectDeployment(replica),
		)
		return nil
	}
	chi.WalkReplicas(replicaProcessor)

	return serviceList
}

func createServiceObjectChi(
	chi *chiv1.ClickHouseInstallation,
	serviceName string,
) *corev1.Service {
	glog.Infof("createServiceObjectChi() for service %s\n", serviceName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: chi.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: chi.Name,
				ChiGeneratedLabel:  chi.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			// ClusterIP: templateDefaultsServiceClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name: chDefaultHTTPPortName,
					Port: chDefaultHTTPPortNumber,
				},
				{
					Name: chDefaultClientPortName,
					Port: chDefaultClientPortNumber,
				},
				{
					Name: chDefaultInterServerPortName,
					Port: chDefaultInterServerPortNumber,
				},
			},
			Selector: map[string]string{
				ChiGeneratedLabel: chi.Name,
			},
			Type: "LoadBalancer",
		},
	}
}

func createServiceObjectDeployment(replica *chiv1.ChiClusterLayoutShardReplica) *corev1.Service {
	serviceName := CreateStatefulSetServiceName(replica)
	statefulSetName := CreateStatefulSetName(replica)

	glog.Infof("createServiceObjectDeployment() for service %s %s\n", serviceName, statefulSetName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: replica.Address.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: replica.Address.ChiName,
				ChiGeneratedLabel:  replica.Address.ChiName,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: chDefaultHTTPPortName,
					Port: chDefaultHTTPPortNumber,
				},
				{
					Name: chDefaultClientPortName,
					Port: chDefaultClientPortNumber,
				},
				{
					Name: chDefaultInterServerPortName,
					Port: chDefaultInterServerPortNumber,
				},
			},
			Selector: map[string]string{
				chDefaultAppLabel: statefulSetName,
			},
			ClusterIP: templateDefaultsServiceClusterIP,
			Type:      "ClusterIP",
		},
	}
}

// createStatefulSetObjects returns a list of apps.StatefulSet objects
func createStatefulSetObjects(chi *chiv1.ClickHouseInstallation) StatefulSetList {
	// Templates index maps template name to template itself
	// Used to provide named access to templates
	podTemplatesIndex := createPodTemplatesIndex(chi)
	volumeClaimTemplatesIndex := createVolumeClaimTemplatesIndex(chi)

	statefulSetList := make(StatefulSetList, 0)

	// Create list of apps.StatefulSet objects
	// StatefulSet is created for each replica.Deployment

	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		glog.Infof("createStatefulSetObjects() for statefulSet %s\n", CreateStatefulSetName(replica))

		// Create and setup apps.StatefulSet object
		statefulSetObject := createStatefulSetObject(replica)
		setupStatefulSetPodTemplate(statefulSetObject, podTemplatesIndex, replica)
		setupStatefulSetVolumeClaimTemplate(statefulSetObject, volumeClaimTemplatesIndex, replica)

		// Append apps.StatefulSet to the list of stateful sets
		statefulSetList = append(statefulSetList, statefulSetObject)

		return nil
	}
	chi.WalkReplicas(replicaProcessor)

	return statefulSetList
}

func createStatefulSetObject(replica *chiv1.ChiClusterLayoutShardReplica) *apps.StatefulSet {
	statefulSetName := CreateStatefulSetName(replica)
	serviceName := CreateStatefulSetServiceName(replica)

	// Create apps.StatefulSet object
	replicasNum := int32(1)
	terminationGracePeriod := int64(0)
	return &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: replica.Address.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: replica.Address.ChiName,
				ChiGeneratedLabel:  replica.Address.ChiName,
				ZkVersionLabel:     replica.Config.ZkFingerprint,
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
						ChopGeneratedLabel: replica.Address.ChiName,
						ChiGeneratedLabel:  replica.Address.ChiName,
						ZkVersionLabel:     replica.Config.ZkFingerprint,
					},
				},
				Spec: corev1.PodSpec{
					Volumes:    nil,
					Containers: nil,
					TerminationGracePeriodSeconds: &terminationGracePeriod,
				},
			},
		},
	}
}

func setupStatefulSetPodTemplate(
	statefulSetObject *apps.StatefulSet,
	podTemplatesIndex podTemplatesIndex,
	replica *chiv1.ChiClusterLayoutShardReplica,
) {
	statefulSetName := CreateStatefulSetName(replica)
	configMapMacrosName := CreateConfigMapDeploymentName(replica)
	configMapCommonName := CreateConfigMapCommonName(replica.Address.ChiName)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(replica.Address.ChiName)
	podTemplateName := replica.Deployment.PodTemplate

	// Specify pod templates - either explicitly defined or default
	if podTemplate, ok := podTemplatesIndex[podTemplateName]; ok {
		// Replica references known PodTemplate
		copyPodTemplateFrom(statefulSetObject, podTemplate)
		glog.Infof("createStatefulSetObjects() for statefulSet %s - template: %s\n", statefulSetName, podTemplateName)
	} else {
		// Replica references UNKNOWN PodTemplate
		copyPodTemplateFrom(statefulSetObject, createDefaultPodTemplate(statefulSetName))
		glog.Infof("createStatefulSetObjects() for statefulSet %s - default template\n", statefulSetName)
	}

	// And now loop over all containers in this template and
	// append all VolumeMounts which are ConfigMap mounts
	for i := range statefulSetObject.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSetObject.Spec.Template.Spec.Containers[i]
		// Append to each Container current VolumeMount's to VolumeMount's declared in template
		container.VolumeMounts = append(
			container.VolumeMounts,
			createVolumeMountObject(configMapCommonName, dirPathConfigd),
			createVolumeMountObject(configMapCommonUsersName, dirPathUsersd),
			createVolumeMountObject(configMapMacrosName, dirPathConfd),
		)
	}

	// Add all ConfigMap objects as Pod's volumes
	statefulSetObject.Spec.Template.Spec.Volumes = append(
		statefulSetObject.Spec.Template.Spec.Volumes,
		createVolumeObjectConfigMap(configMapCommonName),
		createVolumeObjectConfigMap(configMapCommonUsersName),
		createVolumeObjectConfigMap(configMapMacrosName),
	)
}

func setupStatefulSetVolumeClaimTemplate(
	statefulSetObject *apps.StatefulSet,
	volumeClaimTemplatesIndex volumeClaimTemplatesIndex,
	replica *chiv1.ChiClusterLayoutShardReplica,
) {
	statefulSetName := CreateStatefulSetName(replica)
	volumeClaimTemplateName := replica.Deployment.VolumeClaimTemplate

	// Specify volume claim templates - either explicitly defined or default
	volumeClaimTemplate, ok := volumeClaimTemplatesIndex[volumeClaimTemplateName]
	if !ok {
		// Unknown VolumeClaimTemplate
		glog.Infof("createStatefulSetObjects() for statefulSet %s - no VC templates\n", statefulSetName)
		return
	}

	// Known VolumeClaimTemplate

	statefulSetObject.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
		volumeClaimTemplate.PersistentVolumeClaim,
	}

	// Add default corev1.VolumeMount section for ClickHouse data
	if volumeClaimTemplate.UseDefaultName {
		statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			statefulSetObject.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      chDefaultVolumeMountNameData,
				MountPath: dirPathClickHouseData,
			})
		glog.Infof("createStatefulSetObjects() for statefulSet %s - VC template.useDefaultName: %s\n", statefulSetName, volumeClaimTemplateName)
	}
	glog.Infof("createStatefulSetObjects() for statefulSet %s - VC template: %s\n", statefulSetName, volumeClaimTemplateName)
}

func copyPodTemplateFrom(dst *apps.StatefulSet, src *chiv1.ChiPodTemplate) {
	// Prepare .statefulSetObject.Spec.Template.Spec - fill with template's data

	// Setup Container's

	// Copy containers from pod template
	// ... snippet from .spec.templates.podTemplates
	//       containers:
	//      - name: clickhouse
	//        volumeMounts:
	//        - name: clickhouse-data-test
	//          mountPath: /var/lib/clickhouse
	//        image: yandex/clickhouse-server:18.16.2
	dst.Spec.Template.Spec.Containers = make([]corev1.Container, len(src.Containers))
	copy(dst.Spec.Template.Spec.Containers, src.Containers)

	// Setup Volume's
	// Copy volumes from pod template
	dst.Spec.Template.Spec.Volumes = make([]corev1.Volume, len(src.Volumes))
	copy(dst.Spec.Template.Spec.Volumes, src.Volumes)
}

// createDefaultPodTemplate returns default Pod Template to be used with StatefulSet
func createDefaultPodTemplate(name string) *chiv1.ChiPodTemplate {
	return &chiv1.ChiPodTemplate{
		Name: "createDefaultPodTemplate",
		Containers: []corev1.Container{
			{
				Name:  name,
				Image: chDefaultDockerImage,
				Ports: []corev1.ContainerPort{
					{
						Name:          chDefaultHTTPPortName,
						ContainerPort: chDefaultHTTPPortNumber,
					},
					{
						Name:          chDefaultClientPortName,
						ContainerPort: chDefaultClientPortNumber,
					},
					{
						Name:          chDefaultInterServerPortName,
						ContainerPort: chDefaultInterServerPortNumber,
					},
				},
				ReadinessProbe: &corev1.Probe{
					Handler: corev1.Handler {
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/ping",
							Port: intstr.Parse(chDefaultHTTPPortName),
						},
					},
					InitialDelaySeconds: 10,
					PeriodSeconds: 10,
				},
			},
		},
		Volumes: []corev1.Volume{},
	}
}

// createVolumeObjectConfigMap returns corev1.Volume object with defined name
func createVolumeObjectConfigMap(name string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: name,
				},
			},
		},
	}
}

// createVolumeMountObject returns corev1.VolumeMount object with name and mount path
func createVolumeMountObject(name, mountPath string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}

// createVolumeClaimTemplatesIndex returns a map of volumeClaimTemplatesIndexData used as a reference storage for VolumeClaimTemplates
func createVolumeClaimTemplatesIndex(chi *chiv1.ClickHouseInstallation) volumeClaimTemplatesIndex {
	index := make(volumeClaimTemplatesIndex)
	for i := range chi.Spec.Templates.VolumeClaimTemplates {
		// Convenience wrapper
		volumeClaimTemplate := &chi.Spec.Templates.VolumeClaimTemplates[i]

		if volumeClaimTemplate.PersistentVolumeClaim.Name == useDefaultPersistentVolumeClaimMacro {
			volumeClaimTemplate.PersistentVolumeClaim.Name = chDefaultVolumeMountNameData
			volumeClaimTemplate.UseDefaultName = true
		}
		index[volumeClaimTemplate.Name] = volumeClaimTemplate
	}

	return index
}

// createPodTemplatesIndex returns a map of podTemplatesIndexData used as a reference storage for PodTemplates
func createPodTemplatesIndex(chi *chiv1.ClickHouseInstallation) podTemplatesIndex {
	index := make(podTemplatesIndex)
	for i := range chi.Spec.Templates.PodTemplates {
		// Convenience wrapper
		podTemplate := &chi.Spec.Templates.PodTemplates[i]
		index[podTemplate.Name] = podTemplate
	}

	return index
}
