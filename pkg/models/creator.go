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
	"github.com/altinity/clickhouse-operator/pkg/util"
	"strconv"

	"github.com/golang/glog"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type Creator struct {
	appVersion        string
	chi               *chiv1.ClickHouseInstallation
	chopConfig        *config.Config
	chConfigGenerator *ClickHouseConfigGenerator

	podTemplatesIndex         podTemplatesIndex
	volumeClaimTemplatesIndex volumeClaimTemplatesIndex
}

func NewCreator(chi *chiv1.ClickHouseInstallation, chopConfig *config.Config, appVersion string) *Creator {
	creator := &Creator{
		chi:               chi,
		chopConfig:        chopConfig,
		appVersion:        appVersion,
		chConfigGenerator: NewClickHouseConfigGenerator(chi),
	}
	creator.createPodTemplatesIndex()
	creator.createVolumeClaimTemplatesIndex()

	return creator
}

// ChiCreateObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
func (c *Creator) CreateObjects() []interface{} {
	list := make([]interface{}, 0)
	list = append(list, c.createServiceObjects())
	list = append(list, c.createConfigMapObjects())
	list = append(list, c.createStatefulSetObjects())

	return list
}

// createConfigMapObjects returns a list of corev1.ConfigMap objects
func (c *Creator) createConfigMapObjects() ConfigMapList {
	configMapList := make(ConfigMapList, 0)
	configMapList = append(
		configMapList,
		c.createConfigMapObjectsCommon()...,
	)
	configMapList = append(
		configMapList,
		c.createConfigMapObjectsPod()...,
	)

	return configMapList
}

func (c *Creator) createConfigMapObjectsCommon() ConfigMapList {
	var configs configSections

	// commonConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. remote servers
	// 2. zookeeper
	// 3. settings
	// 4. listen
	configs.commonConfigSections = make(map[string]string)
	util.IncludeNonEmpty(configs.commonConfigSections, filenameRemoteServersXML, c.chConfigGenerator.GetRemoteServers())
	util.IncludeNonEmpty(configs.commonConfigSections, filenameZookeeperXML, c.chConfigGenerator.GetZookeeper())
	util.IncludeNonEmpty(configs.commonConfigSections, filenameSettingsXML, c.chConfigGenerator.GetSettings())
	util.IncludeNonEmpty(configs.commonConfigSections, filenameListenXML, c.chConfigGenerator.GetListen())
	// Extra user-specified configs
	for filename, content := range c.chopConfig.ChCommonConfigs {
		util.IncludeNonEmpty(configs.commonConfigSections, filename, content)
	}

	// commonConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	configs.commonUsersConfigSections = make(map[string]string)
	util.IncludeNonEmpty(configs.commonUsersConfigSections, filenameUsersXML, c.chConfigGenerator.GetUsers())
	util.IncludeNonEmpty(configs.commonUsersConfigSections, filenameQuotasXML, c.chConfigGenerator.GetQuotas())
	util.IncludeNonEmpty(configs.commonUsersConfigSections, filenameProfilesXML, c.chConfigGenerator.GetProfiles())
	// Extra user-specified configs
	for filename, content := range c.chopConfig.ChUsersConfigs {
		util.IncludeNonEmpty(configs.commonUsersConfigSections, filename, content)
	}

	// There are two types of configs, kept in ConfigMaps:
	// 1. Common configs - for all resources in the CHI (remote servers, zookeeper setup, etc)
	//    consists of common configs and common users configs
	// 2. Personal configs - macros chopConfig
	// configMapList contains all configs so we need deploymentsNum+2 ConfigMap objects
	// personal chopConfig for each deployment and +2 for common chopConfig + common user chopConfig
	configMapList := make(ConfigMapList, 0)

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapList = append(
		configMapList,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CreateConfigMapCommonName(c.chi.Name),
				Namespace: c.chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: c.appVersion,
					ChiGeneratedLabel:  c.chi.Name,
				},
			},
			// Data contains several sections which are to be several xml chopConfig files
			Data: configs.commonConfigSections,
		},
	)

	// ConfigMap common for all users resources in CHI
	configMapList = append(
		configMapList,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      CreateConfigMapCommonUsersName(c.chi.Name),
				Namespace: c.chi.Namespace,
				Labels: map[string]string{
					ChopGeneratedLabel: c.appVersion,
					ChiGeneratedLabel:  c.chi.Name,
				},
			},
			// Data contains several sections which are to be several xml chopConfig files
			Data: configs.commonUsersConfigSections,
		},
	)

	return configMapList
}

func (c *Creator) createConfigMapObjectsPod() ConfigMapList {
	configMapList := make(ConfigMapList, 0)
	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		// Prepare for this replica deployment chopConfig files map as filename->content
		podConfigSections := make(map[string]string)
		util.IncludeNonEmpty(podConfigSections, filenameMacrosXML, c.chConfigGenerator.GetHostMacros(replica))
		// Extra user-specified configs
		for filename, content := range c.chopConfig.ChPodConfigs {
			util.IncludeNonEmpty(podConfigSections, filename, content)
		}

		// Add corev1.ConfigMap object to the list
		configMapList = append(
			configMapList,
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      CreateConfigMapPodName(replica),
					Namespace: replica.Address.Namespace,
					Labels: map[string]string{
						ChopGeneratedLabel: c.appVersion,
						ChiGeneratedLabel:  replica.Address.ChiName,
					},
				},
				Data: podConfigSections,
			},
		)

		return nil
	}
	c.chi.WalkReplicas(replicaProcessor)

	return configMapList
}

// createServiceObjects returns a list of corev1.Service objects
func (c *Creator) createServiceObjects() ServiceList {
	// We'd like to create "number of deployments" + 1 kubernetes services in order to provide access
	// to each deployment separately and one common predictably-named access point - common service
	serviceList := make(ServiceList, 0)
	serviceList = append(
		serviceList,
		c.createServiceObjectsCommon()...,
	)
	serviceList = append(
		serviceList,
		c.createServiceObjectsPod()...,
	)

	return serviceList
}

func (c *Creator) createServiceObjectsCommon() ServiceList {
	// Create one predictably-named service to access the whole installation
	// NAME                             TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
	// service/clickhouse-replcluster   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   1h
	return ServiceList{
		c.createServiceObjectChi(CreateChiServiceName(c.chi)),
	}
}

func (c *Creator) createServiceObjectsPod() ServiceList {
	// Create "number of pods" service - one service for each stateful set
	// Each replica has its stateful set and each stateful set has it service
	// NAME                             TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
	// service/chi-01a1ce7dce-2         ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   1h
	serviceList := make(ServiceList, 0)

	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		// Add corev1.Service object to the list
		serviceList = append(
			serviceList,
			c.createServiceObjectPod(replica),
		)
		return nil
	}
	c.chi.WalkReplicas(replicaProcessor)

	return serviceList
}

func (c *Creator) createServiceObjectChi(serviceName string) *corev1.Service {
	glog.V(1).Infof("createServiceObjectChi() for service %s\n", serviceName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: c.chi.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: c.appVersion,
				ChiGeneratedLabel:  c.chi.Name,
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
			},
			Selector: map[string]string{
				ChiGeneratedLabel: c.chi.Name,
			},
			Type: "LoadBalancer",
		},
	}
}

func (c *Creator) createServiceObjectPod(replica *chiv1.ChiClusterLayoutShardReplica) *corev1.Service {
	serviceName := CreateStatefulSetServiceName(replica)
	statefulSetName := CreateStatefulSetName(replica)

	glog.V(1).Infof("createServiceObjectPod() for service %s %s\n", serviceName, statefulSetName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: replica.Address.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel:         c.appVersion,
				ChiGeneratedLabel:          replica.Address.ChiName,
				ClusterGeneratedLabel:      replica.Address.ClusterName,
				ClusterIndexGeneratedLabel: strconv.Itoa(replica.Address.ClusterIndex),
				ReplicaIndexGeneratedLabel: strconv.Itoa(replica.Address.ReplicaIndex),
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
func (c *Creator) createStatefulSetObjects() StatefulSetList {
	statefulSetList := make(StatefulSetList, 0)

	// Create list of apps.StatefulSet objects
	// StatefulSet is created for each replica.Deployment

	replicaProcessor := func(replica *chiv1.ChiClusterLayoutShardReplica) error {
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s\n", CreateStatefulSetName(replica))
		// Append new StatefulSet to the list of stateful sets
		statefulSetList = append(statefulSetList, c.createStatefulSetObject(replica))
		return nil
	}
	c.chi.WalkReplicas(replicaProcessor)

	return statefulSetList
}

func (c *Creator) createStatefulSetObject(replica *chiv1.ChiClusterLayoutShardReplica) *apps.StatefulSet {
	statefulSetName := CreateStatefulSetName(replica)
	serviceName := CreateStatefulSetServiceName(replica)

	// Create apps.StatefulSet object
	replicasNum := int32(1)
	statefulSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: replica.Address.Namespace,
			Labels: map[string]string{
				ChopGeneratedLabel: c.appVersion,
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
			// IMPORTANT
			// VolumeClaimTemplates are to be setup later
			VolumeClaimTemplates: nil,

			// IMPORTANT
			// Template is to be setup later
			Template: corev1.PodTemplateSpec{},
		},
	}

	c.setupStatefulSetPodTemplate(statefulSet, replica)
	c.setupStatefulSetVolumeClaimTemplates(statefulSet, replica)

	return statefulSet
}

func (c *Creator) setupStatefulSetPodTemplate(
	statefulSetObject *apps.StatefulSet,
	replica *chiv1.ChiClusterLayoutShardReplica,
) {
	statefulSetName := CreateStatefulSetName(replica)
	podTemplateName := replica.Templates.PodTemplate

	// Initial PodTemplateSpec value
	// All the rest firls would be filled later
	statefulSetObject.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				chDefaultAppLabel:  statefulSetName,
				ChopGeneratedLabel: c.appVersion,
				ChiGeneratedLabel:  replica.Address.ChiName,
				ZkVersionLabel:     replica.Config.ZkFingerprint,
			},
		},
	}

	// Specify pod templates - either explicitly defined or default
	if podTemplate, ok := c.getPodTemplate(podTemplateName); ok {
		// Replica references known PodTemplate
		copyPodTemplateFrom(statefulSetObject, podTemplate)
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - template: %s\n", statefulSetName, podTemplateName)
	} else {
		// Replica references UNKNOWN PodTemplate
		copyPodTemplateFrom(statefulSetObject, createDefaultPodTemplate(statefulSetName))
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - default template\n", statefulSetName)
	}

	c.setupConfigMapVolumes(statefulSetObject, replica)
}

// setupConfigMapVolumes adds to each container in the Pod VolumeMount objects with
func (c *Creator) setupConfigMapVolumes(
	statefulSetObject *apps.StatefulSet,
	replica *chiv1.ChiClusterLayoutShardReplica,
) {
	configMapMacrosName := CreateConfigMapPodName(replica)
	configMapCommonName := CreateConfigMapCommonName(replica.Address.ChiName)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(replica.Address.ChiName)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	statefulSetObject.Spec.Template.Spec.Volumes = append(
		statefulSetObject.Spec.Template.Spec.Volumes,
		createVolumeObjectConfigMap(configMapCommonName),
		createVolumeObjectConfigMap(configMapCommonUsersName),
		createVolumeObjectConfigMap(configMapMacrosName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes
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
}

func (c *Creator) setupStatefulSetVolumeClaimTemplates(
	statefulSetObject *apps.StatefulSet,
	replica *chiv1.ChiClusterLayoutShardReplica,
) {
	// Append VolumeClaimTemplates, that are referenced in Containers' VolumeMount object(s)
	// to StatefulSet's Spec.VolumeClaimTemplates slice, so these
	statefulSetName := CreateStatefulSetName(replica)
	for i := range statefulSetObject.Spec.Template.Spec.Containers {
		container := &statefulSetObject.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			volumeMount := &container.VolumeMounts[j]
			if volumeClaimTemplate, ok := c.getVolumeClaimTemplate(volumeMount.Name); ok {
				// Found VolumeClaimTemplate to mount by VolumeMount
				appendVolumeClaimTemplateFrom(statefulSetObject, volumeClaimTemplate)
			}
		}
	}

	// Now deal with .templates.VolumeClaimTemplate
	//
	// We want to mount this default VolumeClaimTemplate into /var/lib/clickhouse in case:
	// 1. This default VolumeClaimTemplate is not already mounted with any VolumeMount
	// 2. And /var/lib/clickhouse  is not already mounted with any VolumeMount

	defaultVolumeClaimTemplateName := replica.Templates.VolumeClaimTemplate

	if defaultVolumeClaimTemplateName == "" {
		// No .templates.VolumeClaimTemplate specified
		return
	}

	if _, ok := c.getVolumeClaimTemplate(defaultVolumeClaimTemplateName); !ok {
		// Incorrect .templates.VolumeClaimTemplate specified
		return
	}

	// 1. Check explicit usage - whether this default VolumeClaimTemplate is already listed in VolumeMount
	for i := range statefulSetObject.Spec.Template.Spec.Containers[ClickHouseContainerIndex].VolumeMounts {
		// Convenience wrapper
		volumeMount := &statefulSetObject.Spec.Template.Spec.Containers[ClickHouseContainerIndex].VolumeMounts[i]
		if volumeMount.Name == defaultVolumeClaimTemplateName {
			// This .templates.VolumeClaimTemplate is already used in VolumeMount
			glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template 1: %s\n", statefulSetName, volumeMount.Name)
			return
		}
	}

	// This default VolumeClaimTemplate is not used by name - it is unused - what's it's purpose, then?
	// So we want to mount it to /var/lib/clickhouse even more now, because it is unused.
	// However, mount point /var/lib/clickhouse may be used already explicitly. Need to check

	// 2. Check whether /var/lib/clickhouse is already mounted
	for i := range statefulSetObject.Spec.Template.Spec.Containers[ClickHouseContainerIndex].VolumeMounts {
		// Convenience wrapper
		volumeMount := &statefulSetObject.Spec.Template.Spec.Containers[ClickHouseContainerIndex].VolumeMounts[i]
		if volumeMount.MountPath == dirPathClickHouseData {
			// /var/lib/clickhouse is already mounted
			glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template 2: /var/lib/clickhouse already mounte\n", statefulSetName)
			return
		}
	}

	// This default volumeClaimTemplate is not used explicitly by name and /var/lib/clickhouse is not mounted also.
	// Let's mount this default VolumeClaimTemplate into /var/lib/clickhouse
	if template, ok := c.getVolumeClaimTemplate(defaultVolumeClaimTemplateName); ok {
		// Add VolumeClaimTemplate to StatefulSet
		appendVolumeClaimTemplateFrom(statefulSetObject, template)
		// Add VolumeMount to ClickHouse container to /var/lib/clickhouse point
		statefulSetObject.Spec.Template.Spec.Containers[ClickHouseContainerIndex].VolumeMounts = append(
			statefulSetObject.Spec.Template.Spec.Containers[ClickHouseContainerIndex].VolumeMounts,
			createVolumeMountObject(replica.Templates.VolumeClaimTemplate, dirPathClickHouseData),
		)
	}

	glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template.useDefaultName: %s\n", statefulSetName, defaultVolumeClaimTemplateName)
}

func copyPodTemplateFrom(dst *apps.StatefulSet, src *chiv1.ChiPodTemplate) {
	dst.Spec.Template.Name = src.Name
	dst.Spec.Template.Spec = *src.Spec.DeepCopy()
}

func appendVolumeClaimTemplateFrom(dst *apps.StatefulSet, src *chiv1.ChiVolumeClaimTemplate) {
	// Ensure VolumeClaimTemplates slice is in place
	if dst.Spec.VolumeClaimTemplates == nil {
		dst.Spec.VolumeClaimTemplates = make([]corev1.PersistentVolumeClaim, 0, 0)
	}

	// Append copy of PersistentVolumeClaimSpec
	dst.Spec.VolumeClaimTemplates = append(dst.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: src.Name,
		},
		Spec: *src.Spec.DeepCopy(),
	})
}

// createDefaultPodTemplate returns default Pod Template to be used with StatefulSet
func createDefaultPodTemplate(name string) *chiv1.ChiPodTemplate {
	return &chiv1.ChiPodTemplate{
		Name: name,
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "clickhouse",
					Image: defaultClickHouseDockerImage,
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
						Handler: corev1.Handler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/ping",
								Port: intstr.Parse(chDefaultHTTPPortName),
							},
						},
						InitialDelaySeconds: 10,
						PeriodSeconds:       10,
					},
				},
			},
			Volumes: []corev1.Volume{},
		},
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

// createVolumeClaimTemplatesIndex creates a map of volumeClaimTemplatesIndexData used as a reference storage for VolumeClaimTemplates
func (c *Creator) createVolumeClaimTemplatesIndex() {
	c.volumeClaimTemplatesIndex = make(volumeClaimTemplatesIndex)
	for i := range c.chi.Spec.Templates.VolumeClaimTemplates {
		// Convenience wrapper
		volumeClaimTemplate := &c.chi.Spec.Templates.VolumeClaimTemplates[i]
		c.volumeClaimTemplatesIndex[volumeClaimTemplate.Name] = volumeClaimTemplate
	}
}

// getVolumeClaimTemplate gets VolumeClaimTemplate by name
func (c *Creator) getVolumeClaimTemplate(name string) (*chiv1.ChiVolumeClaimTemplate, bool) {
	pvc, ok := c.volumeClaimTemplatesIndex[name]
	return pvc, ok
}

// createPodTemplatesIndex creates a map of podTemplatesIndexData used as a reference storage for PodTemplates
func (c *Creator) createPodTemplatesIndex() {
	c.podTemplatesIndex = make(podTemplatesIndex)
	for i := range c.chi.Spec.Templates.PodTemplates {
		// Convenience wrapper
		podTemplate := &c.chi.Spec.Templates.PodTemplates[i]
		c.podTemplatesIndex[podTemplate.Name] = podTemplate
	}
}

// getPodTemplate gets PodTemplate by name
func (c *Creator) getPodTemplate(name string) (*chiv1.ChiPodTemplate, bool) {
	podTemplateSpec, ok := c.podTemplatesIndex[name]
	return podTemplateSpec, ok
}

func IsChopGeneratedObject(objectMeta *metav1.ObjectMeta) bool {
	// Parse Labels
	// 			Labels: map[string]string{
	//				ChopGeneratedLabel: AppVersion,
	//				ChiGeneratedLabel:  replica.Address.ChiName,
	//				ClusterGeneratedLabel: replica.Address.ClusterName,
	//				ClusterIndexGeneratedLabel: strconv.Itoa(replica.Address.ClusterIndex),
	//				ReplicaIndexGeneratedLabel: strconv.Itoa(replica.Address.ReplicaIndex),
	//			},

	// ObjectMeta must have some labels
	if len(objectMeta.Labels) == 0 {
		return false
	}

	// ObjectMeta must have ChopGeneratedLabel
	_, ok := objectMeta.Labels[ChopGeneratedLabel]

	return ok
}
