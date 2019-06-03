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

package model

import (
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/golang/glog"
)

// Creator is the base struct to create k8s objects
type Creator struct {
	appVersion                string
	chi                       *chiv1.ClickHouseInstallation
	chopConfig                *config.Config
	chConfigGenerator         *ClickHouseConfigGenerator
	chConfigSectionsGenerator *configSections
	labeler                   *Labeler

	podTemplatesIndex         podTemplatesIndex
	volumeClaimTemplatesIndex volumeClaimTemplatesIndex
	reconcile                 *ReconcileFuncs
}

type ReconcileFuncs struct {
	ConfigMap   func(configMap *corev1.ConfigMap) error
	Service     func(service *corev1.Service) error
	StatefulSet func(newStatefulSet *apps.StatefulSet, replica *chiv1.ChiReplica) error
}

// NewCreator creates new creator
func NewCreator(
	chi *chiv1.ClickHouseInstallation,
	chopConfig *config.Config,
	appVersion string,
	reconcile *ReconcileFuncs,
) *Creator {
	creator := &Creator{
		chi:               chi,
		chopConfig:        chopConfig,
		appVersion:        appVersion,
		chConfigGenerator: NewClickHouseConfigGenerator(chi),
		labeler:           NewLabeler(appVersion, chi),
		reconcile:         reconcile,
	}
	creator.chConfigSectionsGenerator = NewConfigSections(creator.chConfigGenerator, creator.chopConfig)
	creator.createPodTemplatesIndex()
	creator.createVolumeClaimTemplatesIndex()

	return creator
}

// ChiCreateObjects returns a map of the k8s objects created based on ClickHouseInstallation Object properties
func (c *Creator) Reconcile() error {
	if err := c.reconcileServiceChi(CreateChiServiceName(c.chi)); err != nil {
		return err
	}

	if err := c.reconcileConfigMapsChi(); err != nil {
		return err
	}

	if err := c.reconcileReplicas(); err != nil {
		return err
	}
	return nil
}

func (c *Creator) reconcileServiceChi(serviceName string) error {
	glog.V(1).Infof("reconcileServiceObjectChi() for service %s", serviceName)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: c.chi.Namespace,
			Labels:    c.labeler.getLabelsCommonObject(),
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
			Selector: c.labeler.getSelectorCommonObject(),
			Type:     "LoadBalancer",
		},
	}

	return c.reconcile.Service(service)
}

// reconcileConfigMapObjectsChi returns a list of corev1.ConfigMap objects
func (c *Creator) reconcileConfigMapsChi() error {
	c.chConfigSectionsGenerator.CreateConfigsUsers()
	c.chConfigSectionsGenerator.CreateConfigsCommon()

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonName(c.chi),
			Namespace: c.chi.Namespace,
			Labels:    c.labeler.getLabelsCommonObject(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigSectionsGenerator.commonConfigSections,
	}
	if err := c.reconcile.ConfigMap(configMapCommon); err != nil {
		return err
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonUsersName(c.chi),
			Namespace: c.chi.Namespace,
			Labels:    c.labeler.getLabelsCommonObject(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigSectionsGenerator.commonUsersConfigSections,
	}
	if err := c.reconcile.ConfigMap(configMapUsers); err != nil {
		return err
	}

	return nil
}

func (c *Creator) reconcileReplicas() error {
	replicaProcessor := func(replica *chiv1.ChiReplica) error {
		// Add replica's Service
		service := c.createService(replica)
		if err := c.reconcile.Service(service); err != nil {
			return err
		}

		// Add replica's ConfigMap
		configMap := c.createConfigMap(replica)
		if err := c.reconcile.ConfigMap(configMap); err != nil {
			return err
		}

		// Add replica's StatefulSet
		statefulSet := c.createStatefulSet(replica)
		if err := c.reconcile.StatefulSet(statefulSet, replica); err != nil {
			return err
		}

		return nil
	}

	return c.chi.WalkReplicasTillError(replicaProcessor)
}

func (c *Creator) createService(replica *chiv1.ChiReplica) *corev1.Service {
	serviceName := CreateStatefulSetServiceName(replica)
	statefulSetName := CreateStatefulSetName(replica)

	glog.V(1).Infof("createService(%s):%s", serviceName, statefulSetName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: replica.Address.Namespace,
			Labels:    c.labeler.getLabelsReplica(replica, false),
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
			Selector:  c.labeler.getSelectorReplica(replica),
			ClusterIP: templateDefaultsServiceClusterIP,
			Type:      "ClusterIP",
		},
	}
}

func (c *Creator) createConfigMap(replica *chiv1.ChiReplica) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapPodName(replica),
			Namespace: replica.Address.Namespace,
			Labels:    c.labeler.getLabelsReplica(replica, false),
		},
		Data: c.chConfigSectionsGenerator.CreateConfigsPod(replica),
	}
}

func (c *Creator) createStatefulSet(replica *chiv1.ChiReplica) *apps.StatefulSet {
	statefulSetName := CreateStatefulSetName(replica)
	serviceName := CreateStatefulSetServiceName(replica)

	// Create apps.StatefulSet object
	replicasNum := int32(1)
	// StatefulSet has additional label - ZK config fingerprint
	statefulSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: replica.Address.Namespace,
			Labels:    c.labeler.getLabelsReplica(replica, true),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    &replicasNum,
			ServiceName: serviceName,
			Selector: &metav1.LabelSelector{
				MatchLabels: c.labeler.getSelectorReplica(replica),
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
	replica *chiv1.ChiReplica,
) {
	statefulSetName := CreateStatefulSetName(replica)
	podTemplateName := replica.Templates.PodTemplate

	// Initial PodTemplateSpec value
	// All the rest fields would be filled later
	statefulSetObject.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: c.labeler.getLabelsReplica(replica, true),
		},
	}

	// Specify pod templates - either explicitly defined or default
	if podTemplate, ok := c.getPodTemplate(podTemplateName); ok {
		// Replica references known PodTemplate
		copyPodTemplateFrom(statefulSetObject, podTemplate)
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - template: %s", statefulSetName, podTemplateName)
	} else {
		// Replica references UNKNOWN PodTemplate
		copyPodTemplateFrom(statefulSetObject, createDefaultPodTemplate(statefulSetName))
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - default template", statefulSetName)
	}

	c.setupConfigMapVolumes(statefulSetObject, replica)
}

// setupConfigMapVolumes adds to each container in the Pod VolumeMount objects with
func (c *Creator) setupConfigMapVolumes(
	statefulSetObject *apps.StatefulSet,
	replica *chiv1.ChiReplica,
) {
	configMapMacrosName := CreateConfigMapPodName(replica)
	configMapCommonName := CreateConfigMapCommonName(c.chi)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(c.chi)

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
	replica *chiv1.ChiReplica,
) {
	// Append VolumeClaimTemplates, that are referenced in Containers' VolumeMount object(s)
	// to StatefulSet's Spec.VolumeClaimTemplates slice, so these
	statefulSetName := CreateStatefulSetName(replica)
	for i := range statefulSetObject.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSetObject.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			// Convenience wrapper
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
			glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template 1: %s", statefulSetName, volumeMount.Name)
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
			glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template 2: /var/lib/clickhouse already mounted", statefulSetName)
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

	glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template.useDefaultName: %s", statefulSetName, defaultVolumeClaimTemplateName)
}

// copyPodTemplateFrom fills StatefulSet.Spec.Template with data from provided 'src' ChiPodTemplate
func copyPodTemplateFrom(dst *apps.StatefulSet, src *chiv1.ChiPodTemplate) {
	dst.Spec.Template.Name = src.Name
	dst.Spec.Template.Spec = *src.Spec.DeepCopy()
}

// appendVolumeClaimTemplateFrom appends to StatefulSet.Spec.VolumeClaimTemplates new entry with data from provided 'src' ChiVolumeClaimTemplate
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
	volumeClaimTemplate, ok := c.volumeClaimTemplatesIndex[name]
	return volumeClaimTemplate, ok
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
	podTemplate, ok := c.podTemplatesIndex[name]
	return podTemplate, ok
}
