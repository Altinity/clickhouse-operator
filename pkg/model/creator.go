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
	"fmt"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"k8s.io/apimachinery/pkg/util/intstr"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/golang/glog"
)

type Creator struct {
	appVersion                string
	chi                       *chiv1.ClickHouseInstallation
	chopConfig                *config.Config
	chConfigGenerator         *ClickHouseConfigGenerator
	chConfigSectionsGenerator *configSections
	labeler                   *Labeler
}

func NewCreator(
	chi *chiv1.ClickHouseInstallation,
	chopConfig *config.Config,
	appVersion string,
) *Creator {
	creator := &Creator{
		appVersion:        appVersion,
		chi:               chi,
		chopConfig:        chopConfig,
		chConfigGenerator: NewClickHouseConfigGenerator(chi),
		labeler:           NewLabeler(appVersion, chi),
	}
	creator.chConfigSectionsGenerator = NewConfigSections(creator.chConfigGenerator, creator.chopConfig)
	return creator
}

// createServiceChi creates new corev1.Service for specified CHI
func (c *Creator) CreateServiceChi() *corev1.Service {
	serviceName := CreateChiServiceName(c.chi)

	glog.V(1).Infof("createServiceChi(%s/%s)", c.chi.Namespace, serviceName)
	if template, ok := c.chi.GetOwnServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			c.chi.Namespace,
			serviceName,
			c.labeler.getLabelsChiScope(),
			c.labeler.getSelectorChiScope(),
		)
	} else {
		// Incorrect/unknown .templates.ServiceTemplate specified
		// Create default Service
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: c.chi.Namespace,
				Labels:    c.labeler.getLabelsChiScope(),
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
				Selector: c.labeler.getSelectorChiScope(),
				Type:     "LoadBalancer",
			},
		}
	}
}

// createServiceCluster creates new corev1.Service for specified Cluster
func (c *Creator) CreateServiceCluster(cluster *chiv1.ChiCluster) *corev1.Service {
	serviceName := CreateClusterServiceName(cluster)

	glog.V(1).Infof("createServiceCluster(%s/%s)", cluster.Address.Namespace, serviceName)
	if template, ok := cluster.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			cluster.Address.Namespace,
			serviceName,
			c.labeler.getLabelsClusterScope(cluster),
			c.labeler.getSelectorClusterScope(cluster),
		)
	} else {
		return nil
	}
}

// createServiceShard creates new corev1.Service for specified Shard
func (c *Creator) CreateServiceShard(shard *chiv1.ChiShard) *corev1.Service {
	serviceName := CreateShardServiceName(shard)

	glog.V(1).Infof("createServiceShard(%s/%s)", shard.Address.Namespace, serviceName)
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			shard.Address.Namespace,
			serviceName,
			c.labeler.getLabelsShardScope(shard),
			c.labeler.getSelectorShardScope(shard),
		)
	} else {
		return nil
	}
}

// createServiceHost creates new corev1.Service for specified host
func (c *Creator) CreateServiceHost(host *chiv1.ChiHost) *corev1.Service {
	serviceName := CreateStatefulSetServiceName(host)
	statefulSetName := CreateStatefulSetName(host)

	glog.V(1).Infof("createServiceHost(%s/%s) for Set %s", host.Address.Namespace, serviceName, statefulSetName)
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			host.Address.Namespace,
			serviceName,
			c.labeler.getLabelsHostScope(host, false),
			c.labeler.GetSelectorHostScope(host),
		)
	} else {
		// Incorrect/unknown .templates.ServiceTemplate specified
		// Create default Service
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: host.Address.Namespace,
				Labels:    c.labeler.getLabelsHostScope(host, false),
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{
					{
						Name:       chDefaultHTTPPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       chDefaultHTTPPortNumber,
						TargetPort: intstr.FromInt(chDefaultHTTPPortNumber),
					},
					{
						Name:       chDefaultClientPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       chDefaultClientPortNumber,
						TargetPort: intstr.FromInt(chDefaultClientPortNumber),
					},
					{
						Name:       chDefaultInterServerPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       chDefaultInterServerPortNumber,
						TargetPort: intstr.FromInt(chDefaultInterServerPortNumber),
					},
				},
				Selector:                 c.labeler.GetSelectorHostScope(host),
				ClusterIP:                templateDefaultsServiceClusterIP,
				Type:                     "ClusterIP",
				PublishNotReadyAddresses: true,
			},
		}
	}
}

// verifyServiceTemplatePorts verifies ChiServiceTemplate to have reasonable ports specified
func (c *Creator) verifyServiceTemplatePorts(template *chiv1.ChiServiceTemplate) error {
	for i := range template.Spec.Ports {
		servicePort := &template.Spec.Ports[i]
		if (servicePort.Port < 1) || (servicePort.Port > 65535) {
			msg := fmt.Sprintf("verifyServiceTemplatePorts(%s) INCORRECT PORT: %d ", template.Name, servicePort.Port)
			glog.V(1).Infof(msg)
			return fmt.Errorf(msg)
		}
	}

	return nil
}

// createServiceFromTemplate create Service from ChiServiceTemplate and additional info
func (c *Creator) createServiceFromTemplate(
	template *chiv1.ChiServiceTemplate,
	namespace string,
	name string,
	labels map[string]string,
	selector map[string]string,
) *corev1.Service {

	// Verify Ports
	if err := c.verifyServiceTemplatePorts(template); err != nil {
		return nil
	}

	// Create Service
	service := &corev1.Service{
		ObjectMeta: *template.ObjectMeta.DeepCopy(),
		Spec:       *template.Spec.DeepCopy(),
	}

	// Overwrite .name and .namespace - they are not allowed to be specified in template
	service.Name = name
	service.Namespace = namespace

	// Append provided Labels to already specified Labels in template
	service.Labels = util.MergeStringMaps(service.Labels, labels)

	// Append provided Selector to already specified Selector in template
	service.Spec.Selector = util.MergeStringMaps(service.Spec.Selector, selector)

	return service
}

// createConfigMapChiCommon creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapChiCommon() *corev1.ConfigMap {
	c.chConfigSectionsGenerator.CreateConfigsCommon()
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonName(c.chi),
			Namespace: c.chi.Namespace,
			Labels:    c.labeler.getLabelsChiScope(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigSectionsGenerator.commonConfigSections,
	}
}

// createConfigMapChiCommonUsers creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapChiCommonUsers() *corev1.ConfigMap {
	c.chConfigSectionsGenerator.CreateConfigsUsers()
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonUsersName(c.chi),
			Namespace: c.chi.Namespace,
			Labels:    c.labeler.getLabelsChiScope(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigSectionsGenerator.commonUsersConfigSections,
	}
}

// createConfigMapHost creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapHost(host *chiv1.ChiHost) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapPodName(host),
			Namespace: host.Address.Namespace,
			Labels:    c.labeler.getLabelsHostScope(host, false),
		},
		Data: c.chConfigSectionsGenerator.CreateConfigsPod(host),
	}
}

// createStatefulSet creates new apps.StatefulSet
func (c *Creator) CreateStatefulSet(host *chiv1.ChiHost) *apps.StatefulSet {
	statefulSetName := CreateStatefulSetName(host)
	serviceName := CreateStatefulSetServiceName(host)

	// Create apps.StatefulSet object
	replicasNum := int32(1)
	// StatefulSet has additional label - ZK config fingerprint
	statefulSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: host.Address.Namespace,
			Labels:    c.labeler.getLabelsHostScope(host, true),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    &replicasNum,
			ServiceName: serviceName,
			Selector: &metav1.LabelSelector{
				MatchLabels: c.labeler.GetSelectorHostScope(host),
			},
			// IMPORTANT
			// VolumeClaimTemplates are to be setup later
			VolumeClaimTemplates: nil,

			// IMPORTANT
			// Template is to be setup later
			Template: corev1.PodTemplateSpec{},
		},
	}

	c.setupStatefulSetPodTemplate(statefulSet, host)
	c.setupStatefulSetVolumeClaimTemplates(statefulSet, host)

	return statefulSet
}

// setupStatefulSetPodTemplate performs PodTemplate setup of StatefulSet
func (c *Creator) setupStatefulSetPodTemplate(statefulSetObject *apps.StatefulSet, host *chiv1.ChiHost) {
	statefulSetName := CreateStatefulSetName(host)

	// Initial PodTemplateSpec value
	// All the rest fields would be filled later
	statefulSetObject.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: c.labeler.getLabelsHostScope(host, true),
		},
	}

	// Specify pod templates - either explicitly defined or default
	if podTemplate, ok := host.GetPodTemplate(); ok {
		// Replica references known PodTemplate
		copyPodTemplateFrom(statefulSetObject, podTemplate)
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - template used", statefulSetName)
	} else {
		// Replica references UNKNOWN PodTemplate
		copyPodTemplateFrom(statefulSetObject, createDefaultPodTemplate(statefulSetName))
		glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - default template", statefulSetName)
	}

	// Pod created by this StatefulSet has to have alias
	statefulSetObject.Spec.Template.Spec.HostAliases = []corev1.HostAlias{
		{
			IP:        "127.0.0.1",
			Hostnames: []string{CreatePodHostname(host)},
		},
	}

	c.setupConfigMapVolumes(statefulSetObject, host)
}

// setupConfigMapVolumes adds to each container in the Pod VolumeMount objects with
func (c *Creator) setupConfigMapVolumes(statefulSetObject *apps.StatefulSet, host *chiv1.ChiHost) {
	configMapMacrosName := CreateConfigMapPodName(host)
	configMapCommonName := CreateConfigMapCommonName(c.chi)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(c.chi)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	statefulSetObject.Spec.Template.Spec.Volumes = append(
		statefulSetObject.Spec.Template.Spec.Volumes,
		createVolumeForConfigMap(configMapCommonName),
		createVolumeForConfigMap(configMapCommonUsersName),
		createVolumeForConfigMap(configMapMacrosName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes
	for i := range statefulSetObject.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSetObject.Spec.Template.Spec.Containers[i]
		// Append to each Container current VolumeMount's to VolumeMount's declared in template
		container.VolumeMounts = append(
			container.VolumeMounts,
			createVolumeMount(configMapCommonName, dirPathConfigd),
			createVolumeMount(configMapCommonUsersName, dirPathUsersd),
			createVolumeMount(configMapMacrosName, dirPathConfd),
		)
	}
}

// setupStatefulSetVolumeClaimTemplates performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (c *Creator) setupStatefulSetVolumeClaimTemplates(
	statefulSet *apps.StatefulSet,
	host *chiv1.ChiHost,
) {
	// Append VolumeClaimTemplates, that are referenced in Containers' VolumeMount object(s)
	// to StatefulSet's Spec.VolumeClaimTemplates slice, so these
	statefulSetName := CreateStatefulSetName(host)
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			// Convenience wrapper
			volumeMount := &container.VolumeMounts[j]
			if volumeClaimTemplate, ok := c.chi.GetVolumeClaimTemplate(volumeMount.Name); ok {
				// Found VolumeClaimTemplate to mount by VolumeMount
				appendVolumeClaimTemplateFrom(statefulSet, volumeClaimTemplate)
			}
		}
	}

	// Now deal with .templates.VolumeClaimTemplate
	//
	// We want to mount this default VolumeClaimTemplate into /var/lib/clickhouse in case:
	// 1. This default VolumeClaimTemplate is not already mounted with any VolumeMount
	// 2. And /var/lib/clickhouse is not already mounted with any VolumeMount

	defaultVolumeClaimTemplateName := host.Templates.VolumeClaimTemplate

	if defaultVolumeClaimTemplateName == "" {
		// No .templates.VolumeClaimTemplate specified
		return
	}

	if _, ok := c.chi.GetVolumeClaimTemplate(defaultVolumeClaimTemplateName); !ok {
		// Incorrect/unknown .templates.VolumeClaimTemplate specified
		return
	}

	// 1. Check explicit usage - whether this default VolumeClaimTemplate is already listed in VolumeMount
	clickHouseContainer := getClickHouseContainer(statefulSet)
	for i := range clickHouseContainer.VolumeMounts {
		// Convenience wrapper
		volumeMount := &clickHouseContainer.VolumeMounts[i]
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
	for i := range clickHouseContainer.VolumeMounts {
		// Convenience wrapper
		volumeMount := &clickHouseContainer.VolumeMounts[i]
		if volumeMount.MountPath == dirPathClickHouseData {
			// /var/lib/clickhouse is already mounted
			glog.V(1).Infof("createStatefulSetObjects() for statefulSet %s - VC template 2: /var/lib/clickhouse already mounted", statefulSetName)
			return
		}
	}

	// This default volumeClaimTemplate is not used explicitly by name and /var/lib/clickhouse is not mounted also.
	// Let's mount this default VolumeClaimTemplate into /var/lib/clickhouse
	if template, ok := c.chi.GetVolumeClaimTemplate(defaultVolumeClaimTemplateName); ok {
		// Add VolumeClaimTemplate to StatefulSet
		appendVolumeClaimTemplateFrom(statefulSet, template)
		// Add VolumeMount to ClickHouse container to /var/lib/clickhouse point
		clickHouseContainer.VolumeMounts = append(
			clickHouseContainer.VolumeMounts,
			createVolumeMount(host.Templates.VolumeClaimTemplate, dirPathClickHouseData),
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

// createVolumeForConfigMap returns corev1.Volume object with defined name
func createVolumeForConfigMap(name string) corev1.Volume {
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

// createVolumeMount returns corev1.VolumeMount object with name and mount path
func createVolumeMount(name, mountPath string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}

// getClickHouseContainer finds Container with ClickHouse among all containers of Pod specified in StatefulSet
func getClickHouseContainer(statefulSet *apps.StatefulSet) *corev1.Container {
	return &statefulSet.Spec.Template.Spec.Containers[ClickHouseContainerIndex]
}
