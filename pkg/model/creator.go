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
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"k8s.io/apimachinery/pkg/util/intstr"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/golang/glog"
	// log "k8s.io/klog"
)

type Creator struct {
	chop                      *chop.CHOp
	chi                       *chiv1.ClickHouseInstallation
	chConfigGenerator         *ClickHouseConfigGenerator
	chConfigSectionsGenerator *configSections
	labeler                   *Labeler
}

func NewCreator(
	chop *chop.CHOp,
	chi *chiv1.ClickHouseInstallation,
) *Creator {
	creator := &Creator{
		chop:              chop,
		chi:               chi,
		chConfigGenerator: NewClickHouseConfigGenerator(chi),
		labeler:           NewLabeler(chop, chi),
	}
	creator.chConfigSectionsGenerator = NewConfigSections(creator.chConfigGenerator, creator.chop.Config())
	return creator
}

// CreateServiceCHI creates new corev1.Service for specified CHI
func (c *Creator) CreateServiceCHI() *corev1.Service {
	serviceName := CreateCHIServiceName(c.chi)

	log.V(1).Infof("CreateServiceCHI(%s/%s)", c.chi.Namespace, serviceName)
	if template, ok := c.chi.GetCHIServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			c.chi.Namespace,
			serviceName,
			c.labeler.getLabelsServiceCHI(),
			c.labeler.getSelectorCHIScope(),
		)
	} else {
		// Incorrect/unknown .templates.ServiceTemplate specified
		// Create default Service
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: c.chi.Namespace,
				Labels:    c.labeler.getLabelsServiceCHI(),
			},
			Spec: corev1.ServiceSpec{
				// ClusterIP: templateDefaultsServiceClusterIP,
				Ports: []corev1.ServicePort{
					{
						Name:       chDefaultHTTPPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       chDefaultHTTPPortNumber,
						TargetPort: intstr.FromString(chDefaultHTTPPortName),
					},
					{
						Name:       chDefaultTCPPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       chDefaultTCPPortNumber,
						TargetPort: intstr.FromString(chDefaultTCPPortName),
					},
				},
				Selector:              c.labeler.getSelectorCHIScope(),
				Type:                  corev1.ServiceTypeLoadBalancer,
				ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
			},
		}
	}
}

// createServiceCluster creates new corev1.Service for specified Cluster
func (c *Creator) CreateServiceCluster(cluster *chiv1.ChiCluster) *corev1.Service {
	serviceName := CreateClusterServiceName(cluster)

	log.V(1).Infof("CreateServiceCluster(%s/%s)", cluster.Address.Namespace, serviceName)
	if template, ok := cluster.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			cluster.Address.Namespace,
			serviceName,
			c.labeler.getLabelsServiceCluster(cluster),
			c.labeler.getSelectorClusterScope(cluster),
		)
	} else {
		return nil
	}
}

// createServiceShard creates new corev1.Service for specified Shard
func (c *Creator) CreateServiceShard(shard *chiv1.ChiShard) *corev1.Service {
	serviceName := CreateShardServiceName(shard)

	log.V(1).Infof("CreateServiceShard(%s/%s)", shard.Address.Namespace, serviceName)
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			shard.Address.Namespace,
			serviceName,
			c.labeler.getLabelsServiceShard(shard),
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

	log.V(1).Infof("CreateServiceHost(%s/%s) for Set %s", host.Address.Namespace, serviceName, statefulSetName)
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			host.Address.Namespace,
			serviceName,
			c.labeler.getLabelsServiceHost(host),
			c.labeler.GetSelectorHostScope(host),
		)
	} else {
		// Incorrect/unknown .templates.ServiceTemplate specified
		// Create default Service
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: host.Address.Namespace,
				Labels:    c.labeler.getLabelsServiceHost(host),
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{
					{
						Name:       chDefaultHTTPPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       host.HTTPPort,
						TargetPort: intstr.FromString(chDefaultHTTPPortName),
					},
					{
						Name:       chDefaultTCPPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       host.TCPPort,
						TargetPort: intstr.FromString(chDefaultTCPPortName),
					},
					{
						Name:       chDefaultInterserverHTTPPortName,
						Protocol:   corev1.ProtocolTCP,
						Port:       host.InterserverHTTPPort,
						TargetPort: intstr.FromString(chDefaultInterserverHTTPPortName),
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
			log.V(1).Infof(msg)
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

// CreateConfigMapCHICommon creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapCHICommon() *corev1.ConfigMap {
	c.chConfigSectionsGenerator.CreateConfigsCommon()
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonName(c.chi),
			Namespace: c.chi.Namespace,
			Labels:    c.labeler.getLabelsConfigMapCHICommon(),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigSectionsGenerator.commonConfigSections,
	}
}

// CreateConfigMapCHICommonUsers creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapCHICommonUsers() *corev1.ConfigMap {
	c.chConfigSectionsGenerator.CreateConfigsUsers()
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateConfigMapCommonUsersName(c.chi),
			Namespace: c.chi.Namespace,
			Labels:    c.labeler.getLabelsConfigMapCHICommonUsers(),
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
			Labels:    c.labeler.getLabelsConfigMapHost(host),
		},
		Data: c.chConfigSectionsGenerator.CreateConfigsHost(host),
	}
}

// createStatefulSet creates new apps.StatefulSet
func (c *Creator) CreateStatefulSet(host *chiv1.ChiHost) *apps.StatefulSet {
	statefulSetName := CreateStatefulSetName(host)
	serviceName := CreateStatefulSetServiceName(host)

	// Create apps.StatefulSet object
	replicasNum := host.GetReplicasNum()
	revisionHistoryLimit := int32(10)
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
			// Template is to be setup later
			Template: corev1.PodTemplateSpec{},

			// IMPORTANT
			// VolumeClaimTemplates are to be setup later
			VolumeClaimTemplates: nil,

			PodManagementPolicy: apps.OrderedReadyPodManagement,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
			RevisionHistoryLimit: &revisionHistoryLimit,
		},
	}

	c.setupStatefulSetPodTemplate(statefulSet, host)
	c.setupStatefulSetVolumeClaimTemplates(statefulSet, host)

	host.StatefulSet = statefulSet

	return statefulSet
}

// PreparePersistentVolume
func (c *Creator) PreparePersistentVolume(pv *corev1.PersistentVolume, host *chiv1.ChiHost) *corev1.PersistentVolume {
	pv.Labels = util.MergeStringMaps(pv.Labels, c.labeler.getLabelsHostScope(host, false))
	return pv
}

// setupStatefulSetPodTemplate performs PodTemplate setup of StatefulSet
func (c *Creator) setupStatefulSetPodTemplate(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {

	// Initial PodTemplateSpec
	statefulSet.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      c.labeler.getLabelsHostScope(host, true),
			Annotations: c.labeler.getAnnotationsHostScope(host),
		},
	}

	// Process Pod Template
	podTemplate := c.getPodTemplate(host)
	statefulSetApplyPodTemplate(statefulSet, podTemplate)

	// Post-process StatefulSet
	c.ensureStatefulSetIntegrity(statefulSet, host)
	c.personalizeStatefulSetTemplate(statefulSet, host)
}

func (c *Creator) ensureStatefulSetIntegrity(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	ensureClickHouseContainer(statefulSet, host)
	ensureNamedPortsSpecified(statefulSet, host)
}

func ensureClickHouseContainer(statefulSet *apps.StatefulSet, _ *chiv1.ChiHost) {
	if _, ok := getClickHouseContainer(statefulSet); !ok {
		// No ClickHouse container available
		addContainer(
			&statefulSet.Spec.Template.Spec,
			newDefaultClickHouseContainer(),
		)
	}
}

func (c *Creator) personalizeStatefulSetTemplate(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	statefulSetName := CreateStatefulSetName(host)

	// Ensure pod created by this StatefulSet has alias 127.0.0.1
	statefulSet.Spec.Template.Spec.HostAliases = []corev1.HostAlias{
		{
			IP:        "127.0.0.1",
			Hostnames: []string{CreatePodHostname(host)},
		},
	}

	// Setup volumes based on ConfigMaps into Pod Template
	c.setupConfigMapVolumes(statefulSet, host)

	// In case we have default LogVolumeClaimTemplate specified - need to append log container to Pod Template
	if host.Templates.LogVolumeClaimTemplate != "" {
		addContainer(&statefulSet.Spec.Template.Spec, corev1.Container{
			Name:  ClickHouseLogContainerName,
			Image: defaultBusyBoxDockerImage,
			Command: []string{
				"/bin/sh", "-c", "--",
			},
			Args: []string{
				"while true; do sleep 30; done;",
			},
		})
		log.V(1).Infof("setupStatefulSetPodTemplate() add log container for statefulSet %s", statefulSetName)
	}
}

// getPodTemplate gets Pod Template to be used to create StatefulSet
func (c *Creator) getPodTemplate(host *chiv1.ChiHost) *chiv1.ChiPodTemplate {
	statefulSetName := CreateStatefulSetName(host)

	// Which pod template would be used - either explicitly defined in or a default one
	podTemplate, ok := host.GetPodTemplate()
	if ok {
		// Host references known PodTemplate
		// Make local copy of this PodTemplate, in order not to spoil the original common-used template
		podTemplate = podTemplate.DeepCopy()
		log.V(1).Infof("getPodTemplate() statefulSet %s use custom template %s", statefulSetName, podTemplate.Name)
	} else {
		// Host references UNKNOWN PodTemplate, will use default one
		podTemplate = newDefaultPodTemplate(statefulSetName)
		log.V(1).Infof("getPodTemplate() statefulSet %s use default generated template", statefulSetName)
	}

	// Here we have local copy of Pod Template, to be used to create StatefulSet
	// Now we can customize this Pod Template for particular host

	c.labeler.prepareAffinity(podTemplate, host)

	return podTemplate
}

// setupConfigMapVolumes adds to each container in the Pod VolumeMount objects with
func (c *Creator) setupConfigMapVolumes(statefulSetObject *apps.StatefulSet, host *chiv1.ChiHost) {
	configMapMacrosName := CreateConfigMapPodName(host)
	configMapCommonName := CreateConfigMapCommonName(c.chi)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(c.chi)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	statefulSetObject.Spec.Template.Spec.Volumes = append(
		statefulSetObject.Spec.Template.Spec.Volumes,
		newVolumeForConfigMap(configMapCommonName),
		newVolumeForConfigMap(configMapCommonUsersName),
		newVolumeForConfigMap(configMapMacrosName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes
	for i := range statefulSetObject.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSetObject.Spec.Template.Spec.Containers[i]
		// Append to each Container current VolumeMount's to VolumeMount's declared in template
		container.VolumeMounts = append(
			container.VolumeMounts,
			newVolumeMount(configMapCommonName, dirPathCommonConfig),
			newVolumeMount(configMapCommonUsersName, dirPathUsersConfig),
			newVolumeMount(configMapMacrosName, dirPathHostConfig),
		)
	}
}

// setupStatefulSetApplyVolumeMounts applies `volumeMounts` of a `container`
func (c *Creator) setupStatefulSetApplyVolumeMounts(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	// Deal with `volumeMounts` of a `container`, located by the path:
	// .spec.templates.podTemplates.*.spec.containers.volumeMounts.*
	// VolumeClaimTemplates, that are directly referenced in Containers' VolumeMount object(s)
	// are appended to StatefulSet's Spec.VolumeClaimTemplates slice
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			// Convenience wrapper
			volumeMount := &container.VolumeMounts[j]
			if volumeClaimTemplate, ok := c.chi.GetVolumeClaimTemplate(volumeMount.Name); ok {
				// Found VolumeClaimTemplate to mount by VolumeMount
				c.statefulSetAppendPVCTemplate(host, statefulSet, volumeClaimTemplate)
			}
		}
	}
}

// setupStatefulSetApplyVolumeClaimTemplates applies Data and Log VolumeClaimTemplates on all containers
func (c *Creator) setupStatefulSetApplyVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	// Mount all named (data and log so far) VolumeClaimTemplates into all containers
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		_ = c.setupStatefulSetApplyVolumeMount(host, statefulSet, container.Name, newVolumeMount(host.Templates.DataVolumeClaimTemplate, dirPathClickHouseData))
		_ = c.setupStatefulSetApplyVolumeMount(host, statefulSet, container.Name, newVolumeMount(host.Templates.LogVolumeClaimTemplate, dirPathClickHouseLog))
	}
}

// setupStatefulSetVolumeClaimTemplates performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (c *Creator) setupStatefulSetVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	c.setupStatefulSetApplyVolumeMounts(statefulSet, host)
	c.setupStatefulSetApplyVolumeClaimTemplates(statefulSet, host)
}

// statefulSetApplyPodTemplate fills StatefulSet.Spec.Template with data from provided 'src' ChiPodTemplate
func statefulSetApplyPodTemplate(dst *apps.StatefulSet, template *chiv1.ChiPodTemplate) {
	// StatefulSet's pod template is not directly compatible with ChiPodTemplate, we need some fields only
	dst.Spec.Template.Name = template.Name
	dst.Spec.Template.Spec = template.Spec
}

func getClickHouseContainer(statefulSet *apps.StatefulSet) (*corev1.Container, bool) {
	if len(statefulSet.Spec.Template.Spec.Containers) > 0 {
		return &statefulSet.Spec.Template.Spec.Containers[0], true
	} else {
		return nil, false
	}
}

func ensureNamedPortsSpecified(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	// Ensure ClickHouse container has all named ports specified
	chContainer, ok := getClickHouseContainer(statefulSet)
	if !ok {
		return
	}
	ensurePortByName(chContainer, chDefaultTCPPortName, host.TCPPort)
	ensurePortByName(chContainer, chDefaultHTTPPortName, host.HTTPPort)
	ensurePortByName(chContainer, chDefaultInterserverHTTPPortName, host.InterserverHTTPPort)
}

func ensurePortByName(container *corev1.Container, name string, port int32) {
	// Find port with specified name
	for i := range container.Ports {
		containerPort := &container.Ports[i]
		if containerPort.Name == name {
			containerPort.HostPort = 0
			containerPort.ContainerPort = port
			return
		}
	}

	// Port with specified name not found. Need to append
	container.Ports = append(container.Ports, corev1.ContainerPort{
		Name:          name,
		ContainerPort: port,
	})
}

// setupStatefulSetApplyVolumeMount applies .templates.volumeClaimTemplates.* to a StatefulSet
func (c *Creator) setupStatefulSetApplyVolumeMount(
	host *chiv1.ChiHost,
	statefulSet *apps.StatefulSet,
	containerName string,
	volumeMount corev1.VolumeMount,
) error {

	// Sanity checks

	// 1. mountPath has to be reasonable
	if volumeMount.MountPath == "" {
		// No mount path specified
		return nil
	}

	volumeClaimTemplateName := volumeMount.Name

	// 2. volumeClaimTemplateName has to be reasonable
	if volumeClaimTemplateName == "" {
		// No VolumeClaimTemplate specified
		return nil
	}

	// 3. Specified (by volumeClaimTemplateName) VolumeClaimTemplate has to be available as well
	if _, ok := c.chi.GetVolumeClaimTemplate(volumeClaimTemplateName); !ok {
		// Incorrect/unknown .templates.VolumeClaimTemplate specified
		log.V(1).Infof("Can not find volumeClaimTemplate %s. Volume claim can not be mounted", volumeClaimTemplateName)
		return nil
	}

	// 4. Specified container has to be available
	container := getContainerByName(statefulSet, containerName)
	if container == nil {
		log.V(1).Infof("Can not find container %s. Volume claim can not be mounted", containerName)
		return nil
	}

	// Looks like all components are in place

	// Mount specified (by volumeMount.Name) VolumeClaimTemplate into volumeMount.Path (say into '/var/lib/clickhouse')
	//
	// A container wants to have this VolumeClaimTemplate mounted into `mountPath` in case:
	// 1. This VolumeClaimTemplate is NOT already mounted in the container with any VolumeMount (to avoid double-mount of a VolumeClaimTemplate)
	// 2. And specified `mountPath` (say '/var/lib/clickhouse') is NOT already mounted with any VolumeMount (to avoid double-mount/rewrite into single `mountPath`)

	for i := range container.VolumeMounts {
		// Convenience wrapper
		existingVolumeMount := &container.VolumeMounts[i]

		// 1. Check whether this VolumeClaimTemplate is already listed in VolumeMount of this container
		if volumeMount.Name == existingVolumeMount.Name {
			// This .templates.VolumeClaimTemplate is already used in VolumeMount
			log.V(1).Infof(
				"setupStatefulSetApplyVolumeClaim(%s) container %s volumeClaimTemplateName %s already used",
				statefulSet.Name,
				container.Name,
				volumeMount.Name,
			)
			return nil
		}

		// 2. Check whether `mountPath` (say '/var/lib/clickhouse') is already mounted
		if volumeMount.MountPath == existingVolumeMount.MountPath {
			// `mountPath` (say /var/lib/clickhouse) is already mounted
			log.V(1).Infof(
				"setupStatefulSetApplyVolumeClaim(%s) container %s mountPath %s already used",
				statefulSet.Name,
				container.Name,
				volumeMount.MountPath,
			)
			return nil
		}
	}

	// This VolumeClaimTemplate is not used explicitly by name and `mountPath` (say /var/lib/clickhouse) is not used also.
	// Let's mount this VolumeClaimTemplate into `mountPath` (say '/var/lib/clickhouse') of a container
	if template, ok := c.chi.GetVolumeClaimTemplate(volumeClaimTemplateName); ok {
		// Add VolumeClaimTemplate to StatefulSet
		c.statefulSetAppendPVCTemplate(host, statefulSet, template)
		// Add VolumeMount to ClickHouse container to `mountPath` point
		container.VolumeMounts = append(
			container.VolumeMounts,
			volumeMount,
		)
	}

	log.V(1).Infof("setupStatefulSetApplyVolumeClaim(%s) container %s mounted %s on %s",
		statefulSet.Name,
		container.Name,
		volumeMount.Name,
		volumeMount.MountPath,
	)

	return nil
}

// statefulSetAppendPVCTemplate appends to StatefulSet.Spec.VolumeClaimTemplates new entry with data from provided 'src' ChiVolumeClaimTemplate
func (c *Creator) statefulSetAppendPVCTemplate(
	host *chiv1.ChiHost,
	statefulSet *apps.StatefulSet,
	volumeClaimTemplate *chiv1.ChiVolumeClaimTemplate,
) {
	// Ensure VolumeClaimTemplates slice is in place
	if statefulSet.Spec.VolumeClaimTemplates == nil {
		statefulSet.Spec.VolumeClaimTemplates = make([]corev1.PersistentVolumeClaim, 0, 0)
	}

	// Check whether this VolumeClaimTemplate is already listed in statefulSet.Spec.VolumeClaimTemplates
	for i := range statefulSet.Spec.VolumeClaimTemplates {
		// Convenience wrapper
		volumeClaimTemplates := &statefulSet.Spec.VolumeClaimTemplates[i]
		if volumeClaimTemplates.Name == volumeClaimTemplate.Name {
			// This VolumeClaimTemplate is already listed in statefulSet.Spec.VolumeClaimTemplates
			// No need to add it second time
			return
		}
	}

	// VolumeClaimTemplate is not listed in statefulSet.Spec.VolumeClaimTemplates - let's add it
	persistentVolumeClaim := corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeClaimTemplate.Name,
			// TODO
			//  this has to wait until proper disk inheritance procedure will be available
			// UPDATE
			//  we are close to proper disk inheritance
			// Right now we hit the following error:
			// "Forbidden: updates to statefulset spec for fields other than 'replicas', 'template', and 'updateStrategy' are forbidden"
			Labels: c.labeler.getLabelsHostScope(host, false),
		},
		Spec: *volumeClaimTemplate.Spec.DeepCopy(),
	}
	// TODO introduce normalization
	volumeMode := corev1.PersistentVolumeFilesystem
	persistentVolumeClaim.Spec.VolumeMode = &volumeMode

	// Append copy of PersistentVolumeClaimSpec
	statefulSet.Spec.VolumeClaimTemplates = append(statefulSet.Spec.VolumeClaimTemplates, persistentVolumeClaim)
}

// newDefaultHostTemplate returns default Host Template to be used with StatefulSet
func newDefaultHostTemplate(name string) *chiv1.ChiHostTemplate {
	return &chiv1.ChiHostTemplate{
		Name: name,
		PortDistribution: []chiv1.ChiPortDistribution{
			{
				Type: chiv1.PortDistributionUnspecified,
			},
		},
		Spec: chiv1.ChiHost{
			Name:                "",
			TCPPort:             chPortNumberMustBeAssignedLater,
			HTTPPort:            chPortNumberMustBeAssignedLater,
			InterserverHTTPPort: chPortNumberMustBeAssignedLater,
			Templates:           chiv1.ChiTemplateNames{},
		},
	}
}

func newDefaultHostTemplateForHostNetwork(name string) *chiv1.ChiHostTemplate {
	return &chiv1.ChiHostTemplate{
		Name: name,
		PortDistribution: []chiv1.ChiPortDistribution{
			{
				Type: chiv1.PortDistributionClusterScopeIndex,
			},
		},
		Spec: chiv1.ChiHost{
			Name:                "",
			TCPPort:             chPortNumberMustBeAssignedLater,
			HTTPPort:            chPortNumberMustBeAssignedLater,
			InterserverHTTPPort: chPortNumberMustBeAssignedLater,
			Templates:           chiv1.ChiTemplateNames{},
		},
	}
}

// newDefaultPodTemplate returns default Pod Template to be used with StatefulSet
func newDefaultPodTemplate(name string) *chiv1.ChiPodTemplate {
	podTemplate := &chiv1.ChiPodTemplate{
		Name: name,
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{},
			Volumes:    []corev1.Volume{},
		},
	}

	addContainer(
		&podTemplate.Spec,
		newDefaultClickHouseContainer(),
	)

	return podTemplate
}

// newDefaultClickHouseContainer returns default ClickHouse Container
func newDefaultClickHouseContainer() corev1.Container {
	return corev1.Container{
		Name:  ClickHouseContainerName,
		Image: defaultClickHouseDockerImage,
		Ports: []corev1.ContainerPort{
			{
				Name:          chDefaultHTTPPortName,
				ContainerPort: chDefaultHTTPPortNumber,
			},
			{
				Name:          chDefaultTCPPortName,
				ContainerPort: chDefaultTCPPortNumber,
			},
			{
				Name:          chDefaultInterserverHTTPPortName,
				ContainerPort: chDefaultInterserverHTTPPortNumber,
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
	}
}

// addContainer adds container to ChiPodTemplate
func addContainer(podSpec *corev1.PodSpec, container corev1.Container) {
	podSpec.Containers = append(podSpec.Containers, container)
}

// newVolumeForConfigMap returns corev1.Volume object with defined name
func newVolumeForConfigMap(name string) corev1.Volume {
	var defaultMode int32 = 0644
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: name,
				},
				DefaultMode: &defaultMode,
			},
		},
	}
}

// newVolumeMount returns corev1.VolumeMount object with name and mount path
func newVolumeMount(name, mountPath string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}

// getContainerByName finds Container with specified name among all containers of Pod Template in StatefulSet
func getContainerByName(statefulSet *apps.StatefulSet, name string) *corev1.Container {
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		if container.Name == name {
			return container
		}
	}

	return nil
}
