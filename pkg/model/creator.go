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

	"github.com/gosimple/slug"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Creator specifies creator object
type Creator struct {
	chi                    *chiv1.ClickHouseInstallation
	chConfigFilesGenerator *ClickHouseConfigFilesGenerator
	labels                 *Labeler
	annotations            *Annotator
	a                      log.Announcer
}

// NewCreator creates new Creator object
func NewCreator(chi *chiv1.ClickHouseInstallation) *Creator {
	return &Creator{
		chi:                    chi,
		chConfigFilesGenerator: NewClickHouseConfigFilesGenerator(NewClickHouseConfigGenerator(chi), chop.Config()),
		labels:                 NewLabeler(chi),
		annotations:            NewAnnotator(chi),
		a:                      log.M(chi),
	}
}

// CreateServiceCHI creates new corev1.Service for specified CHI
func (c *Creator) CreateServiceCHI() *corev1.Service {
	serviceName := CreateCHIServiceName(c.chi)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s", c.chi.Namespace, serviceName)
	if template, ok := c.chi.GetCHIServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			c.chi.Namespace,
			serviceName,
			c.labels.getServiceCHI(c.chi),
			c.annotations.getServiceCHI(c.chi),
			c.labels.getSelectorCHIScopeReady(),
			ownerReferences,
			macro(c.chi),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            serviceName,
			Namespace:       c.chi.Namespace,
			Labels:          macro(c.chi).Map(c.labels.getServiceCHI(c.chi)),
			Annotations:     macro(c.chi).Map(c.annotations.getServiceCHI(c.chi)),
			OwnerReferences: ownerReferences,
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
			Selector:              c.labels.getSelectorCHIScopeReady(),
			Type:                  corev1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
		},
	}
	MakeObjectVersionLabel(&svc.ObjectMeta, svc)
	return svc
}

// CreateServiceCluster creates new corev1.Service for specified Cluster
func (c *Creator) CreateServiceCluster(cluster *chiv1.ChiCluster) *corev1.Service {
	serviceName := CreateClusterServiceName(cluster)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s", cluster.Address.Namespace, serviceName)
	if template, ok := cluster.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			cluster.Address.Namespace,
			serviceName,
			c.labels.getServiceCluster(cluster),
			c.annotations.getServiceCluster(cluster),
			getSelectorClusterScopeReady(cluster),
			ownerReferences,
			macro(cluster),
		)
	}
	// No template specified, no need to create service
	return nil
}

// CreateServiceShard creates new corev1.Service for specified Shard
func (c *Creator) CreateServiceShard(shard *chiv1.ChiShard) *corev1.Service {
	serviceName := CreateShardServiceName(shard)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s", shard.Address.Namespace, serviceName)
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			shard.Address.Namespace,
			serviceName,
			c.labels.getServiceShard(shard),
			c.annotations.getServiceShard(shard),
			getSelectorShardScopeReady(shard),
			ownerReferences,
			macro(shard),
		)
	}
	// No template specified, no need to create service
	return nil
}

// CreateServiceHost creates new corev1.Service for specified host
func (c *Creator) CreateServiceHost(host *chiv1.ChiHost) *corev1.Service {
	serviceName := CreateStatefulSetServiceName(host)
	statefulSetName := CreateStatefulSetName(host)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s for Set %s", host.Address.Namespace, serviceName, statefulSetName)
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			host.Address.Namespace,
			serviceName,
			c.labels.getServiceHost(host),
			c.annotations.getServiceHost(host),
			GetSelectorHostScope(host),
			ownerReferences,
			macro(host),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            serviceName,
			Namespace:       host.Address.Namespace,
			Labels:          macro(host).Map(c.labels.getServiceHost(host)),
			Annotations:     macro(host).Map(c.annotations.getServiceHost(host)),
			OwnerReferences: ownerReferences,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       chDefaultHTTPPortName,
					Protocol:   corev1.ProtocolTCP,
					Port:       host.HTTPPort,
					TargetPort: intstr.FromInt(int(host.HTTPPort)),
				},
				{
					Name:       chDefaultTCPPortName,
					Protocol:   corev1.ProtocolTCP,
					Port:       host.TCPPort,
					TargetPort: intstr.FromInt(int(host.TCPPort)),
				},
				{
					Name:       chDefaultInterserverHTTPPortName,
					Protocol:   corev1.ProtocolTCP,
					Port:       host.InterserverHTTPPort,
					TargetPort: intstr.FromInt(int(host.InterserverHTTPPort)),
				},
			},
			Selector:                 GetSelectorHostScope(host),
			ClusterIP:                templateDefaultsServiceClusterIP,
			Type:                     "ClusterIP",
			PublishNotReadyAddresses: true,
		},
	}
	MakeObjectVersionLabel(&svc.ObjectMeta, svc)
	return svc
}

// verifyServiceTemplatePorts verifies ChiServiceTemplate to have reasonable ports specified
func (c *Creator) verifyServiceTemplatePorts(template *chiv1.ChiServiceTemplate) error {
	for i := range template.Spec.Ports {
		servicePort := &template.Spec.Ports[i]
		if (servicePort.Port < 1) || (servicePort.Port > 65535) {
			msg := fmt.Sprintf("template:%s INCORRECT PORT:%d", template.Name, servicePort.Port)
			c.a.V(1).F().Warning(msg)
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
	annotations map[string]string,
	selector map[string]string,
	ownerReferences []metav1.OwnerReference,
	macro *macrosEngine,
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
	service.OwnerReferences = ownerReferences

	// Combine labels and annotations
	service.Labels = macro.Map(util.MergeStringMapsOverwrite(service.Labels, labels))
	service.Annotations = macro.Map(util.MergeStringMapsOverwrite(service.Annotations, annotations))

	// Append provided Selector to already specified Selector in template
	service.Spec.Selector = util.MergeStringMapsOverwrite(service.Spec.Selector, selector)

	// And after the object is ready we can put version label
	MakeObjectVersionLabel(&service.ObjectMeta, service)

	return service
}

// CreateConfigMapCHICommon creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapCHICommon(options *ClickHouseConfigFilesGeneratorOptions) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            CreateConfigMapCommonName(c.chi),
			Namespace:       c.chi.Namespace,
			Labels:          macro(c.chi).Map(c.labels.getConfigMapCHICommon()),
			Annotations:     macro(c.chi).Map(c.annotations.getConfigMapCHICommon()),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigFilesGenerator.CreateConfigFilesGroupCommon(options),
	}
	// And after the object is ready we can put version label
	MakeObjectVersionLabel(&cm.ObjectMeta, cm)
	return cm
}

// CreateConfigMapCHICommonUsers creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapCHICommonUsers() *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            CreateConfigMapCommonUsersName(c.chi),
			Namespace:       c.chi.Namespace,
			Labels:          macro(c.chi).Map(c.labels.getConfigMapCHICommonUsers()),
			Annotations:     macro(c.chi).Map(c.annotations.getConfigMapCHICommonUsers()),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigFilesGenerator.CreateConfigFilesGroupUsers(),
	}
	// And after the object is ready we can put version label
	MakeObjectVersionLabel(&cm.ObjectMeta, cm)
	return cm
}

// createConfigMapHost creates new corev1.ConfigMap
func (c *Creator) createConfigMapHost(host *chiv1.ChiHost, name string, data map[string]string) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       host.Address.Namespace,
			Labels:          macro(host).Map(c.labels.getConfigMapHost(host)),
			Annotations:     macro(host).Map(c.annotations.getConfigMapHost(host)),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		Data: data,
	}
	// And after the object is ready we can put version label
	MakeObjectVersionLabel(&cm.ObjectMeta, cm)
	return cm
}

// CreateConfigMapHost creates new corev1.ConfigMap
func (c *Creator) CreateConfigMapHost(host *chiv1.ChiHost) *corev1.ConfigMap {
	return c.createConfigMapHost(host, CreateConfigMapHostName(host), c.chConfigFilesGenerator.CreateConfigFilesGroupHost(host))
}

// CreateConfigMapHostMigration creates new corev1.ConfigMap
//func (c *Creator) CreateConfigMapHostMigration(host *chiv1.ChiHost, data map[string]string) *corev1.ConfigMap {
//	return c.createConfigMapHost(host, CreateConfigMapHostMigrationName(host), data)
//}

// MakeConfigMapData makes data for a config mao
func (c *Creator) MakeConfigMapData(names, files []string) map[string]string {
	if len(names) < 1 {
		return nil
	}
	res := make(map[string]string)
	for i := range names {
		name := fmt.Sprintf("%08d_%s.sql", i+1, slug.Make(names[i]))
		file := files[i]
		res[name] = file
	}
	return res
}

// CreateStatefulSet creates new apps.StatefulSet
func (c *Creator) CreateStatefulSet(host *chiv1.ChiHost, shutdown bool) *apps.StatefulSet {
	statefulSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            CreateStatefulSetName(host),
			Namespace:       host.Address.Namespace,
			Labels:          macro(host).Map(c.labels.getHostScope(host, true)),
			Annotations:     macro(host).Map(c.annotations.getHostScope(host)),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    host.GetStatefulSetReplicasNum(shutdown),
			ServiceName: CreateStatefulSetServiceName(host),
			Selector: &metav1.LabelSelector{
				MatchLabels: GetSelectorHostScope(host),
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
			RevisionHistoryLimit: chop.Config().GetRevisionHistoryLimit(),
		},
	}

	c.setupStatefulSetPodTemplate(statefulSet, host)
	c.setupStatefulSetVolumeClaimTemplates(statefulSet, host)
	c.setupStatefulSetVersion(statefulSet)

	host.StatefulSet = statefulSet
	host.DesiredStatefulSet = statefulSet

	return statefulSet
}

// setupStatefulSetVersion
// TODO property of the labeler?
func (c *Creator) setupStatefulSetVersion(statefulSet *apps.StatefulSet) {
	// Version can drift from instance to instance of the CHI StatefulSet even for the same CHI because
	// StatefulSet has owner already specified, which has UID of owner, which is different for different CHIs
	statefulSet.Labels = util.MergeStringMapsOverwrite(
		statefulSet.Labels,
		map[string]string{
			LabelObjectVersion: util.Fingerprint(statefulSet),
		},
	)
	// TODO fix this with verbosity update
	// c.a.V(3).F().Info("StatefulSet(%s/%s)\n%s", statefulSet.Namespace, statefulSet.Name, util.Dump(statefulSet))
}

// GetStatefulSetVersion gets version of the StatefulSet
// TODO property of the labeler?
func (c *Creator) GetStatefulSetVersion(statefulSet *apps.StatefulSet) (string, bool) {
	if statefulSet == nil {
		return "", false
	}
	label, ok := statefulSet.Labels[LabelObjectVersion]
	return label, ok
}

// PreparePersistentVolume prepares PV labels
func (c *Creator) PreparePersistentVolume(pv *corev1.PersistentVolume, host *chiv1.ChiHost) *corev1.PersistentVolume {
	pv.Labels = macro(host).Map(c.labels.getPV(pv, host))
	pv.Annotations = macro(host).Map(c.annotations.getPV(pv, host))
	// And after the object is ready we can put version label
	MakeObjectVersionLabel(&pv.ObjectMeta, pv)
	return pv
}

// PreparePersistentVolumeClaim prepares labels and annotations of the PVC
func (c *Creator) PreparePersistentVolumeClaim(
	pvc *corev1.PersistentVolumeClaim,
	host *chiv1.ChiHost,
	template *chiv1.ChiVolumeClaimTemplate,
) *corev1.PersistentVolumeClaim {
	pvc.Labels = macro(host).Map(c.labels.getPVC(pvc, host, template))
	pvc.Annotations = macro(host).Map(c.annotations.getPVC(pvc, host, template))
	// And after the object is ready we can put version label
	MakeObjectVersionLabel(&pvc.ObjectMeta, pvc)
	return pvc
}

// setupStatefulSetPodTemplate performs PodTemplate setup of StatefulSet
func (c *Creator) setupStatefulSetPodTemplate(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	// Process Pod Template
	podTemplate := c.getPodTemplate(host)
	c.statefulSetApplyPodTemplate(statefulSet, podTemplate, host)

	// Post-process StatefulSet
	ensureStatefulSetTemplateIntegrity(statefulSet, host)
	setupEnvVars(statefulSet, host)
	c.personalizeStatefulSetTemplate(statefulSet, host)
}

// ensureStatefulSetTemplateIntegrity
func ensureStatefulSetTemplateIntegrity(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	ensureClickHouseContainerSpecified(statefulSet)
	ensureProbesSpecified(statefulSet)
	ensureNamedPortsSpecified(statefulSet, host)
}

func setupEnvVars(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	container, ok := getClickHouseContainer(statefulSet)
	if !ok {
		return
	}

	container.Env = append(container.Env, host.GetCHI().Attributes.ExchangeEnv...)
}

// ensureClickHouseContainerSpecified
func ensureClickHouseContainerSpecified(statefulSet *apps.StatefulSet) {
	_, ok := getClickHouseContainer(statefulSet)
	if ok {
		return
	}

	// No ClickHouse container available, let's add one
	addContainer(
		&statefulSet.Spec.Template.Spec,
		newDefaultClickHouseContainer(),
	)
}

// ensureProbesSpecified
func ensureProbesSpecified(statefulSet *apps.StatefulSet) {
	container, ok := getClickHouseContainer(statefulSet)
	if !ok {
		return
	}
	if container.LivenessProbe == nil {
		container.LivenessProbe = newDefaultLivenessProbe()
	}
	if container.ReadinessProbe == nil {
		container.ReadinessProbe = newDefaultReadinessProbe()
	}
}

// personalizeStatefulSetTemplate
func (c *Creator) personalizeStatefulSetTemplate(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	// Ensure pod created by this StatefulSet has alias 127.0.0.1
	statefulSet.Spec.Template.Spec.HostAliases = []corev1.HostAlias{
		{
			IP:        "127.0.0.1",
			Hostnames: []string{CreatePodHostname(host)},
		},
	}

	// Setup volumes based on ConfigMaps into Pod Template
	c.setupConfigMapVolumes(statefulSet, host)
	// Setup statefulSet according to troubleshoot mode (if any)
	c.setupTroubleshoot(statefulSet)
	// Setup dedicated log container
	c.setupLogContainer(statefulSet, host)
}

// setupTroubleshoot
func (c *Creator) setupTroubleshoot(statefulSet *apps.StatefulSet) {
	if !c.chi.IsTroubleshoot() {
		// We are not troubleshooting
		return
	}

	container, ok := getClickHouseContainer(statefulSet)
	if !ok {
		// Unable to locate ClickHouse container
		return
	}

	// Let's setup troubleshooting in ClickHouse container

	sleep := " || sleep 1800"
	if len(container.Command) > 0 {
		// In case we have user-specified command, let's
		// append troubleshooting-capable tail and hope for the best
		container.Command[len(container.Command)-1] += sleep
	} else {
		// Assume standard ClickHouse container is used
		// Substitute entrypoint with troubleshooting-capable command
		container.Command = []string{
			"/bin/sh",
			"-c",
			"/entrypoint.sh" + sleep,
		}
	}
	// Appended `sleep` is not able to respond to probes, and probes would cause unexpected restart.
	// Thus we need to disable all probes.
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
}

// setupLogContainer
func (c *Creator) setupLogContainer(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	statefulSetName := CreateStatefulSetName(host)
	// In case we have default LogVolumeClaimTemplate specified - need to append log container to Pod Template
	if host.Templates.HasLogVolumeClaimTemplate() {
		addContainer(&statefulSet.Spec.Template.Spec, newDefaultLogContainer())
		c.a.V(1).F().Info("add log container for statefulSet %s", statefulSetName)
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
		c.a.V(1).F().Info("statefulSet %s use custom template %s", statefulSetName, podTemplate.Name)
	} else {
		// Host references UNKNOWN PodTemplate, will use default one
		podTemplate = newDefaultPodTemplate(statefulSetName)
		c.a.V(1).F().Info("statefulSet %s use default generated template", statefulSetName)
	}

	// Here we have local copy of Pod Template, to be used to create StatefulSet
	// Now we can customize this Pod Template for particular host

	prepareAffinity(podTemplate, host)

	return podTemplate
}

// setupConfigMapVolumes adds to each container in the Pod VolumeMount objects with
func (c *Creator) setupConfigMapVolumes(statefulSetObject *apps.StatefulSet, host *chiv1.ChiHost) {
	configMapHostName := CreateConfigMapHostName(host)
	//configMapHostMigrationName := CreateConfigMapHostMigrationName(host)
	configMapCommonName := CreateConfigMapCommonName(c.chi)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(c.chi)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	statefulSetObject.Spec.Template.Spec.Volumes = append(
		statefulSetObject.Spec.Template.Spec.Volumes,
		newVolumeForConfigMap(configMapCommonName),
		newVolumeForConfigMap(configMapCommonUsersName),
		newVolumeForConfigMap(configMapHostName),
		//newVolumeForConfigMap(configMapHostMigrationName),
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
			newVolumeMount(configMapHostName, dirPathHostConfig),
			//newVolumeMount(configMapHostMigrationName, dirPathDockerEntrypointInit),
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
		_ = c.setupStatefulSetApplyVolumeMount(host, statefulSet, container.Name, newVolumeMount(host.Templates.GetDataVolumeClaimTemplate(), dirPathClickHouseData))
		_ = c.setupStatefulSetApplyVolumeMount(host, statefulSet, container.Name, newVolumeMount(host.Templates.GetLogVolumeClaimTemplate(), dirPathClickHouseLog))
	}
}

// setupStatefulSetVolumeClaimTemplates performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (c *Creator) setupStatefulSetVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	c.setupStatefulSetApplyVolumeMounts(statefulSet, host)
	c.setupStatefulSetApplyVolumeClaimTemplates(statefulSet, host)
}

// statefulSetApplyPodTemplate fills StatefulSet.Spec.Template with data from provided ChiPodTemplate
func (c *Creator) statefulSetApplyPodTemplate(
	statefulSet *apps.StatefulSet,
	template *chiv1.ChiPodTemplate,
	host *chiv1.ChiHost,
) {
	// StatefulSet's pod template is not directly compatible with ChiPodTemplate,
	// we need to extract some fields from ChiPodTemplate and apply on StatefulSet
	statefulSet.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: template.Name,
			Labels: macro(host).Map(util.MergeStringMapsOverwrite(
				c.labels.getHostScopeReady(host, true),
				template.ObjectMeta.Labels,
			)),
			Annotations: macro(host).Map(util.MergeStringMapsOverwrite(
				c.annotations.getHostScope(host),
				template.ObjectMeta.Annotations,
			)),
		},
		Spec: *template.Spec.DeepCopy(),
	}

	if statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds == nil {
		statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = chop.Config().GetTerminationGracePeriod()
	}
}

// getClickHouseContainer
func getClickHouseContainer(statefulSet *apps.StatefulSet) (*corev1.Container, bool) {
	// Find by name
	for i := range statefulSet.Spec.Template.Spec.Containers {
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		if container.Name == ClickHouseContainerName {
			return container, true
		}
	}

	// Find by index
	if len(statefulSet.Spec.Template.Spec.Containers) > 0 {
		return &statefulSet.Spec.Template.Spec.Containers[0], true
	}

	return nil, false
}

// getClickHouseContainerStatus
func getClickHouseContainerStatus(pod *corev1.Pod) (*corev1.ContainerStatus, bool) {
	// Find by name
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name == ClickHouseContainerName {
			return status, true
		}
	}

	// Find by index
	if len(pod.Status.ContainerStatuses) > 0 {
		return &pod.Status.ContainerStatuses[0], true
	}

	return nil, false
}

// IsStatefulSetGeneration returns whether StatefulSet has requested generation or not
func IsStatefulSetGeneration(statefulSet *apps.StatefulSet, generation int64) bool {
	if statefulSet == nil {
		return false
	}

	// StatefulSet has .spec generation we are looking for
	return (statefulSet.Generation == generation) &&
		// and this .spec generation is being applied to replicas - it is observed right now
		(statefulSet.Status.ObservedGeneration == statefulSet.Generation) &&
		// and all replicas are of expected generation
		(statefulSet.Status.CurrentReplicas == *statefulSet.Spec.Replicas) &&
		// and all replicas are updated - meaning rolling update completed over all replicas
		(statefulSet.Status.UpdatedReplicas == *statefulSet.Spec.Replicas) &&
		// and current revision is an updated one - meaning rolling update completed over all replicas
		(statefulSet.Status.CurrentRevision == statefulSet.Status.UpdateRevision)
}

// IsStatefulSetReady returns whether StatefulSet is ready
func IsStatefulSetReady(statefulSet *apps.StatefulSet) bool {
	if statefulSet == nil {
		return false
	}

	if statefulSet.Spec.Replicas == nil {
		return false
	}
	// All replicas are in "Ready" status - meaning ready to be used - no failure inside
	return statefulSet.Status.ReadyReplicas == *statefulSet.Spec.Replicas
}

// IsStatefulSetNotReady returns whether StatefulSet is not ready
func IsStatefulSetNotReady(statefulSet *apps.StatefulSet) bool {
	if statefulSet == nil {
		return false
	}

	return !IsStatefulSetReady(statefulSet)
}

// StrStatefulSetStatus returns human-friendly string representation of StatefulSet status
func StrStatefulSetStatus(status *apps.StatefulSetStatus) string {
	return fmt.Sprintf(
		"ObservedGeneration:%d Replicas:%d ReadyReplicas:%d CurrentReplicas:%d UpdatedReplicas:%d CurrentRevision:%s UpdateRevision:%s",
		status.ObservedGeneration,
		status.Replicas,
		status.ReadyReplicas,
		status.CurrentReplicas,
		status.UpdatedReplicas,
		status.CurrentRevision,
		status.UpdateRevision,
	)
}

// ensureNamedPortsSpecified
func ensureNamedPortsSpecified(statefulSet *apps.StatefulSet, host *chiv1.ChiHost) {
	// Ensure ClickHouse container has all named ports specified
	container, ok := getClickHouseContainer(statefulSet)
	if !ok {
		return
	}
	ensurePortByName(container, chDefaultTCPPortName, host.TCPPort)
	ensurePortByName(container, chDefaultHTTPPortName, host.HTTPPort)
	ensurePortByName(container, chDefaultInterserverHTTPPortName, host.InterserverHTTPPort)
}

// ensurePortByName
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

// NewPodDisruptionBudget creates new PodDisruptionBudget
func (c *Creator) NewPodDisruptionBudget() *v1beta1.PodDisruptionBudget {
	ownerReferences := getOwnerReferences(c.chi)
	return &v1beta1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.chi.Name,
			Namespace:       c.chi.Namespace,
			Labels:          macro(c.chi).Map(c.labels.getCHIScope()),
			Annotations:     macro(c.chi).Map(c.annotations.getCHIScope()),
			OwnerReferences: ownerReferences,
		},
		Spec: v1beta1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: c.labels.GetSelectorCHIScope(),
			},
			MaxUnavailable: &intstr.IntOrString{
				Type:   intstr.Int,
				IntVal: 1,
			},
		},
	}
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
		c.a.V(1).F().Warning("Can not find volumeClaimTemplate %s. Volume claim can not be mounted", volumeClaimTemplateName)
		return nil
	}

	// 4. Specified container has to be available
	container := getContainerByName(statefulSet, containerName)
	if container == nil {
		c.a.V(1).F().Warning("Can not find container %s. Volume claim can not be mounted", containerName)
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
			c.a.V(1).F().Warning(
				"StatefulSet:%s container:%s volumeClaimTemplateName:%s already used",
				statefulSet.Name,
				container.Name,
				volumeMount.Name,
			)
			return nil
		}

		// 2. Check whether `mountPath` (say '/var/lib/clickhouse') is already mounted
		if volumeMount.MountPath == existingVolumeMount.MountPath {
			// `mountPath` (say /var/lib/clickhouse) is already mounted
			c.a.V(1).F().Warning(
				"StatefulSet:%s container:%s mountPath:%s already used",
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

	c.a.V(1).F().Info(
		"StatefulSet:%s container:%s mounted %s on %s",
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
			Labels:      macro(host).Map(c.labels.getHostScope(host, false)),
			Annotations: macro(host).Map(c.annotations.getHostScope(host)),
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
			Templates:           nil,
		},
	}
}

// newDefaultHostTemplateForHostNetwork
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
			Templates:           nil,
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

// newDefaultLivenessProbe
func newDefaultLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		Handler: corev1.Handler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/ping",
				Port: intstr.Parse(chDefaultHTTPPortName), // What if it is not a default?
			},
		},
		InitialDelaySeconds: 60,
		PeriodSeconds:       3,
		FailureThreshold:    10,
	}
}

// newDefaultReadinessProbe
func newDefaultReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		Handler: corev1.Handler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/ping",
				Port: intstr.Parse(chDefaultHTTPPortName), // What if it is not a default?
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       3,
	}
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
		LivenessProbe:  newDefaultLivenessProbe(),
		ReadinessProbe: newDefaultReadinessProbe(),
	}
}

// newDefaultLogContainer returns default Log Container
func newDefaultLogContainer() corev1.Container {
	return corev1.Container{
		Name:  ClickHouseLogContainerName,
		Image: defaultBusyBoxDockerImage,
		Command: []string{
			"/bin/sh", "-c", "--",
		},
		Args: []string{
			"while true; do sleep 30; done;",
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

func getOwnerReferences(chi *chiv1.ClickHouseInstallation) []metav1.OwnerReference {
	if chi.Attributes.SkipOwnerRef {
		return nil
	}
	controller := true
	block := true
	return []metav1.OwnerReference{
		{
			APIVersion:         chi.APIVersion,
			Kind:               chi.Kind,
			Name:               chi.Name,
			UID:                chi.UID,
			Controller:         &controller,
			BlockOwnerDeletion: &block,
		},
	}
}
