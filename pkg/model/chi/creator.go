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

package chi

import (
	"fmt"

	"github.com/gosimple/slug"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Creator specifies creator object
type Creator struct {
	chi                    *api.ClickHouseInstallation
	chConfigFilesGenerator *ClickHouseConfigFilesGenerator
	labels                 *Labeler
	annotations            *Annotator
	a                      log.Announcer
}

// NewCreator creates new Creator object
func NewCreator(chi *api.ClickHouseInstallation) *Creator {
	return &Creator{
		chi:                    chi,
		chConfigFilesGenerator: NewClickHouseConfigFilesGenerator(NewClickHouseConfigGenerator(chi), chop.Config()),
		labels:                 NewLabeler(chi),
		annotations:            NewAnnotator(chi),
		a:                      log.M(chi),
	}
}

// CreateServiceCHI creates new core.Service for specified CHI
func (c *Creator) CreateServiceCHI() *core.Service {
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
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            serviceName,
			Namespace:       c.chi.Namespace,
			Labels:          macro(c.chi).Map(c.labels.getServiceCHI(c.chi)),
			Annotations:     macro(c.chi).Map(c.annotations.getServiceCHI(c.chi)),
			OwnerReferences: ownerReferences,
		},
		Spec: core.ServiceSpec{
			// ClusterIP: templateDefaultsServiceClusterIP,
			Ports: []core.ServicePort{
				{
					Name:       chDefaultHTTPPortName,
					Protocol:   core.ProtocolTCP,
					Port:       chDefaultHTTPPortNumber,
					TargetPort: intstr.FromString(chDefaultHTTPPortName),
				},
				{
					Name:       chDefaultTCPPortName,
					Protocol:   core.ProtocolTCP,
					Port:       chDefaultTCPPortNumber,
					TargetPort: intstr.FromString(chDefaultTCPPortName),
				},
			},
			Selector:              c.labels.getSelectorCHIScopeReady(),
			Type:                  core.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: core.ServiceExternalTrafficPolicyTypeLocal,
		},
	}
	MakeObjectVersion(&svc.ObjectMeta, svc)
	return svc
}

// CreateServiceCluster creates new core.Service for specified Cluster
func (c *Creator) CreateServiceCluster(cluster *api.Cluster) *core.Service {
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

// CreateServiceShard creates new core.Service for specified Shard
func (c *Creator) CreateServiceShard(shard *api.ChiShard) *core.Service {
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

// CreateServiceHost creates new core.Service for specified host
func (c *Creator) CreateServiceHost(host *api.ChiHost) *core.Service {
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
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            serviceName,
			Namespace:       host.Address.Namespace,
			Labels:          macro(host).Map(c.labels.getServiceHost(host)),
			Annotations:     macro(host).Map(c.annotations.getServiceHost(host)),
			OwnerReferences: ownerReferences,
		},
		Spec: core.ServiceSpec{
			Selector:                 GetSelectorHostScope(host),
			ClusterIP:                templateDefaultsServiceClusterIP,
			Type:                     "ClusterIP",
			PublishNotReadyAddresses: true,
		},
	}
	appendServicePorts(svc, host)
	MakeObjectVersion(&svc.ObjectMeta, svc)
	return svc
}

func appendServicePorts(service *core.Service, host *api.ChiHost) {
	if api.IsPortAssigned(host.TCPPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       chDefaultTCPPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.TCPPort,
				TargetPort: intstr.FromInt(int(host.TCPPort)),
			},
		)
	}
	if api.IsPortAssigned(host.TLSPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       chDefaultTLSPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.TLSPort,
				TargetPort: intstr.FromInt(int(host.TLSPort)),
			},
		)
	}
	if api.IsPortAssigned(host.HTTPPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       chDefaultHTTPPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.HTTPPort,
				TargetPort: intstr.FromInt(int(host.HTTPPort)),
			},
		)
	}
	if api.IsPortAssigned(host.HTTPSPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       chDefaultHTTPSPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.HTTPSPort,
				TargetPort: intstr.FromInt(int(host.HTTPSPort)),
			},
		)
	}
	if api.IsPortAssigned(host.InterserverHTTPPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       chDefaultInterserverHTTPPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.InterserverHTTPPort,
				TargetPort: intstr.FromInt(int(host.InterserverHTTPPort)),
			},
		)
	}
}

// verifyServiceTemplatePorts verifies ChiServiceTemplate to have reasonable ports specified
func (c *Creator) verifyServiceTemplatePorts(template *api.ChiServiceTemplate) error {
	for i := range template.Spec.Ports {
		servicePort := &template.Spec.Ports[i]
		if api.IsPortInvalid(servicePort.Port) {
			msg := fmt.Sprintf("template:%s INCORRECT PORT:%d", template.Name, servicePort.Port)
			c.a.V(1).F().Warning(msg)
			return fmt.Errorf(msg)
		}
	}
	return nil
}

// createServiceFromTemplate create Service from ChiServiceTemplate and additional info
func (c *Creator) createServiceFromTemplate(
	template *api.ChiServiceTemplate,
	namespace string,
	name string,
	labels map[string]string,
	annotations map[string]string,
	selector map[string]string,
	ownerReferences []meta.OwnerReference,
	macro *macrosEngine,
) *core.Service {

	// Verify Ports
	if err := c.verifyServiceTemplatePorts(template); err != nil {
		return nil
	}

	// Create Service
	service := &core.Service{
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
	MakeObjectVersion(&service.ObjectMeta, service)

	return service
}

// CreateConfigMapCHICommon creates new core.ConfigMap
func (c *Creator) CreateConfigMapCHICommon(options *ClickHouseConfigFilesGeneratorOptions) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
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
	MakeObjectVersion(&cm.ObjectMeta, cm)
	return cm
}

// CreateConfigMapCHICommonUsers creates new core.ConfigMap
func (c *Creator) CreateConfigMapCHICommonUsers() *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
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
	MakeObjectVersion(&cm.ObjectMeta, cm)
	return cm
}

// createConfigMapHost creates new core.ConfigMap
func (c *Creator) createConfigMapHost(host *api.ChiHost, name string, data map[string]string) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            name,
			Namespace:       host.Address.Namespace,
			Labels:          macro(host).Map(c.labels.getConfigMapHost(host)),
			Annotations:     macro(host).Map(c.annotations.getConfigMapHost(host)),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		Data: data,
	}
	// And after the object is ready we can put version label
	MakeObjectVersion(&cm.ObjectMeta, cm)
	return cm
}

// CreateConfigMapHost creates new core.ConfigMap
func (c *Creator) CreateConfigMapHost(host *api.ChiHost) *core.ConfigMap {
	return c.createConfigMapHost(host, CreateConfigMapHostName(host), c.chConfigFilesGenerator.CreateConfigFilesGroupHost(host))
}

// CreateConfigMapHostMigration creates new core.ConfigMap
//func (c *Creator) CreateConfigMapHostMigration(host *api.ChiHost, data map[string]string) *core.ConfigMap {
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
func (c *Creator) CreateStatefulSet(host *api.ChiHost, shutdown bool) *apps.StatefulSet {
	statefulSet := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Name:            CreateStatefulSetName(host),
			Namespace:       host.Address.Namespace,
			Labels:          macro(host).Map(c.labels.getHostScope(host, true)),
			Annotations:     macro(host).Map(c.annotations.getHostScope(host)),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    host.GetStatefulSetReplicasNum(shutdown),
			ServiceName: CreateStatefulSetServiceName(host),
			Selector: &meta.LabelSelector{
				MatchLabels: GetSelectorHostScope(host),
			},

			// IMPORTANT
			// Template is to be setup later
			Template: core.PodTemplateSpec{},

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
	MakeObjectVersion(&statefulSet.ObjectMeta, statefulSet)

	return statefulSet
}

// PreparePersistentVolume prepares PV labels
func (c *Creator) PreparePersistentVolume(pv *core.PersistentVolume, host *api.ChiHost) *core.PersistentVolume {
	pv.Labels = macro(host).Map(c.labels.getPV(pv, host))
	pv.Annotations = macro(host).Map(c.annotations.getPV(pv, host))
	// And after the object is ready we can put version label
	MakeObjectVersion(&pv.ObjectMeta, pv)
	return pv
}

// PreparePersistentVolumeClaim prepares PVC - labels and annotations
func (c *Creator) PreparePersistentVolumeClaim(
	pvc *core.PersistentVolumeClaim,
	host *api.ChiHost,
	template *api.ChiVolumeClaimTemplate,
) *core.PersistentVolumeClaim {
	pvc.Labels = macro(host).Map(c.labels.getPVC(pvc, host, template))
	pvc.Annotations = macro(host).Map(c.annotations.getPVC(pvc, host, template))
	// And after the object is ready we can put version label
	MakeObjectVersion(&pvc.ObjectMeta, pvc)
	return pvc
}

// setupStatefulSetPodTemplate performs PodTemplate setup of StatefulSet
func (c *Creator) setupStatefulSetPodTemplate(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Process Pod Template
	podTemplate := c.getPodTemplate(host)
	c.statefulSetApplyPodTemplate(statefulSet, podTemplate, host)

	// Post-process StatefulSet
	ensureStatefulSetTemplateIntegrity(statefulSet, host)
	setupEnvVars(statefulSet, host)
	c.personalizeStatefulSetTemplate(statefulSet, host)
}

// ensureStatefulSetTemplateIntegrity
func ensureStatefulSetTemplateIntegrity(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	ensureClickHouseContainerSpecified(statefulSet, host)
	ensureProbesSpecified(statefulSet, host)
	ensureNamedPortsSpecified(statefulSet, host)
}

// setupEnvVars setup ENV vars for clickhouse container
func setupEnvVars(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	container, ok := getClickHouseContainer(statefulSet)
	if !ok {
		return
	}

	container.Env = append(container.Env, host.GetCHI().Attributes.AdditionalEnvVars...)
}

// ensureClickHouseContainerSpecified
func ensureClickHouseContainerSpecified(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	_, ok := getClickHouseContainer(statefulSet)
	if ok {
		return
	}

	// No ClickHouse container available, let's add one
	addContainer(
		&statefulSet.Spec.Template.Spec,
		newDefaultClickHouseContainer(host),
	)
}

// ensureClickHouseLogContainerSpecified
func ensureClickHouseLogContainerSpecified(statefulSet *apps.StatefulSet) {
	_, ok := getClickHouseLogContainer(statefulSet)
	if ok {
		return
	}

	// No ClickHouse Log container available, let's add one

	addContainer(
		&statefulSet.Spec.Template.Spec,
		newDefaultLogContainer(),
	)
}

// ensureProbesSpecified
func ensureProbesSpecified(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	container, ok := getClickHouseContainer(statefulSet)
	if !ok {
		return
	}
	if container.LivenessProbe == nil {
		container.LivenessProbe = newDefaultLivenessProbe(host)
	}
	if container.ReadinessProbe == nil {
		container.ReadinessProbe = newDefaultReadinessProbe(host)
	}
}

// personalizeStatefulSetTemplate
func (c *Creator) personalizeStatefulSetTemplate(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Ensure pod created by this StatefulSet has alias 127.0.0.1
	statefulSet.Spec.Template.Spec.HostAliases = []core.HostAlias{
		{
			IP:        "127.0.0.1",
			Hostnames: []string{CreatePodHostname(host)},
		},
	}

	// Setup volumes
	c.statefulSetSetupVolumes(statefulSet, host)
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
	// Appended `sleep` command makes Pod unable to respond to probes, and probes would cause unexpected restart.
	// Thus we need to disable all probes in troubleshooting mode.
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
}

// setupLogContainer
func (c *Creator) setupLogContainer(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	statefulSetName := CreateStatefulSetName(host)
	// In case we have default LogVolumeClaimTemplate specified - need to append log container to Pod Template
	if host.Templates.HasLogVolumeClaimTemplate() {
		ensureClickHouseLogContainerSpecified(statefulSet)

		c.a.V(1).F().Info("add log container for statefulSet %s", statefulSetName)
	}
}

// getPodTemplate gets Pod Template to be used to create StatefulSet
func (c *Creator) getPodTemplate(host *api.ChiHost) *api.ChiPodTemplate {
	statefulSetName := CreateStatefulSetName(host)

	// Which pod template would be used - either explicitly defined in or a default one
	podTemplate, ok := host.GetPodTemplate()
	if ok {
		// Host references known PodTemplate
		// Make local copy of this PodTemplate, in order not to spoil the original common-used template
		podTemplate = podTemplate.DeepCopy()
		c.a.V(1).F().Info("statefulSet %s use custom template: %s", statefulSetName, podTemplate.Name)
	} else {
		// Host references UNKNOWN PodTemplate, will use default one
		podTemplate = newDefaultPodTemplate(statefulSetName, host)
		c.a.V(1).F().Info("statefulSet %s use default generated template", statefulSetName)
	}

	// Here we have local copy of Pod Template, to be used to create StatefulSet
	// Now we can customize this Pod Template for particular host

	prepareAffinity(podTemplate, host)

	return podTemplate
}

// statefulSetSetupVolumes setup all volumes
func (c *Creator) statefulSetSetupVolumes(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	c.statefulSetSetupVolumesForConfigMaps(statefulSet, host)
	c.statefulSetSetupVolumesForSecrets(statefulSet, host)
}

// statefulSetSetupVolumesForConfigMaps adds to each container in the Pod VolumeMount objects
func (c *Creator) statefulSetSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	configMapHostName := CreateConfigMapHostName(host)
	configMapCommonName := CreateConfigMapCommonName(c.chi)
	configMapCommonUsersName := CreateConfigMapCommonUsersName(c.chi)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	c.statefulSetAppendVolumes(
		statefulSet,
		newVolumeForConfigMap(configMapCommonName),
		newVolumeForConfigMap(configMapCommonUsersName),
		newVolumeForConfigMap(configMapHostName),
		//newVolumeForConfigMap(configMapHostMigrationName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		c.containerAppendVolumeMounts(
			container,
			newVolumeMount(configMapCommonName, dirPathCommonConfig),
			newVolumeMount(configMapCommonUsersName, dirPathUsersConfig),
			newVolumeMount(configMapHostName, dirPathHostConfig),
		)
	}
}

// statefulSetSetupVolumesForSecrets adds to each container in the Pod VolumeMount objects
func (c *Creator) statefulSetSetupVolumesForSecrets(statefulSet *apps.StatefulSet, host *api.ChiHost) {

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	c.statefulSetAppendVolumes(
		statefulSet,
		host.GetCHI().Attributes.AdditionalVolumes...,
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have Secrets mounted as Volumes
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		c.containerAppendVolumeMounts(
			container,
			host.GetCHI().Attributes.AdditionalVolumeMounts...,
		)
	}
}

// statefulSetAppendUsedPVCTemplates appends all PVC templates which are used (referenced by name) by containers
// to the StatefulSet.Spec.VolumeClaimTemplates list
func (c *Creator) statefulSetAppendUsedPVCTemplates(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// VolumeClaimTemplates, that are directly referenced in containers' VolumeMount object(s)
	// are appended to StatefulSet's Spec.VolumeClaimTemplates slice
	//
	// Deal with `volumeMounts` of a `container`, located by the path:
	// .spec.templates.podTemplates.*.spec.containers.volumeMounts.*
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			if volumeClaimTemplate, ok := c.getVolumeClaimTemplate(&container.VolumeMounts[j]); ok {
				c.statefulSetAppendPVCTemplate(statefulSet, host, volumeClaimTemplate)
			}
		}
	}
}

// statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates
// appends VolumeMounts for Data and Log VolumeClaimTemplates on all containers.
// Creates VolumeMounts for Data and Log volumes in case these volume templates are specified in `templates`.
func (c *Creator) statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Mount all named (data and log so far) VolumeClaimTemplates into all containers
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		c.containerAppendVolumeMounts(
			container,
			newVolumeMount(host.Templates.GetDataVolumeClaimTemplate(), dirPathClickHouseData),
		)
		c.containerAppendVolumeMounts(
			container,
			newVolumeMount(host.Templates.GetLogVolumeClaimTemplate(), dirPathClickHouseLog),
		)
	}
}

// setupStatefulSetVolumeClaimTemplates performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (c *Creator) setupStatefulSetVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	c.statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates(statefulSet, host)
	c.statefulSetAppendUsedPVCTemplates(statefulSet, host)
}

// statefulSetApplyPodTemplate fills StatefulSet.Spec.Template with data from provided ChiPodTemplate
func (c *Creator) statefulSetApplyPodTemplate(
	statefulSet *apps.StatefulSet,
	template *api.ChiPodTemplate,
	host *api.ChiHost,
) {
	// StatefulSet's pod template is not directly compatible with ChiPodTemplate,
	// we need to extract some fields from ChiPodTemplate and apply on StatefulSet
	statefulSet.Spec.Template = core.PodTemplateSpec{
		ObjectMeta: meta.ObjectMeta{
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

// getContainer gets container from the StatefulSet either by name or by index
func getContainer(statefulSet *apps.StatefulSet, name string, index int) (*core.Container, bool) {
	if len(name) > 0 {
		// Find by name
		for i := range statefulSet.Spec.Template.Spec.Containers {
			container := &statefulSet.Spec.Template.Spec.Containers[i]
			if container.Name == name {
				return container, true
			}
		}
	}

	if index >= 0 {
		// Find by index
		if len(statefulSet.Spec.Template.Spec.Containers) > index {
			return &statefulSet.Spec.Template.Spec.Containers[index], true
		}
	}

	return nil, false
}

// getClickHouseContainer
func getClickHouseContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return getContainer(statefulSet, clickHouseContainerName, 0)
}

// getClickHouseLogContainer
func getClickHouseLogContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return getContainer(statefulSet, clickHouseLogContainerName, -1)
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
func ensureNamedPortsSpecified(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Ensure ClickHouse container has all named ports specified
	container, ok := getClickHouseContainer(statefulSet)
	if !ok {
		return
	}
	ensurePortByName(container, chDefaultTCPPortName, host.TCPPort)
	ensurePortByName(container, chDefaultTLSPortName, host.TLSPort)
	ensurePortByName(container, chDefaultHTTPPortName, host.HTTPPort)
	ensurePortByName(container, chDefaultHTTPSPortName, host.HTTPSPort)
	ensurePortByName(container, chDefaultInterserverHTTPPortName, host.InterserverHTTPPort)
}

// ensurePortByName
func ensurePortByName(container *core.Container, name string, port int32) {
	if api.IsPortUnassigned(port) {
		return
	}

	// Find port with specified name
	for i := range container.Ports {
		containerPort := &container.Ports[i]
		if containerPort.Name == name {
			// Assign value to existing port
			containerPort.HostPort = 0
			containerPort.ContainerPort = port
			return
		}
	}

	// Port with specified name not found. Need to append
	container.Ports = append(container.Ports, core.ContainerPort{
		Name:          name,
		ContainerPort: port,
	})
}

// NewPodDisruptionBudget creates new PodDisruptionBudget
func (c *Creator) NewPodDisruptionBudget(cluster *api.Cluster) *policy.PodDisruptionBudget {
	ownerReferences := getOwnerReferences(c.chi)
	return &policy.PodDisruptionBudget{
		ObjectMeta: meta.ObjectMeta{
			Name:            fmt.Sprintf("%s-%s", cluster.Address.CHIName, cluster.Address.ClusterName),
			Namespace:       c.chi.Namespace,
			Labels:          macro(c.chi).Map(c.labels.getClusterScope(cluster)),
			Annotations:     macro(c.chi).Map(c.annotations.getClusterScope(cluster)),
			OwnerReferences: ownerReferences,
		},
		Spec: policy.PodDisruptionBudgetSpec{
			Selector: &meta.LabelSelector{
				MatchLabels: getSelectorClusterScope(cluster),
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
	host *api.ChiHost,
	statefulSet *apps.StatefulSet,
	containerName string,
	volumeMount core.VolumeMount,
) error {
	//
	// Sanity checks
	//

	// Specified (referenced from volumeMount) VolumeClaimTemplate has to be available as well
	if _, ok := c.getVolumeClaimTemplate(&volumeMount); !ok {
		// Incorrect/unknown .templates.VolumeClaimTemplate specified
		c.a.V(1).F().Warning("Can not find VolumeClaimTemplate for VolumeMount: %s. Volume claim can not be mounted", volumeMount.Name)
		return nil
	}

	// Specified container has to be available
	container := getContainerByName(statefulSet, containerName)
	if container == nil {
		c.a.V(1).F().Warning("Can not find container: %s. Volume claim can not be mounted", containerName)
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
				"StatefulSet:%s container:%s volumeClaimTemplateName:%s already used. Skip it and all the rest.",
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
				"StatefulSet:%s container:%s mountPath:%s already used. Skip it and all the rest.",
				statefulSet.Name,
				container.Name,
				volumeMount.MountPath,
			)
			return nil
		}
	}

	// This VolumeClaimTemplate is not used explicitly by name and `mountPath` (say /var/lib/clickhouse) is not used also.
	// Let's mount this VolumeClaimTemplate into `mountPath` (say '/var/lib/clickhouse') of a container
	if volumeClaimTemplate, ok := c.getVolumeClaimTemplate(&volumeMount); ok {
		// Add VolumeClaimTemplate to StatefulSet
		c.statefulSetAppendPVCTemplate(statefulSet, host, volumeClaimTemplate)
		// Add VolumeMount to ClickHouse container to `mountPath` point
		c.containerAppendVolumeMounts(
			container,
			volumeMount,
		)
	}

	c.a.V(1).F().Info(
		"StatefulSet: %s container: %s mounted VolumeMount: %s onto path: %s",
		statefulSet.Name,
		container.Name,
		volumeMount.Name,
		volumeMount.MountPath,
	)

	return nil
}

// statefulSetAppendVolumes appends multiple Volume(s) to the specified StatefulSet
func (c *Creator) statefulSetAppendVolumes(statefulSet *apps.StatefulSet, volumes ...core.Volume) {
	statefulSet.Spec.Template.Spec.Volumes = append(
		statefulSet.Spec.Template.Spec.Volumes,
		volumes...,
	)
}

// containerAppendVolumeMounts appends multiple VolumeMount(s) to the specified container
func (c *Creator) containerAppendVolumeMounts(container *core.Container, volumeMounts ...core.VolumeMount) {
	for _, volumeMount := range volumeMounts {
		c.containerAppendVolumeMount(container, volumeMount)
	}
}

// containerAppendVolumeMount appends one VolumeMount to the specified container
func (c *Creator) containerAppendVolumeMount(container *core.Container, volumeMount core.VolumeMount) {
	//
	// Sanity checks
	//

	if container == nil {
		return
	}

	// VolumeMount has to have reasonable data - Name and MountPath
	if (volumeMount.Name == "") || (volumeMount.MountPath == "") {
		return
	}

	// Check that:
	// 1. Mountable item (VolumeClaimTemplate or Volume) specified in this VolumeMount is NOT already mounted
	//    in this container by any other VolumeMount (to avoid double-mount of a mountable item)
	// 2. And specified `mountPath` (say '/var/lib/clickhouse') is NOT already mounted in this container
	//    by any VolumeMount (to avoid double-mount/rewrite into single `mountPath`)
	for i := range container.VolumeMounts {
		// Convenience wrapper
		existingVolumeMount := &container.VolumeMounts[i]

		// 1. Check whether this mountable item is already listed in VolumeMount of this container
		if volumeMount.Name == existingVolumeMount.Name {
			// This .templates.VolumeClaimTemplate is already used in VolumeMount
			c.a.V(1).F().Warning(
				"container.Name:%s volumeMount.Name:%s already used",
				container.Name,
				volumeMount.Name,
			)
			return
		}

		// 2. Check whether `mountPath` (say '/var/lib/clickhouse') is already mounted
		if volumeMount.MountPath == existingVolumeMount.MountPath {
			// `mountPath` (say /var/lib/clickhouse) is already mounted
			c.a.V(1).F().Warning(
				"container.Name:%s volumeMount.MountPath:%s already used",
				container.Name,
				volumeMount.MountPath,
			)
			return
		}
	}

	// Add VolumeMount to ClickHouse container to `mountPath` point
	container.VolumeMounts = append(
		container.VolumeMounts,
		volumeMount,
	)

	c.a.V(2).F().Info(
		"container:%s volumeMount added: %s on %s",
		container.Name,
		volumeMount.Name,
		volumeMount.MountPath,
	)

	return
}

// createPVC
func (c *Creator) createPVC(
	name string,
	namespace string,
	host *api.ChiHost,
	spec *core.PersistentVolumeClaimSpec,
) core.PersistentVolumeClaim {
	persistentVolumeClaim := core.PersistentVolumeClaim{
		TypeMeta: meta.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			// TODO
			//  this has to wait until proper disk inheritance procedure will be available
			// UPDATE
			//  we are close to proper disk inheritance
			// Right now we hit the following error:
			// "Forbidden: updates to statefulset spec for fields other than 'replicas', 'template', and 'updateStrategy' are forbidden"
			Labels:      macro(host).Map(c.labels.getHostScope(host, false)),
			Annotations: macro(host).Map(c.annotations.getHostScope(host)),
		},
		// Append copy of PersistentVolumeClaimSpec
		Spec: *spec.DeepCopy(),
	}
	// TODO introduce normalization
	// Overwrite .Spec.VolumeMode
	volumeMode := core.PersistentVolumeFilesystem
	persistentVolumeClaim.Spec.VolumeMode = &volumeMode

	return persistentVolumeClaim
}

// CreatePVC creates PVC
func (c *Creator) CreatePVC(name string, host *api.ChiHost, spec *core.PersistentVolumeClaimSpec) *core.PersistentVolumeClaim {
	pvc := c.createPVC(name, host.Address.Namespace, host, spec)
	return &pvc
}

// statefulSetAppendPVCTemplate appends to StatefulSet.Spec.VolumeClaimTemplates new entry with data from provided 'src' ChiVolumeClaimTemplate
func (c *Creator) statefulSetAppendPVCTemplate(
	statefulSet *apps.StatefulSet,
	host *api.ChiHost,
	volumeClaimTemplate *api.ChiVolumeClaimTemplate,
) {
	// Since we have the same names for PVs produced from both VolumeClaimTemplates and Volumes,
	// we need to check naming for all of them

	// Check whether provided VolumeClaimTemplate is already listed in statefulSet.Spec.VolumeClaimTemplates
	for i := range statefulSet.Spec.VolumeClaimTemplates {
		// Convenience wrapper
		_volumeClaimTemplate := &statefulSet.Spec.VolumeClaimTemplates[i]
		if _volumeClaimTemplate.Name == volumeClaimTemplate.Name {
			// This VolumeClaimTemplate is already listed in statefulSet.Spec.VolumeClaimTemplates
			// No need to add it second time
			return
		}
	}

	// Check whether provided VolumeClaimTemplate is already listed in statefulSet.Spec.Template.Spec.Volumes
	for i := range statefulSet.Spec.Template.Spec.Volumes {
		// Convenience wrapper
		_volume := &statefulSet.Spec.Template.Spec.Volumes[i]
		if _volume.Name == volumeClaimTemplate.Name {
			// This VolumeClaimTemplate is already listed in statefulSet.Spec.Template.Spec.Volumes
			// No need to add it second time
			return
		}
	}

	// Provided VolumeClaimTemplate is not listed neither in
	// statefulSet.Spec.Template.Spec.Volumes
	// nor in
	// statefulSet.Spec.VolumeClaimTemplates
	// so, let's add it

	if c.OperatorShouldCreatePVC(host, volumeClaimTemplate) {
		claimName := CreatePVCNameByVolumeClaimTemplate(host, volumeClaimTemplate)
		statefulSet.Spec.Template.Spec.Volumes = append(
			statefulSet.Spec.Template.Spec.Volumes,
			newVolumeForPVC(volumeClaimTemplate.Name, claimName),
		)
	} else {
		statefulSet.Spec.VolumeClaimTemplates = append(
			statefulSet.Spec.VolumeClaimTemplates,
			// For templates we should not specify namespace where PVC would be located
			c.createPVC(volumeClaimTemplate.Name, "", host, &volumeClaimTemplate.Spec),
		)
	}
}

// OperatorShouldCreatePVC checks whether operator should create PVC for specified volumeCLimaTemplate
func (c *Creator) OperatorShouldCreatePVC(host *api.ChiHost, volumeClaimTemplate *api.ChiVolumeClaimTemplate) bool {
	return getPVCProvisioner(host, volumeClaimTemplate) == api.PVCProvisionerOperator
}

// CreateClusterSecret creates cluster secret
func (c *Creator) CreateClusterSecret(name string) *core.Secret {
	return &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Namespace: c.chi.Namespace,
			Name:      name,
		},
		StringData: map[string]string{
			"secret": util.RandStringRange(10, 20),
		},
		Type: core.SecretTypeOpaque,
	}
}

// newDefaultHostTemplate returns default Host Template to be used with StatefulSet
func newDefaultHostTemplate(name string) *api.ChiHostTemplate {
	return &api.ChiHostTemplate{
		Name: name,
		PortDistribution: []api.ChiPortDistribution{
			{
				Type: api.PortDistributionUnspecified,
			},
		},
		Spec: api.ChiHost{
			Name:                "",
			TCPPort:             api.PortUnassigned(),
			TLSPort:             api.PortUnassigned(),
			HTTPPort:            api.PortUnassigned(),
			HTTPSPort:           api.PortUnassigned(),
			InterserverHTTPPort: api.PortUnassigned(),
			Templates:           nil,
		},
	}
}

// newDefaultHostTemplateForHostNetwork
func newDefaultHostTemplateForHostNetwork(name string) *api.ChiHostTemplate {
	return &api.ChiHostTemplate{
		Name: name,
		PortDistribution: []api.ChiPortDistribution{
			{
				Type: api.PortDistributionClusterScopeIndex,
			},
		},
		Spec: api.ChiHost{
			Name:                "",
			TCPPort:             api.PortUnassigned(),
			TLSPort:             api.PortUnassigned(),
			HTTPPort:            api.PortUnassigned(),
			HTTPSPort:           api.PortUnassigned(),
			InterserverHTTPPort: api.PortUnassigned(),
			Templates:           nil,
		},
	}
}

// newDefaultPodTemplate returns default Pod Template to be used with StatefulSet
func newDefaultPodTemplate(name string, host *api.ChiHost) *api.ChiPodTemplate {
	podTemplate := &api.ChiPodTemplate{
		Name: name,
		Spec: core.PodSpec{
			Containers: []core.Container{},
			Volumes:    []core.Volume{},
		},
	}

	addContainer(&podTemplate.Spec, newDefaultClickHouseContainer(host))

	return podTemplate
}

// newDefaultLivenessProbe returns default liveness probe
func newDefaultLivenessProbe(host *api.ChiHost) *core.Probe {
	// Introduce http probe in case http port is specified
	if api.IsPortAssigned(host.HTTPPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path: "/ping",
					Port: intstr.Parse(chDefaultHTTPPortName), // What if it is not a default?
				},
			},
			InitialDelaySeconds: 60,
			PeriodSeconds:       3,
			FailureThreshold:    10,
		}
	}

	// Introduce https probe in case https port is specified
	if api.IsPortAssigned(host.HTTPSPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path:   "/ping",
					Port:   intstr.Parse(chDefaultHTTPSPortName), // What if it is not a default?
					Scheme: core.URISchemeHTTPS,
				},
			},
			InitialDelaySeconds: 60,
			PeriodSeconds:       3,
			FailureThreshold:    10,
		}
	}

	// Probe is not available
	return nil
}

// newDefaultReadinessProbe returns default readiness probe
func newDefaultReadinessProbe(host *api.ChiHost) *core.Probe {
	// Introduce http probe in case http port is specified
	if api.IsPortAssigned(host.HTTPPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path: "/ping",
					Port: intstr.Parse(chDefaultHTTPPortName), // What if port name is not a default?
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       3,
		}
	}

	// Introduce https probe in case https port is specified
	if api.IsPortAssigned(host.HTTPSPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path:   "/ping",
					Port:   intstr.Parse(chDefaultHTTPSPortName), // What if port name is not a default?
					Scheme: core.URISchemeHTTPS,
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       3,
		}
	}

	// Probe is not available
	return nil
}

func appendContainerPorts(container *core.Container, host *api.ChiHost) {
	if api.IsPortAssigned(host.TCPPort) {
		container.Ports = append(container.Ports,
			core.ContainerPort{
				Name:          chDefaultTCPPortName,
				ContainerPort: host.TCPPort,
				Protocol:      core.ProtocolTCP,
			},
		)
	}
	if api.IsPortAssigned(host.TLSPort) {
		container.Ports = append(container.Ports,
			core.ContainerPort{
				Name:          chDefaultTLSPortName,
				ContainerPort: host.TLSPort,
				Protocol:      core.ProtocolTCP,
			},
		)
	}
	if api.IsPortAssigned(host.HTTPPort) {
		container.Ports = append(container.Ports,
			core.ContainerPort{
				Name:          chDefaultHTTPPortName,
				ContainerPort: host.HTTPPort,
				Protocol:      core.ProtocolTCP,
			},
		)
	}
	if api.IsPortAssigned(host.HTTPSPort) {
		container.Ports = append(container.Ports,
			core.ContainerPort{
				Name:          chDefaultHTTPSPortName,
				ContainerPort: host.HTTPSPort,
				Protocol:      core.ProtocolTCP,
			},
		)
	}
	if api.IsPortAssigned(host.InterserverHTTPPort) {
		container.Ports = append(container.Ports,
			core.ContainerPort{
				Name:          chDefaultInterserverHTTPPortName,
				ContainerPort: host.InterserverHTTPPort,
				Protocol:      core.ProtocolTCP,
			},
		)
	}
}

// newDefaultClickHouseContainer returns default ClickHouse Container
func newDefaultClickHouseContainer(host *api.ChiHost) core.Container {
	container := core.Container{
		Name:           clickHouseContainerName,
		Image:          defaultClickHouseDockerImage,
		LivenessProbe:  newDefaultLivenessProbe(host),
		ReadinessProbe: newDefaultReadinessProbe(host),
	}
	appendContainerPorts(&container, host)
	return container
}

// newDefaultLogContainer returns default Log Container
func newDefaultLogContainer() core.Container {
	return core.Container{
		Name:  clickHouseLogContainerName,
		Image: defaultUbiDockerImage,
		Command: []string{
			"/bin/sh", "-c", "--",
		},
		Args: []string{
			"while true; do sleep 30; done;",
		},
	}
}

// addContainer adds container to ChiPodTemplate
func addContainer(podSpec *core.PodSpec, container core.Container) {
	podSpec.Containers = append(podSpec.Containers, container)
}

// newVolumeForPVC returns core.Volume object with defined name
func newVolumeForPVC(name, claimName string) core.Volume {
	return core.Volume{
		Name: name,
		VolumeSource: core.VolumeSource{
			PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
				ClaimName: claimName,
				ReadOnly:  false,
			},
		},
	}
}

// newVolumeForConfigMap returns core.Volume object with defined name
func newVolumeForConfigMap(name string) core.Volume {
	var defaultMode int32 = 0644
	return core.Volume{
		Name: name,
		VolumeSource: core.VolumeSource{
			ConfigMap: &core.ConfigMapVolumeSource{
				LocalObjectReference: core.LocalObjectReference{
					Name: name,
				},
				DefaultMode: &defaultMode,
			},
		},
	}
}

// newVolumeMount returns core.VolumeMount object with name and mount path
func newVolumeMount(name, mountPath string) core.VolumeMount {
	return core.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}

// getContainerByName finds Container with specified name among all containers of Pod Template in StatefulSet
func getContainerByName(statefulSet *apps.StatefulSet, name string) *core.Container {
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		if container.Name == name {
			return container
		}
	}

	return nil
}

func getOwnerReferences(chi *api.ClickHouseInstallation) []meta.OwnerReference {
	if chi.Attributes.SkipOwnerRef {
		return nil
	}
	controller := true
	block := true
	return []meta.OwnerReference{
		{
			APIVersion:         api.SchemeGroupVersion.String(),
			Kind:               api.ClickHouseInstallationCRDResourceKind,
			Name:               chi.Name,
			UID:                chi.UID,
			Controller:         &controller,
			BlockOwnerDeletion: &block,
		},
	}
}
