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

package creator

import (
	"github.com/altinity/clickhouse-operator/pkg/model/chi/volume"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// CreateStatefulSet creates new apps.StatefulSet
func (c *Creator) CreateStatefulSet(host *api.Host, shutdown bool) *apps.StatefulSet {
	statefulSet := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.CreateStatefulSetName(host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          namer.Macro(host).Map(c.tagger.Label(tags.LabelSTS, host)),
			Annotations:     namer.Macro(host).Map(c.tagger.Annotate(tags.AnnotateSTS, host)),
			OwnerReferences: createOwnerReferences(c.chi),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    host.GetStatefulSetReplicasNum(shutdown),
			ServiceName: namer.CreateStatefulSetServiceName(host),
			Selector: &meta.LabelSelector{
				MatchLabels: c.tagger.Selector(tags.SelectorHostScope, host),
			},

			// IMPORTANT
			// Template is to be setup later
			// VolumeClaimTemplates are to be setup later
			Template:             core.PodTemplateSpec{},
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
	tags.MakeObjectVersion(statefulSet.GetObjectMeta(), statefulSet)

	return statefulSet
}

// setupStatefulSetPodTemplate performs PodTemplate setup of StatefulSet
func (c *Creator) setupStatefulSetPodTemplate(statefulSet *apps.StatefulSet, host *api.Host) {
	// Process Pod Template
	podTemplate := c.getPodTemplate(host)
	c.statefulSetApplyPodTemplate(statefulSet, podTemplate, host)

	// Post-process StatefulSet
	ensureStatefulSetTemplateIntegrity(statefulSet, host)
	setupEnvVars(statefulSet, host)
	c.personalizeStatefulSetTemplate(statefulSet, host)
}

// ensureStatefulSetTemplateIntegrity
func ensureStatefulSetTemplateIntegrity(statefulSet *apps.StatefulSet, host *api.Host) {
	ensureMainContainerSpecified(statefulSet, host)
	ensureProbesSpecified(statefulSet, host)
	ensureNamedPortsSpecified(statefulSet, host)
}

// setupEnvVars setup ENV vars for clickhouse container
func setupEnvVars(statefulSet *apps.StatefulSet, host *api.Host) {
	container, ok := getMainContainer(statefulSet)
	if !ok {
		return
	}

	container.Env = append(container.Env, host.GetCR().GetRuntime().GetAttributes().GetAdditionalEnvVars()...)
}

// ensureMainContainerSpecified is a unification wrapper
func ensureMainContainerSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	ensureClickHouseContainerSpecified(statefulSet, host)
}

// ensureLogContainerSpecified is a unification wrapper
func ensureLogContainerSpecified(statefulSet *apps.StatefulSet) {
	ensureClickHouseLogContainerSpecified(statefulSet)
}

// ensureClickHouseContainerSpecified
func ensureClickHouseContainerSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	_, ok := getClickHouseContainer(statefulSet)
	if ok {
		return
	}

	// No ClickHouse container available, let's add one
	k8s.PodSpecAddContainer(
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

	k8s.PodSpecAddContainer(
		&statefulSet.Spec.Template.Spec,
		newDefaultLogContainer(),
	)
}

// ensureProbesSpecified
func ensureProbesSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	container, ok := getMainContainer(statefulSet)
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
func (c *Creator) personalizeStatefulSetTemplate(statefulSet *apps.StatefulSet, host *api.Host) {
	// Ensure pod created by this StatefulSet has alias 127.0.0.1
	statefulSet.Spec.Template.Spec.HostAliases = []core.HostAlias{
		{
			IP: "127.0.0.1",
			Hostnames: []string{
				namer.CreatePodHostname(host),
			},
		},
	}

	// Setup volumes
	c.statefulSetSetupVolumes(statefulSet, host)
	// Setup statefulSet according to troubleshoot mode (if any)
	c.setupTroubleshootingMode(statefulSet, host)
	// Setup dedicated log container
	c.setupLogContainer(statefulSet, host)
}

// setupTroubleshootingMode
func (c *Creator) setupTroubleshootingMode(statefulSet *apps.StatefulSet, host *api.Host) {
	if !host.GetCR().IsTroubleshoot() {
		// We are not troubleshooting
		return
	}

	container, ok := getMainContainer(statefulSet)
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
	// Appended `sleep` command makes Pod unable to respond to probes and probes would fail, causing unexpected restart.
	// Thus we need to disable all probes in troubleshooting mode.
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
}

// setupLogContainer
func (c *Creator) setupLogContainer(statefulSet *apps.StatefulSet, host *api.Host) {
	// In case we have default LogVolumeClaimTemplate specified - need to append log container to Pod Template
	if host.Templates.HasLogVolumeClaimTemplate() {
		ensureLogContainerSpecified(statefulSet)
		c.a.V(1).F().Info("add log container for host: %s", host.Runtime.Address.HostName)
	}
}

// getPodTemplate gets Pod Template to be used to create StatefulSet
func (c *Creator) getPodTemplate(host *api.Host) *api.PodTemplate {
	// Which pod template should be used - either explicitly defined or a default one
	podTemplate, ok := host.GetPodTemplate()
	if ok {
		// Host references known PodTemplate
		// Make local copy of this PodTemplate, in order not to spoil the original common-used template
		podTemplate = podTemplate.DeepCopy()
		c.a.V(3).F().Info("host: %s StatefulSet - use custom template: %s", host.Runtime.Address.HostName, podTemplate.Name)
	} else {
		// Host references UNKNOWN PodTemplate, will use default one
		podTemplate = newDefaultPodTemplate(host)
		c.a.V(3).F().Info("host: %s StatefulSet - use default generated template", host.Runtime.Address.HostName)
	}

	// Here we have local copy of Pod Template, to be used to create StatefulSet
	// Now we can customize this Pod Template for particular host

	model.PrepareAffinity(podTemplate, host)

	return podTemplate
}

// statefulSetSetupVolumes setup all volumes
func (c *Creator) statefulSetSetupVolumes(statefulSet *apps.StatefulSet, host *api.Host) {
	c.statefulSetSetupVolumesForConfigMaps(statefulSet, host)
	c.statefulSetSetupVolumesForSecrets(statefulSet, host)
}

// statefulSetSetupVolumesForConfigMaps adds to each container in the Pod VolumeMount objects
func (c *Creator) statefulSetSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.Host) {
	configMapHostName := namer.CreateConfigMapHostName(host)
	configMapCommonName := namer.CreateConfigMapCommonName(c.chi)
	configMapCommonUsersName := namer.CreateConfigMapCommonUsersName(c.chi)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		createVolumeForConfigMap(configMapCommonName),
		createVolumeForConfigMap(configMapCommonUsersName),
		createVolumeForConfigMap(configMapHostName),
		//createVolumeForConfigMap(configMapHostMigrationName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes
	k8s.StatefulSetAppendVolumeMounts(
		statefulSet,
		createVolumeMount(configMapCommonName, config.DirPathCommonConfig),
		createVolumeMount(configMapCommonUsersName, config.DirPathUsersConfig),
		createVolumeMount(configMapHostName, config.DirPathHostConfig),
	)
}

// statefulSetSetupVolumesForSecrets adds to each container in the Pod VolumeMount objects
func (c *Creator) statefulSetSetupVolumesForSecrets(statefulSet *apps.StatefulSet, host *api.Host) {
	// Add all additional Volumes
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		host.GetCR().GetRuntime().GetAttributes().GetAdditionalVolumes()...,
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have additional volumes mounted as Volumes
	k8s.StatefulSetAppendVolumeMounts(
		statefulSet,
		host.GetCR().GetRuntime().GetAttributes().GetAdditionalVolumeMounts()...,
	)
}

// statefulSetAppendUsedPVCTemplates appends all PVC templates which are used (referenced by name) by containers
// to the StatefulSet.Spec.VolumeClaimTemplates list
func (c *Creator) statefulSetAppendUsedPVCTemplates(statefulSet *apps.StatefulSet, host *api.Host) {
	// VolumeClaimTemplates, that are directly referenced in containers' VolumeMount object(s)
	// are appended to StatefulSet's Spec.VolumeClaimTemplates slice
	//
	// Deal with `volumeMounts` of a `container`, located by the path:
	// .spec.templates.podTemplates.*.spec.containers.volumeMounts.*
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			// Convenience wrapper
			volumeMount := &container.VolumeMounts[j]
			if volumeClaimTemplate, ok := getVolumeClaimTemplate(volumeMount, host); ok {
				c.statefulSetAppendPVCTemplate(statefulSet, host, volumeClaimTemplate)
			}
		}
	}
}

// statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates
// appends VolumeMounts for Data and Log VolumeClaimTemplates on all containers.
// Creates VolumeMounts for Data and Log volumes in case these volume templates are specified in `templates`.
func (c *Creator) statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *api.Host) {
	// Mount all named (data and log so far) VolumeClaimTemplates into all containers
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		k8s.ContainerAppendVolumeMounts(
			container,
			createVolumeMount(host.Templates.GetDataVolumeClaimTemplate(), config.DirPathClickHouseData),
		)
		k8s.ContainerAppendVolumeMounts(
			container,
			createVolumeMount(host.Templates.GetLogVolumeClaimTemplate(), config.DirPathClickHouseLog),
		)
	}
}

// setupStatefulSetVolumeClaimTemplates performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (c *Creator) setupStatefulSetVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *api.Host) {
	c.statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates(statefulSet, host)
	c.statefulSetAppendUsedPVCTemplates(statefulSet, host)
}

// statefulSetApplyPodTemplate fills StatefulSet.Spec.Template with data from provided PodTemplate
func (c *Creator) statefulSetApplyPodTemplate(
	statefulSet *apps.StatefulSet,
	template *api.PodTemplate,
	host *api.Host,
) {
	// StatefulSet's pod template is not directly compatible with PodTemplate,
	// we need to extract some fields from PodTemplate and apply on StatefulSet
	statefulSet.Spec.Template = core.PodTemplateSpec{
		ObjectMeta: meta.ObjectMeta{
			Name: template.Name,
			Labels: namer.Macro(host).Map(util.MergeStringMapsOverwrite(
				c.tagger.Label(tags.LabelPodTemplate, host),
				template.ObjectMeta.GetLabels(),
			)),
			Annotations: namer.Macro(host).Map(util.MergeStringMapsOverwrite(
				c.tagger.Annotate(tags.AnnotatePodTemplate, host),
				template.ObjectMeta.GetAnnotations(),
			)),
		},
		Spec: *template.Spec.DeepCopy(),
	}

	if statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds == nil {
		statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = chop.Config().GetTerminationGracePeriod()
	}
}

// getMainContainer is a unification wrapper
func getMainContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return getClickHouseContainer(statefulSet)
}

// getClickHouseContainer
func getClickHouseContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseContainerName, 0)
}

// getClickHouseLogContainer
func getClickHouseLogContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseLogContainerName, -1)
}

// ensureNamedPortsSpecified
func ensureNamedPortsSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	// Ensure ClickHouse container has all named ports specified
	container, ok := getMainContainer(statefulSet)
	if !ok {
		return
	}
	// Walk over all assigned ports of the host and ensure each port in container
	config.HostWalkAssignedPorts(
		host,
		func(name string, port *api.Int32, protocol core.Protocol) bool {
			k8s.ContainerEnsurePortByName(container, name, port.Value())
			// Do not abort, continue iterating
			return false
		},
	)
}

// statefulSetAppendPVCTemplate appends to StatefulSet.Spec.VolumeClaimTemplates new entry with data from provided 'src' VolumeClaimTemplate
func (c *Creator) statefulSetAppendPVCTemplate(
	statefulSet *apps.StatefulSet,
	host *api.Host,
	volumeClaimTemplate *api.VolumeClaimTemplate,
) {
	// Since we have the same names for PVs produced from both VolumeClaimTemplates and Volumes,
	// we need to check naming for all of them

	// Check whether provided VolumeClaimTemplate is already listed in statefulSet.Spec.VolumeClaimTemplates
	if k8s.StatefulSetHasVolumeClaimTemplateByName(statefulSet, volumeClaimTemplate.Name) {
		// This VolumeClaimTemplate is already listed in statefulSet.Spec.VolumeClaimTemplates
		// No need to add it second time
		return
	}

	// Check whether provided VolumeClaimTemplate is already listed in statefulSet.Spec.Template.Spec.Volumes
	if k8s.StatefulSetHasVolumeByName(statefulSet, volumeClaimTemplate.Name) {
		// This VolumeClaimTemplate is already listed in statefulSet.Spec.Template.Spec.Volumes
		// No need to add it second time
		return
	}

	// Provided VolumeClaimTemplate is not listed neither in
	// statefulSet.Spec.Template.Spec.Volumes
	// nor in
	// statefulSet.Spec.VolumeClaimTemplates
	// so, let's add it

	if volume.OperatorShouldCreatePVC(host, volumeClaimTemplate) {
		claimName := namer.CreatePVCNameByVolumeClaimTemplate(host, volumeClaimTemplate)
		statefulSet.Spec.Template.Spec.Volumes = append(
			statefulSet.Spec.Template.Spec.Volumes,
			createVolumeForPVC(volumeClaimTemplate.Name, claimName),
		)
	} else {
		statefulSet.Spec.VolumeClaimTemplates = append(
			statefulSet.Spec.VolumeClaimTemplates,
			// For templates we should not specify namespace where PVC would be located
			c.createPVC(volumeClaimTemplate.Name, "", host, &volumeClaimTemplate.Spec),
		)
	}
}

// newDefaultPodTemplate is a unification wrapper
func newDefaultPodTemplate(host *api.Host) *api.PodTemplate {
	return newDefaultClickHousePodTemplate(host)
}

// newDefaultClickHousePodTemplate returns default Pod Template to be used with StatefulSet
func newDefaultClickHousePodTemplate(host *api.Host) *api.PodTemplate {
	podTemplate := &api.PodTemplate{
		Name: namer.CreateStatefulSetName(host),
		Spec: core.PodSpec{
			Containers: []core.Container{},
			Volumes:    []core.Volume{},
		},
	}

	// Pod has to have main container.
	k8s.PodSpecAddContainer(&podTemplate.Spec, newDefaultClickHouseContainer(host))

	return podTemplate
}

func appendContainerPorts(container *core.Container, host *api.Host) {
	// Walk over all assigned ports of the host and append each port to the list of container's ports
	config.HostWalkAssignedPorts(
		host,
		func(name string, port *api.Int32, protocol core.Protocol) bool {
			// Append assigned port to the list of container's ports
			container.Ports = append(container.Ports,
				core.ContainerPort{
					Name:          name,
					ContainerPort: port.Value(),
					Protocol:      protocol,
				},
			)
			// Do not abort, continue iterating
			return false
		},
	)
}

// newDefaultClickHouseContainer returns default ClickHouse Container
func newDefaultClickHouseContainer(host *api.Host) core.Container {
	container := core.Container{
		Name:           config.ClickHouseContainerName,
		Image:          config.DefaultClickHouseDockerImage,
		LivenessProbe:  newDefaultClickHouseLivenessProbe(host),
		ReadinessProbe: newDefaultClickHouseReadinessProbe(host),
	}
	appendContainerPorts(&container, host)
	return container
}

// newDefaultLogContainer returns default ClickHouse Log Container
func newDefaultLogContainer() core.Container {
	return core.Container{
		Name:  config.ClickHouseLogContainerName,
		Image: config.DefaultUbiDockerImage,
		Command: []string{
			"/bin/sh", "-c", "--",
		},
		Args: []string{
			"while true; do sleep 30; done;",
		},
	}
}
