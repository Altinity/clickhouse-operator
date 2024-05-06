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
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// CreateStatefulSet creates new apps.StatefulSet
func (c *Creator) CreateStatefulSet(host *api.ChiHost, shutdown bool) *apps.StatefulSet {
	statefulSet := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Name:            model.CreateStatefulSetName(host),
			Namespace:       host.Runtime.Address.Namespace,
			Labels:          model.Macro(host).Map(c.labels.GetHostScope(host, true)),
			Annotations:     model.Macro(host).Map(c.annotations.GetHostScope(host)),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    host.GetStatefulSetReplicasNum(shutdown),
			ServiceName: model.CreateStatefulSetServiceName(host),
			Selector: &meta.LabelSelector{
				MatchLabels: model.GetSelectorHostScope(host),
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
	model.MakeObjectVersion(&statefulSet.ObjectMeta, statefulSet)

	return statefulSet
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
	ensureMainContainerSpecified(statefulSet, host)
	ensureProbesSpecified(statefulSet, host)
	ensureNamedPortsSpecified(statefulSet, host)
}

// setupEnvVars setup ENV vars for clickhouse container
func setupEnvVars(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	container, ok := getMainContainer(statefulSet)
	if !ok {
		return
	}

	container.Env = append(container.Env, host.GetCHI().GetRuntime().GetAttributes().GetAdditionalEnvVars()...)
}

// ensureMainContainerSpecified is a unification wrapper
func ensureMainContainerSpecified(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	ensureClickHouseContainerSpecified(statefulSet, host)
}

// ensureLogContainerSpecified is a unification wrapper
func ensureLogContainerSpecified(statefulSet *apps.StatefulSet) {
	ensureClickHouseLogContainerSpecified(statefulSet)
}

// ensureClickHouseContainerSpecified
func ensureClickHouseContainerSpecified(statefulSet *apps.StatefulSet, host *api.ChiHost) {
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
func ensureProbesSpecified(statefulSet *apps.StatefulSet, host *api.ChiHost) {
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
func (c *Creator) personalizeStatefulSetTemplate(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Ensure pod created by this StatefulSet has alias 127.0.0.1
	statefulSet.Spec.Template.Spec.HostAliases = []core.HostAlias{
		{
			IP: "127.0.0.1",
			Hostnames: []string{
				model.CreatePodHostname(host),
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
func (c *Creator) setupTroubleshootingMode(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	if !host.GetCHI().IsTroubleshoot() {
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
func (c *Creator) setupLogContainer(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// In case we have default LogVolumeClaimTemplate specified - need to append log container to Pod Template
	if host.Templates.HasLogVolumeClaimTemplate() {
		ensureLogContainerSpecified(statefulSet)
		c.a.V(1).F().Info("add log container for host: %s", host.Runtime.Address.HostName)
	}
}

// getPodTemplate gets Pod Template to be used to create StatefulSet
func (c *Creator) getPodTemplate(host *api.ChiHost) *api.PodTemplate {
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
func (c *Creator) statefulSetSetupVolumes(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	c.statefulSetSetupVolumesForConfigMaps(statefulSet, host)
	c.statefulSetSetupVolumesForSecrets(statefulSet, host)
}

// statefulSetSetupVolumesForConfigMaps adds to each container in the Pod VolumeMount objects
func (c *Creator) statefulSetSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	configMapHostName := model.CreateConfigMapHostName(host)
	configMapCommonName := model.CreateConfigMapCommonName(c.chi)
	configMapCommonUsersName := model.CreateConfigMapCommonUsersName(c.chi)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		newVolumeForConfigMap(configMapCommonName),
		newVolumeForConfigMap(configMapCommonUsersName),
		newVolumeForConfigMap(configMapHostName),
		//newVolumeForConfigMap(configMapHostMigrationName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes
	k8s.StatefulSetAppendVolumeMounts(
		statefulSet,
		newVolumeMount(configMapCommonName, model.DirPathCommonConfig),
		newVolumeMount(configMapCommonUsersName, model.DirPathUsersConfig),
		newVolumeMount(configMapHostName, model.DirPathHostConfig),
	)
}

// statefulSetSetupVolumesForSecrets adds to each container in the Pod VolumeMount objects
func (c *Creator) statefulSetSetupVolumesForSecrets(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Add all additional Volumes
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		host.GetCHI().GetRuntime().GetAttributes().GetAdditionalVolumes()...,
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have additional volumes mounted as Volumes
	k8s.StatefulSetAppendVolumeMounts(
		statefulSet,
		host.GetCHI().GetRuntime().GetAttributes().GetAdditionalVolumeMounts()...,
	)
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
func (c *Creator) statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Mount all named (data and log so far) VolumeClaimTemplates into all containers
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		k8s.ContainerAppendVolumeMounts(
			container,
			newVolumeMount(host.Templates.GetDataVolumeClaimTemplate(), model.DirPathClickHouseData),
		)
		k8s.ContainerAppendVolumeMounts(
			container,
			newVolumeMount(host.Templates.GetLogVolumeClaimTemplate(), model.DirPathClickHouseLog),
		)
	}
}

// setupStatefulSetVolumeClaimTemplates performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (c *Creator) setupStatefulSetVolumeClaimTemplates(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	c.statefulSetAppendVolumeMountsForDataAndLogVolumeClaimTemplates(statefulSet, host)
	c.statefulSetAppendUsedPVCTemplates(statefulSet, host)
}

// statefulSetApplyPodTemplate fills StatefulSet.Spec.Template with data from provided PodTemplate
func (c *Creator) statefulSetApplyPodTemplate(
	statefulSet *apps.StatefulSet,
	template *api.PodTemplate,
	host *api.ChiHost,
) {
	// StatefulSet's pod template is not directly compatible with PodTemplate,
	// we need to extract some fields from PodTemplate and apply on StatefulSet
	statefulSet.Spec.Template = core.PodTemplateSpec{
		ObjectMeta: meta.ObjectMeta{
			Name: template.Name,
			Labels: model.Macro(host).Map(util.MergeStringMapsOverwrite(
				c.labels.GetHostScopeReady(host, true),
				template.ObjectMeta.Labels,
			)),
			Annotations: model.Macro(host).Map(util.MergeStringMapsOverwrite(
				c.annotations.GetHostScope(host),
				template.ObjectMeta.Annotations,
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
	return k8s.StatefulSetContainerGet(statefulSet, model.ClickHouseContainerName, 0)
}

// getClickHouseLogContainer
func getClickHouseLogContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, model.ClickHouseLogContainerName, -1)
}

// ensureNamedPortsSpecified
func ensureNamedPortsSpecified(statefulSet *apps.StatefulSet, host *api.ChiHost) {
	// Ensure ClickHouse container has all named ports specified
	container, ok := getMainContainer(statefulSet)
	if !ok {
		return
	}
	// Walk over all assigned ports of the host and ensure each port in container
	model.HostWalkAssignedPorts(
		host,
		func(name string, port *int32, protocol core.Protocol) bool {
			k8s.ContainerEnsurePortByName(container, name, *port)
			// Do not abort, continue iterating
			return false
		},
	)
}

// statefulSetAppendPVCTemplate appends to StatefulSet.Spec.VolumeClaimTemplates new entry with data from provided 'src' VolumeClaimTemplate
func (c *Creator) statefulSetAppendPVCTemplate(
	statefulSet *apps.StatefulSet,
	host *api.ChiHost,
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

	if OperatorShouldCreatePVC(host, volumeClaimTemplate) {
		claimName := model.CreatePVCNameByVolumeClaimTemplate(host, volumeClaimTemplate)
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

// newDefaultPodTemplate is a unification wrapper
func newDefaultPodTemplate(host *api.ChiHost) *api.PodTemplate {
	return newDefaultClickHousePodTemplate(host)
}

// newDefaultClickHousePodTemplate returns default Pod Template to be used with StatefulSet
func newDefaultClickHousePodTemplate(host *api.ChiHost) *api.PodTemplate {
	podTemplate := &api.PodTemplate{
		Name: model.CreateStatefulSetName(host),
		Spec: core.PodSpec{
			Containers: []core.Container{},
			Volumes:    []core.Volume{},
		},
	}

	// Pod has to have main container.
	k8s.PodSpecAddContainer(&podTemplate.Spec, newDefaultClickHouseContainer(host))

	return podTemplate
}

func appendContainerPorts(container *core.Container, host *api.ChiHost) {
	// Walk over all assigned ports of the host and append each port to the list of container's ports
	model.HostWalkAssignedPorts(
		host,
		func(name string, port *int32, protocol core.Protocol) bool {
			// Append assigned port to the list of container's ports
			container.Ports = append(container.Ports,
				core.ContainerPort{
					Name:          name,
					ContainerPort: *port,
					Protocol:      protocol,
				},
			)
			// Do not abort, continue iterating
			return false
		},
	)
}

// newDefaultClickHouseContainer returns default ClickHouse Container
func newDefaultClickHouseContainer(host *api.ChiHost) core.Container {
	container := core.Container{
		Name:           model.ClickHouseContainerName,
		Image:          model.DefaultClickHouseDockerImage,
		LivenessProbe:  newDefaultClickHouseLivenessProbe(host),
		ReadinessProbe: newDefaultClickHouseReadinessProbe(host),
	}
	appendContainerPorts(&container, host)
	return container
}

// newDefaultLogContainer returns default ClickHouse Log Container
func newDefaultLogContainer() core.Container {
	return core.Container{
		Name:  model.ClickHouseLogContainerName,
		Image: model.DefaultUbiDockerImage,
		Command: []string{
			"/bin/sh", "-c", "--",
		},
		Args: []string{
			"while true; do sleep 30; done;",
		},
	}
}
