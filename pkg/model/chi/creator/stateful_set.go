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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/volume"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// CreateStatefulSet creates new apps.StatefulSet
func (c *Creator) CreateStatefulSet(host *api.Host, shutdown bool) *apps.StatefulSet {
	statefulSet := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.Name(namer.NameStatefulSet, host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          namer.Macro(host).Map(c.tagger.Label(tags.LabelSTS, host)),
			Annotations:     namer.Macro(host).Map(c.tagger.Annotate(tags.AnnotateSTS, host)),
			OwnerReferences: createOwnerReferences(c.cr),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    host.GetStatefulSetReplicasNum(shutdown),
			ServiceName: namer.Name(namer.NameStatefulSetService, host),
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

	c.stsSetupApplication(statefulSet, host)
	c.stsSetupStorage(statefulSet, host)

	tags.MakeObjectVersion(statefulSet.GetObjectMeta(), statefulSet)

	return statefulSet
}

// stsSetupApplication performs PodTemplate setup of StatefulSet
func (c *Creator) stsSetupApplication(statefulSet *apps.StatefulSet, host *api.Host) {
	// Apply Pod Template on the StatefulSet
	podTemplate := c.getPodTemplate(host)
	c.stsApplyPodTemplate(statefulSet, podTemplate, host)

	// Post-process StatefulSet
	// Setup application container
	c.stsSetupAppContainer(statefulSet, host)
	// Setup dedicated log container
	c.stsSetupLogContainer(statefulSet, host)
	// Setup additional host alias(es)
	c.stsSetupHostAliases(statefulSet, host)
}

func (c *Creator) stsSetupStorage(statefulSet *apps.StatefulSet, host *api.Host) {
	// Setup system volumes - described by the operator
	c.stsSetupVolumesSystem(statefulSet, host)
	// Setup user data volumes - described by the manifest
	c.stsSetupVolumesUserData(statefulSet, host)
}

func (c *Creator) stsSetupAppContainer(statefulSet *apps.StatefulSet, host *api.Host) {
	// We need to be sure app container is healthy
	c.stsEnsureAppContainerSpecified(statefulSet, host)
	c.stsEnsureAppContainerProbesSpecified(statefulSet, host)
	c.stsEnsureAppContainerNamedPortsSpecified(statefulSet, host)
	// Setup ENV vars for the app
	c.stsAppContainerSetupEnvVars(statefulSet, host)
	// Setup app according to troubleshoot mode (if any)
	c.stsAppContainerSetupTroubleshootingMode(statefulSet, host)
}

// stsEnsureAppContainerSpecified is a unification wrapper.
// Ensures main application container is specified
func (c *Creator) stsEnsureAppContainerSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	c.cm.EnsureAppContainer(statefulSet, host)
}

// stsEnsureLogContainerSpecified is a unification wrapper
// Ensures log container is in place, if required
func (c *Creator) stsEnsureLogContainerSpecified(statefulSet *apps.StatefulSet) {
	c.cm.EnsureLogContainer(statefulSet)
}

// stsAppContainerSetupEnvVars setup ENV vars for main application container
func (c *Creator) stsAppContainerSetupEnvVars(statefulSet *apps.StatefulSet, host *api.Host) {
	container, ok := c.stsGetAppContainer(statefulSet)
	if !ok {
		return
	}

	container.Env = append(container.Env, host.GetCR().GetRuntime().GetAttributes().GetAdditionalEnvVars()...)
}

// stsEnsureAppContainerProbesSpecified
func (c *Creator) stsEnsureAppContainerProbesSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	container, ok := c.stsGetAppContainer(statefulSet)
	if !ok {
		return
	}
	if container.LivenessProbe == nil {
		container.LivenessProbe = c.pm.CreateProbe(ProbeDefaultLiveness, host)
	}
	if container.ReadinessProbe == nil {
		container.ReadinessProbe = c.pm.CreateProbe(ProbeDefaultReadiness, host)
	}
}

// stsSetupHostAliases
func (c *Creator) stsSetupHostAliases(statefulSet *apps.StatefulSet, host *api.Host) {
	// Ensure pod created by this StatefulSet has alias 127.0.0.1
	statefulSet.Spec.Template.Spec.HostAliases = []core.HostAlias{
		{
			IP: "127.0.0.1",
			Hostnames: []string{
				namer.Name(namer.NamePodHostname, host),
			},
		},
	}
}

// stsAppContainerSetupTroubleshootingMode
func (c *Creator) stsAppContainerSetupTroubleshootingMode(statefulSet *apps.StatefulSet, host *api.Host) {
	if !host.GetCR().IsTroubleshoot() {
		// We are not troubleshooting
		return
	}

	container, ok := c.stsGetAppContainer(statefulSet)
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
	// Appended `sleep` command makes Pod unable to respond to probes and probes would fail all the time,
	// causing repeated restarts of the Pod by k8s. Restart is triggered by probes failures.
	// Thus we need to disable all probes in troubleshooting mode.
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
}

// stsSetupLogContainer
func (c *Creator) stsSetupLogContainer(statefulSet *apps.StatefulSet, host *api.Host) {
	// In case we have default LogVolumeClaimTemplate specified - need to append log container to Pod Template
	if host.Templates.HasLogVolumeClaimTemplate() {
		c.stsEnsureLogContainerSpecified(statefulSet)
		c.a.V(1).F().Info("add log container for host: %s", host.Runtime.Address.HostName)
	}
}

// stsSetupVolumesSystem setup all volumes
func (c *Creator) stsSetupVolumesSystem(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumesForConfigMaps(statefulSet, host)
	c.stsSetupVolumesForSecrets(statefulSet, host)
}

func (c *Creator) stsSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumes(VolumesForConfigMaps, statefulSet, host)
}

// stsSetupVolumesForSecrets adds to each container in the Pod VolumeMount objects
func (c *Creator) stsSetupVolumesForSecrets(statefulSet *apps.StatefulSet, host *api.Host) {
	// Add all additional Volumes
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		host.GetCR().GetRuntime().GetAttributes().GetAdditionalVolumes()...,
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have additional volumes mounted as Volumes
	k8s.StatefulSetAppendVolumeMountsInAllContainers(
		statefulSet,
		host.GetCR().GetRuntime().GetAttributes().GetAdditionalVolumeMounts()...,
	)
}

// stsSetupVolumesUserData performs VolumeClaimTemplate setup for Containers in PodTemplate of a StatefulSet
func (c *Creator) stsSetupVolumesUserData(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumesUserDataWithFixedPaths(statefulSet, host)
	c.stsSetupVolumesUserDataWithCustomPaths(statefulSet, host)
}

func (c *Creator) stsSetupVolumesUserDataWithFixedPaths(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumes(VolumesUserDataWithFixedPaths, statefulSet, host)
}

func (c *Creator) stsSetupVolumesUserDataWithCustomPaths(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumesForUsedPVCTemplates(statefulSet, host)
}

func (c *Creator) stsSetupVolumes(what VolumeType, statefulSet *apps.StatefulSet, host *api.Host) {
	c.vm.SetCR(c.cr)
	c.vm.SetupVolumes(what, statefulSet, host)
}

// stsSetupVolumesForUsedPVCTemplates appends all PVC templates which are used (referenced by name) by containers
// to the StatefulSet.Spec.VolumeClaimTemplates list
func (c *Creator) stsSetupVolumesForUsedPVCTemplates(statefulSet *apps.StatefulSet, host *api.Host) {
	// VolumeClaimTemplates, that are directly referenced in containers' VolumeMount object(s)
	// are appended to StatefulSet's Spec.VolumeClaimTemplates slice
	//
	// Deal with `volumeMounts` of a `container`, located by the path:
	// .spec.templates.podTemplates.*.spec.containers.volumeMounts.*
	k8s.StatefulSetWalkVolumeMounts(statefulSet, func(volumeMount *core.VolumeMount) {
		if volumeClaimTemplate, found := findVolumeClaimTemplateUsedForVolumeMount(volumeMount, host); found {
			c.stsSetupVolumeForPVCTemplate(statefulSet, host, volumeClaimTemplate)
		}
	})
}

// stsSetupVolumeForPVCTemplate appends to StatefulSet.Spec.VolumeClaimTemplates new entry with data from provided 'src' VolumeClaimTemplate
func (c *Creator) stsSetupVolumeForPVCTemplate(
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
		claimName := namer.Name(namer.NamePVCNameByVolumeClaimTemplate, host, volumeClaimTemplate)
		volume := k8s.CreateVolumeForPVC(volumeClaimTemplate.Name, claimName)
		k8s.StatefulSetAppendVolumes(statefulSet, volume)
	} else {
		// For templates we should not specify namespace where PVC would be located
		pvc := *c.CreatePVC(volumeClaimTemplate.Name, "", host, &volumeClaimTemplate.Spec)
		k8s.StatefulSetAppendPersistentVolumeClaims(statefulSet, pvc)
	}
}

// stsApplyPodTemplate fills StatefulSet.Spec.Template with data from provided PodTemplate
func (c *Creator) stsApplyPodTemplate(statefulSet *apps.StatefulSet, template *api.PodTemplate, host *api.Host) {
	statefulSet.Spec.Template = c.createPodTemplateSpec(template, host)

	// Adjust TerminationGracePeriodSeconds
	if statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds == nil {
		statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = chop.Config().GetTerminationGracePeriod()
	}
}

func (c *Creator) createPodTemplateSpec(template *api.PodTemplate, host *api.Host) core.PodTemplateSpec {
	// StatefulSet's pod template is not directly compatible with PodTemplate,
	// we need to extract some fields from PodTemplate and apply on StatefulSet
	labels := namer.Macro(host).Map(util.MergeStringMapsOverwrite(
		c.tagger.Label(tags.LabelPodTemplate, host),
		template.ObjectMeta.GetLabels(),
	))
	annotations := namer.Macro(host).Map(util.MergeStringMapsOverwrite(
		c.tagger.Annotate(tags.AnnotatePodTemplate, host),
		template.ObjectMeta.GetAnnotations(),
	))

	return core.PodTemplateSpec{
		ObjectMeta: meta.ObjectMeta{
			Name:        template.Name,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: *template.Spec.DeepCopy(),
	}
}

// stsGetAppContainer is a unification wrapper
func (c *Creator) stsGetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return c.cm.GetAppContainer(statefulSet)
}

// stsEnsureAppContainerNamedPortsSpecified
func (c *Creator) stsEnsureAppContainerNamedPortsSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	// Ensure ClickHouse container has all named ports specified
	container, ok := c.stsGetAppContainer(statefulSet)
	if !ok {
		return
	}
	// Walk over all assigned ports of the host and ensure each port in container
	host.WalkAssignedPorts(
		func(name string, port *api.Int32, protocol core.Protocol) bool {
			k8s.ContainerEnsurePortByName(container, name, port.Value())
			// Do not abort, continue iterating
			return false
		},
	)
}
