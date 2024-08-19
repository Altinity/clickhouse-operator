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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/common/volume"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

func (c *Creator) stsSetupStorage(statefulSet *apps.StatefulSet, host *api.Host) {
	// Setup system volumes - described by the operator
	c.stsSetupVolumesSystem(statefulSet, host)
	// Setup user data volumes - described by the manifest
	c.stsSetupVolumesUserData(statefulSet, host)
}

// stsSetupVolumesSystem setup system volumes - described by the operator
func (c *Creator) stsSetupVolumesSystem(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumesForConfigMaps(statefulSet, host)
	c.stsSetupVolumesForSecrets(statefulSet, host)
}

func (c *Creator) stsSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumes(interfaces.VolumesForConfigMaps, statefulSet, host)
}

func (c *Creator) stsSetupVolumes(what interfaces.VolumeType, statefulSet *apps.StatefulSet, host *api.Host) {
	c.vm.SetCR(c.cr)
	c.vm.SetupVolumes(what, statefulSet, host)
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
	c.stsSetupVolumes(interfaces.VolumesUserDataWithFixedPaths, statefulSet, host)
}

func (c *Creator) stsSetupVolumesUserDataWithCustomPaths(statefulSet *apps.StatefulSet, host *api.Host) {
	c.stsSetupVolumesForUsedPVCTemplates(statefulSet, host)
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
		if volumeClaimTemplate, found := model.HostFindVolumeClaimTemplateUsedForVolumeMount(host, volumeMount); found {
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
		claimName := c.nm.Name(interfaces.NamePVCNameByVolumeClaimTemplate, host, volumeClaimTemplate)
		volume := k8s.CreateVolumeForPVC(volumeClaimTemplate.Name, claimName)
		k8s.StatefulSetAppendVolumes(statefulSet, volume)
	} else {
		// For templates we should not specify namespace where PVC would be located
		pvc := *c.CreatePVC(volumeClaimTemplate.Name, "", host, &volumeClaimTemplate.Spec)
		k8s.StatefulSetAppendPersistentVolumeClaims(statefulSet, pvc)
	}
}
