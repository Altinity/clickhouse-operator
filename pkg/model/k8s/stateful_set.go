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

package k8s

import (
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
)

// StatefulSetContainerGet gets container from the StatefulSet either by name or by index
func StatefulSetContainerGet(statefulSet *apps.StatefulSet, namesOrIndexes ...any) (*core.Container, bool) {
	for _, nameOrIndex := range namesOrIndexes {
		switch typed := nameOrIndex.(type) {
		// Find by name
		case string:
			name := typed
			if len(name) > 0 {
				for i := range statefulSet.Spec.Template.Spec.Containers {
					// Convenience wrapper
					container := &statefulSet.Spec.Template.Spec.Containers[i]
					if container.Name == name {
						return container, true
					}
				}
			}
		// Find by index
		case int:
			index := typed
			if index >= 0 {
				if len(statefulSet.Spec.Template.Spec.Containers) > index {
					return &statefulSet.Spec.Template.Spec.Containers[index], true
				}
			}
		}
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

func StatefulSetHasVolumeClaimTemplateByName(statefulSet *apps.StatefulSet, name string) bool {
	// Check whether provided VolumeClaimTemplate name is already listed in statefulSet.Spec.VolumeClaimTemplates
	for i := range statefulSet.Spec.VolumeClaimTemplates {
		// Convenience wrapper
		volumeClaimTemplate := &statefulSet.Spec.VolumeClaimTemplates[i]
		if volumeClaimTemplate.Name == name {
			// This VolumeClaimTemplate name is already listed in statefulSet.Spec.VolumeClaimTemplates
			return true
		}
	}

	return false
}

func StatefulSetHasVolumeByName(statefulSet *apps.StatefulSet, name string) bool {
	for i := range statefulSet.Spec.Template.Spec.Volumes {
		// Convenience wrapper
		volume := &statefulSet.Spec.Template.Spec.Volumes[i]
		if volume.Name == name {
			// This Volume name is already listed in statefulSet.Spec.Template.Spec.Volumes
			return true
		}
	}

	return false
}

// StatefulSetAppendVolumes appends multiple Volume(s) to the specified StatefulSet
func StatefulSetAppendVolumes(statefulSet *apps.StatefulSet, volumes ...core.Volume) {
	statefulSet.Spec.Template.Spec.Volumes = append(
		statefulSet.Spec.Template.Spec.Volumes,
		volumes...,
	)
}

func StatefulSetAppendPersistentVolumeClaims(statefulSet *apps.StatefulSet, pvcs ...core.PersistentVolumeClaim) {
	statefulSet.Spec.VolumeClaimTemplates = append(
		statefulSet.Spec.VolumeClaimTemplates,
		pvcs...,
	)
}

func StatefulSetAppendVolumeMountsInAllContainers(statefulSet *apps.StatefulSet, volumeMounts ...core.VolumeMount) {
	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have VolumeMounts mounted as Volumes
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		ContainerAppendVolumeMounts(
			container,
			volumeMounts...,
		)
	}
}

func StatefulSetWalkContainers(statefulSet *apps.StatefulSet, f func(*core.Container)) {
	for i := range statefulSet.Spec.Template.Spec.Containers {
		// Convenience wrapper
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		f(container)
	}
}

func StatefulSetWalkVolumeMounts(statefulSet *apps.StatefulSet, f func(*core.VolumeMount)) {
	StatefulSetWalkContainers(statefulSet, func(container *core.Container) {
		for j := range container.VolumeMounts {
			// Convenience wrapper
			volumeMount := &container.VolumeMounts[j]
			f(volumeMount)
		}
	})
}
