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
	core "k8s.io/api/core/v1"
)

// Maps container name => restarts count. Given names or indexes, it maps only the container PodSpecContainerGet
// finds by them.
func PodContainersRestartCountsGet(pod *core.Pod, namesOrIndexes ...any) map[string]int {
	if pod == nil {
		return nil
	}
	if len(pod.Status.ContainerStatuses) < 1 {
		return nil
	}
	only := ""
	if len(namesOrIndexes) > 0 {
		container, ok := PodSpecContainerGet(&pod.Spec, namesOrIndexes...)
		if !ok {
			return nil
		}
		only = container.Name
	}
	res := map[string]int{}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if (only == "") || (containerStatus.Name == only) {
			res[containerStatus.Name] = int(containerStatus.RestartCount)
		}
	}
	return res
}

func PodHasCrushedContainers(pod *core.Pod) bool {
	// pod.Status.ContainerStatuses[0].State.Waiting.Reason
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Waiting != nil {
			if containerStatus.State.Waiting.Reason == "CrashLoopBackOff" {
				// Crashed
				return true
			}
		}
	}
	// No crashed
	return false
}

func PodHasNotReadyContainers(pod *core.Pod) bool {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if !containerStatus.Ready {
			// Not ready
			return true
		}
	}
	// All are ready
	return false
}

func PodHasAllContainersStarted(pod *core.Pod) bool {
	allContainersStarted := true
	for _, containerStatus := range pod.Status.ContainerStatuses {
		// Started indicates whether the container has finished its postStart lifecycle hook
		// and passed its startup probe.
		// Initialized as false, becomes true after startupProbe is considered
		// successful. Resets to false when the container is restarted, or if kubelet
		// loses state temporarily. In both cases, startup probes will run again.
		// Is always true when no startupProbe is defined and container is running and
		// has passed the postStart lifecycle hook. The null value must be treated the
		// same as false.
		if (containerStatus.Started != nil) && (*containerStatus.Started) {
			// Current container is started. no changes in all status
		} else {
			// Current container is NOT started
			allContainersStarted = false
		}
	}
	return allContainersStarted
}

func PodHasNotStartedContainers(pod *core.Pod) bool {
	return !PodHasAllContainersStarted(pod)
}

func PodPhaseIsRunning(pod *core.Pod) bool {
	return pod.Status.Phase == core.PodRunning
}

func IsPodOK(pod *core.Pod) bool {
	if len(pod.Status.ContainerStatuses) < 1 {
		return false
	}
	if PodHasCrushedContainers(pod) {
		return false
	}
	if PodHasNotReadyContainers(pod) {
		return false
	}
	if PodHasNotStartedContainers(pod) {
		return false
	}
	if !PodPhaseIsRunning(pod) {
		return false
	}
	return true
}
