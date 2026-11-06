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
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
)

// newRestartedPod is a pod of the containers, each restarted as often as given, in this order
func newRestartedPod(containers []string, restarts map[string]int32) *core.Pod {
	pod := &core.Pod{}
	for _, name := range containers {
		pod.Spec.Containers = append(pod.Spec.Containers, core.Container{Name: name})
		if count, ok := restarts[name]; ok {
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, core.ContainerStatus{Name: name, RestartCount: count})
		}
	}
	return pod
}

// The restart counts are those of every container, or of the one container found by name or index as
// PodSpecContainerGet finds it - by its index where no container has the name
func TestPodContainersRestartCountsGet(t *testing.T) {
	pod := newRestartedPod([]string{"server", "clickhouse", "sidecar"}, map[string]int32{"server": 1, "clickhouse": 2, "sidecar": 3})

	require.Equal(t, map[string]int{"server": 1, "clickhouse": 2, "sidecar": 3}, PodContainersRestartCountsGet(pod))
	require.Equal(t, map[string]int{"clickhouse": 2}, PodContainersRestartCountsGet(pod, "clickhouse", 0))
	require.Equal(t, map[string]int{"server": 1}, PodContainersRestartCountsGet(pod, "missing", 0), "the index is the fallback")
	require.Nil(t, PodContainersRestartCountsGet(pod, "missing"), "no such container")
	require.Empty(t, PodContainersRestartCountsGet(newRestartedPod([]string{"clickhouse", "sidecar"}, map[string]int32{"sidecar": 3}), "clickhouse"),
		"a container without status has no count")
	require.Nil(t, PodContainersRestartCountsGet(nil, "clickhouse"))
	require.Nil(t, PodContainersRestartCountsGet(&core.Pod{}))
}

func TestPodHasAllContainersStartedRequiresObservedStatuses(t *testing.T) {
	started := true
	notStarted := false
	t.Run("nil pod", func(t *testing.T) {
		require.False(t, PodHasAllContainersStarted(nil))
	})
	t.Run("status not yet reported for image pull", func(t *testing.T) {
		pod := &core.Pod{Spec: core.PodSpec{
			Containers: []core.Container{{Name: "keeper"}},
		}}
		require.False(t, PodHasAllContainersStarted(pod))
		require.True(t, PodHasNotStartedContainers(pod))
	})
	t.Run("broken image reports waiting", func(t *testing.T) {
		pod := &core.Pod{
			Spec: core.PodSpec{Containers: []core.Container{{Name: "keeper"}}},
			Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{{
				Name: "keeper", Started: &notStarted,
				State: core.ContainerState{Waiting: &core.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
			}}},
		}
		require.False(t, PodHasAllContainersStarted(pod))
	})
	t.Run("one missing sidecar status", func(t *testing.T) {
		pod := &core.Pod{
			Spec: core.PodSpec{Containers: []core.Container{{Name: "keeper"}, {Name: "sidecar"}}},
			Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{
				{Name: "keeper", Started: &started},
			}},
		}
		require.False(t, PodHasAllContainersStarted(pod))
	})
	t.Run("all containers started but not necessarily ready", func(t *testing.T) {
		pod := &core.Pod{
			Spec: core.PodSpec{Containers: []core.Container{{Name: "keeper"}, {Name: "sidecar"}}},
			Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{
				{Name: "sidecar", Started: &started},
				{Name: "keeper", Started: &started},
			}},
		}
		require.True(t, PodHasAllContainersStarted(pod))
	})
	t.Run("a nil Started status cannot count as started", func(t *testing.T) {
		pod := &core.Pod{
			Spec: core.PodSpec{Containers: []core.Container{{Name: "keeper"}}},
			Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{{Name: "keeper"}}},
		}
		require.False(t, PodHasAllContainersStarted(pod))
	})
}
