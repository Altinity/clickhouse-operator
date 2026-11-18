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

package kube

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeFake "k8s.io/client-go/kubernetes/fake"

	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// A software restart of a host waits for its ClickHouse to restart, so only the ClickHouse container's restarts
// count: the liveness probe of the backup sidecar restarts the sidecar alone - also while ClickHouse is shut down to
// restart - which must not pass for a restart of the host. The ClickHouse container is found as the operator finds it
// in the pod template: by its name, or else as the first container that is none of the operator's sidecars.
func TestPodGetRestartCountersClickHouseOnly(t *testing.T) {
	const (
		namespace = "ns"
		podName   = "chi-backup-c-0-0-0"
	)
	for name, tc := range map[string]struct {
		clickHouse   string
		sidecarFirst bool
	}{
		"ClickHouse container by name":           {clickHouse: config.ClickHouseContainerName},
		"ClickHouse container by position":       {clickHouse: "server"},
		"ClickHouse container after the sidecar": {clickHouse: "server", sidecarFirst: true},
	} {
		t.Run(name, func(t *testing.T) {
			clickHouse := tc.clickHouse
			containers := []core.Container{{Name: clickHouse}, {Name: config.ClickHouseBackupContainerName}}
			if tc.sidecarFirst {
				slices.Reverse(containers)
			}
			pod := &core.Pod{
				ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: podName},
				Spec:       core.PodSpec{Containers: containers},
				Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{
					{Name: config.ClickHouseBackupContainerName, RestartCount: 4},
					{Name: clickHouse, RestartCount: 1},
				}},
			}
			client := kubeFake.NewSimpleClientset(pod)
			pods := NewPod(client, managers.NewNameManager(managers.NameManagerTypeClickHouse))
			ctx := context.Background()

			before, err := pods.GetRestartCounters(ctx, namespace, podName)
			require.NoError(t, err)
			require.Equal(t, map[string]int{clickHouse: 1}, before)

			pod.Status.ContainerStatuses[0].RestartCount++
			_, err = client.CoreV1().Pods(namespace).UpdateStatus(ctx, pod, meta.UpdateOptions{})
			require.NoError(t, err)
			sidecarRestarted, err := pods.GetRestartCounters(ctx, namespace, podName)
			require.NoError(t, err)
			require.True(t, util.MapsAreTheSame(before, sidecarRestarted), "a restart of the sidecar is no restart of the host")

			pod.Status.ContainerStatuses[1].RestartCount++
			_, err = client.CoreV1().Pods(namespace).UpdateStatus(ctx, pod, meta.UpdateOptions{})
			require.NoError(t, err)
			restarted, err := pods.GetRestartCounters(ctx, namespace, podName)
			require.NoError(t, err)
			require.False(t, util.MapsAreTheSame(before, restarted), "a restart of ClickHouse is a restart of the host")
		})
	}
}
