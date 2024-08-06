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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type ContainerManager struct {
	probe *ProbeManager
}

func NewContainerManager(probe *ProbeManager) *ContainerManager {
	return &ContainerManager{
		probe: probe,
	}
}

func (cm *ContainerManager) NewDefaultAppContainer(host *api.Host) core.Container {
	return cm.newDefaultContainerClickHouse(host)
}

func (cm *ContainerManager) GetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return cm.getClickHouseContainer(statefulSet)
}

func (cm *ContainerManager) EnsureAppContainer(statefulSet *apps.StatefulSet, host *api.Host) {
	cm.ensureClickHouseContainerSpecified(statefulSet, host)
}

func (cm *ContainerManager) EnsureLogContainer(statefulSet *apps.StatefulSet) {
	cm.ensureClickHouseLogContainerSpecified(statefulSet)
}

// getClickHouseContainer
func (cm *ContainerManager) getClickHouseContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseContainerName, 0)
}

// getClickHouseLogContainer
func (cm *ContainerManager) getClickHouseLogContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseLogContainerName)
}

// ensureClickHouseContainerSpecified
func (cm *ContainerManager) ensureClickHouseContainerSpecified(statefulSet *apps.StatefulSet, host *api.Host) {
	_, ok := cm.getClickHouseContainer(statefulSet)
	if ok {
		return
	}

	// No ClickHouse container available, let's add one
	k8s.PodSpecAddContainer(
		&statefulSet.Spec.Template.Spec,
		cm.newDefaultContainerClickHouse(host),
	)
}

// ensureClickHouseLogContainerSpecified
func (cm *ContainerManager) ensureClickHouseLogContainerSpecified(statefulSet *apps.StatefulSet) {
	_, ok := cm.getClickHouseLogContainer(statefulSet)
	if ok {
		return
	}

	// No ClickHouse Log container available, let's add one

	k8s.PodSpecAddContainer(
		&statefulSet.Spec.Template.Spec,
		cm.newDefaultContainerLog(),
	)
}

// newDefaultContainerClickHouse returns default ClickHouse Container
func (cm *ContainerManager) newDefaultContainerClickHouse(host *api.Host) core.Container {
	container := core.Container{
		Name:           config.ClickHouseContainerName,
		Image:          config.DefaultClickHouseDockerImage,
		LivenessProbe:  cm.probe.createDefaultClickHouseLivenessProbe(host),
		ReadinessProbe: cm.probe.createDefaultClickHouseReadinessProbe(host),
	}
	host.AppendSpecifiedPortsToContainer(&container)
	return container
}

// newDefaultContainerLog returns default ClickHouse Log Container
func (cm *ContainerManager) newDefaultContainerLog() core.Container {
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
