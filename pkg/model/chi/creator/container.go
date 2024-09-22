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

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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

func (cm *ContainerManager) NewDefaultAppContainer(host *chi.Host) core.Container {
	return cm.newDefaultContainerClickHouse(host)
}

func (cm *ContainerManager) GetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return cm.getContainerClickHouse(statefulSet)
}

func (cm *ContainerManager) EnsureAppContainer(statefulSet *apps.StatefulSet, host *chi.Host) {
	cm.ensureContainerSpecifiedClickHouse(statefulSet, host)
}

func (cm *ContainerManager) EnsureLogContainer(statefulSet *apps.StatefulSet) {
	cm.ensureContainerSpecifiedClickHouseLog(statefulSet)
}

// getContainerClickHouse(
func (cm *ContainerManager) getContainerClickHouse(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseContainerName, 0)
}

// getContainerClickHouseLog
func (cm *ContainerManager) getContainerClickHouseLog(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseLogContainerName)
}

// ensureContainerSpecifiedClickHouse
func (cm *ContainerManager) ensureContainerSpecifiedClickHouse(statefulSet *apps.StatefulSet, host *chi.Host) {
	_, ok := cm.getContainerClickHouse(statefulSet)
	if ok {
		return
	}

	// No container available, let's add one
	k8s.PodSpecAddContainer(
		&statefulSet.Spec.Template.Spec,
		cm.newDefaultContainerClickHouse(host),
	)
}

// newDefaultContainerClickHouse returns default ClickHouse Container
func (cm *ContainerManager) newDefaultContainerClickHouse(host *chi.Host) core.Container {
	container := core.Container{
		Name:           config.ClickHouseContainerName,
		Image:          config.DefaultClickHouseDockerImage,
		LivenessProbe:  cm.probe.createDefaultLivenessProbe(host),
		ReadinessProbe: cm.probe.createDefaultReadinessProbe(host),
	}
	host.AppendSpecifiedPortsToContainer(&container)
	return container
}

// ensureContainerSpecifiedClickHouseLog
func (cm *ContainerManager) ensureContainerSpecifiedClickHouseLog(statefulSet *apps.StatefulSet) {
	_, ok := cm.getContainerClickHouseLog(statefulSet)
	if ok {
		return
	}

	// No ClickHouse Log container available, let's add one

	k8s.PodSpecAddContainer(
		&statefulSet.Spec.Template.Spec,
		cm.newDefaultContainerLog(),
	)
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
