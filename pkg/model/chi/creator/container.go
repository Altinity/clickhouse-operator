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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
)

// getClickHouseContainer
func getClickHouseContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseContainerName, 0)
}

// getClickHouseLogContainer
func getClickHouseLogContainer(statefulSet *apps.StatefulSet) (*core.Container, bool) {
	return k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseLogContainerName, -1)
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
		LivenessProbe:  createDefaultClickHouseLivenessProbe(host),
		ReadinessProbe: createDefaultClickHouseReadinessProbe(host),
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
