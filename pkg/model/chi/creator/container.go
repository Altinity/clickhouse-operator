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
)

type IContainerManager interface {
	NewDefaultAppContainer(host *api.Host) core.Container
	GetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool)
	EnsureAppContainer(statefulSet *apps.StatefulSet, host *api.Host)
	EnsureLogContainer(statefulSet *apps.StatefulSet)
}

type ContainerManagerType string

const (
	ContainerManagerTypeClickHouse ContainerManagerType = "clickhouse"
	ContainerManagerTypeKeeper     ContainerManagerType = "keeper"
)

func NewContainerManager(what ContainerManagerType) IContainerManager {
	switch what {
	case ContainerManagerTypeClickHouse:
		return NewContainerManagerClickHouse(NewProbeManagerClickHouse())
	}
	panic("unknown container manager type")
}

func containerAppendSpecifiedPorts(container *core.Container, host *api.Host) {
	// Walk over all assigned ports of the host and append each port to the list of container's ports
	host.WalkAssignedPorts(
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
