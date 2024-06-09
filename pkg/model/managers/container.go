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

package managers

import (
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	chkCreator "github.com/altinity/clickhouse-operator/pkg/model/chk/creator"
)

type ContainerManagerType string

const (
	ContainerManagerTypeClickHouse ContainerManagerType = "clickhouse"
	ContainerManagerTypeKeeper     ContainerManagerType = "keeper"
)

func NewContainerManager(what ContainerManagerType) interfaces.IContainerManager {
	switch what {
	case ContainerManagerTypeClickHouse:
		return chiCreator.NewContainerManagerClickHouse(chiCreator.NewProbeManagerClickHouse())
	case ContainerManagerTypeKeeper:
		return chkCreator.NewContainerManagerKeeper(chkCreator.NewProbeManagerKeeper())
	}
	panic("unknown container manager type")
}
