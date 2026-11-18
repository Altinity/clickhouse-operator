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

package chbackup

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// productionCreator returns the creator the operator builds the objects of the installation with
func productionCreator(inMemory *api.ClickHouseInstallation) *commonCreator.Creator {
	return commonCreator.NewCreator(
		inMemory,
		managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeClickHouse, inMemory, &config.GeneratorOptions{
			Settings: inMemory.GetSpecT().GetConfiguration().GetSettings(),
			Files:    inMemory.GetSpecT().GetConfiguration().GetFiles(),
		}),
		managers.NewContainerManager(managers.ContainerManagerTypeClickHouse),
		managers.NewTagManager(managers.TagManagerTypeClickHouse, inMemory),
		managers.NewProbeManager(managers.ProbeManagerTypeClickHouse),
		managers.NewServiceManager(managers.ServiceManagerTypeClickHouse),
		managers.NewVolumeManager(managers.VolumeManagerTypeClickHouse),
		managers.NewConfigMapManager(managers.ConfigMapManagerTypeClickHouse),
		managers.NewNameManager(managers.NameManagerTypeClickHouse),
		managers.NewOwnerReferencesManager(managers.OwnerReferencesManagerTypeClickHouse),
		namer.New(),
		macro.New(),
		labeler.New(inMemory),
	)
}
