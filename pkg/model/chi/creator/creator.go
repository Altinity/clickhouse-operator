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
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
)

// Creator specifies creator object
type Creator struct {
	cr                   api.ICustomResource
	configFilesGenerator interfaces.IConfigFilesGenerator
	tagger               interfaces.ITagger
	a                    log.Announcer
	cm                   interfaces.IContainerManager
	pm                   IProbeManager
	sm                   IServiceManager
	vm                   interfaces.IVolumeManager
	cmm                  interfaces.IConfigMapManager
	nm                   interfaces.INameManager
	// container builder
	// probes builder
	// default pod template builder
	// service builder
	// config map-based system volumes
	// fixed paths user volumes

	// namer
	// port walker
	// config maps
}

// NewCreator creates new Creator object
func NewCreator(
	cr api.ICustomResource,
	configFilesGenerator interfaces.IConfigFilesGenerator,
	containerManager interfaces.IContainerManager,
	tagger interfaces.ITagger,
	probeManager IProbeManager,
	serviceManager IServiceManager,
	volumeManager interfaces.IVolumeManager,
	configMapManager interfaces.IConfigMapManager,
	nameManager interfaces.INameManager,
) *Creator {
	return &Creator{
		cr:                   cr,
		configFilesGenerator: configFilesGenerator,
		tagger:               tagger,
		a:                    log.M(cr),
		cm:                   containerManager,
		pm:                   probeManager,
		sm:                   serviceManager,
		vm:                   volumeManager,
		cmm:                  configMapManager,
		nm:                   nameManager,
	}
}
