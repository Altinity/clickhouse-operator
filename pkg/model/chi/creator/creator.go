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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/annotator"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

type iTagger interface {
	Annotate(what annotator.AnnotateType, params ...any) map[string]string
	Label(what labeler.LabelType, params ...any) map[string]string
	Selector(what labeler.SelectorType, params ...any) map[string]string
}

// Creator specifies creator object
type Creator struct {
	cr                   api.ICustomResource
	configFilesGenerator managers.IConfigFilesGenerator
	tagger               iTagger
	a                    log.Announcer
	cm                   IContainerManager
	pm                   IProbeManager
	sm                   IServiceManager
	vm                   managers.IVolumeManager
	cmm                  IConfigMapManager
	nm                   managers.INameManager
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
	configFilesGenerator managers.IConfigFilesGenerator,
	containerManager IContainerManager,
	probeManager IProbeManager,
	serviceManager IServiceManager,
	volumeManager managers.IVolumeManager,
	configMapManager IConfigMapManager,
	nameManager managers.INameManager,
) *Creator {
	return &Creator{
		cr:                   cr,
		configFilesGenerator: configFilesGenerator,
		tagger:               managers.NewTagger(cr),
		a:                    log.M(cr),
		cm:                   containerManager,
		pm:                   probeManager,
		sm:                   serviceManager,
		vm:                   volumeManager,
		cmm:                  configMapManager,
		nm:                   nameManager,
	}
}
