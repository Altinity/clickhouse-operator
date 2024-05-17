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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags"
)

type iTagger interface {
	Annotate(what tags.AnnotateType, params ...any) map[string]string
	Label(what tags.LabelType, params ...any) map[string]string
	Selector(what tags.SelectorType, params ...any) map[string]string
}

type iConfigFileGenerator interface {
	CreateConfigFiles(what config.FilesGroupType, params ...any) map[string]string
}

// Creator specifies creator object
type Creator struct {
	cr                   api.ICustomResource
	configFilesGenerator iConfigFileGenerator
	tagger               iTagger
	a                    log.Announcer
	cm                   IContainerManager
	pm                   IProbeManager
	sm                   IServiceManager
	// container builder
	// probes builder
	// default pod template builder
	// service builder

	// namer
	// config map-based system volumes
	// fixed paths user volumes
	// port walker
}

// NewCreator creates new Creator object
func NewCreator(
	cr api.ICustomResource,
	configFilesGenerator iConfigFileGenerator,
	containerManager IContainerManager,
	probeManager IProbeManager,
	serviceManager IServiceManager,
) *Creator {
	return &Creator{
		cr:                   cr,
		configFilesGenerator: configFilesGenerator,
		tagger:               tags.NewTagger(cr),
		a:                    log.M(cr),
		cm:                   containerManager,
		pm:                   probeManager,
		sm:                   serviceManager,
	}
}
