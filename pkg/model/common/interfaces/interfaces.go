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

package interfaces

import (
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/annotator"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonConfig "github.com/altinity/clickhouse-operator/pkg/model/common/config"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
)

type IConfigMapManager interface {
	CreateConfigMap(what commonCreator.ConfigMapType, params ...any) *core.ConfigMap
	SetCR(cr api.ICustomResource)
	SetTagger(tagger ITagger)
	SetConfigFilesGenerator(configFilesGenerator IConfigFilesGenerator)
}

type IConfigFilesGenerator interface {
	CreateConfigFiles(what commonConfig.FilesGroupType, params ...any) map[string]string
}

type INameManager interface {
	Names(what namer.NameType, params ...any) []string
	Name(what namer.NameType, params ...any) string
}

type IAnnotator interface {
	Annotate(what annotator.AnnotateType, params ...any) map[string]string
}

type ILabeler interface {
	Label(what labeler.LabelType, params ...any) map[string]string
	Selector(what labeler.SelectorType, params ...any) map[string]string
}

type ITagger interface {
	Annotate(what annotator.AnnotateType, params ...any) map[string]string
	Label(what labeler.LabelType, params ...any) map[string]string
	Selector(what labeler.SelectorType, params ...any) map[string]string
}

type IVolumeManager interface {
	SetupVolumes(what commonCreator.VolumeType, statefulSet *apps.StatefulSet, host *api.Host)
	SetCR(cr api.ICustomResource)
}

type IContainerManager interface {
	NewDefaultAppContainer(host *api.Host) core.Container
	GetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool)
	EnsureAppContainer(statefulSet *apps.StatefulSet, host *api.Host)
	EnsureLogContainer(statefulSet *apps.StatefulSet)
}
