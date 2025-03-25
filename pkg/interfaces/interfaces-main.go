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
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type IConfigMapManager interface {
	CreateConfigMap(what ConfigMapType, params ...any) *core.ConfigMap
	SetCR(cr api.ICustomResource)
	SetTagger(tagger ITagger)
	SetConfigFilesGenerator(configFilesGenerator IConfigFilesGenerator)
}

type IConfigFilesGenerator interface {
	CreateConfigFiles(what FilesGroupType, params ...any) map[string]string
}

type INameManager interface {
	Names(what NameType, params ...any) []string
	Name(what NameType, params ...any) string
}

type IAnnotator interface {
	Annotate(what AnnotateType, params ...any) map[string]string
}

type IMacro interface {
	Get(string) string
	Scope(scope any) IMacro
	Line(line string) string
	Map(_map map[string]string) map[string]string
}

type ILabeler interface {
	Label(what LabelType, params ...any) map[string]string
	Selector(what SelectorType, params ...any) map[string]string
	MakeObjectVersion(meta meta.Object, obj interface{})
	GetObjectVersion(meta meta.Object) (string, bool)
	Get(string) string
}

type ITagger interface {
	Annotate(what AnnotateType, params ...any) map[string]string
	Label(what LabelType, params ...any) map[string]string
	Selector(what SelectorType, params ...any) map[string]string
}

type IVolumeManager interface {
	SetupVolumes(what VolumeType, statefulSet *apps.StatefulSet, host *api.Host)
	SetCR(cr api.ICustomResource)
}

type IContainerManager interface {
	NewDefaultAppContainer(host *api.Host) core.Container
	GetAppContainer(statefulSet *apps.StatefulSet) (*core.Container, bool)
	GetAppImageTag(statefulSet *apps.StatefulSet) (string, bool)
	EnsureAppContainer(statefulSet *apps.StatefulSet, host *api.Host)
	EnsureLogContainer(statefulSet *apps.StatefulSet)
	SetupAdditionalEnvVars(host *api.Host, container *core.Container)
}

type IProbeManager interface {
	CreateProbe(what ProbeType, host *api.Host) *core.Probe
}

type IServiceManager interface {
	CreateService(what ServiceType, params ...any) util.Slice[*core.Service]
	SetCR(cr api.ICustomResource)
	SetTagger(tagger ITagger)
}

type ICreator interface {
	CreateConfigMap(what ConfigMapType, params ...any) *core.ConfigMap
	CreatePodDisruptionBudget(cluster api.ICluster) *policy.PodDisruptionBudget
	CreatePVC(
		name string,
		namespace string,
		host *api.Host,
		spec *core.PersistentVolumeClaimSpec,
	) *core.PersistentVolumeClaim
	AdjustPVC(
		pvc *core.PersistentVolumeClaim,
		host *api.Host,
		template *api.VolumeClaimTemplate,
	) *core.PersistentVolumeClaim
	CreateClusterSecret(name string) *core.Secret
	CreateService(what ServiceType, params ...any) util.Slice[*core.Service]
	CreateStatefulSet(host *api.Host, shutdown bool) *apps.StatefulSet
}

type IEventEmitter interface {
	Event(level string, obj meta.Object, action string, reason string, message string)
	EventInfo(obj meta.Object, action string, reason string, message string)
	EventWarning(obj meta.Object, action string, reason string, message string)
	EventError(obj meta.Object, action string, reason string, message string)
}

type IOwnerReferencesManager interface {
	CreateOwnerReferences(owner api.ICustomResource) []meta.OwnerReference
}
