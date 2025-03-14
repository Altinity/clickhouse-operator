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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// CreateStatefulSet creates new apps.StatefulSet
func (c *Creator) CreateStatefulSet(host *api.Host, shutdown bool) *apps.StatefulSet {
	statefulSet := &apps.StatefulSet{
		TypeMeta: meta.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:            c.nm.Name(interfaces.NameStatefulSet, host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          c.macro.Scope(host).Map(c.tagger.Label(interfaces.LabelSTS, host)),
			Annotations:     c.macro.Scope(host).Map(c.tagger.Annotate(interfaces.AnnotateSTS, host)),
			OwnerReferences: c.or.CreateOwnerReferences(c.cr),
		},
		Spec: apps.StatefulSetSpec{
			Replicas:    host.GetStatefulSetReplicasNum(shutdown),
			ServiceName: c.nm.Name(interfaces.NameStatefulSetService, host),
			Selector: &meta.LabelSelector{
				MatchLabels: c.tagger.Selector(interfaces.SelectorHostScope, host),
			},

			// IMPORTANT
			// Template is to be setup later
			// VolumeClaimTemplates are to be setup later
			Template:             core.PodTemplateSpec{},
			VolumeClaimTemplates: nil,

			PodManagementPolicy: apps.OrderedReadyPodManagement,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
			RevisionHistoryLimit: chop.Config().GetRevisionHistoryLimit(),
		},
	}

	// Setup basic pod template, than fine-tune application and storage
	c.stsSetupPodTemplate(statefulSet, host)
	c.stsSetupApplication(statefulSet, host)
	c.stsSetupStorage(statefulSet, host)

	c.labeler.MakeObjectVersion(statefulSet.GetObjectMeta(), statefulSet)

	return statefulSet
}

// stsSetupPodTemplate performs basic PodTemplate setup of StatefulSet
func (c *Creator) stsSetupPodTemplate(statefulSet *apps.StatefulSet, host *api.Host) {
	// Apply Pod Template on the StatefulSet
	podTemplate := c.getPodTemplate(host)
	c.stsApplyPodTemplate(statefulSet, podTemplate, host)
}

// stsApplyPodTemplate fills StatefulSet.Spec.Template with data from provided PodTemplate
func (c *Creator) stsApplyPodTemplate(statefulSet *apps.StatefulSet, template *api.PodTemplate, host *api.Host) {
	statefulSet.Spec.Template = c.createPodTemplateSpec(template, host)

	// Adjust TerminationGracePeriodSeconds
	if statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds == nil {
		statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = chop.Config().GetTerminationGracePeriod()
	}
}

// createPodTemplateSpec creates core.PodTemplateSpec object
func (c *Creator) createPodTemplateSpec(template *api.PodTemplate, host *api.Host) core.PodTemplateSpec {
	// Prepare labels and annotations for the core.PodTemplateSpec

	labels := c.macro.Scope(host).Map(util.MergeStringMapsOverwrite(
		c.tagger.Label(interfaces.LabelPodTemplate, host),
		template.ObjectMeta.GetLabels(),
	))
	annotations := c.macro.Scope(host).Map(util.MergeStringMapsOverwrite(
		c.tagger.Annotate(interfaces.AnnotatePodTemplate, host),
		template.ObjectMeta.GetAnnotations(),
	))

	return core.PodTemplateSpec{
		ObjectMeta: meta.ObjectMeta{
			Name:        template.Name,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: *template.Spec.DeepCopy(),
	}
}
