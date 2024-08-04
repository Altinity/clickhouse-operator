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
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

// getPodTemplate gets Pod Template to be used to create StatefulSet
func (c *Creator) getPodTemplate(host *api.Host) *api.PodTemplate {
	// Which pod template should be used - either explicitly defined or a default one
	podTemplate, found := host.GetPodTemplate()
	if found {
		// Host references known PodTemplate
		// Make local copy of this PodTemplate, in order not to spoil the original common-used template
		podTemplate = podTemplate.DeepCopy()
		c.a.V(3).F().Info("host: %s StatefulSet - use custom template: %s", host.Runtime.Address.HostName, podTemplate.Name)
	} else {
		// Host references UNKNOWN PodTemplate, will use default one
		podTemplate = c.newAppPodTemplateDefault(host)
		c.a.V(3).F().Info("host: %s StatefulSet - use default generated template", host.Runtime.Address.HostName)
	}

	// Here we have local copy of Pod Template, to be used to create StatefulSet
	// Now we can customize this Pod Template for particular host

	model.PrepareAffinity(podTemplate, host)

	return podTemplate
}

// newAppPodTemplateDefault is a unification wrapper
func (c *Creator) newAppPodTemplateDefault(host *api.Host) *api.PodTemplate {
	podTemplate := &api.PodTemplate{
		Name: namer.New().Name(interfaces.NameStatefulSet, host),
		Spec: core.PodSpec{
			Containers: []core.Container{},
			Volumes:    []core.Volume{},
		},
	}

	// Pod has to have application container.
	k8s.PodSpecAddContainer(&podTemplate.Spec, c.cm.NewDefaultAppContainer(host))

	return podTemplate
}
