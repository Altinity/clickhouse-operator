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

package normalizer

import (
	"slices"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// Request specifies normalization Request
type Request struct {
	*normalizer.Request[api.ClickHouseInstallation]

	// removedSecretRefReported records whether this pass has already aborted over a user field
	// using the removed k8s_secret_ syntax, so the abort is raised once however many fields and
	// users carry it. Pass-local on purpose: status.Errors is inherited into the next
	// normalization target, so deduplicating against it would suppress the abort from the second
	// reconcile onward and let the CR through.
	removedSecretRefReported bool

	// podTemplateLayers names, per pod template, the distinct layers that define it - templates in
	// the order first applied, then the installation. A pod template defined by more than one is a
	// merge of authors that need not know each other's container names, which is where containers
	// pairing by name can leave one container as two.
	podTemplateLayers map[string][]string
}

// AddPodTemplateLayer records that the named layer defines the pod templates given. A layer is
// recorded once per pod template, however often it is applied or defines it: a template both applied
// automatically and listed in useTemplates, or a pod template listed twice in one resource, has a
// single author.
func (c *Request) AddPodTemplateLayer(layer string, podTemplates []api.PodTemplate) {
	if c == nil {
		return
	}
	if c.podTemplateLayers == nil {
		c.podTemplateLayers = make(map[string][]string)
	}
	for i := range podTemplates {
		name := podTemplates[i].Name
		if !slices.Contains(c.podTemplateLayers[name], layer) {
			c.podTemplateLayers[name] = append(c.podTemplateLayers[name], layer)
		}
	}
}

// GetPodTemplateLayers gets the layers that define the named pod template, in merge order.
func (c *Request) GetPodTemplateLayers(name string) []string {
	if c == nil {
		return nil
	}
	return c.podTemplateLayers[name]
}

// RemovedSecretRefReported reports whether this normalization pass already raised the removed
// secret-ref abort, and marks it as raised. Returns false exactly once per pass.
func (c *Request) RemovedSecretRefReported() bool {
	if c == nil {
		return true
	}
	reported := c.removedSecretRefReported
	c.removedSecretRefReported = true
	return reported
}

// NewRequest creates new Request
func NewRequest(options *normalizer.Options[api.ClickHouseInstallation]) *Request {
	return &Request{
		Request: normalizer.NewRequest(options),
	}
}

func (c *Request) GetTarget() *api.ClickHouseInstallation {
	return c.Request.GetTarget().(*api.ClickHouseInstallation)
}

func (c *Request) SetTarget(target *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	return c.Request.SetTarget(target).(*api.ClickHouseInstallation)
}
