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

package templates

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
)

// NormalizeHostTemplate normalizes .spec.templates.hostTemplates
func NormalizeHostTemplate(template *api.HostTemplate) {
	// Name

	// PortDistribution

	if template.PortDistribution == nil {
		// In case no PortDistribution provided - setup default one
		template.PortDistribution = []api.PortDistribution{
			{
				Type: deployment.PortDistributionUnspecified,
			},
		}
	}

	// Normalize PortDistribution
	for i := range template.PortDistribution {
		portDistribution := &template.PortDistribution[i]
		switch portDistribution.Type {
		case
			deployment.PortDistributionUnspecified,
			deployment.PortDistributionClusterScopeIndex:
			// distribution is known
		default:
			// distribution is not known
			portDistribution.Type = deployment.PortDistributionUnspecified
		}
	}

	// Spec
	normalizeHostTemplateSpec(&template.Spec)
}

// normalizeHostTemplateSpec is the same as normalizeHost but for a template
func normalizeHostTemplateSpec(host *api.ChiHost) {
}
