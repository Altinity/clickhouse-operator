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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
)

// NewDefaultHostTemplate returns default Host Template to be used with StatefulSet
func NewDefaultHostTemplate(name string) *api.ChiHostTemplate {
	return &api.ChiHostTemplate{
		Name: name,
		PortDistribution: []api.ChiPortDistribution{
			{
				Type: deployment.PortDistributionUnspecified,
			},
		},
		Spec: api.ChiHost{
			Name:                "",
			TCPPort:             api.PortUnassigned(),
			TLSPort:             api.PortUnassigned(),
			HTTPPort:            api.PortUnassigned(),
			HTTPSPort:           api.PortUnassigned(),
			InterserverHTTPPort: api.PortUnassigned(),
			Templates:           nil,
		},
	}
}

// NewDefaultHostTemplateForHostNetwork
func NewDefaultHostTemplateForHostNetwork(name string) *api.ChiHostTemplate {
	return &api.ChiHostTemplate{
		Name: name,
		PortDistribution: []api.ChiPortDistribution{
			{
				Type: deployment.PortDistributionClusterScopeIndex,
			},
		},
		Spec: api.ChiHost{
			Name:                "",
			TCPPort:             api.PortUnassigned(),
			TLSPort:             api.PortUnassigned(),
			HTTPPort:            api.PortUnassigned(),
			HTTPSPort:           api.PortUnassigned(),
			InterserverHTTPPort: api.PortUnassigned(),
			Templates:           nil,
		},
	}
}

func HostTemplateName(host *api.ChiHost) string {
	return "HostTemplate" + host.Name
}
