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

type HostTemplateType string

const (
	HostTemplateCommon      HostTemplateType = "ht common"
	HostTemplateHostNetwork HostTemplateType = "ht host net"
)

func NewHostTemplate(what HostTemplateType, name string) *api.HostTemplate {
	switch what {
	case HostTemplateCommon:
		return newDefaultHostTemplate(name)
	case HostTemplateHostNetwork:
		return newDefaultHostTemplateForHostNetwork(name)
	default:
		return nil
	}
}

// newDefaultHostTemplate returns default Host Template to be used with StatefulSet
func newDefaultHostTemplate(name string) *api.HostTemplate {
	return &api.HostTemplate{
		Name: name,
		PortDistribution: []api.PortDistribution{
			{
				Type: deployment.PortDistributionUnspecified,
			},
		},
	}
}

// newDefaultHostTemplateForHostNetwork
func newDefaultHostTemplateForHostNetwork(name string) *api.HostTemplate {
	return &api.HostTemplate{
		Name: name,
		PortDistribution: []api.PortDistribution{
			{
				Type: deployment.PortDistributionClusterScopeIndex,
			},
		},
	}
}
