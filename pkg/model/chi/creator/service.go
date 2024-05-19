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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type ServiceType string

const (
	ServiceCHI        ServiceType = "svc chi"
	ServiceCHICluster ServiceType = "svc chi cluster"
	ServiceCHIShard   ServiceType = "svc chi shard"
	ServiceCHIHost    ServiceType = "svc chi host"
)

type IServiceManager interface {
	CreateService(what ServiceType, params ...any) *core.Service
	SetCR(cr api.ICustomResource)
	SetTagger(tagger iTagger)
}

type ServiceManagerType string

const (
	ServiceManagerTypeClickHouse ServiceManagerType = "clickhouse"
	ServiceManagerTypeKeeper     ServiceManagerType = "keeper"
)

func NewServiceManager(what ServiceManagerType) IServiceManager {
	switch what {
	case ServiceManagerTypeClickHouse:
		return NewServiceManagerClickHouse()
	}
	panic("unknown service manager type")
}

func (c *Creator) CreateService(what ServiceType, params ...any) *core.Service {
	c.sm.SetCR(c.cr)
	c.sm.SetTagger(c.tagger)
	return c.sm.CreateService(what, params...)
}

func svcAppendSpecifiedPorts(service *core.Service, host *api.Host) {
	// Walk over all assigned ports of the host and append each port to the list of service's ports
	host.WalkAssignedPorts(
		func(name string, port *api.Int32, protocol core.Protocol) bool {
			// Append assigned port to the list of service's ports
			service.Spec.Ports = append(service.Spec.Ports,
				core.ServicePort{
					Name:       name,
					Protocol:   protocol,
					Port:       port.Value(),
					TargetPort: intstr.FromInt(port.IntValue()),
				},
			)
			// Do not abort, continue iterating
			return false
		},
	)
}

// createServiceFromTemplate create Service from ServiceTemplate and additional info
func createServiceFromTemplate(
	template *api.ServiceTemplate,
	namespace string,
	name string,
	labels map[string]string,
	annotations map[string]string,
	selector map[string]string,
	ownerReferences []meta.OwnerReference,
	macro *macro.MacrosEngine,
) *core.Service {

	// Verify Ports
	if err := k8s.ServiceSpecVerifyPorts(&template.Spec); err != nil {
		return nil
	}

	// Create Service
	service := &core.Service{
		ObjectMeta: *template.ObjectMeta.DeepCopy(),
		Spec:       *template.Spec.DeepCopy(),
	}

	// Overwrite .name and .namespace - they are not allowed to be specified in template
	service.Name = name
	service.Namespace = namespace
	service.OwnerReferences = ownerReferences

	// Combine labels and annotations
	service.Labels = macro.Map(util.MergeStringMapsOverwrite(service.Labels, labels))
	service.Annotations = macro.Map(util.MergeStringMapsOverwrite(service.Annotations, annotations))

	// Append provided Selector to already specified Selector in template
	service.Spec.Selector = util.MergeStringMapsOverwrite(service.Spec.Selector, selector)

	// And after the object is ready we can put version label
	labeler.MakeObjectVersion(service.GetObjectMeta(), service)

	return service
}
