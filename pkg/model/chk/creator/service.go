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
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
)

type ServiceManager struct {
	cr     apiChi.ICustomResource
	tagger interfaces.ITagger
}

func NewServiceManager() *ServiceManager {
	return &ServiceManager{}
}

func (m *ServiceManager) CreateService(what interfaces.ServiceType, params ...any) *core.Service {
	switch what {
	case interfaces.ServiceCluster:
		if len(params) > 0 {
			chk := params[0].(*apiChk.ClickHouseKeeperInstallation)
			return m.CreateClientService(chk)
		}
	case interfaces.ServiceHost:
		if len(params) > 0 {
			chk := params[0].(*apiChk.ClickHouseKeeperInstallation)
			return m.CreateHeadlessService(chk)
		}
	}
	panic("unknown service type")
}

func (m *ServiceManager) SetCR(cr apiChi.ICustomResource) {
	m.cr = cr
}
func (m *ServiceManager) SetTagger(tagger interfaces.ITagger) {
	m.tagger = tagger
}

// CreateClientService returns a client service resource for the clickhouse keeper cluster
func (m *ServiceManager) CreateClientService(chk *apiChk.ClickHouseKeeperInstallation) *core.Service {
	// Client port is mandatory
	svcPorts := []core.ServicePort{
		core.ServicePort{
			Name: "client",
			Port: int32(chk.Spec.GetClientPort()),
		},
	}

	// Prometheus port is optional
	prometheusPort := chk.Spec.GetPrometheusPort()
	if prometheusPort != -1 {
		svcPorts = append(svcPorts,
			core.ServicePort{
				Name: "prometheus",
				Port: int32(prometheusPort),
			},
		)
	}

	return createService(chk.Name, chk, svcPorts, true)
}

// CreateHeadlessService returns an internal headless-service for the chk stateful-set
func (m *ServiceManager) CreateHeadlessService(chk *apiChk.ClickHouseKeeperInstallation) *core.Service {
	svcPorts := []core.ServicePort{
		{
			Name: "raft",
			Port: int32(chk.Spec.GetRaftPort()),
		},
	}

	return createService(namer.New().Name(interfaces.NameStatefulSetService, m.cr), chk, svcPorts, false)
}

func createService(name string, chk *apiChk.ClickHouseKeeperInstallation, ports []core.ServicePort, clusterIP bool) *core.Service {
	service := core.Service{
		TypeMeta: meta.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: chk.Namespace,
		},
		Spec: core.ServiceSpec{
			Ports:    ports,
			Selector: labeler.GetPodLabels(chk),
		},
	}
	if !clusterIP {
		service.Spec.ClusterIP = core.ClusterIPNone
	}
	return &service
}
