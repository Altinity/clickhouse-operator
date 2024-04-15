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
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
)

// newDefaultLivenessProbe is a unification wrapper
func newDefaultLivenessProbe(host *api.ChiHost) *core.Probe {
	return newDefaultClickHouseLivenessProbe(host)
}

// newDefaultReadinessProbe is a unification wrapper
func newDefaultReadinessProbe(host *api.ChiHost) *core.Probe {
	return newDefaultClickHouseReadinessProbe(host)
}

// newDefaultClickHouseLivenessProbe returns default ClickHouse liveness probe
func newDefaultClickHouseLivenessProbe(host *api.ChiHost) *core.Probe {
	// Introduce http probe in case http port is specified
	if api.IsPortAssigned(host.HTTPPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path: "/ping",
					Port: intstr.Parse(model.ChDefaultHTTPPortName), // What if it is not a default?
				},
			},
			InitialDelaySeconds: 60,
			PeriodSeconds:       3,
			FailureThreshold:    10,
		}
	}

	// Introduce https probe in case https port is specified
	if api.IsPortAssigned(host.HTTPSPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path:   "/ping",
					Port:   intstr.Parse(model.ChDefaultHTTPSPortName), // What if it is not a default?
					Scheme: core.URISchemeHTTPS,
				},
			},
			InitialDelaySeconds: 60,
			PeriodSeconds:       3,
			FailureThreshold:    10,
		}
	}

	// Probe is not available
	return nil
}

// newDefaultClickHouseReadinessProbe returns default ClickHouse readiness probe
func newDefaultClickHouseReadinessProbe(host *api.ChiHost) *core.Probe {
	// Introduce http probe in case http port is specified
	if api.IsPortAssigned(host.HTTPPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path: "/ping",
					Port: intstr.Parse(model.ChDefaultHTTPPortName), // What if port name is not a default?
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       3,
		}
	}

	// Introduce https probe in case https port is specified
	if api.IsPortAssigned(host.HTTPSPort) {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path:   "/ping",
					Port:   intstr.Parse(model.ChDefaultHTTPSPortName), // What if port name is not a default?
					Scheme: core.URISchemeHTTPS,
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       3,
		}
	}

	// Probe is not available
	return nil
}
