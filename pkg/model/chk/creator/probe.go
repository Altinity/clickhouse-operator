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
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type ProbeManager struct {
}

func NewProbeManager() *ProbeManager {
	return &ProbeManager{}
}

func (m *ProbeManager) CreateProbe(what interfaces.ProbeType, host *api.Host) *core.Probe {
	switch what {
	case interfaces.ProbeDefaultLiveness:
		return m.createDefaultLivenessProbe(host)
	case interfaces.ProbeDefaultReadiness:
		return nil
		//return m.createDefaultReadinessProbe(host)
	}
	panic("unknown probe type")
}

// createDefaultLivenessProbe returns default liveness probe
func (m *ProbeManager) createDefaultLivenessProbe(host *api.Host) *core.Probe {
	return &core.Probe{
		ProbeHandler: core.ProbeHandler{
			Exec: &core.ExecAction{
				Command: []string{
					"bash",
					"-xc",
					livenessProbeScript(host.ZKPort.IntValue()),
				},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		FailureThreshold:    12,
	}
}

func livenessProbeScript(port int) string {
	return fmt.Sprintf(
		`date && `+
			`OK=$(exec 3<>/dev/tcp/127.0.0.1/%d; printf 'ruok' >&3; IFS=; tee <&3; exec 3<&-;);`+
			`if [[ "${OK}" == "imok" ]]; then exit 0; else exit 1; fi`,
		port,
	)
}

// createDefaultReadinessProbe returns default readiness probe
func (m *ProbeManager) createDefaultReadinessProbe(host *api.Host) *core.Probe {
	return &core.Probe{
		ProbeHandler: core.ProbeHandler{
			HTTPGet: &core.HTTPGetAction{
				Path: "/ready",
				Port: intstr.Parse("9182"),
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		FailureThreshold:    12,
	}
}
