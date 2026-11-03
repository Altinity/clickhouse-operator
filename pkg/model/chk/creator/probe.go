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
	case interfaces.ProbeDefaultStartup:
		return m.createDefaultStartupProbe(host)
	case interfaces.ProbeDefaultLiveness:
		return m.createDefaultLivenessProbe(host)
	case interfaces.ProbeDefaultReadiness:
		return m.createDefaultReadinessProbe(host)
	}
	panic("unknown probe type")
}

// createDefaultStartupProbe returns default startup probe.
// Uses pgrep to check that the clickhouse-keeper process is running.
// This is intentionally quorum-independent: the probe succeeds as soon as the
// process starts, allowing the operator to proceed to the next host without
// waiting for Raft quorum. FailureThreshold is generous to handle slow starts.
func (m *ProbeManager) createDefaultStartupProbe(_ *api.Host) *core.Probe {
	return &core.Probe{
		ProbeHandler: core.ProbeHandler{
			Exec: &core.ExecAction{
				Command: []string{"pgrep", "-f", "clickhouse-keeper"},
			},
		},
		InitialDelaySeconds: 1,
		PeriodSeconds:       3,
		FailureThreshold:    60,
	}
}

// createDefaultLivenessProbe returns default liveness probe.
// Uses the ruok/imok 4LW handshake against the keeper's plaintext TCP client
// port to detect not just process existence but actual responsiveness — a
// hung keeper (Raft thread blocked, IO stalled) passes pgrep but fails ruok,
// which is the case liveness must catch. ruok itself is quorum-independent
// (returns "imok" the moment the listener is up, no Raft state check), so the
// probe will not cause cascading restarts during leader election or cluster
// formation.
//
// Implemented via bash /dev/tcp redirection — the canonical repo idiom.
// Avoids depending on `nc` which is not always present in the keeper image.
//
// 4LW falls back to pgrep when the host opts out of the plaintext port
// (host.IsInsecure() == false): upstream Keeper does not serve 4LW commands
// over `tcp_port_secure`, so bash /dev/tcp would have nothing to talk to.
// pgrep loses the "hung-but-running" detection but keeps the probe from
// CrashLoopBackOff'ing every secure-only Keeper. Readiness (HTTP /ready on
// 9182) still catches quorum-level failures, and the dedicated startup probe
// already uses pgrep, so this degrades gracefully.
func (m *ProbeManager) createDefaultLivenessProbe(host *api.Host) *core.Probe {
	if !host.IsInsecure() {
		return &core.Probe{
			ProbeHandler: core.ProbeHandler{
				Exec: &core.ExecAction{
					Command: []string{"pgrep", "-f", "clickhouse-keeper"},
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      3,
			FailureThreshold:    12,
		}
	}
	port := host.ZKPort.Normalize(api.KpDefaultZKPortNumber).Value()
	cmd := fmt.Sprintf(
		`OK=$(exec 3<>/dev/tcp/127.0.0.1/%d; printf "ruok" >&3; IFS=; tee <&3; exec 3<&-); [ "$OK" = "imok" ]`,
		port,
	)
	return &core.Probe{
		ProbeHandler: core.ProbeHandler{
			Exec: &core.ExecAction{
				Command: []string{"bash", "-c", cmd},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		FailureThreshold:    12,
	}
}

// createDefaultReadinessProbe returns default readiness probe.
// Checks the /ready HTTP endpoint which reflects Raft quorum status.
func (m *ProbeManager) createDefaultReadinessProbe(_ *api.Host) *core.Probe {
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
