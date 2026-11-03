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
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// portNames extracts the port names emitted onto a Service for assertion.
func portNames(svc *core.Service) []string {
	names := make([]string, 0, len(svc.Spec.Ports))
	for _, p := range svc.Spec.Ports {
		names = append(names, p.Name)
	}
	return names
}

// TestAppendHostExposedPortsRaftToggle pins the per-host Service port partition (issue #1982):
// the peer/Raft Service keeps the raft port (includeRaftPort=true) while the client-facing
// Service drops it (includeRaftPort=false) and retains only the ZK client ports.
func TestAppendHostExposedPortsRaftToggle(t *testing.T) {
	newHost := func() *chi.Host {
		return &chi.Host{
			HostSecure: chi.HostSecure{Insecure: types.NewStringBool(true)},
			HostPorts: chi.HostPorts{
				ZKPort:   types.NewInt32(2181),
				RaftPort: types.NewInt32(9234),
			},
		}
	}

	t.Run("peer keeps raft port", func(t *testing.T) {
		svc := &core.Service{}
		appendHostExposedPorts(svc, newHost(), true)
		names := portNames(svc)
		require.Contains(t, names, chi.KpDefaultZKPortName)
		require.Contains(t, names, chi.KpDefaultRaftPortName)
	})

	t.Run("client drops raft port, keeps zk", func(t *testing.T) {
		svc := &core.Service{}
		appendHostExposedPorts(svc, newHost(), false)
		names := portNames(svc)
		require.Contains(t, names, chi.KpDefaultZKPortName)
		require.NotContains(t, names, chi.KpDefaultRaftPortName)
	})
}
