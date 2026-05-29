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

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// TestBuildZKServicePorts pins the byte-stability contract for the CR-scope
// Keeper Service: the legacy (insecure-only) port set must remain identical
// to the prior-release output so existing CHKs see no Service spec churn on
// operator upgrade. The secure port appears only when explicitly opted in.
func TestBuildZKServicePorts(t *testing.T) {
	t.Run("insecure only (legacy)", func(t *testing.T) {
		ports := buildZKServicePorts(true, false)
		require.Len(t, ports, 2)
		require.Equal(t, chi.KpDefaultZKPortName, ports[0].Name)
		require.Equal(t, chi.KpDefaultZKPortNumber, ports[0].Port)
		require.Equal(t, chi.KpDefaultRaftPortName, ports[1].Name)
		require.Equal(t, chi.KpDefaultRaftPortNumber, ports[1].Port)
	})
	t.Run("secure only", func(t *testing.T) {
		ports := buildZKServicePorts(false, true)
		require.Len(t, ports, 2)
		require.Equal(t, chi.KpDefaultZKSecurePortName, ports[0].Name)
		require.Equal(t, chi.KpDefaultZKSecurePortNumber, ports[0].Port)
		require.Equal(t, chi.KpDefaultRaftPortName, ports[1].Name)
	})
	t.Run("both insecure and secure", func(t *testing.T) {
		ports := buildZKServicePorts(true, true)
		require.Len(t, ports, 3)
		require.Equal(t, chi.KpDefaultZKPortName, ports[0].Name)
		require.Equal(t, chi.KpDefaultZKSecurePortName, ports[1].Name)
		require.Equal(t, chi.KpDefaultRaftPortName, ports[2].Name)
	})
	t.Run("neither (raft-only fallback)", func(t *testing.T) {
		// Defensive: even a fully unconfigured CR keeps the Raft port so
		// inter-Keeper peering can still resolve at the Service level.
		ports := buildZKServicePorts(false, false)
		require.Len(t, ports, 1)
		require.Equal(t, chi.KpDefaultRaftPortName, ports[0].Name)
	})
}
