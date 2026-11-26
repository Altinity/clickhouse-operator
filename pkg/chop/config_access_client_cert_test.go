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

package chop

import (
	"testing"

	sigsyaml "github.com/kubernetes-sigs/yaml"
	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// TestAccessClientCertUnmarshalRoundTrip reads clickhouse.access.clientCertFile/clientKeyFile with
// the same yaml package the config loader uses, and writes them back out.
func TestAccessClientCertUnmarshalRoundTrip(t *testing.T) {
	const cfg = `
clickhouse:
  access:
    clientCertFile: /etc/clickhouse-operator/client-tls/tls.crt
    clientKeyFile: /etc/clickhouse-operator/client-tls/tls.key
`
	var oc api.OperatorConfig
	require.NoError(t, sigsyaml.Unmarshal([]byte(cfg), &oc))
	require.Equal(t, "/etc/clickhouse-operator/client-tls/tls.crt", oc.ClickHouse.Access.ClientCertFile)
	require.Equal(t, "/etc/clickhouse-operator/client-tls/tls.key", oc.ClickHouse.Access.ClientKeyFile)

	out, err := sigsyaml.Marshal(&oc)
	require.NoError(t, err)
	var back api.OperatorConfig
	require.NoError(t, sigsyaml.Unmarshal(out, &back))
	require.Equal(t, oc.ClickHouse.Access.ClientCertFile, back.ClickHouse.Access.ClientCertFile)
	require.Equal(t, oc.ClickHouse.Access.ClientKeyFile, back.ClickHouse.Access.ClientKeyFile)

	// Unset: the keys are omitted, so an existing config serialises as before.
	var empty api.OperatorConfig
	require.NoError(t, sigsyaml.Unmarshal([]byte("clickhouse:\n  access:\n    scheme: https\n"), &empty))
	require.Empty(t, empty.ClickHouse.Access.ClientCertFile)
	require.Empty(t, empty.ClickHouse.Access.ClientKeyFile)
	out, err = sigsyaml.Marshal(&empty)
	require.NoError(t, err)
	require.NotContains(t, string(out), "clientCertFile")
	require.NotContains(t, string(out), "clientKeyFile")
}

// TestAccessClientCertMergesFromCR proves a ClickHouseOperatorConfiguration CR carries the paths
// into the unified config through OperatorConfig.MergeFrom, as it does access.rootCASecretRef.
func TestAccessClientCertMergesFromCR(t *testing.T) {
	base := &api.OperatorConfig{}
	const crSpec = `
clickhouse:
  access:
    clientCertFile: /etc/client/tls.crt
    clientKeyFile: /etc/client/tls.key
`
	var cr api.OperatorConfig
	require.NoError(t, sigsyaml.Unmarshal([]byte(crSpec), &cr))
	require.NoError(t, base.MergeFrom(&cr))
	require.Equal(t, "/etc/client/tls.crt", base.ClickHouse.Access.ClientCertFile)
	require.Equal(t, "/etc/client/tls.key", base.ClickHouse.Access.ClientKeyFile)
}
