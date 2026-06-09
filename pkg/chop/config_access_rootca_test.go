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

// TestAccessRootCASecretRefUnmarshalAndResolve guards the clickhouse.access
// rootCASecretRef wiring end to end with the SAME yaml package the config loader
// uses (kubernetes-sigs/yaml -> YAML->JSON->json.Unmarshal), so it exercises the
// JSON struct tags exactly as getFileBasedConfig does. It then runs the shared
// resolver to confirm the Secret PEM lands in Access.RootCA, which is what
// NewClusterConnectionParamsFromCHOpConfig reads into the operator's TLS config.
func TestAccessRootCASecretRefUnmarshalAndResolve(t *testing.T) {
	// Explicit key.
	const cfg = `
clickhouse:
  access:
    rootCA: ""
    rootCASecretRef:
      name: my-ca-secret
      key: my.crt
`
	var oc api.OperatorConfig
	require.NoError(t, sigsyaml.Unmarshal([]byte(cfg), &oc))
	require.Equal(t, "my-ca-secret", oc.ClickHouse.Access.RootCASecretRef.Name)
	require.Equal(t, "my.crt", oc.ClickHouse.Access.RootCASecretRef.Key)
	require.Equal(t, "", oc.ClickHouse.Access.RootCA)

	fakeGet := func(ns, name string) (map[string][]byte, error) {
		return map[string][]byte{"my.crt": []byte("PEM-EXPLICIT")}, nil
	}
	resolveRootCAFromSecret(&oc.ClickHouse.Access.RootCA, oc.ClickHouse.Access.RootCASecretRef.Name,
		oc.ClickHouse.Access.RootCASecretRef.Key, "op-ns", "test", fakeGet)
	require.Equal(t, "PEM-EXPLICIT", oc.ClickHouse.Access.RootCA)

	// Empty key -> ca.crt default.
	const cfgDefault = `
clickhouse:
  access:
    rootCASecretRef:
      name: only-name
`
	var oc2 api.OperatorConfig
	require.NoError(t, sigsyaml.Unmarshal([]byte(cfgDefault), &oc2))
	require.Equal(t, "only-name", oc2.ClickHouse.Access.RootCASecretRef.Name)
	require.Equal(t, "", oc2.ClickHouse.Access.RootCASecretRef.Key)

	defGet := func(ns, name string) (map[string][]byte, error) {
		return map[string][]byte{"ca.crt": []byte("PEM-DEFAULT")}, nil
	}
	resolveRootCAFromSecret(&oc2.ClickHouse.Access.RootCA, oc2.ClickHouse.Access.RootCASecretRef.Name,
		oc2.ClickHouse.Access.RootCASecretRef.Key, "op-ns", "test", defGet)
	require.Equal(t, "PEM-DEFAULT", oc2.ClickHouse.Access.RootCA)
}
