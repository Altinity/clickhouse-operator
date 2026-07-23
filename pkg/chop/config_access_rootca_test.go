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

// TestAccessRootCASecretRefMergesFromCR proves the ClickHouseOperatorConfiguration
// (CRD) path reaches the same resolver as the file config: getAllCRBasedConfigs ->
// buildUnifiedConfig -> OperatorConfig.MergeFrom (mergo deep-merge) must carry the
// nested anonymous-struct access.rootCASecretRef from a CR spec into the unified
// config, where fetchAccessRootCA then resolves it. Guards CRD parity for the field.
func TestAccessRootCASecretRefMergesFromCR(t *testing.T) {
	// base = file config with no access ref; cr = a chopconf CR spec carrying the ref.
	base := &api.OperatorConfig{}
	const crSpec = `
clickhouse:
  access:
    rootCASecretRef:
      name: cr-ca-secret
      key: cr.crt
`
	var cr api.OperatorConfig
	require.NoError(t, sigsyaml.Unmarshal([]byte(crSpec), &cr))
	require.NoError(t, base.MergeFrom(&cr))

	// The ref survived the mergo deep-merge of the anonymous Access struct.
	require.Equal(t, "cr-ca-secret", base.ClickHouse.Access.RootCASecretRef.Name)
	require.Equal(t, "cr.crt", base.ClickHouse.Access.RootCASecretRef.Key)

	// ...and resolves through the shared resolver into Access.RootCA.
	fakeGet := func(ns, name string) (map[string][]byte, error) {
		return map[string][]byte{"cr.crt": []byte("PEM-FROM-CR-SECRET")}, nil
	}
	resolveRootCAFromSecret(&base.ClickHouse.Access.RootCA, base.ClickHouse.Access.RootCASecretRef.Name,
		base.ClickHouse.Access.RootCASecretRef.Key, "op-ns", "test cr-merge", fakeGet)
	require.Equal(t, "PEM-FROM-CR-SECRET", base.ClickHouse.Access.RootCA)
}

// TestAccessRootCASecretRefMergePrecedence locks how access CA settings merge across
// layered config sources (file + ClickHouseOperatorConfiguration CRs). RootCASecretRef
// is a value struct, so OperatorConfig.MergeFrom (mergo WithOverride) merges it FIELD
// BY FIELD: a higher-priority source's empty field does NOT clear a lower-priority
// non-empty one. This matches every other clickhouse.access.* value field (username,
// password, secret.*); only the security.clickhouse.tls pointer ref replaces wholesale.
// Consequence: layered configs should set the FULL ref, not a partial override.
func TestAccessRootCASecretRefMergePrecedence(t *testing.T) {
	// A higher-priority CR ref does NOT override a lower-priority inline rootCA
	// (mergo keeps the non-empty inline); the resolver then applies inline-wins.
	base := &api.OperatorConfig{}
	base.ClickHouse.Access.RootCA = "FILE-INLINE"
	cr := &api.OperatorConfig{}
	cr.ClickHouse.Access.RootCASecretRef.Name = "cr-secret"
	require.NoError(t, base.MergeFrom(cr))
	require.Equal(t, "FILE-INLINE", base.ClickHouse.Access.RootCA)
	require.Equal(t, "cr-secret", base.ClickHouse.Access.RootCASecretRef.Name)

	// A higher-priority CR overriding only the name RETAINS the lower-priority key
	// (field-by-field merge) — hence "set the full ref in layered configs".
	b2 := &api.OperatorConfig{}
	b2.ClickHouse.Access.RootCASecretRef.Name = "file-secret"
	b2.ClickHouse.Access.RootCASecretRef.Key = "file.crt"
	c2 := &api.OperatorConfig{}
	c2.ClickHouse.Access.RootCASecretRef.Name = "cr-secret"
	require.NoError(t, b2.MergeFrom(c2))
	require.Equal(t, "cr-secret", b2.ClickHouse.Access.RootCASecretRef.Name)
	require.Equal(t, "file.crt", b2.ClickHouse.Access.RootCASecretRef.Key)
}
