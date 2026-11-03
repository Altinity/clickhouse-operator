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

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The deprecated flat `chScheme` key must migrate into ClickHouse.Access.Scheme. It used to land
// in Access.Password instead, which both lost the scheme and corrupted the operator's ClickHouse
// password with the string "http"/"https" - silently, for anyone still using the flat key.
func TestDeprecatedCHSchemeMigratesToScheme(t *testing.T) {
	c := &OperatorConfig{}
	c.CHScheme = ChSchemeHTTPS

	c.move()

	require.Equal(t, ChSchemeHTTPS, c.ClickHouse.Access.Scheme, "chScheme must migrate into Access.Scheme")
	require.Empty(t, c.ClickHouse.Access.Password, "chScheme must not leak into Access.Password")
}

// The three flat credential keys are independent: each must reach its own field. Ordering matters
// because chPassword is applied after chScheme and would mask a mis-targeted chScheme.
func TestDeprecatedFlatCredentialsMigrateIndependently(t *testing.T) {
	c := &OperatorConfig{}
	c.CHScheme = ChSchemeHTTP
	c.CHUsername = "operator"
	c.CHPassword = "secret"

	c.move()

	require.Equal(t, ChSchemeHTTP, c.ClickHouse.Access.Scheme)
	require.Equal(t, "operator", c.ClickHouse.Access.Username)
	require.Equal(t, "secret", c.ClickHouse.Access.Password)
}
