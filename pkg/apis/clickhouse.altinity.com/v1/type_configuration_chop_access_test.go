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

// Operator-managed backup runs with ClickHouse credentials the configuration sets itself, inline or by its Secret,
// never with the defaults the operator fills in for metrics and schema propagation.
func TestClickHouseAccessIsExplicitOnlyWhenTheConfigurationSetsIt(t *testing.T) {
	normalized := func(edit func(c *OperatorConfig)) *OperatorConfig {
		c := &OperatorConfig{}
		edit(c)
		c.normalizeSectionClickHouseAccess()
		return c
	}

	defaults := normalized(func(*OperatorConfig) {})
	require.Equal(t, defaultChUsername, defaults.ClickHouse.Access.Username)
	require.False(t, defaults.HasExplicitClickHouseAccess(), "the default credentials are no credentials of the configuration")

	usernameOnly := normalized(func(c *OperatorConfig) { c.ClickHouse.Access.Username = "backup" })
	require.False(t, usernameOnly.HasExplicitClickHouseAccess(), "a username without a password")

	inline := normalized(func(c *OperatorConfig) {
		c.ClickHouse.Access.Username, c.ClickHouse.Access.Password = "clickhouse_operator", "clickhouse_operator_password"
	})
	require.True(t, inline.HasExplicitClickHouseAccess(), "set inline, even to the values of the defaults")

	secret := normalized(func(c *OperatorConfig) {
		c.ClickHouse.Access.Secret.Runtime.Username, c.ClickHouse.Access.Secret.Runtime.Password = "op", "secret"
	})
	require.True(t, secret.HasExplicitClickHouseAccess(), "set by the Secret")
	require.Equal(t, "op", secret.ClickHouse.Access.Username)

	defaults.normalizeSectionClickHouseAccess()
	require.False(t, defaults.HasExplicitClickHouseAccess(), "a second normalization takes no default for the configuration's own")
	require.False(t, (*OperatorConfig)(nil).HasExplicitClickHouseAccess())
}
