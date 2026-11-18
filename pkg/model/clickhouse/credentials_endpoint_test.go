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

package clickhouse

import (
	"strings"
	"testing"

	goch "github.com/mailru/go-clickhouse/v2"
	"github.com/stretchr/testify/require"
)

// TestEndpointDSN_ReservedCharsInCredentials verifies that the driver reads back
// the exact username and password, even when they hold URL-reserved characters.
func TestEndpointDSN_ReservedCharsInCredentials(t *testing.T) {
	cases := []struct {
		name     string
		username string
		password string
	}{
		{"plain", "clickhouse_operator", "clickhouse_operator_password"},
		{"hash", "clickhouse_operator", "cs#secret"},
		{"at, slash, question mark", "clickhouse_operator", "p@ss/w?rd"},
		{"colon", "user:name", "pass:word"},
		{"percent", "clickhouse_operator", "100%25sure%"},
		{"space and plus", "clickhouse_operator", "a b+c"},
		{"no password", "clickhouse_operator", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, scheme := range []string{"http", "https"} {
				c := NewEndpointCredentials(scheme, "chi-host", tc.username, tc.password, "", 8123)
				for _, dsn := range []string{c.GetDSN(), c.GetDSNLogQueries()} {
					cfg, err := goch.ParseDSN(dsn)
					require.NoError(t, err)
					require.Equal(t, "chi-host:8123", cfg.Host)
					require.Equal(t, tc.username, cfg.User)
					require.Equal(t, tc.password, cfg.Password)
				}
			}
		})
	}
}

// TestEndpointDSN_HiddenCredentials verifies that the DSN used for logging does
// not contain the password, encoded or not.
func TestEndpointDSN_HiddenCredentials(t *testing.T) {
	c := NewEndpointCredentials("http", "chi-host", "clickhouse_operator", "cs#secret", "", 8123)
	dsn := c.GetDSNWithHiddenCredentials()
	require.Equal(t, "http://clickhouse_operator:***@chi-host:8123", dsn)
	require.False(t, strings.Contains(dsn, "secret"))
}
