// Copyright 2019 Altinity Ltd and/or its affiliates.
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

package normalizer

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func normalizeTestUser(settingsByName map[string]*chi.Setting) *chi.SettingsUser {
	target := &chi.ClickHouseInstallation{}

	n := New(nil)
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	for name, value := range settingsByName {
		settings.Set("user1/"+name, value)
	}
	user := chi.NewSettingsUser(settings, "user1")
	n.normalizeConfigurationUser(user)
	return user
}

func requireNoPassword(t *testing.T, user *chi.SettingsUser) {
	t.Helper()
	for _, field := range []string{"password", "password_sha256_hex", "password_double_sha1_hex"} {
		require.False(t, user.Has(field), "ClickHouse accepts a single authentication method per user, but %q was added", field)
	}
}

// A user that already has a non-password authentication method must not get the template
// default password: ClickHouse rejects a user with more than one method.
func TestUserWithOtherAuthMethodGetsNoDefaultPassword(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]*chi.Setting
		authKey  string
	}{
		{"ssl_certificates common_name", map[string]*chi.Setting{"ssl_certificates/common_name": chi.NewSettingScalar("cn")}, "ssl_certificates/common_name"},
		{"ssl_certificates subject_alt_name", map[string]*chi.Setting{"ssl_certificates/subject_alt_name": chi.NewSettingScalar("URI:spiffe://example.org/ns/x/sa/y")}, "ssl_certificates/subject_alt_name"},
		{"ldap", map[string]*chi.Setting{"ldap/server": chi.NewSettingScalar("myldap")}, "ldap/server"},
		{"kerberos", map[string]*chi.Setting{"kerberos/realm": chi.NewSettingScalar("EXAMPLE.ORG")}, "kerberos/realm"},
		{"http_authentication", map[string]*chi.Setting{"http_authentication/scheme": chi.NewSettingScalar("basic")}, "http_authentication/scheme"},
		{"ssh_keys", map[string]*chi.Setting{"ssh_keys/ssh_key/type": chi.NewSettingScalar("ssh-ed25519")}, "ssh_keys/ssh_key/type"},
		{"no_password", map[string]*chi.Setting{"no_password": chi.NewSettingScalar("")}, "no_password"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user := normalizeTestUser(tt.settings)
			requireNoPassword(t, user)
			require.True(t, user.Has(tt.authKey), "the configured authentication method must be kept")
		})
	}
}

// Users without any authentication method keep getting the template default password,
// and an explicit plaintext password is still hashed.
func TestUserPasswordHandlingUnchanged(t *testing.T) {
	t.Run("no auth method gets default password", func(t *testing.T) {
		user := normalizeTestUser(map[string]*chi.Setting{"networks/ip": chi.NewSettingScalar("::/0")})
		require.True(t, user.Has("password_sha256_hex"))
	})
	t.Run("explicit password is hashed", func(t *testing.T) {
		user := normalizeTestUser(map[string]*chi.Setting{"password": chi.NewSettingScalar("secret")})
		require.True(t, user.Has("password_sha256_hex"))
		require.False(t, user.Has("password"))
	})
}
