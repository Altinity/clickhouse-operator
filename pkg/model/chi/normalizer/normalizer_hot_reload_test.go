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

package normalizer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

func init() {
	chop.New(nil, nil, "")
}

func hotReloadPassword(name, key string) *chi.Setting {
	return chi.NewSettingSource(&chi.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: name},
				Key:                  key,
			},
			HotReload: true,
		},
	})
}

func secretGetter(values map[string]string) func(namespace, name string) (*core.Secret, error) {
	return func(namespace, name string) (*core.Secret, error) {
		data := map[string][]byte{}
		for k, v := range values {
			data[k] = []byte(v)
		}
		return &core.Secret{Data: data}, nil
	}
}

func TestHotReloadSurvivesSettingsJSON(t *testing.T) {
	raw := []byte(`{"alice/password":{"valueFrom":{"secretKeyRef":{"name":"clickhouse-passwords","key":"alice_password"},"hotReload":true}}}`)
	settings := chi.NewSettings()
	require.NoError(t, json.Unmarshal(raw, settings))
	setting := settings.Get("alice/password")
	require.True(t, setting.IsHotReload(), "json hotReload must be kept on the setting")
	require.True(t, setting.Clone().IsHotReload(), "clone must keep hotReload")

	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi"
	cr.Namespace = "own-ns"
	cr.Spec.Configuration = &chi.Configuration{
		Users: settings,
		Clusters: []*chi.Cluster{{
			Name:   "default",
			Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
		}},
	}
	got, err := New(secretGetter(map[string]string{"alice_password": "secret-value-1"})).
		CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)
	require.True(t, got.GetRuntime().GetAttributes().GetHotReloadUsers())
	require.True(t, got.GetSpecT().GetConfiguration().GetUsers().Get("alice/password").IsHotReload())
}

func TestHotReloadPasswordRendersHashWithoutChangingTheCR(t *testing.T) {
	const password = "secret-value-1"
	sum := sha256.Sum256([]byte(password))
	wantHash := hex.EncodeToString(sum[:])

	target := &chi.ClickHouseInstallation{}
	target.Namespace = "own-ns"
	n := New(secretGetter(map[string]string{"alice_password": password}))
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	settings.Set("alice/password", hotReloadPassword("clickhouse-passwords", "alice_password"))
	settings.Set("alice/profile", chi.NewSettingScalar("default"))
	user := chi.NewSettingsUser(settings, "alice")

	n.normalizeConfigurationUser(user)

	require.NotEqual(t, chi.StatusAborted, target.EnsureStatus().GetStatus())
	require.True(t, target.GetRuntime().GetAttributes().GetHotReloadUsers())
	require.True(t, user.Get("password").IsHotReload(), "the CR keeps the Secret reference")
	require.False(t, user.Has("password_sha256_hex"), "the hash must not be stored on the CHI")
	require.NotContains(t, user.Get("password").StringFull(), "from_env")

	xml, err := RenderHotReloadUsersXML(settings, "own-ns", secretGetter(map[string]string{"alice_password": password}))
	require.NoError(t, err)
	require.Contains(t, xml, wantHash)
	require.NotContains(t, xml, password)
	require.NotContains(t, xml, "from_env")
	require.Contains(t, xml, "<profile>default</profile>")
	require.True(t, user.Get("password").IsHotReload(), "rendering must not rewrite the caller's settings")
}

func TestHotReloadKeepsProvidedSHA256(t *testing.T) {
	const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	settings := chi.NewSettings()
	settings.Set("bob/password_sha256_hex", hotReloadPassword("clickhouse-passwords", "bob_password"))

	xml, err := RenderHotReloadUsersXML(settings, "own-ns", secretGetter(map[string]string{"bob_password": hash}))
	require.NoError(t, err)
	require.Contains(t, xml, hash)
	require.Equal(t, 1, strings.Count(xml, hash), "an already-hashed value must not be hashed again")
	require.NotContains(t, xml, "from_env")
}

func TestHotReloadMissingSecretDoesNotApplyDefaultPassword(t *testing.T) {
	target := &chi.ClickHouseInstallation{}
	target.Namespace = "own-ns"
	getter := func(namespace, name string) (*core.Secret, error) {
		return nil, errNotFound
	}
	n := New(getter)
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	settings.Set("alice/password", hotReloadPassword("clickhouse-passwords", "alice_password"))
	user := chi.NewSettingsUser(settings, "alice")
	n.normalizeConfigurationUser(user)

	require.NotEqual(t, chi.StatusAborted, target.EnsureStatus().GetStatus())
	require.True(t, target.GetRuntime().GetAttributes().GetHotReloadUsers())
	require.True(t, user.Get("password").IsHotReload(), "normalization keeps the Secret reference")
	require.False(t, user.Has("password_sha256_hex"), "a missing Secret must not fall back to the default password")

	xml, err := RenderHotReloadUsersXML(settings, "own-ns", getter)
	require.ErrorIs(t, err, ErrHotReloadSecretUnresolved)
	require.Empty(t, xml)
	require.True(t, settings.Get("alice/password").IsHotReload(), "a failed render must not rewrite the CHI")
}

func TestHotReloadRejectedOnUnsupportedUserField(t *testing.T) {
	target := &chi.ClickHouseInstallation{}
	target.Namespace = "own-ns"
	n := New(secretGetter(map[string]string{"ip": "127.0.0.1"}))
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	settings.Set("alice/networks/ip", hotReloadPassword("clickhouse-passwords", "ip"))
	user := chi.NewSettingsUser(settings, "alice")
	n.normalizeConfigurationUserSecretRef(user)

	require.Equal(t, chi.StatusAborted, target.EnsureStatus().GetStatus())
	require.Contains(t, strings.Join(target.EnsureStatus().GetErrors(), " "), chi.StatusReasonHotReloadRejected)
	require.False(t, user.Get("networks/ip").HasAttributes(), "an unsupported field must not become from_env")
}

func TestHotReloadOmittedKeepsEnvMapping(t *testing.T) {
	target := &chi.ClickHouseInstallation{}
	target.Namespace = "own-ns"
	n := New(func(namespace, name string) (*core.Secret, error) {
		t.Fatal("variable mapping must not read the Secret")
		return nil, nil
	})
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	settings.Set("alice/password", chi.NewSettingSource(&chi.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "clickhouse-passwords"},
				Key:                  "alice_password",
			},
		},
	}))
	user := chi.NewSettingsUser(settings, "alice")
	n.normalizeConfigurationUserSecretRef(user)

	require.NotEqual(t, chi.StatusAborted, target.EnsureStatus().GetStatus())
	require.True(t, user.Get("password").HasAttributes())
	require.Contains(t, user.Get("password").StringFull(), "from_env")
	require.False(t, target.GetRuntime().GetAttributes().GetHotReloadUsers())
}

func TestGeneratedUsersFileOmitsHotReloadUsers(t *testing.T) {
	settings := chi.NewSettings()
	settings.Set("default/password", chi.NewSettingScalar("default"))
	settings.Set("alice/password", hotReloadPassword("clickhouse-passwords", "alice_password"))
	settings.Set("bob/password_sha256_hex", hotReloadPassword("clickhouse-passwords", "bob_password"))
	settings.Set("carol/password", chi.NewSettingSource(&chi.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "clickhouse-passwords"},
				Key:                  "carol_password",
			},
		},
	}))
	cr := &chi.ClickHouseInstallation{}
	cr.Name = "test-011-hot-reload"
	cr.Namespace = "own-ns"
	cr.Spec.Configuration = &chi.Configuration{
		Users: settings,
		Clusters: []*chi.Cluster{{
			Name:   "default",
			Layout: &chi.ChiClusterLayout{ShardsCount: 2, ReplicasCount: 1},
		}},
	}
	got, err := New(secretGetter(map[string]string{
		"alice_password": "secret-value-1",
		"bob_password":   "9f03ef1533a68d2f506f81ef463c1183a82a6bd40e45613f36e6fe1889cf1b99",
		"carol_password": "carol-secret-1",
	})).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)
	require.True(t, got.GetRuntime().GetAttributes().GetHotReloadUsers())

	users := got.GetSpecT().GetConfiguration().GetUsers()
	gen := config.NewGenerator(got, nil, &config.GeneratorOptions{Users: users})
	sections := map[string]string{}
	config.NewFilesGeneratorDomain(gen).CreateConfigFilesGroupUsers(sections)
	usersXML := sections[config.ChopGeneratedUsersConfigFilename()]
	require.Contains(t, usersXML, "<carol>")
	require.Contains(t, usersXML, "from_env")
	require.Contains(t, usersXML, `remove="1"`)
	require.NotContains(t, usersXML, "<alice>")
	require.NotContains(t, usersXML, "<bob>")

	hotXML, err := RenderHotReloadUsersXML(users, "own-ns", secretGetter(map[string]string{
		"alice_password": "secret-value-1",
		"bob_password":   "9f03ef1533a68d2f506f81ef463c1183a82a6bd40e45613f36e6fe1889cf1b99",
	}))
	require.NoError(t, err)
	require.Contains(t, hotXML, "<alice>")
	require.Contains(t, hotXML, "<bob>")
	require.NotContains(t, hotXML, "<carol>")
}

func TestHotReloadMissingSecretThenSucceedsOnTheSameNormalizer(t *testing.T) {
	var available bool
	n := New(func(namespace, name string) (*core.Secret, error) {
		if !available {
			return nil, errNotFound
		}
		return &core.Secret{Data: map[string][]byte{
			"alice_password": []byte("secret-value-1"),
			"bob_password":   []byte("secret-value-2"),
		}}, nil
	})
	cr := hotReloadUsersCHI()
	opts := commonNormalizer.NewOptions[chi.ClickHouseInstallation]()

	first, err := n.CreateTemplated(cr, opts)
	require.NoError(t, err)
	require.NotEqual(t, chi.StatusAborted, first.EnsureStatus().GetStatus())
	require.True(t, first.GetRuntime().GetAttributes().GetHotReloadUsers())
	users := first.GetSpecT().GetConfiguration().GetUsers()
	xml, err := RenderHotReloadUsersXML(users, cr.Namespace, n.secretGet)
	require.ErrorIs(t, err, ErrHotReloadSecretUnresolved)
	require.Empty(t, xml)
	require.True(t, users.Get("alice/password").IsHotReload())
	require.True(t, users.Get("bob/password").IsHotReload())

	available = true
	second, err := n.CreateTemplated(cr, opts)
	require.NoError(t, err)
	require.NotEqual(t, chi.StatusAborted, second.EnsureStatus().GetStatus())
	require.True(t, second.GetRuntime().GetAttributes().GetHotReloadUsers())
	require.True(t, second.GetSpecT().GetConfiguration().GetUsers().Get("alice/password").IsHotReload())
	require.True(t, second.GetSpecT().GetConfiguration().GetUsers().Get("bob/password").IsHotReload())
	xml, err = RenderHotReloadUsersXML(second.GetSpecT().GetConfiguration().GetUsers(), cr.Namespace, n.secretGet)
	require.NoError(t, err)
	require.Contains(t, xml, "<alice>")
	require.Contains(t, xml, "<bob>")
}

func TestHotReloadRejectedOnProfilesAndQuotas(t *testing.T) {
	n := New(secretGetter(map[string]string{"k": "v"}))
	opts := commonNormalizer.NewOptions[chi.ClickHouseInstallation]()
	for _, section := range []string{"profiles", "quotas"} {
		got, err := n.CreateTemplated(hotReloadSectionCHI(section, "readonly"), opts)
		require.NoError(t, err)
		require.Equal(t, chi.StatusAborted, got.EnsureStatus().GetStatus(), section)
		require.Contains(t, strings.Join(got.EnsureStatus().GetErrors(), " "), chi.StatusReasonHotReloadRejected, section)
	}
}

func TestHotReloadRejectsMalformedPasswordHash(t *testing.T) {
	const (
		badSHA256 = "abcd"
		badSHA1   = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	)
	for _, tc := range []struct {
		field string
		value string
	}{
		{"password_sha256_hex", badSHA256},
		{"password_double_sha1_hex", badSHA1},
	} {
		target := &chi.ClickHouseInstallation{}
		target.Namespace = "own-ns"
		n := New(secretGetter(map[string]string{"credential": tc.value}))
		n.req = NewRequest(nil)
		n.req.SetTarget(target)

		settings := chi.NewSettings()
		settings.Set("bob/"+tc.field, hotReloadPassword("clickhouse-passwords", "credential"))
		user := chi.NewSettingsUser(settings, "bob")
		n.normalizeConfigurationUser(user)

		require.NotEqual(t, chi.StatusAborted, target.EnsureStatus().GetStatus(), tc.field)
		require.True(t, target.GetRuntime().GetAttributes().GetHotReloadUsers(), tc.field)
		require.True(t, user.Get(tc.field).IsHotReload(), "normalization keeps the Secret reference")
		require.NotEqual(t, tc.value, user.Get(tc.field).String())

		xml, err := RenderHotReloadUsersXML(settings, "own-ns", secretGetter(map[string]string{"credential": tc.value}))
		require.ErrorIs(t, err, ErrHotReloadCredentialRejected)
		require.Empty(t, xml)
		require.True(t, settings.Get("bob/"+tc.field).IsHotReload(), "a failed render must not rewrite the CHI")
	}
}

func TestHotReloadDefaultUserHashKeepsPasswordRemove(t *testing.T) {
	const password = "secret-value-1"
	sum := sha256.Sum256([]byte(password))
	wantHash := hex.EncodeToString(sum[:])

	settings := chi.NewSettings()
	settings.Set("default/password", hotReloadPassword("clickhouse-passwords", "default_password"))
	xml, err := RenderHotReloadUsersXML(settings, "own-ns", secretGetter(map[string]string{"default_password": password}))
	require.NoError(t, err)
	require.Contains(t, xml, wantHash)
	require.Contains(t, xml, `remove="1"`)
	require.NotContains(t, xml, password)
	require.True(t, settings.Get("default/password").IsHotReload())
}

func hotReloadUsersCHI() *chi.ClickHouseInstallation {
	users := chi.NewSettings()
	users.Set("alice/password", hotReloadPassword("clickhouse-passwords", "alice_password"))
	users.Set("bob/password", hotReloadPassword("clickhouse-passwords", "bob_password"))
	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi"
	cr.Namespace = "own-ns"
	cr.Spec.Configuration = &chi.Configuration{
		Users: users,
		Clusters: []*chi.Cluster{{
			Name:   "default",
			Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
		}},
	}
	return cr
}

func hotReloadSectionCHI(section, key string) *chi.ClickHouseInstallation {
	cr := &chi.ClickHouseInstallation{}
	cr.Name = "chi"
	cr.Namespace = "own-ns"
	settings := chi.NewSettings()
	settings.Set(key, hotReloadPassword("clickhouse-passwords", "k"))
	conf := &chi.Configuration{
		Clusters: []*chi.Cluster{{
			Name:   "default",
			Layout: &chi.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1},
		}},
	}
	switch section {
	case "settings":
		conf.Settings = settings
	case "profiles":
		conf.Profiles = settings
	case "quotas":
		conf.Quotas = settings
	}
	cr.Spec.Configuration = conf
	return cr
}

var errNotFound = errString("not found")

type errString string

func (e errString) Error() string { return string(e) }
