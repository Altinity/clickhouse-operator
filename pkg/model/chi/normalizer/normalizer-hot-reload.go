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
	"fmt"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst"
)

func userHasHotReloadCredential(user *api.SettingsUser) bool {
	// Get is scoped to this user. Walk is not: SettingsUser shares the settings
	// map, so a walk would see every other user's hotReload password and skip
	// hashing for default.
	for _, field := range []string{"password", "password_sha256_hex", "password_double_sha1_hex"} {
		if user.Get(field).IsHotReload() {
			return true
		}
	}
	return false
}

// acceptHotReloadUserField checks that hotReload is on a supported user password
// field and that the Secret key can be read. The value is not kept: the CR continues
// to store the reference, and RenderHotReloadUsersXML reads it again when writing
// the managed Secret.
func (n *Normalizer) acceptHotReloadUserField(user *api.SettingsUser, name string, setting *api.Setting) {
	if !api.IsHotReloadUserAuthField(name) || !setting.HasSecretKeyRef() {
		n.rejectHotReload(user.Username() + "/" + name)
		return
	}
	if n.req == nil || n.req.GetTarget() == nil || n.secretGet == nil {
		n.rejectHotReloadSecret(user.Username(), name)
		return
	}
	addr, err := setting.FetchDataSourceAddress(n.req.GetTargetNamespace())
	if err != nil {
		n.rejectHotReloadSecret(user.Username(), name)
		return
	}
	value, err := subst.FetchSecretFieldValue(addr, n.secretGet)
	if err != nil || value == "" {
		n.rejectHotReloadSecret(user.Username(), name)
		return
	}
	n.req.GetTarget().GetRuntime().GetAttributes().SetHotReloadUsers(true)
}

func (n *Normalizer) rejectHotReload(name string) {
	target := n.req.GetTarget()
	if target == nil || n.hotReloadReported {
		return
	}
	n.hotReloadReported = true
	target.EnsureStatus().ReconcileAbortWithReason(
		api.StatusReasonHotReloadRejected,
		fmt.Sprintf(
			"setting %q: hotReload is supported only on user password, password_sha256_hex, and password_double_sha1_hex with secretKeyRef",
			name,
		),
	)
}

func (n *Normalizer) rejectHotReloadSecret(username, field string) {
	target := n.req.GetTarget()
	if target == nil || n.hotReloadReported {
		return
	}
	n.hotReloadReported = true
	target.EnsureStatus().ReconcileAbortWithReason(
		api.StatusReasonHotReloadSecretUnresolved,
		fmt.Sprintf("user %q: hotReload field %q: unable to read the referenced Secret key in namespace %q", username, field, n.req.GetTargetNamespace()),
	)
}

// RenderHotReloadUsersXML builds chop-generated-hot-reload-users.xml from Secret
// values. Only users that opt into hotReload are included. The caller's settings
// are not modified, so password material is not written back onto the CHI. An
// error leaves the caller free to keep the last written Secret.
func RenderHotReloadUsersXML(users *api.Settings, namespace string, secretGet subst.SecretGetter) (string, error) {
	if users == nil || secretGet == nil {
		return "", fmt.Errorf("hotReload users configuration is incomplete")
	}
	resolved := users.OnlyHotReloadUsers()
	usernames := resolved.HotReloadUsernames()
	if len(usernames) == 0 {
		return "", fmt.Errorf("hotReload users configuration has no password fields")
	}
	for _, username := range usernames {
		user := api.NewSettingsUser(resolved, username)
		if err := resolveHotReloadUser(user, namespace, secretGet); err != nil {
			return "", err
		}
		normalizeResolvedUserPassword(user)
	}
	xml := resolved.ClickHouseConfig(config.UsersSection)
	if xml == "" || strings.Contains(xml, ">data source<") {
		return "", fmt.Errorf("hotReload users configuration did not render")
	}
	return xml, nil
}

func resolveHotReloadUser(user *api.SettingsUser, namespace string, secretGet subst.SecretGetter) error {
	var err error
	user.WalkSafe(func(name string, setting *api.Setting) {
		if err != nil || !setting.IsHotReload() {
			return
		}
		if !api.IsHotReloadUserAuthField(name) || !setting.HasSecretKeyRef() {
			err = fmt.Errorf("user %q: hotReload is not supported on %q", user.Username(), name)
			return
		}
		addr, addrErr := setting.FetchDataSourceAddress(namespace)
		if addrErr != nil {
			err = fmt.Errorf("user %q: hotReload field %q: unable to read the referenced Secret key", user.Username(), name)
			return
		}
		value, valueErr := subst.FetchSecretFieldValue(addr, secretGet)
		if valueErr != nil || value == "" {
			err = fmt.Errorf("user %q: hotReload field %q: unable to read the referenced Secret key", user.Username(), name)
			return
		}
		user.Set(name, api.NewSettingScalar(value))
	})
	return err
}

// normalizeResolvedUserPassword applies the same authentication priority as
// normalizeConfigurationUserPassword, on a copy whose hotReload fields are already scalars.
func normalizeResolvedUserPassword(user *api.SettingsUser) {
	n := &Normalizer{}
	n.normalizeConfigurationUserPassword(user)
}
