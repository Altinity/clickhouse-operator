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
	"errors"
	"fmt"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst"
)

// ErrHotReloadSecretUnresolved and ErrHotReloadCredentialRejected are returned
// by RenderHotReloadUsersXML. Callers must not publish users configuration when
// either is set.
var (
	ErrHotReloadSecretUnresolved   = errors.New("unable to read the referenced Secret key")
	ErrHotReloadCredentialRejected = errors.New("value is not a valid password hash")
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

// acceptHotReloadUserField checks that hotReload names a supported user password
// field and a Secret key in this namespace. The Secret value is read later, by
// RenderHotReloadUsersXML, so a missing or malformed credential fails the publish
// and leaves the last written configuration in place.
func (n *Normalizer) acceptHotReloadUserField(user *api.SettingsUser, name string, setting *api.Setting) {
	if !api.IsHotReloadUserAuthField(name) || !setting.HasSecretKeyRef() {
		n.rejectUnsupportedHotReload(user.Username() + "/" + name)
		return
	}
	if n.req == nil || n.req.GetTarget() == nil {
		return
	}
	if _, err := setting.FetchDataSourceAddress(n.req.GetTargetNamespace()); err != nil {
		n.rejectHotReloadSecret(user.Username(), name)
		return
	}
	n.req.GetTarget().GetRuntime().GetAttributes().SetHotReloadUsers(true)
}

// rejectHotReloadInSettings aborts when profiles, quotas, or any other settings
// section opts into hotReload. Those sections are not user authentication fields.
func (n *Normalizer) rejectHotReloadInSettings(settings *api.Settings) {
	if settings == nil {
		return
	}
	settings.WalkSafe(func(name string, setting *api.Setting) {
		if setting.IsHotReload() {
			n.rejectUnsupportedHotReload(name)
		}
	})
}

// rejectHotReload records the first hotReload failure of this normalization pass.
// Status is not carried onto the next pass, so a later reconcile can abort again
// without a flag on the Normalizer. A pass that is already aborted keeps its first error.
func (n *Normalizer) rejectHotReload(reason, message string) {
	if n.req == nil {
		return
	}
	target := n.req.GetTarget()
	if target == nil {
		return
	}
	status := target.EnsureStatus()
	if status.GetStatus() == api.StatusAborted {
		return
	}
	status.ReconcileAbortWithReason(reason, message)
}

func (n *Normalizer) rejectUnsupportedHotReload(name string) {
	n.rejectHotReload(
		api.StatusReasonHotReloadRejected,
		fmt.Sprintf(
			"setting %q: hotReload is supported only on user password, password_sha256_hex, and password_double_sha1_hex with secretKeyRef",
			name,
		),
	)
}

func (n *Normalizer) rejectHotReloadSecret(username, field string) {
	namespace := ""
	if n.req != nil && n.req.GetTarget() != nil {
		namespace = n.req.GetTargetNamespace()
	}
	n.rejectHotReload(
		api.StatusReasonHotReloadSecretUnresolved,
		fmt.Sprintf("user %q: hotReload field %q: unable to read the referenced Secret key in namespace %q", username, field, namespace),
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
			err = fmt.Errorf("user %q: hotReload field %q: %w", user.Username(), name, ErrHotReloadSecretUnresolved)
			return
		}
		value, valueErr := subst.FetchSecretFieldValue(addr, secretGet)
		if valueErr != nil || value == "" {
			err = fmt.Errorf("user %q: hotReload field %q: %w", user.Username(), name, ErrHotReloadSecretUnresolved)
			return
		}
		if !validHotReloadCredential(name, value) {
			err = fmt.Errorf("user %q: hotReload field %q: %w", user.Username(), name, ErrHotReloadCredentialRejected)
			return
		}
		user.Set(name, api.NewSettingScalar(value))
	})
	return err
}

// normalizeResolvedUserPassword applies the same authentication priority as
// normalizeConfigurationUserPassword, on a copy whose hotReload fields are already scalars.
// The default user's hash must still carry remove="1": stock users.xml ships an empty
// <password>, and ClickHouse rejects the user when that element remains beside a hash.
func normalizeResolvedUserPassword(user *api.SettingsUser) {
	n := &Normalizer{}
	n.normalizeConfigurationUserPassword(user)
	if user.Username() == defaultUsername {
		n.removePlainPassword(user)
	}
}

// validHotReloadCredential checks a Secret value before it is written.
// password is plaintext. The hash fields must already be hex of the length ClickHouse expects.
func validHotReloadCredential(name, value string) bool {
	switch hotReloadField(name) {
	case "password":
		return value != ""
	case "password_sha256_hex":
		return isHexLen(value, 64)
	case "password_double_sha1_hex":
		return isHexLen(value, 40)
	default:
		return false
	}
}

func hotReloadField(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

func isHexLen(value string, n int) bool {
	if len(value) != n {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}
