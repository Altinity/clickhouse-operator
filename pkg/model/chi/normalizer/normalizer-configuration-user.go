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
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/subst_settings"
)

const envVarNamePrefixConfigurationUsers = "CONFIGURATION_USERS"

func (n *Normalizer) normalizeConfigurationUser(user *api.SettingsUser) {
	n.normalizeConfigurationUserSecretRef(user)
	n.normalizeConfigurationUserPassword(user)
	n.normalizeConfigurationUserEnsureMandatoryFields(user)
}

func (n *Normalizer) normalizeConfigurationUserSecretRef(user *api.SettingsUser) {
	user.WalkSafe(func(name string, _ *api.Setting) {
		if strings.HasPrefix(name, "k8s_secret_") {
			// TODO remove as obsoleted
			// Skip this user field, it will be processed later
		} else {
			subst_settings.SubstSettingsFieldWithEnvRefToSecretField(
				n.ctx,
				user,
				name,
				name,
				envVarNamePrefixConfigurationUsers,
				false,
			)
		}
	})
}

// normalizeConfigurationUserPassword deals with user passwords
func (n *Normalizer) normalizeConfigurationUserPassword(user *api.SettingsUser) {
	// Values from the secret have higher priority
	subst_settings.SubstSettingsFieldWithSecretFieldValue(n.ctx, user, "password", "k8s_secret_password", n.secretGet)
	subst_settings.SubstSettingsFieldWithSecretFieldValue(n.ctx, user, "password_double_sha1_hex", "k8s_secret_password_double_sha1_hex", n.secretGet)
	subst_settings.SubstSettingsFieldWithSecretFieldValue(n.ctx, user, "password_sha256_hex", "k8s_secret_password_sha256_hex", n.secretGet)

	// Values from the secret passed via ENV have even higher priority
	subst_settings.SubstSettingsFieldWithEnvRefToSecretField(n.ctx, user, "password", "k8s_secret_env_password", envVarNamePrefixConfigurationUsers, true)
	subst_settings.SubstSettingsFieldWithEnvRefToSecretField(n.ctx, user, "password_sha256_hex", "k8s_secret_env_password_sha256_hex", envVarNamePrefixConfigurationUsers, true)
	subst_settings.SubstSettingsFieldWithEnvRefToSecretField(n.ctx, user, "password_double_sha1_hex", "k8s_secret_env_password_double_sha1_hex", envVarNamePrefixConfigurationUsers, true)

	// Out of all passwords, password_double_sha1_hex has top priority, thus keep it only
	if user.Has("password_double_sha1_hex") {
		user.Delete("password_sha256_hex")
		user.Delete("password")
		// This is all for this user
		return
	}

	// Than goes password_sha256_hex, thus keep it only
	if user.Has("password_sha256_hex") {
		user.Delete("password_double_sha1_hex")
		user.Delete("password")
		// This is all for this user
		return
	}

	// From now on we either have a plaintext password specified (explicitly or via ENV), or no password at all

	if user.Get("password").HasAttributes() {
		// Have plaintext password with attributes - means we have plaintext password explicitly specified via ENV var
		// This is fine
		// This is all for this user
		return
	}

	// From now on we either have plaintext password specified as an explicit string, or no password at all

	passwordPlaintext := user.Get("password").String()

	// Apply default password for password-less non-default users
	// 1. NB "default" user keeps empty password in here.
	// 2. ClickHouse user gets password from his section of CHOp configuration
	// 3. All the rest users get default password
	if passwordPlaintext == "" {
		switch user.Username() {
		case defaultUsername:
			// NB "default" user keeps empty password in here.
		case chop.Config().ClickHouse.Access.Username:
			// User used by CHOp to access ClickHouse instances.
			// Gets ClickHouse access password from "ClickHouse.Access.Password"
			passwordPlaintext = chop.Config().ClickHouse.Access.Password
		default:
			// All the rest users get default password from "ClickHouse.Config.User.Default.Password"
			passwordPlaintext = chop.Config().ClickHouse.Config.User.Default.Password
		}
	}

	// It may come that plaintext password is still empty.
	// For example, user `default` quite often has empty password.
	if passwordPlaintext == "" {
		// This is fine
		// This is all for this user
		return
	}

	// Have plaintext password specified.
	// Replace plaintext password with encrypted one
	passwordSHA256 := sha256.Sum256([]byte(passwordPlaintext))
	user.Set("password_sha256_hex", api.NewSettingScalar(hex.EncodeToString(passwordSHA256[:])))
	// And keep only one password specification - delete all the rest (if any exists)
	user.Delete("password_double_sha1_hex")
	user.Delete("password")
}

func (n *Normalizer) normalizeConfigurationUserEnsureMandatoryFields(user *api.SettingsUser) {
	//
	// Ensure each user has mandatory fields:
	//
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/networks/host_regexp
	profile := chop.Config().ClickHouse.Config.User.Default.Profile
	quota := chop.Config().ClickHouse.Config.User.Default.Quota
	ips := append([]string{}, chop.Config().ClickHouse.Config.User.Default.NetworksIP...)
	hostRegexp := n.namer.Name(interfaces.NamePodHostnameRegexp, n.ctx.GetTarget(), chop.Config().ClickHouse.Config.Network.HostRegexpTemplate)

	// Some users may have special options for mandatory fields
	switch user.Username() {
	case defaultUsername:
		// "default" user
		ips = append(ips, n.ctx.Options().DefaultUserAdditionalIPs...)
		if !n.ctx.Options().DefaultUserInsertHostRegex {
			hostRegexp = ""
		}
	case chop.Config().ClickHouse.Access.Username:
		// User used by CHOp to access ClickHouse instances.
		ip, _ := chop.Get().ConfigManager.GetRuntimeParam(deployment.OPERATOR_POD_IP)

		profile = chopProfile
		quota = ""
		ips = []string{ip}
		hostRegexp = ""
	}

	// Ensure required values are in place and apply non-empty values in case no own value(s) provided
	n.setMandatoryUserFields(user, &userFields{
		profile:    profile,
		quota:      quota,
		ips:        ips,
		hostRegexp: hostRegexp,
	})
}

type userFields struct {
	profile    string
	quota      string
	ips        []string
	hostRegexp string
}

// setMandatoryUserFields sets user fields
func (n *Normalizer) setMandatoryUserFields(user *api.SettingsUser, fields *userFields) {
	// Ensure required values are in place and apply non-empty values in case no own value(s) provided
	if fields.profile != "" {
		user.SetIfNotExists("profile", api.NewSettingScalar(fields.profile))
	}
	if fields.quota != "" {
		user.SetIfNotExists("quota", api.NewSettingScalar(fields.quota))
	}
	if len(fields.ips) > 0 {
		user.Set("networks/ip", api.NewSettingVector(fields.ips).MergeFrom(user.Get("networks/ip")))
	}
	if fields.hostRegexp != "" {
		user.SetIfNotExists("networks/host_regexp", api.NewSettingScalar(fields.hostRegexp))
	}
}
