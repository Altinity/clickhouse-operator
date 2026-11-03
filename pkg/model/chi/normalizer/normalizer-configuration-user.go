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
	"fmt"
	"slices"
	"strings"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst"
)

const envVarNamePrefixConfigurationUsers = "CONFIGURATION_USERS"

// Fields of a user's settings
const (
	userFieldProfile               = "profile"
	userFieldQuota                 = "quota"
	userFieldPassword              = "password"
	userFieldPasswordSHA256Hex     = "password_sha256_hex"
	userFieldPasswordDoubleSHA1Hex = "password_double_sha1_hex"
	userFieldNetworksIP            = "networks/ip"
	userFieldNetworksHostRegexp    = "networks/host_regexp"
)

// removedSecretRefFieldPrefix is the user settings field prefix removed in 0.27.4. It covers
// `k8s_secret_env_` too, which shares it. The syntax took a namespace/name/key triple and read
// the Secret with the operator's own ServiceAccount, so it could reach any namespace.
// Superseded by valueFrom.secretKeyRef, which cannot name a namespace at all.
const removedSecretRefFieldPrefix = "k8s_secret_"

// ensureBackupUser sets up the user the clickhouse-backup sidecars connect to ClickHouse as. The operator owns
// it: whatever the CR sets for it, its profile and quota aside, is dropped - another authentication method or
// network would lock the sidecars out or let the user in from outside the pod.
//
// Its password is read from the backup Secret here rather than passed through an env var: users are common for
// all clusters, so the env var would go into every ClickHouse container of the CR, and enabling backup on one
// cluster would restart every host. Set as plaintext, the password is replaced by its hash like any user's; it
// is random, so the hash in the users ConfigMap gives nothing away. Until the Secret is there - the operator
// creates it before it normalizes the CR for reconcile - the user is left out altogether.
func (n *Normalizer) ensureBackupUser(users *api.Settings) {
	password, ok := n.fetchBackupPassword()
	user := api.NewSettingsUser(users, config.ClickHouseBackupUsername)
	users.WalkKeysSafe(func(key string, _ *api.Setting) {
		name, own := strings.CutPrefix(key, user.Name2Key(""))
		keep := ok && ((name == userFieldProfile) || (name == userFieldQuota))
		if own && !keep {
			users.DeleteKey(key)
		}
	})
	if !ok {
		return
	}

	user.Set(userFieldPassword, api.NewSettingScalar(password))
	user.Set(userFieldNetworksIP, api.NewSettingVector(slices.Clone(config.ClickHouseBackupUserNetworksIP)))
}

// fetchBackupPassword fetches the password of the backup user from the backup Secret, reporting whether there is
// one. Only a Secret not there yet, or not filled in yet, leaves the user pending: the operator completes the
// Secret and normalizes anew. A Secret that cannot be read for any other reason aborts the CR, as going on
// without the user would lock every sidecar out.
func (n *Normalizer) fetchBackupPassword() (string, bool) {
	if n.secretGet == nil {
		return "", false
	}
	namespace := n.req.GetTargetNamespace()
	name := n.namer.Name(interfaces.NameBackupAutoSecret, n.req.GetTarget())
	secret, err := n.secretGet(namespace, name)
	switch {
	case apiErrors.IsNotFound(err):
		return "", false
	case err != nil:
		n.req.GetTarget().EnsureStatus().ReconcileAbortWithReason(
			api.StatusReasonBackupSecretUnresolved,
			fmt.Sprintf("backup Secret %s/%s cannot be read: %v", namespace, name, err),
		)
		return "", false
	}
	password := string(secret.Data[config.ClickHouseBackupSecretKeyClickHousePassword])
	return password, password != ""
}

func (n *Normalizer) normalizeConfigurationUser(user *api.SettingsUser) {
	n.normalizeConfigurationUserSecretRef(user)
	n.normalizeConfigurationUserPassword(user)
	n.normalizeConfigurationUserEnsureMandatoryFields(user)
}

func (n *Normalizer) normalizeConfigurationUserSecretRef(user *api.SettingsUser) {
	user.WalkSafe(func(name string, _ *api.Setting) {
		if strings.HasPrefix(name, removedSecretRefFieldPrefix) {
			n.rejectRemovedSecretRefField(user, name)
		} else {
			subst.ReplaceSettingsFieldWithEnvRefToSecretField(
				n.req,
				user,
				name,
				name,
				envVarNamePrefixConfigurationUsers,
			)
		}
	})
}

// rejectRemovedSecretRefField drops a user field that still uses the removed k8s_secret_ /
// k8s_secret_env_ syntax and aborts the reconcile - at most once per normalization pass, however
// many fields and users carry it.
//
// Aborting is the point: the two alternatives both fail open. Leaving the field in place renders
// it verbatim into the generated users ConfigMap, publishing the Secret's coordinates; and either
// way the account ends up with no password of its own, so normalizeConfigurationUserPassword
// hands it ClickHouse.Config.User.Default.Password - the literal string "default" as shipped.
// A Secret-protected account would silently become reachable with a documented credential.
func (n *Normalizer) rejectRemovedSecretRefField(user *api.SettingsUser, name string) {
	// Drop every offending field, so none of them can reach the generated config.
	user.Delete(name)

	target := n.req.GetTarget()
	if target == nil {
		return
	}

	// Abort once per pass. The walk visits every field of every user, and each abort also pushes a
	// completed-taskID entry, so repeating it fills status.errors and status.taskIDsCompleted with
	// near-identical entries and evicts anything useful - both are capped at 10.
	if n.req.RemovedSecretRefReported() {
		return
	}
	target.EnsureStatus().ReconcileAbortWithReason(
		api.StatusReasonRemovedSecretRefSyntax,
		fmt.Sprintf(
			"user %q: setting %q uses the k8s_secret_/k8s_secret_env_ syntax removed in 0.27.4 - "+
				"migrate every such field to valueFrom.secretKeyRef, see docs/security_hardening.md",
			user.Username(), name,
		),
	)
}

// normalizeConfigurationUserPassword deals with user passwords
func (n *Normalizer) normalizeConfigurationUserPassword(user *api.SettingsUser) {
	// Out of all passwords, password_double_sha1_hex has top priority, thus keep it only
	if user.Has(userFieldPasswordDoubleSHA1Hex) {
		user.Delete(userFieldPasswordSHA256Hex)
		user.Delete(userFieldPassword)
		// This is all for this user
		return
	}

	// Than by priority goes password_sha256_hex, thus keep it only
	if user.Has(userFieldPasswordSHA256Hex) {
		user.Delete(userFieldPasswordDoubleSHA1Hex)
		user.Delete(userFieldPassword)
		// This is all for this user
		return
	}

	// From now on we either have a plaintext password specified (explicitly or via ENV), or no password at all

	if user.Get(userFieldPassword).HasAttributes() {
		// Have plaintext password with attributes - means we have plaintext password explicitly specified via ENV var
		// This is fine
		// This is all for this user
		return
	}

	// From now on we either have plaintext password specified as an explicit string, or no password at all

	passwordPlaintext := user.Get(userFieldPassword).String()

	// Apply default password for password-less non-default users
	// 1. NB "default" user keeps empty password in here.
	// 2. CHOp ClickHouse user gets password from the section of CHOp configuration "ClickHouse.Access.Password"
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
	// Password shouldn't be used when ssl_certificate set
	if !user.Has("ssl_certificates/common_name") {
		passwordSHA256 := sha256.Sum256([]byte(passwordPlaintext))
		user.Set(userFieldPasswordSHA256Hex, api.NewSettingScalar(hex.EncodeToString(passwordSHA256[:])))
		// And keep only one password specification - delete all the rest (if any exists)
		user.Delete(userFieldPasswordDoubleSHA1Hex)
		user.Delete(userFieldPassword)
	}
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
	hostRegexp := n.namer.Name(interfaces.NamePodHostnameRegexp, n.req.GetTarget(), chop.Config().ClickHouse.Config.Network.HostRegexpTemplate)

	// Some users may have special options for mandatory fields
	switch user.Username() {
	case defaultUsername:
		// "default" user
		ips = append(ips, n.req.Options().DefaultUserAdditionalIPs...)
		if !n.req.Options().DefaultUserInsertHostRegex {
			hostRegexp = ""
		}
	case chop.Config().ClickHouse.Access.Username:
		// User used by CHOp to access ClickHouse instances.
		ip, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_IP)

		profile = chopProfile
		quota = ""
		ips = []string{ip}
		hostRegexp = ""
	case config.ClickHouseBackupUsername:
		// User the clickhouse-backup sidecars connect as - from inside the pod only, never from the default networks.
		// Without operator-managed backup a user of this name is the installation's own.
		if n.req.GetTarget().IsBackupEnabled() {
			ips = slices.Clone(config.ClickHouseBackupUserNetworksIP)
			hostRegexp = ""
		}
	}

	// Ensure required values are in place and apply non-empty values in case no own value(s) provided
	setMandatoryUserFields(user, &userFields{
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
func setMandatoryUserFields(user *api.SettingsUser, fields *userFields) {
	// Ensure required values are in place and apply non-empty values in case no own value(s) provided
	if fields.profile != "" {
		user.SetIfNotExists(userFieldProfile, api.NewSettingScalar(fields.profile))
	}
	if fields.quota != "" {
		user.SetIfNotExists(userFieldQuota, api.NewSettingScalar(fields.quota))
	}
	if len(fields.ips) > 0 {
		user.Set(userFieldNetworksIP, api.NewSettingVector(fields.ips).MergeFrom(user.Get(userFieldNetworksIP)))
	}
	if fields.hostRegexp != "" {
		user.SetIfNotExists(userFieldNetworksHostRegexp, api.NewSettingScalar(fields.hostRegexp))
	}
}
