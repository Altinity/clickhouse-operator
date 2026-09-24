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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// removedSecretRefFields is every field the k8s_secret_ / k8s_secret_env_ syntax accepted.
var removedSecretRefFields = []string{
	"k8s_secret_password",
	"k8s_secret_password_sha256_hex",
	"k8s_secret_password_double_sha1_hex",
	"k8s_secret_env_password",
	"k8s_secret_env_password_sha256_hex",
	"k8s_secret_env_password_double_sha1_hex",
}

// The removed syntax must never reach the Kubernetes API. The secretGet below fails the test if
// called, which is the actual proof: before removal these fields resolved a Secret by
// namespace/name/key, using the operator's ServiceAccount, from any namespace.
func TestRemovedSecretRefSyntaxNeverReadsSecret(t *testing.T) {
	for _, field := range removedSecretRefFields {
		t.Run(field, func(t *testing.T) {
			secretGet := func(namespace, name string) (*core.Secret, error) {
				t.Fatalf("removed syntax %q reached the k8s API: secret %s/%s", field, namespace, name)
				return nil, nil
			}

			target := &chi.ClickHouseInstallation{}
			target.Namespace = "own-ns"

			n := New(secretGet)
			n.req = NewRequest(nil)
			n.req.SetTarget(target)

			settings := chi.NewSettings()
			settings.Set("user1/"+field, chi.NewSettingScalar("other-ns/creds/password"))
			user := chi.NewSettingsUser(settings, "user1")

			n.normalizeConfigurationUser(user)

			require.Equal(t, chi.StatusAborted, target.EnsureStatus().GetStatus(),
				"a CR using the removed syntax must abort, not reconcile")
			require.Contains(t, strings.Join(target.EnsureStatus().GetErrors(), " "),
				chi.StatusReasonRemovedSecretRefSyntax)
			require.False(t, user.Has(field),
				"the removed field must be dropped, otherwise it renders into the users ConfigMap "+
					"and publishes the Secret's coordinates")
		})
	}
}

// Documents WHY the abort has to be honoured by every writer of the users config, rather than
// being treated as advisory.
//
// A user whose only credential used the removed syntax has no password left, so the normalizer's
// fallback hands it ClickHouse.Config.User.Default.Password - the literal string "default" as
// shipped. Suppressing that fallback would be worse, not better: an account rendered with no
// password element at all is open. So the normalizer's output for such a CR is unsafe by
// construction, and the protection is that an aborted CR's config is never written -
// see finalizeCR in pkg/controller/chi/worker.go, which skips the users config map when aborted.
//
// If this test ever starts failing, the fallback changed and that gate should be revisited.
func TestRemovedSecretRefSyntaxLeavesUnsafeOutputBehindTheAbort(t *testing.T) {
	target := &chi.ClickHouseInstallation{}
	target.Namespace = "own-ns"

	n := New(nil)
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	settings.Set("user1/k8s_secret_password", chi.NewSettingScalar("other-ns/creds/password"))
	user := chi.NewSettingsUser(settings, "user1")

	n.normalizeConfigurationUser(user)

	require.Equal(t, chi.StatusAborted, target.EnsureStatus().GetStatus(),
		"the abort is the only thing standing between this output and the cluster")
	require.True(t, user.Has("password_sha256_hex"),
		"fallback still derives a password - documented here so the finalizeCR gate is not removed")
}

// The modern replacement must keep working untouched - it is the migration target, and it cannot
// name a foreign namespace because SecretKeySelector has no namespace field. Asserting the
// from_env attribute rather than just "not aborted", so a silently no-op substitution fails here.
func TestValueFromSecretKeyRefStillNormalizes(t *testing.T) {
	target := &chi.ClickHouseInstallation{}
	target.Namespace = "own-ns"

	n := New(nil)
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	settings.Set("user1/password", chi.NewSettingSource(&chi.SettingSource{
		ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "creds"},
				Key:                  "password",
			},
		},
	}))
	user := chi.NewSettingsUser(settings, "user1")

	n.normalizeConfigurationUserSecretRef(user)

	require.NotEqual(t, chi.StatusAborted, target.EnsureStatus().GetStatus(),
		"the modern secretKeyRef syntax must not be rejected")
	require.True(t, user.Get("password").HasAttributes(),
		"secretKeyRef must be substituted into an ENV reference, not silently dropped")
}

// Regression guard: status.Errors is inherited into the next normalization target
// (FieldGroupInheritable copies Errors but NOT Status), so deduplicating the abort against
// persisted errors suppressed it from the second reconcile onward - the CR then normalized
// clean and the rejected user was written with the default password. The "already reported"
// state must be pass-local.
func TestRemovedSecretRefSyntaxAbortsAgainOnLaterReconcile(t *testing.T) {
	target := &chi.ClickHouseInstallation{}
	target.Namespace = "own-ns"
	// Reconcile #1 aborted and its tagged error was persisted, then inherited back here.
	// Status itself is not inherited, so it starts blank - exactly the real second-pass shape.
	target.EnsureStatus().ReconcileAbortWithReason(
		chi.StatusReasonRemovedSecretRefSyntax, "persisted by an earlier reconcile")
	target.Status.Status = ""

	n := New(nil)
	n.req = NewRequest(nil)
	n.req.SetTarget(target)

	settings := chi.NewSettings()
	settings.Set("user1/k8s_secret_password", chi.NewSettingScalar("other-ns/creds/password"))
	user := chi.NewSettingsUser(settings, "user1")

	n.normalizeConfigurationUser(user)

	require.Equal(t, chi.StatusAborted, target.EnsureStatus().GetStatus(),
		"a later reconcile of the same CR must abort again, not inherit its way past the check")
}
