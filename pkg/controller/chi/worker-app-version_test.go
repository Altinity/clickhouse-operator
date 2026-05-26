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

package chi

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
)

// withFIPSImagePolicy temporarily sets chop.Config().Security.Images.Policy
// for the duration of a test case. Restores prior Security block on cleanup so
// the package-global doesn't leak across cases (chop.New runs once in init()
// in controller_test.go).
func withFIPSImagePolicy(t *testing.T, policy string) {
	t.Helper()
	cfg := chop.Config()
	prev := cfg.Security
	t.Cleanup(func() { cfg.Security = prev })

	cfg.Security = api.OperatorConfigSecurity{}
	if policy != "" {
		cfg.Security.Images = &api.OperatorConfigSecurityImages{
			Policy: types.NewString(policy),
		}
	}
}

// makeHostWithCR builds a *api.Host wired to a synthetic CHI via SetCR so the
// gate's host.GetCR().IEnsureStatus() lookup resolves without a real reconcile.
// Returns the CR alongside so the caller can inspect the concrete *Status —
// IStatus interface only exposes mutators, not the StatusAborted assertion path.
func makeHostWithCR(hostName, crName string) (*api.Host, *api.ClickHouseInstallation) {
	cr := &api.ClickHouseInstallation{}
	cr.Name = crName
	cr.Namespace = "ns"
	host := &api.Host{}
	host.Name = hostName
	host.SetCR(cr)
	return host, cr
}

// TestEnforceFIPSImagePolicyRuntime exercises the post-Ready version-string
// gate in worker-app-version.go. The cells we care about:
//   - Permissive (or unset policy) → no-op, never abort
//   - Required + nil version → fail-open (transient SQL hiccup must not flip CR to Aborted)
//   - Required + version contains "fips" → no abort
//   - Required + non-fips version → abort with FIPSImagePolicyViolation reason +
//     ErrCRUDAbort sentinel propagated to the caller
//
// The sentinel return value is load-bearing: worker-reconciler-chi.go matches on
// errors.Is(err, common.ErrCRUDAbort) to route the failure through
// markReconcileCompletedUnsuccessfully — the established abort-persistence path.
// Without the sentinel the success-path finalizer re-fetches the CR from the
// API server and overwrites the in-memory Aborted with Completed.
func TestEnforceFIPSImagePolicyRuntime(t *testing.T) {
	w := &worker{}

	t.Run("Permissive (default) — non-fips version is a no-op", func(t *testing.T) {
		withFIPSImagePolicy(t, "")
		host, cr := makeHostWithCR("h0", "chi")
		err := w.enforceFIPSImagePolicyRuntime(host, swversion.NewSoftWareVersion("25.3.8.30001"))
		require.NoError(t, err)
		require.Nil(t, cr.Status, "Permissive must not touch CR status (Status stays nil)")
	})

	t.Run("Required + nil version — fail-open", func(t *testing.T) {
		withFIPSImagePolicy(t, string(api.FIPSImagePolicyRequired))
		host, cr := makeHostWithCR("h0", "chi")
		err := w.enforceFIPSImagePolicyRuntime(host, nil)
		require.NoError(t, err, "transient `SELECT version()` failure must not abort the CR")
		require.Nil(t, cr.Status)
	})

	t.Run("Required + fips-tagged version — no abort", func(t *testing.T) {
		withFIPSImagePolicy(t, string(api.FIPSImagePolicyRequired))
		host, cr := makeHostWithCR("h0", "chi")
		err := w.enforceFIPSImagePolicyRuntime(host, swversion.NewSoftWareVersion("25.3.8.30001.altinityfips"))
		require.NoError(t, err)
		require.Nil(t, cr.Status)
	})

	t.Run("Required + non-fips version — aborts with ErrCRUDAbort sentinel", func(t *testing.T) {
		withFIPSImagePolicy(t, string(api.FIPSImagePolicyRequired))
		host, cr := makeHostWithCR("h0", "chi")
		err := w.enforceFIPSImagePolicyRuntime(host, swversion.NewSoftWareVersion("25.3.8.30001"))

		require.ErrorIs(t, err, common.ErrCRUDAbort, "must return ErrCRUDAbort for caller short-circuit + persistence")

		require.NotNil(t, cr.Status)
		require.Equal(t, api.StatusAborted, cr.Status.Status)
		require.NotEmpty(t, cr.Status.Errors)
		require.True(t, strings.HasPrefix(cr.Status.Errors[0], "["+api.StatusReasonFIPSImagePolicyViolation+"] "),
			"reason tag must lead the error string for auto-recovery prefix-match: got %q", cr.Status.Errors[0])
		require.Contains(t, cr.Status.Errors[0], "25.3.8.30001", "abort message must echo the offending version()")
		require.Contains(t, cr.Status.Errors[0], "h0", "abort message must name the host")
	})

	t.Run("Required + uppercase FIPS in version — case-insensitive match passes", func(t *testing.T) {
		withFIPSImagePolicy(t, string(api.FIPSImagePolicyRequired))
		host, cr := makeHostWithCR("h0", "chi")
		err := w.enforceFIPSImagePolicyRuntime(host, swversion.NewSoftWareVersion("25.3.8.FIPS-rc1"))
		require.NoError(t, err, "VersionHasFIPSMarker is case-insensitive — uppercase FIPS must pass")
		require.Nil(t, cr.Status)
	})
}
