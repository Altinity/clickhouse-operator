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
	"context"
	"fmt"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller/domain"
	utilfips "github.com/altinity/clickhouse-operator/pkg/util/fips"
)

func (w *worker) getTagBasedVersion(host *api.Host) *swversion.SoftWareVersion {
	// Fetch tag from the image
	var tagBasedVersion *swversion.SoftWareVersion
	if tag, tagFound := w.task.Creator().GetAppImageTag(host); tagFound {
		tagBasedVersion = swversion.NewSoftWareVersionFromTag(tag)
	}
	return tagBasedVersion
}

// getHostClickHouseVersion gets host ClickHouse version
func (w *worker) getHostClickHouseVersion(ctx context.Context, host *api.Host) *swversion.SoftWareVersion {
	version, err := w.ensureClusterSchemer(host).HostClickHouseVersion(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Warning("Failed to get ClickHouse version on host: %s err: %v", host.GetName(), err)
		return nil
	}

	w.a.V(1).M(host).F().Info("Get ClickHouse version on host: %s version: %s", host.GetName(), version)
	return swversion.NewSoftWareVersion(version)
}

func (w *worker) pollHostForClickHouseVersion(ctx context.Context, host *api.Host) (version *swversion.SoftWareVersion, err error) {
	err = domain.PollHost(
		ctx,
		host,
		func(_ctx context.Context, _host *api.Host) bool {
			version = w.getHostClickHouseVersion(_ctx, _host)
			if version.IsKnown() {
				return true
			}
			w.a.V(1).M(host).F().Warning("Host is NOT alive: %s ", host.GetName())
			return false
		},
	)
	return
}

// enforceFIPSImagePolicyRuntime checks the running binary's `SELECT version()`
// reply against security.images.policy. Called after the host is alive
// and `version()` has been fetched. Returns common.ErrCRUDAbort when the CR
// is aborted, so the error propagates to reconcile() which routes it through
// markReconcileCompletedUnsuccessfully (the established abort persistence
// path). Returning nil for success or fail-open avoids the bug where the
// in-memory abort gets overwritten by the success-path finalizer that
// re-fetches the CR from the API server.
//
// Fail-open semantics: nil version (transient query failure) does NOT abort —
// admission already rejected images known to be non-FIPS; a SQL hiccup against
// a running CR should not flip it to Aborted. The next reconcile re-evaluates.
//
// No-op (returns nil) when policy is Permissive or unset.
func (w *worker) enforceFIPSImagePolicyRuntime(host *api.Host, version *swversion.SoftWareVersion) error {
	if !chop.Config().Security.GetImages().IsRequired() {
		return nil
	}
	if version == nil {
		return nil
	}
	if utilfips.VersionHasFIPSMarker(version.GetOriginal()) {
		return nil
	}
	host.GetCR().IEnsureStatus().ReconcileAbortWithReason(
		api.StatusReasonFIPSImagePolicyViolation,
		fmt.Sprintf("host %s SELECT version()=%q lacks 'fips' substring (security.images.policy=Required)",
			host.GetName(), version.GetOriginal()),
	)
	return common.ErrCRUDAbort
}
