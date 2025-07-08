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
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller/domain"
)

var errUnknownVersion = fmt.Errorf("unknown version")

func (w *worker) getTagBasedVersion(host *api.Host) *swversion.SoftWareVersion {
	// Fetch tag from the image
	var tagBasedVersion *swversion.SoftWareVersion
	if tag, tagFound := w.task.Creator().GetAppImageTag(host); tagFound {
		tagBasedVersion = swversion.NewSoftWareVersionFromTag(tag)
	}
	return tagBasedVersion
}

// getHostClickHouseVersion gets host ClickHouse version
func (w *worker) getHostClickHouseVersion(ctx context.Context, host *api.Host) (*swversion.SoftWareVersion, error) {
	version, err := w.ensureClusterSchemer(host).HostClickHouseVersion(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Warning("Failed to get ClickHouse version on host: %s", host.GetName())
		return nil, err
	}

	w.a.V(1).M(host).F().Info("Get ClickHouse version on host: %s version: %s", host.GetName(), version)
	v := swversion.NewSoftWareVersion(version)
	if v.IsUnknown() {
		return nil, errUnknownVersion
	}

	return v, nil
}

func (w *worker) pollHostForClickHouseVersion(ctx context.Context, host *api.Host) (version *swversion.SoftWareVersion, err error) {
	err = domain.PollHost(
		ctx,
		host,
		func(_ctx context.Context, _host *api.Host) bool {
			var e error
			version, e = w.getHostClickHouseVersion(_ctx, _host)
			if e == nil {
				return true
			}
			w.a.V(1).M(host).F().Warning("Host is NOT alive: %s ", host.GetName())
			return false
		},
	)
	return
}
