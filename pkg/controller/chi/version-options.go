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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const unknownVersion = "failed to query"

type versionOptions struct {
	skipNew             bool
	skipStopped         bool
	skipStoppedAncestor bool
}

func (opts versionOptions) shouldSkip(host *api.Host) (bool, string) {
	if opts.skipNew {
		if !host.HasAncestor() {
			return true, "host is a new one, version is not not applicable"
		}
	}

	if opts.skipStopped {
		if host.IsStopped() {
			return true, "host is stopped, version is not applicable"
		}
	}

	if opts.skipStoppedAncestor {
		if host.HasAncestor() && host.GetAncestor().IsStopped() {
			return true, "host ancestor is stopped, version is not applicable"
		}
	}

	return false, ""
}
