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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// Request specifies normalization Request
type Request struct {
	*normalizer.Request[api.ClickHouseInstallation]

	// removedSecretRefReported records whether this pass has already aborted over a user field
	// using the removed k8s_secret_ syntax, so the abort is raised once however many fields and
	// users carry it. Pass-local on purpose: status.Errors is inherited into the next
	// normalization target, so deduplicating against it would suppress the abort from the second
	// reconcile onward and let the CR through.
	removedSecretRefReported bool
}

// RemovedSecretRefReported reports whether this normalization pass already raised the removed
// secret-ref abort, and marks it as raised. Returns false exactly once per pass.
func (c *Request) RemovedSecretRefReported() bool {
	if c == nil {
		return true
	}
	reported := c.removedSecretRefReported
	c.removedSecretRefReported = true
	return reported
}

// NewRequest creates new Request
func NewRequest(options *normalizer.Options[api.ClickHouseInstallation]) *Request {
	return &Request{
		Request: normalizer.NewRequest(options),
	}
}

func (c *Request) GetTarget() *api.ClickHouseInstallation {
	return c.Request.GetTarget().(*api.ClickHouseInstallation)
}

func (c *Request) SetTarget(target *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	return c.Request.SetTarget(target).(*api.ClickHouseInstallation)
}
