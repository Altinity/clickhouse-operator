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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// Request specifies normalization Request
type Request struct {
	*normalizer.Request
}

// NewRequest creates new Request
func NewRequest(options *normalizer.Options) *Request {
	return &Request{
		normalizer.NewRequest(options),
	}
}

func (c *Request) GetTarget() *api.ClickHouseKeeperInstallation {
	return c.Request.GetTarget().(*api.ClickHouseKeeperInstallation)
}

func (c *Request) SetTarget(target *api.ClickHouseKeeperInstallation) *api.ClickHouseKeeperInstallation {
	return c.Request.SetTarget(target).(*api.ClickHouseKeeperInstallation)
}
