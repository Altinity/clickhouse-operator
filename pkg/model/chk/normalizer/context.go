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

// Context specifies normalization context
type Context struct {
	*normalizer.Context
}

// NewContext creates new Context
func NewContext(options *normalizer.Options) *Context {
	return &Context{
		normalizer.NewContext(options),
	}
}

func (c *Context) GetTarget() *api.ClickHouseKeeperInstallation {
	return c.Context.GetTarget().(*api.ClickHouseKeeperInstallation)
}

func (c *Context) SetTarget(target *api.ClickHouseKeeperInstallation) *api.ClickHouseKeeperInstallation {
	return c.Context.SetTarget(target).(*api.ClickHouseKeeperInstallation)
}
