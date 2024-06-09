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

package v1

import (
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func (c *ChkCluster) GetLayout() *apiChi.ClusterLayout {
	if c == nil {
		return nil
	}
	return c.Layout
}

func (c *ChkCluster) GetName() string {
	return c.Name
}
func (c *ChkCluster) GetRuntime() apiChi.IClusterRuntime {
	return nil
}
func (c *ChkCluster) GetServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	return nil, false
}
func (c *ChkCluster) GetSecret() *apiChi.ClusterSecret {
	return nil
}
func (c *ChkCluster) GetPDBMaxUnavailable() *apiChi.Int32 {
	return apiChi.NewInt32(1)
}

func (c *ChkCluster) WalkShards(f func(index int, shard apiChi.IShard) error) []error {
	return nil
}
func (c *ChkCluster) WalkHosts(func(host *apiChi.Host) error) []error {
	return nil
}
