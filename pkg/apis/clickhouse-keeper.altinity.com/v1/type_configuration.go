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

// NewConfiguration creates new ChkConfiguration objects
func NewConfiguration() *ChkConfiguration {
	return new(ChkConfiguration)
}

func (c *ChkConfiguration) GetSettings() *apiChi.Settings {
	if c == nil {
		return nil
	}

	return c.Settings
}

func (c *ChkConfiguration) GetClusters() []*ChkCluster {
	if c == nil {
		return nil
	}

	return c.Clusters
}

func (c *ChkConfiguration) GetCluster(i int) *ChkCluster {
	clusters := c.GetClusters()
	if clusters == nil {
		return nil
	}
	if i >= len(clusters) {
		return nil
	}
	return clusters[i]
}

// MergeFrom merges from specified source
func (configuration *ChkConfiguration) MergeFrom(from *ChkConfiguration, _type apiChi.MergeType) *ChkConfiguration {
	if from == nil {
		return configuration
	}

	if configuration == nil {
		configuration = NewConfiguration()
	}

	configuration.Settings = configuration.Settings.MergeFrom(from.Settings)

	// TODO merge clusters
	// Copy Clusters for now
	configuration.Clusters = from.Clusters

	return configuration
}
