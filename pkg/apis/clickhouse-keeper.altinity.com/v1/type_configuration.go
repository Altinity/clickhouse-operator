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

// Configuration defines configuration section of .spec
type Configuration struct {
	Settings *apiChi.Settings `json:"settings,omitempty"  yaml:"settings,omitempty"`
	Files    *apiChi.Settings `json:"files,omitempty"     yaml:"files,omitempty"`
	Clusters []*ChkCluster    `json:"clusters,omitempty"  yaml:"clusters,omitempty"`
}

// NewConfiguration creates new ChkConfiguration objects
func NewConfiguration() *Configuration {
	return new(Configuration)
}

func (c *Configuration) GetProfiles() *apiChi.Settings {
	return nil
}

func (c *Configuration) GetQuotas() *apiChi.Settings {
	return nil
}

func (c *Configuration) GetSettings() *apiChi.Settings {
	if c == nil {
		return nil
	}

	return c.Settings
}

func (c *Configuration) GetFiles() *apiChi.Settings {
	return c.Files
}

func (c *Configuration) GetClusters() []*ChkCluster {
	if c == nil {
		return nil
	}

	return c.Clusters
}

func (c *Configuration) GetCluster(i int) *ChkCluster {
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
func (c *Configuration) MergeFrom(from *Configuration, _type apiChi.MergeType) *Configuration {
	if from == nil {
		return c
	}

	if c == nil {
		c = NewConfiguration()
	}

	c.Settings = c.Settings.MergeFrom(from.Settings)
	c.Files = c.Files.MergeFrom(from.Files)

	// TODO merge clusters
	// Copy Clusters for now
	c.Clusters = from.Clusters

	return c
}
