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
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// Context specifies normalization context
type Context struct {
	// target specifies current target being normalized
	target api.ICustomResource
	// options specifies normalization options
	options *Options
}

// NewContext creates new Context
func NewContext(options *Options) *Context {
	return &Context{
		options: options,
	}
}

func (c *Context) GetTarget() api.ICustomResource {
	if c == nil {
		return nil
	}
	return c.target
}

func (c *Context) SetTarget(target api.ICustomResource) api.ICustomResource {
	if c == nil {
		return nil
	}
	c.target = target
	return c.target
}

func (c *Context) Options() *Options {
	if c == nil {
		return nil
	}
	return c.options
}

func (c *Context) GetTargetNamespace() string {
	return c.GetTarget().GetNamespace()
}

func (c *Context) AppendAdditionalEnvVar(envVar core.EnvVar) {
	if c == nil {
		return
	}
	c.GetTarget().GetRuntime().GetAttributes().AppendAdditionalEnvVarIfNotExists(envVar)
}

func (c *Context) AppendAdditionalVolume(volume core.Volume) {
	if c == nil {
		return
	}
	c.GetTarget().GetRuntime().GetAttributes().AppendAdditionalVolumeIfNotExists(volume)
}

func (c *Context) AppendAdditionalVolumeMount(volumeMount core.VolumeMount) {
	if c == nil {
		return
	}
	c.GetTarget().GetRuntime().GetAttributes().AppendAdditionalVolumeMountIfNotExists(volumeMount)
}
