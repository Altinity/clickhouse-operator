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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// Request specifies normalization request
type Request[T chi.ClickHouseInstallation | chk.ClickHouseKeeperInstallation] struct {
	// target specifies current target being normalized
	target chi.ICustomResource
	// options specifies normalization options
	options *Options[T]
}

// NewRequest creates new Context
func NewRequest[T chi.ClickHouseInstallation | chk.ClickHouseKeeperInstallation](options *Options[T]) *Request[T] {
	return &Request[T]{
		options: options,
	}
}

func (c *Request[T]) GetTarget() chi.ICustomResource {
	if c == nil {
		return nil
	}
	return c.target
}

func (c *Request[T]) SetTarget(target chi.ICustomResource) chi.ICustomResource {
	if c == nil {
		return nil
	}
	c.target = target
	return c.target
}

func (c *Request[T]) Options() *Options[T] {
	if c == nil {
		return nil
	}
	return c.options
}

func (c *Request[_]) GetTargetNamespace() string {
	return c.GetTarget().GetNamespace()
}

func (c *Request[_]) AppendAdditionalEnvVar(envVar core.EnvVar) {
	if c == nil {
		return
	}
	log.V(2).F().Info("going to add env var %s len()=%d", envVar.Name, len(c.GetTarget().GetRuntime().GetAttributes().GetAdditionalEnvVars()))
	c.GetTarget().GetRuntime().GetAttributes().AppendAdditionalEnvVarIfNotExists(envVar)
	log.V(2).F().Info("added env var %s len()=%d", envVar.Name, len(c.GetTarget().GetRuntime().GetAttributes().GetAdditionalEnvVars()))
}

func (c *Request[_]) AppendAdditionalVolume(volume core.Volume) {
	if c == nil {
		return
	}
	c.GetTarget().GetRuntime().GetAttributes().AppendAdditionalVolumeIfNotExists(volume)
}

func (c *Request[_]) AppendAdditionalVolumeMount(volumeMount core.VolumeMount) {
	if c == nil {
		return
	}
	c.GetTarget().GetRuntime().GetAttributes().AppendAdditionalVolumeMountIfNotExists(volumeMount)
}
