// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
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

package poller

import (
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	waitStatefulSetGenerationTimeoutBeforeStartBothering = 60
	waitStatefulSetGenerationTimeoutToCreateStatefulSet  = 30
)

// Options specifies polling options
type Options struct {
	StartBotheringAfterTimeout time.Duration
	GetErrorTimeout            time.Duration
	Timeout                    time.Duration
	MainInterval               time.Duration
	BackgroundInterval         time.Duration
}

// NewPollerOptions creates new poll options
func NewPollerOptions() *Options {
	return &Options{}
}

// Ensure ensures poll options do exist
func (o *Options) Ensure() *Options {
	if o == nil {
		return NewPollerOptions()
	}
	return o
}

// FromConfig makes poll options from config
func (o *Options) FromConfig(config *api.OperatorConfig) *Options {
	if o == nil {
		return nil
	}
	o.StartBotheringAfterTimeout = time.Duration(waitStatefulSetGenerationTimeoutBeforeStartBothering) * time.Second
	o.GetErrorTimeout = time.Duration(waitStatefulSetGenerationTimeoutToCreateStatefulSet) * time.Second
	o.Timeout = time.Duration(config.Reconcile.StatefulSet.Update.Timeout) * time.Second
	o.MainInterval = time.Duration(config.Reconcile.StatefulSet.Update.PollInterval) * time.Second
	o.BackgroundInterval = 1 * time.Second
	return o
}

// SetCreateTimeout sets create timeout
func (o *Options) SetGetErrorTimeout(timeout time.Duration) *Options {
	if o == nil {
		return nil
	}
	o.GetErrorTimeout = timeout
	return o
}
