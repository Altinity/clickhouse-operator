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
	"fmt"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	BackupTimeoutMin            = time.Second
	BackupActionTimeoutMax      = 720 * time.Hour
	BackupUnreachableTimeoutMax = 168 * time.Hour
	BackupHTTPReceiveTimeoutMax = 168 * time.Hour
	BackupErrorResponsesMaxMax  = 1000 // keep in sync with the CRD maximum
)

// BackupTimeouts: an unset field comes from the schedule, then from the operator backup.timeouts, then the default.
type BackupTimeouts struct {
	// ActionTimeout is per command per host.
	ActionTimeout *meta.Duration `json:"actionTimeout,omitempty" yaml:"actionTimeout,omitempty"`
	// UnreachableTimeout is how long a host that does not answer is retried before it fails.
	UnreachableTimeout *meta.Duration `json:"unreachableTimeout,omitempty" yaml:"unreachableTimeout,omitempty"`
	// ErrorResponsesMax is how many polls in a row may find no command status before the command counts as lost.
	ErrorResponsesMax *int32 `json:"errorResponsesMax,omitempty" yaml:"errorResponsesMax,omitempty"`
	// HTTPReceiveTimeout must cover synchronous sidecar commands, such as deletes.
	HTTPReceiveTimeout *meta.Duration `json:"httpReceiveTimeout,omitempty" yaml:"httpReceiveTimeout,omitempty"`
}

func durationOutOfBounds(value *meta.Duration, maximum time.Duration) bool {
	return (value != nil) && ((value.Duration < BackupTimeoutMin) || (value.Duration > maximum))
}

func validateDuration(field string, value *meta.Duration, maximum time.Duration) error {
	if durationOutOfBounds(value, maximum) {
		return fmt.Errorf("%w: %s %s must be between %s and %s", ErrInvalidBackupSpec, field, value.Duration,
			BackupTimeoutMin, maximum)
	}
	return nil
}

func errorResponsesOutOfBounds(value *int32) bool {
	return (value != nil) && ((*value < 1) || (*value > BackupErrorResponsesMaxMax))
}

func (t *BackupTimeouts) Validate() error {
	if t == nil {
		return nil
	}
	if err := validateDuration("timeouts.actionTimeout", t.ActionTimeout, BackupActionTimeoutMax); err != nil {
		return err
	}
	if err := validateDuration("timeouts.unreachableTimeout", t.UnreachableTimeout, BackupUnreachableTimeoutMax); err != nil {
		return err
	}
	if err := validateDuration("timeouts.httpReceiveTimeout", t.HTTPReceiveTimeout, BackupHTTPReceiveTimeoutMax); err != nil {
		return err
	}
	if errorResponsesOutOfBounds(t.ErrorResponsesMax) {
		return fmt.Errorf("%w: timeouts.errorResponsesMax must be between 1 and %d", ErrInvalidBackupSpec, BackupErrorResponsesMaxMax)
	}
	return nil
}

// Sanitized unsets out-of-limit values, which the unvalidated operator configuration can hold.
func (t *BackupTimeouts) Sanitized() *BackupTimeouts {
	if t == nil {
		return nil
	}
	out := t.DeepCopy()
	if durationOutOfBounds(out.ActionTimeout, BackupActionTimeoutMax) {
		out.ActionTimeout = nil
	}
	if durationOutOfBounds(out.UnreachableTimeout, BackupUnreachableTimeoutMax) {
		out.UnreachableTimeout = nil
	}
	if durationOutOfBounds(out.HTTPReceiveTimeout, BackupHTTPReceiveTimeoutMax) {
		out.HTTPReceiveTimeout = nil
	}
	if errorResponsesOutOfBounds(out.ErrorResponsesMax) {
		out.ErrorResponsesMax = nil
	}
	return out
}

func (t *BackupTimeouts) MergeFrom(from ...*BackupTimeouts) *BackupTimeouts {
	out := &BackupTimeouts{}
	if t != nil {
		out = t.DeepCopy()
	}
	for _, layer := range from {
		if layer == nil {
			continue
		}
		layer = layer.DeepCopy()
		if out.ActionTimeout == nil {
			out.ActionTimeout = layer.ActionTimeout
		}
		if out.UnreachableTimeout == nil {
			out.UnreachableTimeout = layer.UnreachableTimeout
		}
		if out.ErrorResponsesMax == nil {
			out.ErrorResponsesMax = layer.ErrorResponsesMax
		}
		if out.HTTPReceiveTimeout == nil {
			out.HTTPReceiveTimeout = layer.HTTPReceiveTimeout
		}
	}
	return out
}

func duration(value *meta.Duration) time.Duration {
	if value == nil {
		return 0
	}
	return value.Duration
}

func (t *BackupTimeouts) GetActionTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return duration(t.ActionTimeout)
}

func (t *BackupTimeouts) GetUnreachableTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return duration(t.UnreachableTimeout)
}

func (t *BackupTimeouts) GetHTTPReceiveTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return duration(t.HTTPReceiveTimeout)
}

func (t *BackupTimeouts) GetErrorResponsesMax() int {
	if (t == nil) || (t.ErrorResponsesMax == nil) {
		return 0
	}
	return int(*t.ErrorResponsesMax)
}
