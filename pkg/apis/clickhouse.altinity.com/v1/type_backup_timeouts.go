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

// Bounds of the timeouts of a backup, a restore or a schedule, which Validate checks.
const (
	// BackupTimeoutMin is the least timeout
	BackupTimeoutMin = time.Second
	// BackupActionTimeoutMax bounds actionTimeout: 30 days
	BackupActionTimeoutMax = 720 * time.Hour
	// BackupUnreachableTimeoutMax bounds unreachableTimeout: 7 days
	BackupUnreachableTimeoutMax = 168 * time.Hour
	// BackupHTTPReceiveTimeoutMax bounds httpReceiveTimeout: 7 days
	BackupHTTPReceiveTimeoutMax = 168 * time.Hour
	// BackupErrorResponsesMaxMax bounds errorResponsesMax. Mirrored by the CRD schema maximum.
	BackupErrorResponsesMaxMax = 1000
)

// BackupTimeouts bound how the operator waits for the commands it runs on the clickhouse-backup sidecars of a cluster
// through system.backup_actions of their ClickHouse. Every field is optional: one a resource leaves unset is that of
// the schedule a backup runs, else that of backup.timeouts of the operator configuration, else the built-in default.
// The durations are Go durations, e.g. "12h", "90m", "1h30m".
type BackupTimeouts struct {
	// ActionTimeout bounds how long the operator waits for one command on one host. Default 12h.
	ActionTimeout *meta.Duration `json:"actionTimeout,omitempty" yaml:"actionTimeout,omitempty"`
	// UnreachableTimeout bounds how long a host whose ClickHouse does not answer - a connection or a query error, an
	// HTTP error of the sidecar the URL engine reports - is retried, from the first error in a row, before the host
	// fails. Default 10m.
	UnreachableTimeout *meta.Duration `json:"unreachableTimeout,omitempty" yaml:"unreachableTimeout,omitempty"`
	// ErrorResponsesMax is how many polls in a row may find no row of a submitted command before the operator takes
	// the command as lost, as after a restart of the sidecar. Default 5.
	ErrorResponsesMax *int32 `json:"errorResponsesMax,omitempty" yaml:"errorResponsesMax,omitempty"`
	// HTTPReceiveTimeout is the http_receive_timeout and http_send_timeout of every insert into system.backup_actions,
	// the timeouts of the URL engine towards the sidecar: a synchronous command, delete local or delete remote,
	// answers once it is done. Default 4h.
	HTTPReceiveTimeout *meta.Duration `json:"httpReceiveTimeout,omitempty" yaml:"httpReceiveTimeout,omitempty"`
}

// durationOutOfBounds reports whether an optional duration is set and out of its bounds.
func durationOutOfBounds(value *meta.Duration, maximum time.Duration) bool {
	return (value != nil) && ((value.Duration < BackupTimeoutMin) || (value.Duration > maximum))
}

// validateDuration checks an optional duration against its bounds.
func validateDuration(field string, value *meta.Duration, maximum time.Duration) error {
	if durationOutOfBounds(value, maximum) {
		return fmt.Errorf("%w: %s %s must be between %s and %s", ErrInvalidBackupSpec, field, value.Duration,
			BackupTimeoutMin, maximum)
	}
	return nil
}

// errorResponsesOutOfBounds reports whether an optional errorResponsesMax is set and out of its bounds.
func errorResponsesOutOfBounds(value *int32) bool {
	return (value != nil) && ((*value < 1) || (*value > BackupErrorResponsesMaxMax))
}

// Validate checks the timeouts against their bounds. Nil-safe.
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

// Sanitized returns the timeouts with every value out of its bounds left unset, as the operator configuration, which
// nothing validates before the operator reads it, may hold such. Nil-safe.
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

// MergeFrom returns the timeouts with every field they leave unset taken from the timeouts given, the first one that
// sets it. Nil-safe: nil merges into a new set.
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

// duration returns an optional duration, 0 when unset.
func duration(value *meta.Duration) time.Duration {
	if value == nil {
		return 0
	}
	return value.Duration
}

// GetActionTimeout returns the action timeout, 0 when unset. Nil-safe.
func (t *BackupTimeouts) GetActionTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return duration(t.ActionTimeout)
}

// GetUnreachableTimeout returns the unreachable timeout, 0 when unset. Nil-safe.
func (t *BackupTimeouts) GetUnreachableTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return duration(t.UnreachableTimeout)
}

// GetHTTPReceiveTimeout returns the HTTP receive timeout, 0 when unset. Nil-safe.
func (t *BackupTimeouts) GetHTTPReceiveTimeout() time.Duration {
	if t == nil {
		return 0
	}
	return duration(t.HTTPReceiveTimeout)
}

// GetErrorResponsesMax returns the number of polls without a row before a command is lost, 0 when unset. Nil-safe.
func (t *BackupTimeouts) GetErrorResponsesMax() int {
	if (t == nil) || (t.ErrorResponsesMax == nil) {
		return 0
	}
	return int(*t.ErrorResponsesMax)
}
