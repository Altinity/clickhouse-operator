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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

const (
	// PortMayBeAssignedLaterOrLeftUnused value means that port
	// is not assigned yet and is expected to be assigned later.
	PortMayBeAssignedLaterOrLeftUnused = int32(0)
)

// PortUnassigned returns value for unassigned port
func PortUnassigned() int32 {
	return PortMayBeAssignedLaterOrLeftUnused
}

// IsPortAssigned checks whether port is assigned
func IsPortAssigned(port int32) bool {
	return port != PortUnassigned()
}

// IsPortUnassigned checks whether port is unassigned
func IsPortUnassigned(port int32) bool {
	return port == PortUnassigned()
}

// IsPortInvalid checks whether port is invalid
func IsPortInvalid(port int32) bool {
	return (port <= 0) || (port >= 65535)
}

// EnsurePortValue ensures port either:
// - already has own value assigned
// - or has provided value
// - or value is fell back to default
func EnsurePortValue(port, value, _default *types.Int32) *types.Int32 {
	// Port may already be explicitly specified in podTemplate or by portDistribution
	if port.HasValue() {
		// Port has a value already
		return port
	}

	// Port has no explicitly assigned value

	// Let's use provided value real value
	if value.HasValue() {
		// Provided value is a real value, use it
		return value
	}

	// Fallback to default value
	return _default
}
