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

package types

import (
	"fmt"
)

// ReconcileAttributes defines reconcile status and attributes
type ReconcileAttributes struct {
	status ObjectStatus

	exclude bool
}

// NewReconcileAttributes creates new reconcile attributes
func NewReconcileAttributes() *ReconcileAttributes {
	return &ReconcileAttributes{}
}

// HasIntersectionWith checks whether attributes `a` has intersection with `b`
func (a *ReconcileAttributes) HasIntersectionWith(b *ReconcileAttributes) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return false
	}
	switch {
	case a.GetStatus().Is(b.GetStatus()):
		return true
	case a.exclude && b.exclude:
		return true
	}
	return false
}

// SetStatus sets object status
func (a *ReconcileAttributes) SetStatus(status ObjectStatus) *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.status = status
	return a
}

// GetStatus gets object status
func (a *ReconcileAttributes) GetStatus() ObjectStatus {
	if a == nil {
		return ObjectStatusUnknown
	}
	return a.status
}

// SetExclude sets 'exclude' attribute
func (a *ReconcileAttributes) SetExclude() *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.exclude = true
	return a
}

// UnsetExclude unsets 'exclude' attribute
func (a *ReconcileAttributes) UnsetExclude() *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.exclude = false
	return a
}

// IsExclude checks whether 'exclude' attribute is set
func (a *ReconcileAttributes) IsExclude() bool {
	if a == nil {
		return false
	}
	return a.exclude
}

// String returns string form
func (a *ReconcileAttributes) String() string {
	if a == nil {
		return "(nil)"
	}

	return fmt.Sprintf("status: %s, exclude: %t", a.status, a.exclude)
}

// ReconcileAttributesCounters defines reconcile status and attributes counters
type ReconcileAttributesCounters struct {
	status map[ObjectStatus]int
	total int
	counters int

	_exclude int
}

// NewReconcileAttributesCounters creates new reconcile attributes counters
func NewReconcileAttributesCounters() *ReconcileAttributesCounters {
	return &ReconcileAttributesCounters{
		status: make(map[ObjectStatus]int),
	}
}

// Add adds to counters provided ReconcileAttributes
func (c *ReconcileAttributesCounters) Add(a *ReconcileAttributes) {
	if c == nil {
		return
	}

	value, ok := c.status[a.GetStatus()]
	if ok {
		value++
	} else {
		value = 1
	}

	c.status[a.GetStatus()] = value
	c.total++
	c.counters = len(c.status)

	if a.IsExclude() {
		c._exclude++
	}
}

// getCounterByStatus
func (c *ReconcileAttributesCounters) getCounterByStatus(status ObjectStatus) int {
	if c == nil {
		return 0
	}
	if num, ok := c.status[status]; ok {
		return num
	}
	return 0
}

// IsNewOnly checks whether counters have 'New' items only
func (c *ReconcileAttributesCounters) IsNewOnly() bool {
	return c.getCounterByStatus(ObjectStatusNew) == c.total
}

func (c *ReconcileAttributesCounters) String() string {
	if c == nil {
		return ""
	}

	res := ""
	for k, v := range c.status {
		res += fmt.Sprintf("%s: %d ",k, v)
	}
	return res
}
