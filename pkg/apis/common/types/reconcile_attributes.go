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

	// Attributes are used by config generator

	add    bool
	remove bool
	modify bool
	found  bool

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
	case a.add && b.add:
		return true
	case a.remove && b.remove:
		return true
	case a.modify && b.modify:
		return true
	case a.found && b.found:
		return true
	case a.exclude && b.exclude:
		return true
	}
	return false
}

// SetStatus sets status
func (a *ReconcileAttributes) SetStatus(status ObjectStatus) *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.status = status
	return a
}

// GetStatus gets status
func (a *ReconcileAttributes) GetStatus() ObjectStatus {
	if a == nil {
		return ObjectStatusUnknown
	}
	return a.status
}

// SetAdd sets 'add' attribute
func (a *ReconcileAttributes) SetAdd() *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.add = true
	return a
}

// UnsetAdd unsets 'add' attribute
func (a *ReconcileAttributes) UnsetAdd() *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.add = false
	return a
}

// IsAdd checks whether 'add' attribute is set
func (a *ReconcileAttributes) IsAdd() bool {
	if a == nil {
		return false
	}
	return a.add
}

// SetRemove sets 'remove' attribute
func (a *ReconcileAttributes) SetRemove() *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.remove = true
	return a
}

// IsRemove checks whether 'remove' attribute is set
func (a *ReconcileAttributes) IsRemove() bool {
	if a == nil {
		return false
	}
	return a.remove
}

// SetModify sets 'modify' attribute
func (a *ReconcileAttributes) SetModify() *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.modify = true
	return a
}

// IsModify checks whether 'modify' attribute is set
func (a *ReconcileAttributes) IsModify() bool {
	if a == nil {
		return false
	}
	return a.modify
}

// SetFound sets 'found' attribute
func (a *ReconcileAttributes) SetFound() *ReconcileAttributes {
	if a == nil {
		return a
	}
	a.found = true
	return a
}

// IsFound checks whether 'found' attribute is set
func (a *ReconcileAttributes) IsFound() bool {
	if a == nil {
		return false
	}
	return a.found
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

	return fmt.Sprintf(
		"status: %s, add: %t, remove: %t, modify: %t, found: %t, exclude: %t",
		a.status,
		a.add,
		a.remove,
		a.modify,
		a.found,
		a.exclude,
	)
}

// ReconcileAttributesCounters defines reconcile status and attributes counters
type ReconcileAttributesCounters struct {
	status map[ObjectStatus]int

	// Attributes are used by config generator

	_add    int
	_remove int
	_modify int
	_found  int

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
		value = value + 1
	} else {
		value = 1
	}
	c.status[a.GetStatus()] = value

	if a.IsAdd() {
		c._add++
	}
	if a.IsRemove() {
		c._remove++
	}
	if a.IsModify() {
		c._modify++
	}
	if a.IsFound() {
		c._found++
	}
	if a.IsExclude() {
		c._exclude++
	}
}

// getAdd gets added
func (c *ReconcileAttributesCounters) getAdd() int {
	if c == nil {
		return 0
	}
	return c._add
}

// getRemove gets removed
func (c *ReconcileAttributesCounters) getRemove() int {
	if c == nil {
		return 0
	}
	return c._remove
}

// getModify gets modified
func (c *ReconcileAttributesCounters) getModify() int {
	if c == nil {
		return 0
	}
	return c._modify
}

// getFound gets found
func (c *ReconcileAttributesCounters) getFound() int {
	if c == nil {
		return 0
	}
	return c._found
}

// IsAddOnly checks whether counters have Add() only items
func (c *ReconcileAttributesCounters) IsAddOnly() bool {
	return (c.getAdd() > 0) && (c.getFound() == 0) && (c.getModify() == 0) && (c.getRemove() == 0)
}

func (c *ReconcileAttributesCounters) String() string {
	return fmt.Sprintf("a: %d f: %d m: %d r: %d", c.getAdd(), c.getFound(), c.getModify(), c.getRemove())
}
