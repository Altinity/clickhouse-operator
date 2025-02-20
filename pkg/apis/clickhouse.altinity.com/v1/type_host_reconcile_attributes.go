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

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// HostReconcileAttributes defines host reconcile status and attributes
type HostReconcileAttributes struct {
	status types.ObjectStatus

	// Attributes are used by config generator

	add    bool
	remove bool
	modify bool
	found  bool

	exclude bool
}

// NewHostReconcileAttributes creates new reconcile attributes
func NewHostReconcileAttributes() *HostReconcileAttributes {
	return &HostReconcileAttributes{}
}

// Any checks whether any of the attributes is set
func (a *HostReconcileAttributes) Any(of *HostReconcileAttributes) bool {
	if a == nil {
		return false
	}
	if of == nil {
		return false
	}
	return false ||
		(a.add && of.add) ||
		(a.remove && of.remove) ||
		(a.modify && of.modify) ||
		(a.found && of.found) ||
		(a.exclude && of.exclude)
}

// SetStatus sets status
func (a *HostReconcileAttributes) SetStatus(status types.ObjectStatus) *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.status = status
	return a
}

// GetStatus gets status
func (a *HostReconcileAttributes) GetStatus() types.ObjectStatus {
	if a == nil {
		return types.ObjectStatusUnknown
	}
	return a.status
}

// SetAdd sets 'add' attribute
func (a *HostReconcileAttributes) SetAdd() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.add = true
	return a
}

// UnsetAdd unsets 'add' attribute
func (a *HostReconcileAttributes) UnsetAdd() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.add = false
	return a
}

// IsAdd checks whether 'add' attribute is set
func (a *HostReconcileAttributes) IsAdd() bool {
	if a == nil {
		return false
	}
	return a.add
}

// SetRemove sets 'remove' attribute
func (a *HostReconcileAttributes) SetRemove() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.remove = true
	return a
}

// IsRemove checks whether 'remove' attribute is set
func (a *HostReconcileAttributes) IsRemove() bool {
	if a == nil {
		return false
	}
	return a.remove
}

// SetModify sets 'modify' attribute
func (a *HostReconcileAttributes) SetModify() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.modify = true
	return a
}

// IsModify checks whether 'modify' attribute is set
func (a *HostReconcileAttributes) IsModify() bool {
	if a == nil {
		return false
	}
	return a.modify
}

// SetFound sets 'found' attribute
func (a *HostReconcileAttributes) SetFound() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.found = true
	return a
}

// IsFound checks whether 'found' attribute is set
func (a *HostReconcileAttributes) IsFound() bool {
	if a == nil {
		return false
	}
	return a.found
}

// SetExclude sets 'exclude' attribute
func (a *HostReconcileAttributes) SetExclude() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.exclude = true
	return a
}

// UnsetExclude unsets 'exclude' attribute
func (a *HostReconcileAttributes) UnsetExclude() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.exclude = false
	return a
}

// IsExclude checks whether 'exclude' attribute is set
func (a *HostReconcileAttributes) IsExclude() bool {
	if a == nil {
		return false
	}
	return a.exclude
}

// String returns string form
func (a *HostReconcileAttributes) String() string {
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

// HostReconcileAttributesCounters defines host reconcile status and attributes counters
type HostReconcileAttributesCounters struct {
	status map[types.ObjectStatus]int

	// Attributes are used by config generator

	_add    int
	_remove int
	_modify int
	_found  int

	_exclude int
}

// NewHostReconcileAttributesCounters creates new reconcile attributes
func NewHostReconcileAttributesCounters() *HostReconcileAttributesCounters {
	return &HostReconcileAttributesCounters{
		status: make(map[types.ObjectStatus]int),
	}
}

// add adds to counters provided HostReconcileAttributes
func (c *HostReconcileAttributesCounters) add(a *HostReconcileAttributes) {
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
func (c *HostReconcileAttributesCounters) getAdd() int {
	if c == nil {
		return 0
	}
	return c._add
}

// getRemove gets removed
func (c *HostReconcileAttributesCounters) getRemove() int {
	if c == nil {
		return 0
	}
	return c._remove
}

// getModify gets modified
func (c *HostReconcileAttributesCounters) getModify() int {
	if c == nil {
		return 0
	}
	return c._modify
}

// getFound gets found
func (c *HostReconcileAttributesCounters) getFound() int {
	if c == nil {
		return 0
	}
	return c._found
}

// IsAddOnly checks whether counters have Add() only items
func (c *HostReconcileAttributesCounters) IsAddOnly() bool {
	return (c.getAdd() > 0) && (c.getFound() == 0) && (c.getModify() == 0) && (c.getRemove() == 0)
}

func (c *HostReconcileAttributesCounters) String() string {
	return fmt.Sprintf("a: %d f: %d m: %d r: %d", c.getAdd(), c.getFound(), c.getModify(), c.getRemove())
}

type iWalkHosts interface {
	WalkHosts(func(host *Host) error) []error
}

func (c *HostReconcileAttributesCounters) Count(src iWalkHosts) *HostReconcileAttributesCounters {
	src.WalkHosts(func(host *Host) error {
		c.add(host.GetReconcileAttributes())
		return nil
	})
	return c
}
