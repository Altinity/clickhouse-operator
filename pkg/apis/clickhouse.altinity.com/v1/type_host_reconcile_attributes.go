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

// ObjectStatus specifies object status
type ObjectStatus string

// Possible values for object status
const (
	ObjectStatusModified ObjectStatus = "modified"
	ObjectStatusNew      ObjectStatus = "new"
	ObjectStatusSame     ObjectStatus = "same"
	ObjectStatusUnknown  ObjectStatus = "unknown"
)

const (
	TagExclude types.Tag = "exclude"
)

// HostReconcileAttributes defines host reconcile status and attributes
type HostReconcileAttributes struct {
	status ObjectStatus

	// Attributes are used by config generator

	add    bool
	remove bool
	modify bool
	found  bool

	tags types.Tags
}

// NewHostReconcileAttributes creates new reconcile attributes
func NewHostReconcileAttributes() *HostReconcileAttributes {
	return &HostReconcileAttributes{}
}

// Equal checks whether reconcile attributes are equal
func (a *HostReconcileAttributes) Equal(to HostReconcileAttributes) bool {
	if a == nil {
		return false
	}
	return true &&
		(a.add == to.add) &&
		(a.remove == to.remove) &&
		(a.modify == to.modify) &&
		(a.found == to.found) &&
		(a.tags.Equal(to.tags))
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
		a.tags.HaveIntersection(of.tags)
}

// SetStatus sets status
func (a *HostReconcileAttributes) SetStatus(status ObjectStatus) *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.status = status
	return a
}

// GetStatus gets status
func (a *HostReconcileAttributes) GetStatus() ObjectStatus {
	if a == nil {
		return ObjectStatus("")
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
	a.tags.Set(TagExclude)
	return a
}

// UnsetExclude unsets 'exclude' attribute
func (a *HostReconcileAttributes) UnsetExclude() *HostReconcileAttributes {
	if a == nil {
		return a
	}
	a.tags.UnSet(TagExclude)
	return a
}

// IsExclude checks whether 'exclude' attribute is set
func (a *HostReconcileAttributes) IsExclude() bool {
	if a == nil {
		return false
	}
	return a.tags.Has(TagExclude)
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
		a.tags,
	)
}

// HostReconcileAttributesCounters defines host reconcile status and attributes counters
type HostReconcileAttributesCounters struct {
	status map[ObjectStatus]int

	// Attributes are used by config generator

	add    int
	remove int
	modify int
	found  int

	tags types.Tags
}

// NewHostReconcileAttributesCounters creates new reconcile attributes
func NewHostReconcileAttributesCounters() *HostReconcileAttributesCounters {
	return &HostReconcileAttributesCounters{
		status: make(map[ObjectStatus]int),
	}
}

// Add adds to counters provided HostReconcileAttributes
func (c *HostReconcileAttributesCounters) Add(a *HostReconcileAttributes) {
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
		c.add++
	}
	if a.IsRemove() {
		c.remove++
	}
	if a.IsModify() {
		c.modify++
	}
	if a.IsFound() {
		c.found++
	}
	c.tags.Add(a.tags)
}

// GetAdd gets added
func (c *HostReconcileAttributesCounters) GetAdd() int {
	if c == nil {
		return 0
	}
	return c.add
}

// GetRemove gets removed
func (c *HostReconcileAttributesCounters) GetRemove() int {
	if c == nil {
		return 0
	}
	return c.remove
}

// GetModify gets modified
func (c *HostReconcileAttributesCounters) GetModify() int {
	if c == nil {
		return 0
	}
	return c.modify
}

// GetFound gets found
func (c *HostReconcileAttributesCounters) GetFound() int {
	if c == nil {
		return 0
	}
	return c.found
}

// GetExclude gets exclude
func (c *HostReconcileAttributesCounters) GetExclude() int {
	if c == nil {
		return 0
	}
	return c.tags.Get(TagExclude)
}

// AddOnly checks whether counters have Add() only items
func (c *HostReconcileAttributesCounters) AddOnly() bool {
	return c.GetAdd() > 0 && c.GetFound() == 0 && c.GetModify() == 0 && c.GetRemove() == 0
}

func (c *HostReconcileAttributesCounters) String() string {
	return fmt.Sprintf("a: %d f: %d m: %d r: %d", c.GetAdd(), c.GetFound(), c.GetModify(), c.GetRemove())
}

type iWalkHosts interface {
	WalkHosts(func(host *Host) error) []error
}

func (c *HostReconcileAttributesCounters) Count(src iWalkHosts) *HostReconcileAttributesCounters {
	src.WalkHosts(func(host *Host) error {
		c.Add(host.GetReconcileAttributes())
		return nil
	})
	return c
}
