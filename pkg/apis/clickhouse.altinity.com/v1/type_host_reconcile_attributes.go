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

// HostReconcileAttributes defines host reconcile status and attributes
type HostReconcileAttributes struct {
	status ObjectStatus

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

// Equal checks whether reconcile attributes are equal
func (s *HostReconcileAttributes) Equal(to HostReconcileAttributes) bool {
	if s == nil {
		return false
	}
	return true &&
		(s.add == to.add) &&
		(s.remove == to.remove) &&
		(s.modify == to.modify) &&
		(s.found == to.found) &&
		(s.exclude == to.exclude)
}

// Any checks whether any of the attributes is set
func (s *HostReconcileAttributes) Any(of *HostReconcileAttributes) bool {
	if s == nil {
		return false
	}
	if of == nil {
		return false
	}
	return false ||
		(s.add && of.add) ||
		(s.remove && of.remove) ||
		(s.modify && of.modify) ||
		(s.found && of.found) ||
		(s.exclude && of.exclude)
}

// SetStatus sets status
func (s *HostReconcileAttributes) SetStatus(status ObjectStatus) *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.status = status
	return s
}

// GetStatus gets status
func (s *HostReconcileAttributes) GetStatus() ObjectStatus {
	if s == nil {
		return ObjectStatus("")
	}
	return s.status
}

// SetAdd sets 'add' attribute
func (s *HostReconcileAttributes) SetAdd() *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.add = true
	return s
}

// UnsetAdd unsets 'add' attribute
func (s *HostReconcileAttributes) UnsetAdd() *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.add = false
	return s
}

// IsAdd checks whether 'add' attribute is set
func (s *HostReconcileAttributes) IsAdd() bool {
	if s == nil {
		return false
	}
	return s.add
}

// SetRemove sets 'remove' attribute
func (s *HostReconcileAttributes) SetRemove() *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.remove = true
	return s
}

// IsRemove checks whether 'remove' attribute is set
func (s *HostReconcileAttributes) IsRemove() bool {
	if s == nil {
		return false
	}
	return s.remove
}

// SetModify sets 'modify' attribute
func (s *HostReconcileAttributes) SetModify() *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.modify = true
	return s
}

// IsModify checks whether 'modify' attribute is set
func (s *HostReconcileAttributes) IsModify() bool {
	if s == nil {
		return false
	}
	return s.modify
}

// SetFound sets 'found' attribute
func (s *HostReconcileAttributes) SetFound() *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.found = true
	return s
}

// IsFound checks whether 'found' attribute is set
func (s *HostReconcileAttributes) IsFound() bool {
	if s == nil {
		return false
	}
	return s.found
}

// SetExclude sets 'exclude' attribute
func (s *HostReconcileAttributes) SetExclude() *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.exclude = true
	return s
}

// UnsetExclude unsets 'exclude' attribute
func (s *HostReconcileAttributes) UnsetExclude() *HostReconcileAttributes {
	if s == nil {
		return s
	}
	s.exclude = false
	return s
}

// IsExclude checks whether 'exclude' attribute is set
func (s *HostReconcileAttributes) IsExclude() bool {
	if s == nil {
		return false
	}
	return s.exclude
}

// String returns string form
func (s *HostReconcileAttributes) String() string {
	if s == nil {
		return "(nil)"
	}

	return fmt.Sprintf(
		"status: %s, add: %t, remove: %t, modify: %t, found: %t, exclude: %t",
		s.status,
		s.add,
		s.remove,
		s.modify,
		s.found,
		s.exclude,
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

	exclude int
}

// NewHostReconcileAttributesCounters creates new reconcile attributes
func NewHostReconcileAttributesCounters() *HostReconcileAttributesCounters {
	return &HostReconcileAttributesCounters{
		status: make(map[ObjectStatus]int),
	}
}

// Add adds to counters provided HostReconcileAttributes
func (s *HostReconcileAttributesCounters) Add(a *HostReconcileAttributes) {
	if s == nil {
		return
	}

	value, ok := s.status[a.GetStatus()]
	if ok {
		value = value + 1
	} else {
		value = 1
	}
	s.status[a.GetStatus()] = value

	if a.IsAdd() {
		s.add++
	}
	if a.IsRemove() {
		s.remove++
	}
	if a.IsModify() {
		s.modify++
	}
	if a.IsFound() {
		s.found++
	}
	if a.IsExclude() {
		s.exclude++
	}
}

// GetAdd gets added
func (s *HostReconcileAttributesCounters) GetAdd() int {
	if s == nil {
		return 0
	}
	return s.add
}

// GetRemove gets removed
func (s *HostReconcileAttributesCounters) GetRemove() int {
	if s == nil {
		return 0
	}
	return s.remove
}

// GetModify gets modified
func (s *HostReconcileAttributesCounters) GetModify() int {
	if s == nil {
		return 0
	}
	return s.modify
}

// GetFound gets found
func (s *HostReconcileAttributesCounters) GetFound() int {
	if s == nil {
		return 0
	}
	return s.found
}

// GetExclude gets exclude
func (s *HostReconcileAttributesCounters) GetExclude() int {
	if s == nil {
		return 0
	}
	return s.exclude
}

// AddOnly checks whether counters have Add() only items
func (s *HostReconcileAttributesCounters) AddOnly() bool {
	return s.GetAdd() > 0 && s.GetFound() == 0 && s.GetModify() == 0 && s.GetRemove() == 0
}

func (s *HostReconcileAttributesCounters) String() string {
	return fmt.Sprintf("a: %d f: %d m: %d r: %d", s.GetAdd(), s.GetFound(), s.GetModify(), s.GetRemove())
}
