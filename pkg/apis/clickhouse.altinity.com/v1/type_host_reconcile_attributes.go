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

// StatefulSetStatus specifies StatefulSet status
type StatefulSetStatus string

// Possible values for StatefulSet status
const (
	StatefulSetStatusModified StatefulSetStatus = "modified"
	StatefulSetStatusNew      StatefulSetStatus = "new"
	StatefulSetStatusSame     StatefulSetStatus = "same"
	StatefulSetStatusUnknown  StatefulSetStatus = "unknown"
)

// ChiHostReconcileAttributes defines host reconcile status and attributes
type ChiHostReconcileAttributes struct {
	status StatefulSetStatus

	// Attributes are used by config generator

	add    bool
	remove bool
	modify bool
	found  bool
}

// NewChiHostReconcileAttributes creates new reconcile attributes
func NewChiHostReconcileAttributes() *ChiHostReconcileAttributes {
	return &ChiHostReconcileAttributes{}
}

// Equal checks whether reconcile attributes are equal
func (s *ChiHostReconcileAttributes) Equal(to ChiHostReconcileAttributes) bool {
	if s == nil {
		return false
	}
	return true &&
		(s.add == to.add) &&
		(s.remove == to.remove) &&
		(s.modify == to.modify) &&
		(s.found == to.found)
}

// Any checks whether any of the attributes is set
func (s *ChiHostReconcileAttributes) Any(of *ChiHostReconcileAttributes) bool {
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
		(s.found && of.found)
}

// SetStatus sets status
func (s *ChiHostReconcileAttributes) SetStatus(status StatefulSetStatus) *ChiHostReconcileAttributes {
	if s == nil {
		return s
	}
	s.status = status
	return s
}

// GetStatus gets status
func (s *ChiHostReconcileAttributes) GetStatus() StatefulSetStatus {
	if s == nil {
		return StatefulSetStatus("")
	}
	return s.status
}

// SetAdd sets 'add' attribute
func (s *ChiHostReconcileAttributes) SetAdd() *ChiHostReconcileAttributes {
	if s == nil {
		return s
	}
	s.add = true
	return s
}

// UnsetAdd unsets 'add' attribute
func (s *ChiHostReconcileAttributes) UnsetAdd() *ChiHostReconcileAttributes {
	if s == nil {
		return s
	}
	s.add = false
	return s
}

// SetRemove sets 'remove' attribute
func (s *ChiHostReconcileAttributes) SetRemove() *ChiHostReconcileAttributes {
	if s == nil {
		return s
	}
	s.remove = true
	return s
}

// SetModify sets 'modify' attribute
func (s *ChiHostReconcileAttributes) SetModify() *ChiHostReconcileAttributes {
	if s == nil {
		return s
	}
	s.modify = true
	return s
}

// SetFound sets 'found' attribute
func (s *ChiHostReconcileAttributes) SetFound() *ChiHostReconcileAttributes {
	if s == nil {
		return s
	}
	s.found = true
	return s
}

// IsAdd checks whether 'add' attribute is set
func (s *ChiHostReconcileAttributes) IsAdd() bool {
	if s == nil {
		return false
	}
	return s.add
}

// IsRemove checks whether 'remove' attribute is set
func (s *ChiHostReconcileAttributes) IsRemove() bool {
	if s == nil {
		return false
	}
	return s.remove
}

// IsModify checks whether 'modify' attribute is set
func (s *ChiHostReconcileAttributes) IsModify() bool {
	if s == nil {
		return false
	}
	return s.modify
}

// IsFound checks whether 'found' attribute is set
func (s *ChiHostReconcileAttributes) IsFound() bool {
	if s == nil {
		return false
	}
	return s.found
}

// String returns string form
func (s *ChiHostReconcileAttributes) String() string {
	if s == nil {
		return "(nil)"
	}

	return fmt.Sprintf(
		"status: %s, add: %t, remove: %t, modify: %t, found: %t",
		s.status,
		s.add,
		s.remove,
		s.modify,
		s.found,
	)
}
