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

// Cleanup defines cleanup
type Cleanup struct {
	// UnknownObjects specifies cleanup of unknown objects
	UnknownObjects *ObjectsCleanup `json:"unknownObjects,omitempty" yaml:"unknownObjects,omitempty"`
	// ReconcileFailedObjects specifies cleanup of failed objects
	ReconcileFailedObjects *ObjectsCleanup `json:"reconcileFailedObjects,omitempty" yaml:"reconcileFailedObjects,omitempty"`
}

// NewCleanup creates new cleanup
func NewCleanup() *Cleanup {
	return new(Cleanup)
}

// MergeFrom merges from specified cleanup
func (t *Cleanup) MergeFrom(from *Cleanup, _type MergeType) *Cleanup {
	if from == nil {
		return t
	}

	if t == nil {
		t = NewCleanup()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
	case MergeTypeOverrideByNonEmptyValues:
	}

	t.UnknownObjects = t.UnknownObjects.MergeFrom(from.UnknownObjects, _type)
	t.ReconcileFailedObjects = t.ReconcileFailedObjects.MergeFrom(from.ReconcileFailedObjects, _type)

	return t
}

// GetUnknownObjects gets unknown objects cleanup
func (t *Cleanup) GetUnknownObjects() *ObjectsCleanup {
	if t == nil {
		return nil
	}
	return t.UnknownObjects
}

// DefaultUnknownObjects makes default cleanup for known objects
func (t *Cleanup) DefaultUnknownObjects() *ObjectsCleanup {
	return NewObjectsCleanup().
		SetStatefulSet(ObjectsCleanupDelete).
		SetPVC(ObjectsCleanupDelete).
		SetConfigMap(ObjectsCleanupDelete).
		SetService(ObjectsCleanupDelete)
}

// GetReconcileFailedObjects gets failed objects cleanup
func (t *Cleanup) GetReconcileFailedObjects() *ObjectsCleanup {
	if t == nil {
		return nil
	}
	return t.ReconcileFailedObjects
}

// DefaultReconcileFailedObjects makes default cleanup for failed objects
func (t *Cleanup) DefaultReconcileFailedObjects() *ObjectsCleanup {
	return NewObjectsCleanup().
		SetStatefulSet(ObjectsCleanupRetain).
		SetPVC(ObjectsCleanupRetain).
		SetConfigMap(ObjectsCleanupRetain).
		SetService(ObjectsCleanupRetain)
}

// SetDefaults set defaults for cleanup
func (t *Cleanup) SetDefaults() *Cleanup {
	if t == nil {
		return nil
	}
	t.UnknownObjects = t.DefaultUnknownObjects()
	t.ReconcileFailedObjects = t.DefaultReconcileFailedObjects()
	return t
}

// Possible objects cleanup options
const (
	ObjectsCleanupUnspecified = "Unspecified"
	ObjectsCleanupRetain      = "Retain"
	ObjectsCleanupDelete      = "Delete"
)

// ObjectsCleanup specifies object cleanup struct
type ObjectsCleanup struct {
	StatefulSet string `json:"statefulSet,omitempty" yaml:"statefulSet,omitempty"`
	PVC         string `json:"pvc,omitempty"         yaml:"pvc,omitempty"`
	ConfigMap   string `json:"configMap,omitempty"   yaml:"configMap,omitempty"`
	Service     string `json:"service,omitempty"     yaml:"service,omitempty"`
	Secret      string `json:"secret,omitempty"      yaml:"secret,omitempty"`
}

// NewObjectsCleanup creates new object cleanup
func NewObjectsCleanup() *ObjectsCleanup {
	return new(ObjectsCleanup)
}

// MergeFrom merges from specified cleanup
func (c *ObjectsCleanup) MergeFrom(from *ObjectsCleanup, _type MergeType) *ObjectsCleanup {
	if from == nil {
		return c
	}

	if c == nil {
		c = NewObjectsCleanup()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if c.StatefulSet == "" {
			c.StatefulSet = from.StatefulSet
		}
		if c.PVC == "" {
			c.PVC = from.PVC
		}
		if c.ConfigMap == "" {
			c.ConfigMap = from.ConfigMap
		}
		if c.Service == "" {
			c.Service = from.Service
		}
		if c.Secret == "" {
			c.Secret = from.Secret
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.StatefulSet != "" {
			// Override by non-empty values only
			c.StatefulSet = from.StatefulSet
		}
		if from.PVC != "" {
			// Override by non-empty values only
			c.PVC = from.PVC
		}
		if from.ConfigMap != "" {
			// Override by non-empty values only
			c.ConfigMap = from.ConfigMap
		}
		if from.Service != "" {
			// Override by non-empty values only
			c.Service = from.Service
		}
		if from.Secret != "" {
			// Override by non-empty values only
			c.Secret = from.Secret
		}
	}

	return c
}

// GetStatefulSet gets stateful set
func (c *ObjectsCleanup) GetStatefulSet() string {
	if c == nil {
		return ""
	}
	return c.StatefulSet
}

// SetStatefulSet sets stateful set
func (c *ObjectsCleanup) SetStatefulSet(v string) *ObjectsCleanup {
	if c == nil {
		return nil
	}
	c.StatefulSet = v
	return c
}

// GetPVC gets PVC
func (c *ObjectsCleanup) GetPVC() string {
	if c == nil {
		return ""
	}
	return c.PVC
}

// SetPVC sets PVC
func (c *ObjectsCleanup) SetPVC(v string) *ObjectsCleanup {
	if c == nil {
		return nil
	}
	c.PVC = v
	return c
}

// GetConfigMap gets config map
func (c *ObjectsCleanup) GetConfigMap() string {
	if c == nil {
		return ""
	}
	return c.ConfigMap
}

// SetConfigMap sets config map
func (c *ObjectsCleanup) SetConfigMap(v string) *ObjectsCleanup {
	if c == nil {
		return nil
	}
	c.ConfigMap = v
	return c
}

// GetService gets service
func (c *ObjectsCleanup) GetService() string {
	if c == nil {
		return ""
	}
	return c.Service
}

// SetService sets service
func (c *ObjectsCleanup) SetService(v string) *ObjectsCleanup {
	if c == nil {
		return nil
	}
	c.Service = v
	return c
}

// GetSecret gets secret
func (c *ObjectsCleanup) GetSecret() string {
	if c == nil {
		return ""
	}
	return c.Secret
}

// SetSecret sets service
func (c *ObjectsCleanup) SetSecret(v string) *ObjectsCleanup {
	if c == nil {
		return nil
	}
	c.Secret = v
	return c
}
