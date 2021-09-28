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

package model

import (
	"fmt"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// EntityType specifies registry entity type
type EntityType string

// Possible entity types
const (
	StatefulSet EntityType = "StatefulSet"
	ConfigMap   EntityType = "ConfigMap"
	Service     EntityType = "Service"
	PVC         EntityType = "PVC"
	PV          EntityType = "PV"
)

// Registry specifies registry struct
type Registry struct {
	r map[EntityType][]v1.ObjectMeta
}

// NewRegistry creates new registry
func NewRegistry() *Registry {
	return &Registry{
		r: make(map[EntityType][]v1.ObjectMeta),
	}
}

// Len return len of the whole registry or specified entity types
func (r *Registry) Len(_what ...EntityType) int {
	if r == nil {
		return 0
	}
	if len(_what) == 0 {
		return len(r.r)
	}
	what := _what[0]
	return len(r.r[what])
}

// Walk walks over registry
func (r *Registry) Walk(f func(entityType EntityType, meta v1.ObjectMeta)) {
	if r == nil {
		return
	}
	for et := range r.r {
		for _, m := range r.r[et] {
			f(et, m)
		}
	}
}

// String makes string representation of the registry
func (r *Registry) String() string {
	if r == nil {
		return ""
	}
	s := ""
	r.Walk(func(entityType EntityType, meta v1.ObjectMeta) {
		s += fmt.Sprintf("%s: %s/%s\n", entityType, meta.Namespace, meta.Name)
	})
	return s
}

// registerEntity register entity
func (r *Registry) registerEntity(entityType EntityType, meta v1.ObjectMeta) {
	if r == nil {
		return
	}

	if r.hasEntity(entityType, meta) {
		return
	}
	// Does not have such an entity
	m := v1.ObjectMeta{
		Namespace:   meta.Namespace,
		Name:        meta.Name,
		Labels:      util.MergeStringMapsOverwrite(nil, meta.Labels),
		Annotations: util.MergeStringMapsOverwrite(nil, meta.Annotations),
	}
	r.r[entityType] = append(r.r[entityType], m)
}

// RegisterStatefulSet registers StatefulSet
func (r *Registry) RegisterStatefulSet(meta v1.ObjectMeta) {
	r.registerEntity(StatefulSet, meta)
}

// HasStatefulSet checks whether registry has specified StatefulSet
func (r *Registry) HasStatefulSet(meta v1.ObjectMeta) bool {
	return r.hasEntity(StatefulSet, meta)
}

// NumStatefulSet gets number of StatefulSet
func (r *Registry) NumStatefulSet() int {
	return r.Len(StatefulSet)
}

// RegisterConfigMap register ConfigMap
func (r *Registry) RegisterConfigMap(meta v1.ObjectMeta) {
	r.registerEntity(ConfigMap, meta)
}

// HasConfigMap checks whether registry has specified ConfigMap
func (r *Registry) HasConfigMap(meta v1.ObjectMeta) bool {
	return r.hasEntity(ConfigMap, meta)
}

// NumConfigMap gets number of ConfigMap
func (r *Registry) NumConfigMap() int {
	return r.Len(ConfigMap)
}

// RegisterService register Service
func (r *Registry) RegisterService(meta v1.ObjectMeta) {
	r.registerEntity(Service, meta)
}

// HasService checks whether registry has specified Service
func (r *Registry) HasService(meta v1.ObjectMeta) bool {
	return r.hasEntity(Service, meta)
}

// NumService gets number of Service
func (r *Registry) NumService() int {
	return r.Len(Service)
}

// RegisterPVC register PVC
func (r *Registry) RegisterPVC(meta v1.ObjectMeta) {
	r.registerEntity(PVC, meta)
}

// HasPVC checks whether registry has specified PVC
func (r *Registry) HasPVC(meta v1.ObjectMeta) bool {
	return r.hasEntity(PVC, meta)
}

// NumPVC gets number of PVC
func (r *Registry) NumPVC() int {
	return r.Len(PVC)
}

// RegisterPV register PV
func (r *Registry) RegisterPV(meta v1.ObjectMeta) {
	r.registerEntity(PV, meta)
}

// HasPV checks whether registry has specified PV
func (r *Registry) HasPV(meta v1.ObjectMeta) bool {
	return r.hasEntity(PV, meta)
}

// NumPV gets number of PV
func (r *Registry) NumPV() int {
	return r.Len(PV)
}

// hasEntity
func (r *Registry) hasEntity(entityType EntityType, meta v1.ObjectMeta) bool {
	if r.Len() == 0 {
		return false
	}

	for et := range r.r {
		if et == entityType {
			// This is searched entityType
			for _, m := range r.r[et] {
				if r.isEqual(m, meta) {
					// This is the element which is looked for.
					return true
				}
			}
			return false
		}
	}

	return false
}

// isEqual
func (r *Registry) isEqual(a, b v1.ObjectMeta) bool {
	return (a.Namespace == b.Namespace) && (a.Name == b.Name)
}

// deleteEntity
func (r *Registry) deleteEntity(entityType EntityType, meta v1.ObjectMeta) bool {
	if r.Len() == 0 {
		return false
	}

	for et := range r.r {
		if et == entityType {
			// This is searched entityType
			for i, m := range r.r[et] {
				if r.isEqual(m, meta) {
					// This is the element which is looked for.
					// Remove the element at index i
					//
					// Copy last element to index i.
					r.r[et][i] = r.r[et][len(r.r[et])-1]
					// Erase last element - help GC to collect
					r.r[et][len(r.r[et])-1] = v1.ObjectMeta{}
					// Truncate slice.
					r.r[et] = r.r[et][:len(r.r[et])-1]
					return true
				}
			}
			return false
		}
	}

	return false
}

// Subtract subtracts specified registry from main
func (r *Registry) Subtract(sub *Registry) *Registry {
	if sub.Len() == 0 {
		// Nothing to subtract, return base
		return r
	}
	if r.Len() == 0 {
		// Nowhere to subtract from
		return r
	}

	sub.Walk(func(entityType EntityType, entity v1.ObjectMeta) {
		r.deleteEntity(entityType, entity)
	})

	return r
}
