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
	"sync"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// EntityType specifies registry entity type
type EntityType string

// Possible entity types
const (
	// StatefulSet describes StatefulSet entity type
	StatefulSet EntityType = "StatefulSet"
	// ConfigMap describes ConfigMap entity type
	ConfigMap EntityType = "ConfigMap"
	// Service describes Service entity type
	Service EntityType = "Service"
	// Secret describes Secret entity type
	Secret EntityType = "Secret"
	// PVC describes PersistentVolumeClaim entity type
	PVC EntityType = "PVC"
	// PV describes PersistentVolume entity type
	PV EntityType = "PV"
	// PDB describes PodDisruptionBudget entity type
	PDB EntityType = "PDB"
)

// Registry specifies registry struct
type Registry struct {
	r  map[EntityType]*objectMetaSet
	mu sync.RWMutex
}

// objectMetaIdentity is a simple subset of ObjectMeta used strictly within Registry for identifying
// ObjectMeta without inspecting their full contents. I.e., this is used for existence checks and comparisons.
type objectMetaIdentity struct {
	name      string
	namespace string
}

// objectMetaSet is an internal collections type used for efficient lookups of object metadata by identity,
// defined in this context as the combination of namespace and name. A set is expected to correspond to a single
// entity type, but set additions to not validate this property to avoid the introduction of error results onto
// associated interfaces (including the exported Registry functions).
// All accesses are synchronized.
type objectMetaSet struct {
	entityType EntityType
	contents   map[objectMetaIdentity]v1.ObjectMeta
	sync.RWMutex
}

// NewRegistry creates new registry
func NewRegistry() *Registry {
	return &Registry{
		r: make(map[EntityType]*objectMetaSet),
	}
}

// Len return len of the whole registry or specified entity types
// Note that this is unsafe to call recursively, including in calls to other synchronized Registry functions
// like Walk (and therefore in the "work" function passed into iterators like Walk and WalkEntityType).
func (r *Registry) Len(_what ...EntityType) int {
	if r == nil {
		return 0
	}

	// Avoid coarse grained locking when we will just return the number of entity types in the registry.
	if len(_what) == 0 {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return len(r.r)
	}

	result := 0
	for _, entityType := range _what {
		os := r.ensureObjectSetForType(entityType)
		result += os.len()
	}
	return result
}

// Walk walks over registry.
// Note: this is fairly expensive in the sense that it locks the entire registry from being written
// for the full duration of whatever workload is applied throughout iteration. Avoid calling when you know
// the entity type you want.
func (r *Registry) Walk(f func(entityType EntityType, meta v1.ObjectMeta)) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	for et, os := range r.r {
		os.walk(func(meta v1.ObjectMeta) {
			f(et, meta)
		})
	}
}

// WalkEntityType walks over registry
func (r *Registry) WalkEntityType(entityType EntityType, f func(meta v1.ObjectMeta)) {
	if r == nil {
		return
	}

	setForType := r.ensureObjectSetForType(entityType)
	setForType.walk(f)
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

	// Try to minimize coarse grained locking at the registry level. Immediately getOrCreate for the entity type
	// and then begin operating on that (it uses a finer grained lock).
	setForType := r.ensureObjectSetForType(entityType)

	// Create the representation that we'll attempt to add.
	newObj := v1.ObjectMeta{
		Namespace:   meta.Namespace,
		Name:        meta.Name,
		Labels:      util.MergeStringMapsOverwrite(nil, meta.Labels),
		Annotations: util.MergeStringMapsOverwrite(nil, meta.Annotations),
	}

	// Add the object, which will only happen if no other object with the same identity is present in the set.
	setForType.maybeAdd(&newObj)
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

// WalkStatefulSet walk over specified entity types
func (r *Registry) WalkStatefulSet(f func(meta v1.ObjectMeta)) {
	r.WalkEntityType(StatefulSet, f)
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

// WalkConfigMap walk over specified entity types
func (r *Registry) WalkConfigMap(f func(meta v1.ObjectMeta)) {
	r.WalkEntityType(ConfigMap, f)
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

// WalkService walk over specified entity types
func (r *Registry) WalkService(f func(meta v1.ObjectMeta)) {
	r.WalkEntityType(Service, f)
}

// RegisterSecret register Secret
func (r *Registry) RegisterSecret(meta v1.ObjectMeta) {
	r.registerEntity(Secret, meta)
}

// HasSecret checks whether registry has specified Secret
func (r *Registry) HasSecret(meta v1.ObjectMeta) bool {
	return r.hasEntity(Secret, meta)
}

// NumSecret gets number of Secret
func (r *Registry) NumSecret() int {
	return r.Len(Secret)
}

// WalkSecret walk over specified entity types
func (r *Registry) WalkSecret(f func(meta v1.ObjectMeta)) {
	r.WalkEntityType(Secret, f)
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

// WalkPVC walk over specified entity types
func (r *Registry) WalkPVC(f func(meta v1.ObjectMeta)) {
	r.WalkEntityType(PVC, f)
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

// WalkPV walk over specified entity types
func (r *Registry) WalkPV(f func(meta v1.ObjectMeta)) {
	r.WalkEntityType(PV, f)
}

// RegisterPDB register PDB
func (r *Registry) RegisterPDB(meta v1.ObjectMeta) {
	r.registerEntity(PDB, meta)
}

// HasPDB checks whether registry has specified PDB
func (r *Registry) HasPDB(meta v1.ObjectMeta) bool {
	return r.hasEntity(PDB, meta)
}

// NumPDB gets number of PDB
func (r *Registry) NumPDB() int {
	return r.Len(PDB)
}

// WalkPDB walk over specified entity types
func (r *Registry) WalkPDB(f func(meta v1.ObjectMeta)) {
	r.WalkEntityType(PDB, f)
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

// hasEntity
func (r *Registry) hasEntity(entityType EntityType, meta v1.ObjectMeta) bool {
	// Try to minimize coarse grained locking at the registry level. Immediately getOrCreate for the entity type
	// and then begin operating on that (it uses a finer grained lock).
	setForType := r.ensureObjectSetForType(entityType)

	// Having acquired the type-specific ObjectMeta set, return the result of a membership check.
	return setForType.contains(&meta)
}

// isEqual
func (r *Registry) isEqual(a, b v1.ObjectMeta) bool {
	return (a.Namespace == b.Namespace) && (a.Name == b.Name)
}

// deleteEntity
func (r *Registry) deleteEntity(entityType EntityType, meta v1.ObjectMeta) bool {
	// Try to minimize coarse grained locking at the registry level. Immediately getOrCreate for the entity type
	// and then begin operating on that (it uses a finer grained lock).
	setForType := r.ensureObjectSetForType(entityType)

	// Having acquired the type-specific ObjectMeta set, return the result of removal success.
	return setForType.remove(&meta)
}

// ensureObjectSetForType resolves the singleton objectMetaSet for this registry, of the given entityType.
// This uses a coarse grained lock on the entire registry.
func (r *Registry) ensureObjectSetForType(entityType EntityType) *objectMetaSet {
	if r == nil {
		return nil
	}

	// get case (optimize for the most common condition of the set already existing)
	r.mu.RLock()
	existing, ok := r.r[entityType]
	r.mu.RUnlock()
	if ok {
		return existing
	}

	// create case (requires write lock, but note that we have to double for existence within critical section)
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok = r.r[entityType]
	if ok {
		return existing
	}
	newSet := newObjectMetaSet(entityType)
	r.r[entityType] = newSet
	return newSet
}

// objIdentity derives a objectMetaIdentity from an ObjectMeta
func (s *objectMetaSet) objIdentity(obj *v1.ObjectMeta) objectMetaIdentity {
	return objectMetaIdentity{
		name:      obj.Name,
		namespace: obj.Namespace,
	}
}

// maybeAdd adds an ObjectMeta to the set if an object with an equivalent identity is not already present
func (s *objectMetaSet) maybeAdd(o *v1.ObjectMeta) bool {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.contents[s.objIdentity(o)]; ok {
		return false
	}
	s.contents[s.objIdentity(o)] = *o
	return true
}

// remove deletes an ObjectMeta from the set, matching only on identity
func (s *objectMetaSet) remove(o *v1.ObjectMeta) bool {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.contents[s.objIdentity(o)]; !ok {
		return false
	}
	delete(s.contents, s.objIdentity(o))
	return true
}

// contains determines if an ObjectMeta exists in the set (based on identity only)
func (s *objectMetaSet) contains(o *v1.ObjectMeta) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.contents[s.objIdentity(o)]
	return ok
}

// walk provides an iterator-like access to the ObjectMetas contained in the set
// Note that this function is not safe to call recursively, due to the RWLock usage.
// This seems unlikely to be a problem.
func (s *objectMetaSet) walk(f func(meta v1.ObjectMeta)) {
	s.RLock()
	defer s.RUnlock()

	for _, obj := range s.contents {
		f(obj)
	}
}

// len returns the number of ObjectMeta in the set
func (s *objectMetaSet) len() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.contents)
}

// newObjectMetaSet constructor for a set holding ObjectMeta with reader/writer synchronization
func newObjectMetaSet(entityType EntityType) *objectMetaSet {
	return &objectMetaSet{
		entityType: entityType,
		contents:   make(map[objectMetaIdentity]v1.ObjectMeta),
	}
}
