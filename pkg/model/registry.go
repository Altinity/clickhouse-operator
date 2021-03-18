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
	"github.com/altinity/clickhouse-operator/pkg/util"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type EntityType string

const StatefulSet EntityType = "StatefulSet"
const ConfigMap EntityType = "ConfigMap"
const Service EntityType = "Service"
const PVC EntityType = "PVC"
const PV EntityType = "PV"

type Registry struct {
	r map[EntityType][]v1.ObjectMeta
}

func NewRegistry() *Registry {
	return &Registry{
		r: make(map[EntityType][]v1.ObjectMeta),
	}
}

func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	return len(r.r)
}

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

func (r *Registry) RegisterStatefulSet(meta v1.ObjectMeta) {
	r.registerEntity(StatefulSet, meta)
}

func (r *Registry) RegisterConfigMap(meta v1.ObjectMeta) {
	r.registerEntity(ConfigMap, meta)
}

func (r *Registry) RegisterService(meta v1.ObjectMeta) {
	r.registerEntity(Service, meta)
}

func (r *Registry) RegisterPVC(meta v1.ObjectMeta) {
	r.registerEntity(PVC, meta)
}

func (r *Registry) RegisterPV(meta v1.ObjectMeta) {
	r.registerEntity(PV, meta)
}

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

func (r *Registry) isEqual(a, b v1.ObjectMeta) bool {
	return (a.Namespace == b.Namespace) && (a.Name == b.Name)
}

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
