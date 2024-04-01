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

// HostTemplatesIndex describes index of host templates
type HostTemplatesIndex struct {
	// templates maps 'name of the template' -> 'template itself'
	templates map[string]*ChiHostTemplate `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// NewHostTemplatesIndex creates new HostTemplatesIndex object
func NewHostTemplatesIndex() *HostTemplatesIndex {
	return &HostTemplatesIndex{
		templates: make(map[string]*ChiHostTemplate),
	}
}

// Has checks whether index has entity `name`
func (i *HostTemplatesIndex) Has(name string) bool {
	if i == nil {
		return false
	}
	if i.templates == nil {
		return false
	}
	_, ok := i.templates[name]
	return ok
}

// Get returns entity `name` from the index
func (i *HostTemplatesIndex) Get(name string) *ChiHostTemplate {
	if !i.Has(name) {
		return nil
	}
	return i.templates[name]
}

// Set sets named template into index
func (i *HostTemplatesIndex) Set(name string, entry *ChiHostTemplate) {
	if i == nil {
		return
	}
	if i.templates == nil {
		return
	}
	i.templates[name] = entry
}

// Walk calls specified function over each entry in the index
func (i *HostTemplatesIndex) Walk(f func(template *ChiHostTemplate)) {
	if i == nil {
		return
	}
	for _, entry := range i.templates {
		f(entry)
	}
}

// PodTemplatesIndex describes index of pod templates
type PodTemplatesIndex struct {
	// templates maps 'name of the template' -> 'template itself'
	templates map[string]*ChiPodTemplate `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// NewPodTemplatesIndex creates new PodTemplatesIndex object
func NewPodTemplatesIndex() *PodTemplatesIndex {
	return &PodTemplatesIndex{
		templates: make(map[string]*ChiPodTemplate),
	}
}

// Has checks whether index has entity `name`
func (i *PodTemplatesIndex) Has(name string) bool {
	if i == nil {
		return false
	}
	if i.templates == nil {
		return false
	}
	_, ok := i.templates[name]
	return ok
}

// Get returns entity `name` from the index
func (i *PodTemplatesIndex) Get(name string) *ChiPodTemplate {
	if !i.Has(name) {
		return nil
	}
	return i.templates[name]
}

// Set sets named template into index
func (i *PodTemplatesIndex) Set(name string, entry *ChiPodTemplate) {
	if i == nil {
		return
	}
	if i.templates == nil {
		return
	}
	i.templates[name] = entry
}

// Walk calls specified function over each entry in the index
func (i *PodTemplatesIndex) Walk(f func(template *ChiPodTemplate)) {
	if i == nil {
		return
	}
	for _, entry := range i.templates {
		f(entry)
	}
}

// VolumeClaimTemplatesIndex describes index of volume claim templates
type VolumeClaimTemplatesIndex struct {
	// templates maps 'name of the template' -> 'template itself'
	templates map[string]*VolumeClaimTemplate `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// NewVolumeClaimTemplatesIndex creates new VolumeClaimTemplatesIndex object
func NewVolumeClaimTemplatesIndex() *VolumeClaimTemplatesIndex {
	return &VolumeClaimTemplatesIndex{
		templates: make(map[string]*VolumeClaimTemplate),
	}
}

// Has checks whether index has entity `name`
func (i *VolumeClaimTemplatesIndex) Has(name string) bool {
	if i == nil {
		return false
	}
	if i.templates == nil {
		return false
	}
	_, ok := i.templates[name]
	return ok
}

// Get returns entity `name` from the index
func (i *VolumeClaimTemplatesIndex) Get(name string) *VolumeClaimTemplate {
	if !i.Has(name) {
		return nil
	}
	return i.templates[name]
}

// Set sets named template into index
func (i *VolumeClaimTemplatesIndex) Set(name string, entry *VolumeClaimTemplate) {
	if i == nil {
		return
	}
	if i.templates == nil {
		return
	}
	i.templates[name] = entry
}

// Walk calls specified function over each entry in the index
func (i *VolumeClaimTemplatesIndex) Walk(f func(template *VolumeClaimTemplate)) {
	if i == nil {
		return
	}
	for _, entry := range i.templates {
		f(entry)
	}
}

// ServiceTemplatesIndex describes index of service templates
type ServiceTemplatesIndex struct {
	// templates maps 'name of the template' -> 'template itself'
	templates map[string]*ServiceTemplate `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// NewServiceTemplatesIndex creates new ServiceTemplatesIndex object
func NewServiceTemplatesIndex() *ServiceTemplatesIndex {
	return &ServiceTemplatesIndex{
		templates: make(map[string]*ServiceTemplate),
	}
}

// Has checks whether index has entity `name`
func (i *ServiceTemplatesIndex) Has(name string) bool {
	if i == nil {
		return false
	}
	if i.templates == nil {
		return false
	}
	_, ok := i.templates[name]
	return ok
}

// Get returns entity `name` from the index
func (i *ServiceTemplatesIndex) Get(name string) *ServiceTemplate {
	if !i.Has(name) {
		return nil
	}
	return i.templates[name]
}

// Set sets named template into index
func (i *ServiceTemplatesIndex) Set(name string, entry *ServiceTemplate) {
	if i == nil {
		return
	}
	if i.templates == nil {
		return
	}
	i.templates[name] = entry
}

// Walk calls specified function over each entry in the index
func (i *ServiceTemplatesIndex) Walk(f func(template *ServiceTemplate)) {
	if i == nil {
		return
	}
	for _, entry := range i.templates {
		f(entry)
	}
}
