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

// CycleSpec defines spec of a cycle, such as size
type CycleSpec struct {
	// Size specifies size of a cycle
	Size int
}

// NewCycleSpec creates new CycleSpec
func NewCycleSpec(size int) *CycleSpec {
	return &CycleSpec{
		Size: size,
	}
}

// IsValid specifies whether spec is a valid one
func (s *CycleSpec) IsValid() bool {
	if s == nil {
		return false
	}

	return s.Size > 0
}

// CycleAddress defines cycle address of an entity
type CycleAddress struct {
	// CycleIndex specifies index of the cycle within something bigger
	CycleIndex int
	// Index specifies index within the cycle
	Index int
}

// NewCycleAddress creates new CycleAddress
func NewCycleAddress() *CycleAddress {
	return &CycleAddress{}
}

// Init initializes the CycleAddress
func (s *CycleAddress) Init() {
	if s == nil {
		return
	}
	s.CycleIndex = 0
	s.Index = 0
}

// Inc increases the CycleAddress
func (s *CycleAddress) Inc(spec *CycleSpec) {
	if s == nil {
		return
	}
	// Shift index within the cycle
	s.Index++
	// In case of overflow - shift to next cycle
	if spec.IsValid() && (s.Index >= spec.Size) {
		s.CycleIndex++
		s.Index = 0
	}
}

// ScopeAddress defines scope address of an entity
type ScopeAddress struct {
	// CycleSpec specifies cycle which to be used to specify CycleAddress
	CycleSpec *CycleSpec
	// CycleAddress specifies CycleAddress within the scope
	CycleAddress *CycleAddress
	// Index specifies index within the scope
	Index int
}

// NewScopeAddress creates new ScopeAddress
func NewScopeAddress(cycleSize int) *ScopeAddress {
	return &ScopeAddress{
		CycleSpec:    NewCycleSpec(cycleSize),
		CycleAddress: NewCycleAddress(),
	}
}

// Init initializes the ScopeAddress
func (s *ScopeAddress) Init() {
	if s == nil {
		return
	}
	s.CycleAddress.Init()
	s.Index = 0
}

// Inc increases the ScopeAddress
func (s *ScopeAddress) Inc() {
	if s == nil {
		return
	}
	s.CycleAddress.Inc(s.CycleSpec)
	s.Index++
}

// HostScopeAddress specifies address of a host
type HostScopeAddress struct {
	// CHIScopeAddress specifies address of a host within CHI scope
	CHIScopeAddress *ScopeAddress
	// ClusterScopeAddress specifies address of a host within cluster scope
	ClusterScopeAddress *ScopeAddress
	// ClusterIndex specifies index of a cluster within CHI
	ClusterIndex int
	// ShardIndex specifies index of a shard within cluster
	ShardIndex int
	// ReplicaIndex specifies index of a replica within cluster
	ReplicaIndex int
}

// NewHostScopeAddress creates new HostScopeAddress
func NewHostScopeAddress(chiScopeCycleSize, clusterScopeCycleSize int) (a *HostScopeAddress) {
	a = &HostScopeAddress{
		CHIScopeAddress:     NewScopeAddress(chiScopeCycleSize),
		ClusterScopeAddress: NewScopeAddress(clusterScopeCycleSize),
	}
	return a
}

// WalkHostsAddressFn specifies function to walk over hosts
type WalkHostsAddressFn func(
	chi *ClickHouseInstallation,
	cluster *Cluster,
	shard *ChiShard,
	replica *ChiReplica,
	host *ChiHost,
	address *HostScopeAddress,
) error
