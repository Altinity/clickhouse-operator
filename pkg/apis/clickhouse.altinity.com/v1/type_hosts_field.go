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

// HostsField specifies field of hosts
type HostsField struct {
	ShardsCount   int
	ReplicasCount int
	Field         [][]*ChiHost
}

// NewHostsField creates new field of hosts
func NewHostsField(shards, replicas int) *HostsField {
	hf := new(HostsField)

	hf.ShardsCount = shards
	hf.ReplicasCount = replicas

	hf.Field = make([][]*ChiHost, hf.ShardsCount)
	for shard := 0; shard < hf.ShardsCount; shard++ {
		hf.Field[shard] = make([]*ChiHost, hf.ReplicasCount)
	}

	return hf
}

// Set sets host on specified coordinates
func (hf *HostsField) Set(shard, replica int, host *ChiHost) {
	hf.Field[shard][replica] = host
}

// Get gets host from specified coordinates
func (hf *HostsField) Get(shard, replica int) *ChiHost {
	return hf.Field[shard][replica]
}

// GetOrCreate gets and creates if necessary
func (hf *HostsField) GetOrCreate(shard, replica int) *ChiHost {
	if hf.Field[shard][replica] == nil {
		hf.Field[shard][replica] = new(ChiHost)
	}
	return hf.Field[shard][replica]
}

// WalkHosts walks hosts with a function
func (hf *HostsField) WalkHosts(f func(shard, replica int, host *ChiHost) error) []error {
	res := make([]error, 0)

	for shardIndex := range hf.Field {
		for replicaIndex := range hf.Field[shardIndex] {
			if host := hf.Get(shardIndex, replicaIndex); host != nil {
				res = append(res, f(shardIndex, replicaIndex, host))
			}
		}
	}

	return res
}

// HostsCount returns hosts number
func (hf *HostsField) HostsCount() int {
	count := 0
	hf.WalkHosts(func(shard, replica int, host *ChiHost) error {
		count++
		return nil
	})
	return count
}
