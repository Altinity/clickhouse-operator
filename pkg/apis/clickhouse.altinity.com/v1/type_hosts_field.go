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

type HostsField struct {
	Shards   int
	Replicas int
	Field    [][]*ChiHost
}

func NewHostsField(shards, replicas int) *HostsField {
	hf := &HostsField{}

	hf.Shards = shards
	hf.Replicas = replicas

	hf.Field = make([][]*ChiHost, hf.Shards)
	for shard := 0; shard < hf.Shards; shard++ {
		hf.Field[shard] = make([]*ChiHost, hf.Replicas)
	}

	return hf
}

func (hf *HostsField) Set(shard, replica int, host *ChiHost) {
	hf.Field[shard][replica] = host
}

func (hf *HostsField) Get(shard, replica int) *ChiHost {
	return hf.Field[shard][replica]
}

func (hf *HostsField) WalkHosts(
	f func(shard, replica int, host *ChiHost) error,
) []error {
	res := make([]error, 0)

	for shard := 0; shard < hf.Shards; shard++ {
		for replica := 0; replica < hf.Replicas; replica++ {
			res = append(res, f(shard, replica, hf.Get(shard, replica)))
		}
	}

	return res
}

func (hf *HostsField) HostsCount() int {
	count := 0
	hf.WalkHosts(func(shard, replica int, host *ChiHost) error {
		if host != nil {
			count++
		}
		return nil
	})
	return count
}
