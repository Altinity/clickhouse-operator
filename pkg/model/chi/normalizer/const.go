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

package normalizer

const (
	// defaultReconcileShardsThreadsNumber specifies the default number of threads usable for concurrent shard reconciliation
	// within a single cluster reconciliation. Defaults to 1, which means strictly sequential shard reconciliation.
	defaultReconcileShardsThreadsNumber = 1

	// defaultReconcileShardsMaxConcurrencyPercent specifies the maximum integer percentage of shards that may be reconciled
	// concurrently during cluster reconciliation. This counterbalances the fact that this is an operator setting,
	// that different clusters will have different shard counts, and that the shard concurrency capacity is specified
	// above in terms of a number of threads to use (up to). Example: overriding to 100 means all shards may be
	// reconciled concurrently, if the number of shard reconciliation threads is greater than or equal to the number
	// of shards in the cluster.
	defaultReconcileShardsMaxConcurrencyPercent = 50
)
