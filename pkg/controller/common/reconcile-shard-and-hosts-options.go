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

package common

// ReconcileShardsAndHostsOptionsCtxKeyType specifies type for ReconcileShardsAndHostsOptionsCtxKey
// More details here on why do we need special type
// https://stackoverflow.com/questions/40891345/fix-should-not-use-basic-type-string-as-key-in-context-withvalue-golint
type ReconcileShardsAndHostsOptionsCtxKeyType string

// ReconcileShardsAndHostsOptionsCtxKey specifies name of the key to be used for ReconcileShardsAndHostsOptions
const ReconcileShardsAndHostsOptionsCtxKey ReconcileShardsAndHostsOptionsCtxKeyType = "ReconcileShardsAndHostsOptions"

// ReconcileShardsAndHostsOptions is and options for reconciler
type ReconcileShardsAndHostsOptions struct {
	FullFanOut bool
}
