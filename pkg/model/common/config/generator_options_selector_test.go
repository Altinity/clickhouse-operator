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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// Documents the contract the CHK staged-raft mechanism relies on:
// a selector excluding TagExclude drops exactly the hosts that carry the tag,
// regardless of their object status.
func TestHostSelectorExcludesByExcludeTag(t *testing.T) {
	selector := NewHostSelector().ExcludeReconcileAttributes(
		types.NewReconcileAttributes().SetExclude(),
	)

	staged := &chi.Host{}
	staged.GetReconcileAttributes().SetStatus(types.ObjectStatusRequested).SetExclude()

	published := &chi.Host{}
	published.GetReconcileAttributes().SetStatus(types.ObjectStatusRequested)

	found := &chi.Host{}
	found.GetReconcileAttributes().SetStatus(types.ObjectStatusFound)

	require.False(t, selector.Include(staged), "staged host must be excluded from raft XML")
	require.True(t, selector.Include(published), "un-staged new host must be included")
	require.True(t, selector.Include(found), "established host must be included")

	staged.GetReconcileAttributes().UnsetExclude()
	require.True(t, selector.Include(staged), "host must be included after include-into-raft unsets the tag")
}
