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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKeeperRefGetServiceType verifies the accessor defaults to Replicas, folds any
// accepted casing to the canonical const, and passes unrecognized values through so the
// resolver can flag them as invalid.
func TestKeeperRefGetServiceType(t *testing.T) {
	tests := []struct {
		name string
		ref  *KeeperRef
		want KeeperServiceType
	}{
		{"nil defaults to Replicas", nil, KeeperServiceTypeReplicas},
		{"empty defaults to Replicas", &KeeperRef{}, KeeperServiceTypeReplicas},
		{"canonical Replicas", &KeeperRef{ServiceType: "Replicas"}, KeeperServiceTypeReplicas},
		{"lowercase replicas folds", &KeeperRef{ServiceType: "replicas"}, KeeperServiceTypeReplicas},
		{"canonical Service", &KeeperRef{ServiceType: "Service"}, KeeperServiceTypeService},
		{"uppercase SERVICE folds", &KeeperRef{ServiceType: "SERVICE"}, KeeperServiceTypeService},
		{"unrecognized passes through", &KeeperRef{ServiceType: "bogus"}, KeeperServiceType("bogus")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.ref.GetServiceType())
		})
	}
}
