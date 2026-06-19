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

// TestPVCReclaimPolicyNormalize verifies casing-folding to the canonical humped const,
// so the CRD can accept both humped and all-lowercase forms.
func TestPVCReclaimPolicyNormalize(t *testing.T) {
	tests := []struct {
		in       string
		expected PVCReclaimPolicy
	}{
		{"Retain", PVCReclaimPolicyRetain},
		{"retain", PVCReclaimPolicyRetain},
		{"RETAIN", PVCReclaimPolicyRetain},
		{"Delete", PVCReclaimPolicyDelete},
		{"delete", PVCReclaimPolicyDelete},
		{"DELETE", PVCReclaimPolicyDelete},
		{"", PVCReclaimPolicyUnspecified},
		{"bogus", "bogus"}, // unrecognized: returned unchanged (caller's IsValid resets)
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.expected, PVCReclaimPolicy(tc.in).Normalize())
		})
	}
}

// TestPVCProvisionerNormalize verifies casing-folding to the canonical humped const.
func TestPVCProvisionerNormalize(t *testing.T) {
	tests := []struct {
		in       string
		expected PVCProvisioner
	}{
		{"StatefulSet", PVCProvisionerStatefulSet},
		{"statefulset", PVCProvisionerStatefulSet},
		{"STATEFULSET", PVCProvisionerStatefulSet},
		{"Operator", PVCProvisionerOperator},
		{"operator", PVCProvisionerOperator},
		{"", PVCProvisionerUnspecified},
		{"bogus", "bogus"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.expected, PVCProvisioner(tc.in).Normalize())
		})
	}
}
