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
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func ReplicaCanDeletePVC(replica *chiv1.ChiReplica) bool {
	templateName := replica.Templates.VolumeClaimTemplate
	template, ok := replica.Chi.GetVolumeClaimTemplate(templateName)
	if !ok {
		// Unknown template name, however, this is strange
		return true
	}

	switch template.PVCReclaimPolicy {
	case chiv1.PVCReclaimPolicyRetain:
		return false
	case chiv1.PVCReclaimPolicyDelete:
		return true
	default:
		// Unknown PVCReclaimPolicy
		return true
	}

}
