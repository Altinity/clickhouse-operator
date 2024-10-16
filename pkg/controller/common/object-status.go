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

import (
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// GetObjectStatusFromMetas gets StatefulSet status from cur and new meta infos
func GetObjectStatusFromMetas(labeler interfaces.ILabeler, curMeta, newMeta meta.Object) api.ObjectStatus {
	// Try to perform label-based version comparison
	curVersion, curHasLabel := labeler.GetObjectVersion(curMeta)
	newVersion, newHasLabel := labeler.GetObjectVersion(newMeta)

	if !curHasLabel || !newHasLabel {
		log.M(newMeta).F().Warning(
			"Not enough labels to compare objects, can not say for sure what exactly is going on. Object: %s",
			util.NamespaceNameString(newMeta),
		)
		return api.ObjectStatusUnknown
	}

	//
	// We have both set of labels, can compare them
	//

	if curVersion == newVersion {
		log.M(newMeta).F().Info(
			"cur and new objects are equal based on object version label. Update of the object is not required. Object: %s",
			util.NamespaceNameString(newMeta),
		)
		return api.ObjectStatusSame
	}

	log.M(newMeta).F().Info(
		"cur and new objects ARE DIFFERENT based on object version label: Update of the object is required. Object: %s",
		util.NamespaceNameString(newMeta),
	)

	return api.ObjectStatusModified
}
