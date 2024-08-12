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
	log "github.com/altinity/clickhouse-operator/pkg/announcer"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
)

// LogCR writes a CR into the log
func LogCR(name string, cr api.ICustomResource) {
	log.V(1).M(cr).Info(
		"logCHI %s start--------------------------------------------:\n%s\nlogCHI %s end--------------------------------------------",
		name,
		name,
		cr.YAML(types.CopyCROptions{SkipStatus: true, SkipManagedFields: true}),
	)
}

// LogActionPlan logs action plan
func LogActionPlan(ap *action_plan.ActionPlan) {
	log.Info(
		"ActionPlan start---------------------------------------------:\n%s\nActionPlan end---------------------------------------------",
		ap,
	)
}

// LogOldAndNew writes old and new CHIs into the log
func LogOldAndNew(name string, old, new api.ICustomResource) {
	LogCR(name+" old", old)
	LogCR(name+" new", new)
}
