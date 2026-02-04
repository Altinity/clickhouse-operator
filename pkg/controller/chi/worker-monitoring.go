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

package chi

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
)

// excludeFromMonitoring excludes stopped CR from monitoring
func (w *worker) excludeFromMonitoring(cr *api.ClickHouseInstallation) {
	if !cr.IsStopped() {
		// No need to exclude non-stopped CR
		return
	}

	// CR is stopped, let's exclude it from monitoring
	// because it makes no sense to send SQL requests to stopped instances
	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileInProgress).
		WithAction(cr).
		M(cr).F().
		Info("exclude CR from monitoring")
	w.c.deleteWatch(cr)
}

// addToMonitoring adds CR to monitoring
func (w *worker) addToMonitoring(cr *api.ClickHouseInstallation) {
	if cr.IsStopped() {
		// No need to add stopped CR
		return
	}

	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileInProgress).
		WithAction(cr).
		M(cr).F().
		Info("add CR to monitoring")
	w.c.updateWatch(cr)
}
