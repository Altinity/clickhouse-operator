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

// addHostToMonitoring adds a single host to monitoring.
// Used during reconcile to enable monitoring for individual hosts as they become ready.
func (w *worker) addHostToMonitoring(host *api.Host) {
	if host.GetCR().IsStopped() {
		return
	}

	w.a.V(1).M(host).F().Info("add host to monitoring: %s", host.Runtime.Address.FQDN)
	w.c.addHostWatch(host)
}

// prepareMonitoring prepares monitoring state before reconcile begins.
// For stopped CR - excludes from monitoring.
// For running CR with ancestor - preserves old topology in monitoring.
// For new running CR - allocates an empty slot in monitoring index.
func (w *worker) prepareMonitoring(cr *api.ClickHouseInstallation) {

	if cr.IsStopped() {
		// CR is stopped
		// Exclude it from monitoring cause it makes no sense to send SQL requests to stopped instances

		w.a.V(1).
			WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileInProgress).
			WithAction(cr).
			M(cr).F().
			Info("exclude CHI from monitoring")
		w.c.deleteWatch(cr)
	} else {
		// CR is NOT stopped, it is running
		// Ensure CR is registered in monitoring
		w.a.V(1).
			WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileInProgress).
			WithAction(cr).
			M(cr).F().
			Info("ensure CHI in monitoring")

		if cr.HasAncestor() {
			// Ensure CR is watched
			w.c.updateWatch(cr.GetAncestorT())
		} else {
			// CR is a new one - allocate monitoring
			w.c.allocateWatch(cr)
		}
	}
}

// addToMonitoring adds CR to monitoring
func (w *worker) addToMonitoring(cr *api.ClickHouseInstallation) {
	// Important
	// Include into monitoring RUN-ning CR
	// Stopped CR is not touched

	if cr.IsStopped() {
		// No need to add stopped CR
		return
	}

	// CR is running
	// Include it into monitoring

	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileInProgress).
		WithAction(cr).
		M(cr).F().
		Info("add CHI to monitoring")
	w.c.updateWatch(cr)
}
