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
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	// Event type (Info, Warning, Error) specifies what event type is this
	eventTypeInfo    = "Info"
	eventTypeWarning = "Warning"
	eventTypeError   = "Error"
)

const (
	// Event action describes what action was taken
	eventActionReconcile = "Reconcile"
	eventActionCreate    = "Create"
	eventActionUpdate    = "Update"
	eventActionDelete    = "Delete"
)

const (
	// Short, machine understandable string that gives the reason for the transition into the object's current status
	eventReasonReconcileStarted    = "ReconcileStarted"
	eventReasonReconcileInProgress = "ReconcileInProgress"
	eventReasonReconcileCompleted  = "ReconcileCompleted"
	eventReasonReconcileFailed     = "ReconcileFailed"
	eventReasonCreateStarted       = "CreateStarted"
	eventReasonCreateInProgress    = "CreateInProgress"
	eventReasonCreateCompleted     = "CreateCompleted"
	eventReasonCreateFailed        = "CreateFailed"
	eventReasonUpdateStarted       = "UpdateStarted"
	eventReasonUpdateInProgress    = "UpdateInProgress"
	eventReasonUpdateCompleted     = "UpdateCompleted"
	eventReasonUpdateFailed        = "UpdateFailed"
	eventReasonDeleteStarted       = "DeleteStarted"
	eventReasonDeleteInProgress    = "DeleteInProgress"
	eventReasonDeleteCompleted     = "DeleteCompleted"
	eventReasonDeleteFailed        = "DeleteFailed"
)

// EventInfo emits event Info
func (c *Controller) EventInfo(
	chi *chop.ClickHouseInstallation,
	action string,
	reason string,
	message string,
) {
	c.emitEvent(chi, eventTypeInfo, action, reason, message)
}

// EventWarning emits event Warning
func (c *Controller) EventWarning(
	chi *chop.ClickHouseInstallation,
	action string,
	reason string,
	message string,
) {
	c.emitEvent(chi, eventTypeWarning, action, reason, message)
}

// EventError emits event Error
func (c *Controller) EventError(
	chi *chop.ClickHouseInstallation,
	action string,
	reason string,
	message string,
) {
	c.emitEvent(chi, eventTypeError, action, reason, message)
}

// emitEvent creates CHI-related event
// typ - type of the event - Normal, Warning, etc, one of eventType*
// action - what action was attempted, and then succeeded/failed regarding to the Involved Object. One of eventAction*
// reason - short, machine understandable string, one of eventReason*
// message - human-readable description
func (c *Controller) emitEvent(
	chi *chop.ClickHouseInstallation,
	_type string,
	action string,
	reason string,
	message string,
) {
	now := time.Now()
	kind := "ClickHouseInstallation"
	namespace := chi.Namespace
	name := chi.Name
	uid := chi.UID
	resourceVersion := chi.ResourceVersion

	event := &core.Event{
		ObjectMeta: meta.ObjectMeta{
			GenerateName: "chop-chi-",
		},
		InvolvedObject: core.ObjectReference{
			Kind:            kind,
			Namespace:       namespace,
			Name:            name,
			UID:             uid,
			APIVersion:      "clickhouse.altinity.com/v1",
			ResourceVersion: resourceVersion,
		},
		Reason:  reason,
		Message: message,
		Source: core.EventSource{
			Component: componentName,
		},
		FirstTimestamp: meta.Time{
			Time: now,
		},
		LastTimestamp: meta.Time{
			Time: now,
		},
		Count:               1,
		Type:                _type,
		Action:              action,
		ReportingController: componentName,
		// ID of the controller instance, e.g. `kubelet-xyzf`.
		// ReportingInstance:
	}
	_, err := c.kubeClient.CoreV1().Events(namespace).Create(newContext(), event, newCreateOptions())

	if err != nil {
		log.M(chi).F().Error("Create Event failed: %v", err)
	}
}
