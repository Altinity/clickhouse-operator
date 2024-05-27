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
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
)

const (
	// Event type (Info, Warning, Error) specifies what event type is this
	eventTypeInfo    = "Info"
	eventTypeWarning = "Warning"
	eventTypeError   = "Error"
)

const (
	// Event action describes what action was taken
	EventActionReconcile = "Reconcile"
	EventActionCreate    = "Create"
	EventActionUpdate    = "Update"
	EventActionDelete    = "Delete"
	EventActionProgress  = "Progress"
)

const (
	// Short, machine understandable string that gives the reason for the transition into the object's current status
	EventReasonReconcileStarted       = "ReconcileStarted"
	EventReasonReconcileInProgress    = "ReconcileInProgress"
	EventReasonReconcileCompleted     = "ReconcileCompleted"
	EventReasonReconcileFailed        = "ReconcileFailed"
	EventReasonCreateStarted          = "CreateStarted"
	EventReasonCreateInProgress       = "CreateInProgress"
	EventReasonCreateCompleted        = "CreateCompleted"
	EventReasonCreateFailed           = "CreateFailed"
	EventReasonUpdateStarted          = "UpdateStarted"
	EventReasonUpdateInProgress       = "UpdateInProgress"
	EventReasonUpdateCompleted        = "UpdateCompleted"
	EventReasonUpdateFailed           = "UpdateFailed"
	EventReasonDeleteStarted          = "DeleteStarted"
	EventReasonDeleteInProgress       = "DeleteInProgress"
	EventReasonDeleteCompleted        = "DeleteCompleted"
	EventReasonDeleteFailed           = "DeleteFailed"
	EventReasonProgressHostsCompleted = "ProgressHostsCompleted"
)

type EventEmitter struct {
	kubeEvent    interfaces.IKubeEvent
	kind         string
	generateName string
	component    string
}

func NewEventEmitter(
	kubeEvent interfaces.IKubeEvent,
	kind string,
	generateName string,
	component string,
) *EventEmitter {
	return &EventEmitter{
		kubeEvent:    kubeEvent,
		kind:         kind,
		generateName: generateName,
		component:    component,
	}
}

// EventInfo emits event Info
func (c *EventEmitter) EventInfo(obj meta.Object, action string, reason string, message string) {
	c.emitEvent(obj, eventTypeInfo, action, reason, message)
}

// EventWarning emits event Warning
func (c *EventEmitter) EventWarning(obj meta.Object, action string, reason string, message string) {
	c.emitEvent(obj, eventTypeWarning, action, reason, message)
}

// EventError emits event Error
func (c *EventEmitter) EventError(obj meta.Object, action string, reason string, message string) {
	c.emitEvent(obj, eventTypeError, action, reason, message)
}

// emitEvent creates CHI-related event
// typ - type of the event - Normal, Warning, etc, one of eventType*
// action - what action was attempted, and then succeeded/failed regarding to the Involved Object. One of eventAction*
// reason - short, machine understandable string, one of eventReason*
// message - human-readable description
func (c *EventEmitter) emitEvent(
	obj meta.Object,
	_type string,
	action string,
	reason string,
	message string,
) {
	now := time.Now()
	namespace := obj.GetNamespace()
	name := obj.GetName()
	uid := obj.GetUID()
	resourceVersion := obj.GetResourceVersion()

	event := &core.Event{
		ObjectMeta: meta.ObjectMeta{
			GenerateName: c.generateName,
			Namespace:    namespace,
		},
		InvolvedObject: core.ObjectReference{
			Kind:            c.kind,
			Namespace:       namespace,
			Name:            name,
			UID:             uid,
			APIVersion:      "clickhouse.altinity.com/v1",
			ResourceVersion: resourceVersion,
		},
		Reason:  reason,
		Message: message,
		Source: core.EventSource{
			Component: c.component,
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
		ReportingController: c.component,
		// ID of the controller instance, e.g. `kubelet-xyzf`.
		// ReportingInstance:
	}
	_, err := c.kubeEvent.Create(controller.NewContext(), event)

	if err != nil {
		log.M(obj).F().Error("Create Event failed: %v", err)
	}

	log.V(2).M(obj).Info("Wrote event at: %s type: %s action: %s reason: %s message: %s", now, _type, action, reason, message)
}
