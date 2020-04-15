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
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	log "github.com/golang/glog"
	// log "k8s.io/klog"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"time"
)

const (
	// Event type (Normal, Warning) specifies what event type is this
	eventTypeNormal  = "Normal"
	eventTypeWarning = "Warning"
	eventTypeError   = "Error"
)

const (
	// Event action describes what action was taken
	eventActionCreate = "Create"
	eventActionUpdate = "Update"
	eventActionDelete = "Delete"
)

const (
	// Short, machine understandable string that gives the reason for the transition into the object's current status
	eventReasonCreateStarted    = "CreateStarted"
	eventReasonCreateInProgress = "CreateInProgress"
	eventReasonCreateCompleted  = "CreateCompleted"
	eventReasonCreateFailed     = "CreateFailed"
	eventReasonUpdateStarted    = "UpdateStarted"
	eventReasonUpdateInProgress = "UpdateInProgress"
	eventReasonUpdateCompleted  = "UpdateCompleted"
	eventReasonUpdateFailed     = "UpdateFailed"
	eventReasonDeleteStarted    = "DeleteStarted"
	eventReasonDeleteInProgress = "DeleteInProgress"
	eventReasonDeleteCompleted  = "DeleteCompleted"
	eventReasonDeleteFailed     = "DeleteFailed"
)

// eventCHI creates CHI-related event
// typ - type of the event - Normal, Warning, etc, one of eventType*
// action - what action was attempted, and then succeeded/failed regarding to the Involved Object. One of eventAction*
// reason - short, machine understandable string, one of eventReason*
// message - human-readable description
func (c *Controller) eventCHI(
	chi *chop.ClickHouseInstallation,
	typ string,
	action string,
	reason string,
	message string,
) {
	now := time.Now()
	event := &core.Event{
		ObjectMeta: meta.ObjectMeta{
			GenerateName: "chop-chi-",
		},
		InvolvedObject: core.ObjectReference{
			Kind:            "ClickHouseInstallation",
			Namespace:       chi.Namespace,
			Name:            chi.Name,
			UID:             chi.UID,
			APIVersion:      "clickhouse.altinity.com/v1",
			ResourceVersion: chi.ResourceVersion,
		},
		Reason:  reason,
		Message: message,
		Source: core.EventSource{
			Component: "clickhouse-operator",
		},
		FirstTimestamp: meta.Time{
			Time: now,
		},
		LastTimestamp: meta.Time{
			Time: now,
		},
		Count:               1,
		Type:                typ,
		Action:              action,
		ReportingController: "clickhouse-operator",
		// ID of the controller instance, e.g. `kubelet-xyzf`.
		// ReportingInstance:
	}
	_, err := c.kubeClient.CoreV1().Events(chi.Namespace).Create(event)

	if err != nil {
		log.V(1).Infof("Create Event failed: %v", err)
	}
}
