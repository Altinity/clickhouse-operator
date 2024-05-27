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
	"context"
	"fmt"
	"time"

	log "github.com/golang/glog"

	a "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type IEventEmitter interface {
	EventInfo(obj meta.Object, action string, reason string, message string)
	EventWarning(obj meta.Object, action string, reason string, message string)
	EventError(obj meta.Object, action string, reason string, message string)
}

// UpdateStatusOptions defines how to update CHI status
type UpdateStatusOptions struct {
	api.CopyStatusOptions
	TolerateAbsence bool
}

type IKubeStatusUpdater interface {
	Update(ctx context.Context, chi api.ICustomResource, opts UpdateStatusOptions) (err error)
}

// Announcer handler all log/event/status messages going outside of controller/worker
type Announcer struct {
	a.Announcer

	eventEmitter  IEventEmitter
	statusUpdater IKubeStatusUpdater
	cr            api.ICustomResource

	// writeEvent specifies whether to produce k8s event into chi, therefore requires chi to be specified
	// See k8s event for details.
	// https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/
	writeEvent bool
	// eventAction specifies k8s event action
	eventAction string
	// event reason specifies k8s event reason
	eventReason string

	// writeStatusAction specifies whether to produce action into `ClickHouseInstallation.Status.Action` of chi,
	// therefore requires chi to be specified
	writeStatusAction bool
	// writeStatusAction specifies whether to produce action into `ClickHouseInstallation.Status.Actions` of chi,
	// therefore requires chi to be specified
	writeStatusActions bool
	// writeStatusAction specifies whether to produce action into `ClickHouseInstallation.Status.Error` of chi,
	// therefore requires chi to be specified
	writeStatusError bool
}

// NewAnnouncer creates new announcer
func NewAnnouncer(eventEmitter IEventEmitter, statusUpdater IKubeStatusUpdater) Announcer {
	return Announcer{
		Announcer:     a.New(),
		eventEmitter:  eventEmitter,
		statusUpdater: statusUpdater,
	}
}

// Silence produces silent announcer
func (a Announcer) Silence() Announcer {
	b := a
	b.Announcer = b.Announcer.Silence()
	return b
}

// V is inspired by log.V()
func (a Announcer) V(level log.Level) Announcer {
	b := a
	b.Announcer = b.Announcer.V(level)
	return b
}

// F adds function name
func (a Announcer) F() Announcer {
	b := a
	b.Announcer = b.Announcer.F()
	return b
}

// L adds line number
func (a Announcer) L() Announcer {
	b := a
	b.Announcer = b.Announcer.L()
	return b
}

// FL adds filename
func (a Announcer) FL() Announcer {
	b := a
	b.Announcer = b.Announcer.FL()
	return b
}

// A adds full code address as 'file:line:function'
func (a Announcer) A() Announcer {
	b := a
	b.Announcer = b.Announcer.A()
	return b
}

// S adds 'start of the function' tag
func (a Announcer) S() Announcer {
	b := a
	b.Announcer = b.Announcer.S()
	return b
}

// E adds 'end of the function' tag
func (a Announcer) E() Announcer {
	b := a
	b.Announcer = b.Announcer.E()
	return b
}

// M adds object meta as 'namespace/name'
func (a Announcer) M(m ...interface{}) Announcer {
	b := a
	b.Announcer = b.Announcer.M(m...)
	return b
}

// P triggers log to print line
func (a Announcer) P() {
	a.Info("")
}

// Info is inspired by log.Infof()
func (a Announcer) Info(format string, args ...interface{}) {
	// Produce classic log line
	a.Announcer.Info(format, args...)

	// Produce k8s event
	if a.writeEvent && a.capable() {
		if len(args) > 0 {
			a.eventEmitter.EventInfo(a.cr, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
		} else {
			a.eventEmitter.EventInfo(a.cr, a.eventAction, a.eventReason, fmt.Sprint(format))
		}
	}

	// Produce chi status record
	a.writeStatus(format, args...)
}

// Warning is inspired by log.Warningf()
func (a Announcer) Warning(format string, args ...interface{}) {
	// Produce classic log line
	a.Announcer.Warning(format, args...)

	// Produce k8s event
	if a.writeEvent && a.capable() {
		if len(args) > 0 {
			a.eventEmitter.EventWarning(a.cr, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
		} else {
			a.eventEmitter.EventWarning(a.cr, a.eventAction, a.eventReason, fmt.Sprint(format))
		}
	}

	// Produce chi status record
	a.writeStatus(format, args...)
}

// Error is inspired by log.Errorf()
func (a Announcer) Error(format string, args ...interface{}) {
	// Produce classic log line
	a.Announcer.Error(format, args...)

	// Produce k8s event
	if a.writeEvent && a.capable() {
		if len(args) > 0 {
			a.eventEmitter.EventError(a.cr, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
		} else {
			a.eventEmitter.EventError(a.cr, a.eventAction, a.eventReason, fmt.Sprint(format))
		}
	}

	// Produce chi status record
	a.writeStatus(format, args...)
}

// Fatal is inspired by log.Fatalf()
func (a Announcer) Fatal(format string, args ...interface{}) {
	// Produce k8s event
	if a.writeEvent && a.capable() {
		if len(args) > 0 {
			a.eventEmitter.EventError(a.cr, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
		} else {
			a.eventEmitter.EventError(a.cr, a.eventAction, a.eventReason, fmt.Sprint(format))
		}
	}

	// Produce chi status record
	a.writeStatus(format, args...)

	// Write and exit
	a.Announcer.Fatal(format, args...)
}

// WithEvent is used in chained calls in order to produce event into `chi`
func (a Announcer) WithEvent(
	cr api.ICustomResource,
	action string,
	reason string,
) Announcer {
	b := a
	if cr == nil {
		b.writeEvent = false
		b.cr = nil
		b.eventAction = ""
		b.eventReason = ""
	} else {
		b.writeEvent = true
		b.cr = cr
		b.eventAction = action
		b.eventReason = reason
	}
	return b
}

// WithStatusAction is used in chained calls in order to produce action into `ClickHouseInstallation.Status.Action`
func (a Announcer) WithStatusAction(cr api.ICustomResource) Announcer {
	b := a
	if cr == nil {
		b.cr = nil
		b.writeStatusAction = false
	} else {
		b.cr = cr
		b.writeStatusAction = true
	}
	return b
}

// WithStatusActions is used in chained calls in order to produce action in ClickHouseInstallation.Status.Actions
func (a Announcer) WithStatusActions(cr api.ICustomResource) Announcer {
	b := a
	if cr == nil {
		b.cr = nil
		b.writeStatusActions = false
	} else {
		b.cr = cr
		b.writeStatusActions = true
	}
	return b
}

// WithStatusError is used in chained calls in order to produce error in ClickHouseInstallation.Status.Error
func (a Announcer) WithStatusError(cr api.ICustomResource) Announcer {
	b := a
	if cr == nil {
		b.cr = nil
		b.writeStatusError = false
	} else {
		b.cr = cr
		b.writeStatusError = true
	}
	return b
}

// capable checks whether announcer is capable to produce chi-based announcements
func (a Announcer) capable() bool {
	return (a.eventEmitter != nil) && (a.cr != nil)
}

// writeStatus is internal function which writes ClickHouseInstallation.Status
func (a Announcer) writeStatus(format string, args ...interface{}) {
	if !a.capable() {
		return
	}

	now := time.Now()
	prefix := now.Format(time.RFC3339Nano) + " "
	shouldUpdateStatus := false

	if a.writeStatusAction {
		shouldUpdateStatus = true
		if len(args) > 0 {
			a.cr.IEnsureStatus().SetAction(fmt.Sprintf(format, args...))
		} else {
			a.cr.IEnsureStatus().SetAction(fmt.Sprint(format))
		}
	}
	if a.writeStatusActions {
		shouldUpdateStatus = true
		if len(args) > 0 {
			a.cr.IEnsureStatus().PushAction(prefix + fmt.Sprintf(format, args...))
		} else {
			a.cr.IEnsureStatus().PushAction(prefix + fmt.Sprint(format))
		}
	}
	if a.writeStatusError {
		shouldUpdateStatus = true
		if len(args) > 0 {
			// PR review question: should we prefix the string in the SetError call? If so, we can SetAndPushError.
			a.cr.IEnsureStatus().SetError(fmt.Sprintf(format, args...))
			a.cr.IEnsureStatus().PushError(prefix + fmt.Sprintf(format, args...))
		} else {
			a.cr.IEnsureStatus().SetError(fmt.Sprint(format))
			a.cr.IEnsureStatus().PushError(prefix + fmt.Sprint(format))
		}
	}

	// Propagate status updates into object
	if shouldUpdateStatus {
		_ = a.statusUpdater.Update(context.Background(), a.cr, UpdateStatusOptions{
			TolerateAbsence: true,
			CopyStatusOptions: api.CopyStatusOptions{
				Actions: true,
				Errors:  true,
			},
		})
	}
}
