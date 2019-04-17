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
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// createEventChi creates CHI-related event
// reason - short, machine understandable string, ex.: SuccessfulCreate
// message - human-readable description
// typ - type of the event - Normal, Warning, etc
func (c *Controller) createEventChi(
	chi *chop.ClickHouseInstallation,
	reason string,
	message string,
	typ string,
) error {
	now := time.Now()
	event := core.Event{
		ObjectMeta: meta.ObjectMeta{
			GenerateName: "chop-chi-",
		},
		InvolvedObject: core.ObjectReference{
			Kind: "ClickHouseInstallation",
			Namespace: chi.Namespace,
			Name: chi.Name,
			UID: chi.UID,
			APIVersion: "clickhouse.altinity.com/v1",
			ResourceVersion: chi.ResourceVersion,
		},
		Reason: reason,
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
		Count: 1,
		Type: typ,
		Action: "Created",
		ReportingController: "clickhouse-operator",
		// ID of the controller instance, e.g. `kubelet-xyzf`.
		// ReportingInstance:
	}
	_, err := c.kubeClient.CoreV1().Events(chi.Namespace).Create(&event)
	return err
}
