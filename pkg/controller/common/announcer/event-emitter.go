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

package announcer

import (
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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

	// EventReasonAutoRecoveryTriggered fires when the operator auto-recovers a CR whose
	// reconcile was aborted, on observing a recovery signal (e.g. a pod became Ready).
	EventReasonAutoRecoveryTriggered = "AutoRecoveryTriggered"

	// EventReasonStuckHostRecoveryTriggered fires when the operator re-enqueues a CR for
	// reconcile because one of its hosts has been Ready=False for longer than the threshold
	// of whichever recovery scope the CR is in - Completed or Aborted.
	EventReasonStuckHostRecoveryTriggered = "StuckHostRecoveryTriggered"

	// EventReasonHostStuckNotReady fires when shouldForceRestartHost decides to force
	// a host restart because the pod has been Ready=False past the configured threshold.
	EventReasonHostStuckNotReady = "HostStuckNotReady"

	// EventReasonHostPodForceDeleted fires when a pod outlasts the whole scale-to-0 budget and
	// is removed with grace period 0 so the StatefulSet delete can finish. This is the operator
	// SIGKILLing a database, and the alternative it was weighed against - stranding the host with
	// no pod - is invisible in kubectl, so the decision belongs in the event stream.
	EventReasonHostPodForceDeleted = "HostPodForceDeleted"

	// EventReasonKeeperUpdateNoEndpointChange fires when the operator observes a referenced
	// CHK reconcile completing but decides not to trigger a CHI reconcile because the resolved
	// zookeeper endpoints have not changed.
	EventReasonKeeperUpdateNoEndpointChange = "KeeperUpdateNoEndpointChange"

	// EventReasonHostReconcileDeferredShardSafety fires when a host's restart or StatefulSet
	// roll is postponed because its shard has no other healthy replica to serve meanwhile.
	// The shard keeps serving; the reconcile retries once a peer is back.
	EventReasonHostReconcileDeferredShardSafety = "HostReconcileDeferredShardSafety"

	// EventReasonHookSkippedUnreachableHost fires when a cluster-scoped reconcile hook does not
	// run on one of its target hosts because that host's pod cannot serve SQL - during a
	// scale-up it may not exist yet. The hook still succeeds on the hosts it could reach.
	EventReasonHookSkippedUnreachableHost = "HookSkippedUnreachableHost"

	// EventReasonCleanupPostponed fires when a deferred pass skips reconcileCR's success block, so
	// removed hosts keep their StatefulSets, PVCs and ZooKeeper replica paths. Cleanup cannot run
	// there: a deferred host never reaches RegistryReconciled, and clean() purges discovery minus
	// that registry, so it would delete the live StatefulSet the deferral is protecting.
	EventReasonCleanupPostponed = "CleanupPostponed"

	// EventReasonBackupStorageSecretMissing fires on every reconcile while the storage Secret of a
	// backup-enabled cluster does not exist, or holds no configuration file. The configuration file of the
	// sidecars then names no storage, so every backup and restore of the cluster fails until the Secret
	// holds one, which the sidecars pick up without a restart.
	EventReasonBackupStorageSecretMissing = "BackupStorageSecretMissing"

	// EventReasonBackupStorageConfigInvalid fires on every reconcile while the storage Secret of a
	// backup-enabled cluster holds a configuration file that is no YAML mapping of the sections of the
	// configuration file of clickhouse-backup. The sidecars keep the configuration file they have until it is
	// fixed.
	EventReasonBackupStorageConfigInvalid = "BackupStorageConfigInvalid"

	// EventReasonBackupNoStorage fires on every reconcile while a cluster turns backup on without the
	// settings of a storage - neither a storage Secret nor a pod template of its sidecar. Its sidecar
	// would have no storage to back up to, so backup stays off there - no sidecar, and backups and
	// restores of the cluster are refused - until the spec gives the cluster one or the other.
	EventReasonBackupNoStorage = "BackupNoStorage"

	// EventReasonBackupAPIUnpublished fires on every reconcile while hosts of a backup-enabled
	// cluster have a host Service reachable from outside the cluster. Such a Service does not
	// publish the plain-HTTP clickhouse-backup API, so backups and restores cannot reach these hosts
	// until the Service is made cluster-internal.
	EventReasonBackupAPIUnpublished = "BackupAPIUnpublished"
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

func (c *EventEmitter) Event(level string, obj meta.Object, action string, reason string, message string) {
	switch level {
	case "info":
		c.EventInfo(obj, action, reason, message)
	case "warning":
		c.EventWarning(obj, action, reason, message)
	case "error":
		c.EventError(obj, action, reason, message)
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
