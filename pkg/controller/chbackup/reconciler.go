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

// Package chbackup contains the controllers that reconcile the ClickHouseBackup,
// ClickHouseBackupSchedule and ClickHouseRestore custom resources into the Jobs and CronJobs that
// drive the operator-managed clickhouse-backup sidecars, and the controller that restores a new
// installation from the backups its spec.backup.recovery names.
package chbackup

import (
	"context"
	"fmt"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconciler holds what every backup controller needs. controller-runtime shares one controller
// across all of its reconcile goroutines, so these fields are set once, at construction, and
// never reassigned by a reconcile.
type reconciler struct {
	client.Client
	// apiReader reads past the cache where an absence decides what happens next.
	apiReader client.Reader
	scheme    *apiMachinery.Scheme
	// cr reads an installation together with the normalized state its last reconcile completed.
	cr     interfaces.IKubeCR
	events *a.EventEmitter
	kind   string
	// clock tells the time due times and starting deadlines are compared with.
	clock clock.PassiveClock
}

func newReconciler(
	c client.Client,
	apiReader client.Reader,
	scheme *apiMachinery.Scheme,
	cr interfaces.IKubeCR,
	kind string,
) *reconciler {
	return &reconciler{
		Client:    c,
		apiReader: apiReader,
		scheme:    scheme,
		cr:        cr,
		events:    a.NewEventEmitter(kube.NewEvent(c), kind, eventGenerateName, componentName),
		kind:      kind,
		clock:     clock.RealClock{},
	}
}

// skip reports whether a reconcile request has to be dropped. The predicates on every watch
// already filter unwatched namespaces; this also covers requests a predicate never saw.
func (r *reconciler) skip(ctx context.Context, namespace string) bool {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return true
	}
	if !chop.Config().IsNamespaceWatched(namespace) {
		log.V(2).Info("skip reconcile, namespace '%s' is not watched", namespace)
		return true
	}
	return false
}

// getConfirmed reads obj through the cache, confirming an absence against the API server: the
// cache may not have caught up with an object created moments ago, or hold only the Jobs and
// CronJobs its selector selects, and an absence decides whether an object is created, or an
// operation is reported lost.
func (r *reconciler) getConfirmed(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	err := r.Get(ctx, key, obj)
	if apiErrors.IsNotFound(err) {
		err = r.apiReader.Get(ctx, key, obj)
	}
	return err
}

// meta names obj in log lines, which have no notion of the backup kinds.
func (r *reconciler) meta(obj meta.Object) string {
	return fmt.Sprintf("%s:%s/%s", r.kind, obj.GetNamespace(), obj.GetName())
}

// announcements are the events and metrics of a status transition. They are emitted only after
// the status write that makes the transition has succeeded: a write lost to a conflict is redone
// by the next reconcile, which would otherwise announce the same transition twice.
type announcements []func()

func (out *announcements) add(f func()) {
	*out = append(*out, f)
}

func (out announcements) emit() {
	for _, f := range out {
		f()
	}
}

// info returns an announcement that logs and emits an informational event on obj.
func (r *reconciler) info(obj meta.Object, action, reason, format string, args ...any) func() {
	return func() {
		message := fmt.Sprintf(format, args...)
		log.V(1).M(r.meta(obj)).Info(message)
		r.events.EventInfo(obj, action, reason, message)
	}
}

// warning returns an announcement that logs and emits a warning event on obj.
func (r *reconciler) warning(obj meta.Object, action, reason, format string, args ...any) func() {
	return func() {
		message := fmt.Sprintf(format, args...)
		log.V(1).M(r.meta(obj)).Warning(message)
		r.events.EventWarning(obj, action, reason, message)
	}
}

// commitStatus writes the status of obj when it changed, then emits the announcements of the
// transition. The write carries the resourceVersion obj was read with, so it fails rather than
// overwrite a status written meanwhile; the transition is then recomputed on fresh state.
func (r *reconciler) commitStatus(
	ctx context.Context,
	obj client.Object,
	changed bool,
	result ctrl.Result,
	out announcements,
) (ctrl.Result, error) {
	if !changed {
		return result, nil
	}
	if err := r.Status().Update(ctx, obj); err != nil {
		if apiErrors.IsConflict(err) {
			log.V(1).M(r.meta(obj)).F().Info("status changed concurrently, reconcile again")
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	out.emit()
	return result, nil
}

func setCondition(
	conditions *[]meta.Condition,
	generation int64,
	conditionType string,
	status meta.ConditionStatus,
	reason string,
	message string,
) {
	apiMeta.SetStatusCondition(conditions, meta.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}
