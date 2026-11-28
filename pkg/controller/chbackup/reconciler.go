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

// Package chbackup contains the backup, schedule, restore and recovery controllers.
package chbackup

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/kube"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
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
	// clock tells the time scheduled times and starting deadlines are compared with.
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

// The cache can lag behind a new object, and a false "not found" would create a duplicate.
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

func (r *reconciler) announcePhase(obj meta.Object, previous, phase api.OperationPhase, conditions []meta.Condition, out *announcements) {
	if (previous == phase) || (phase == "") {
		return
	}
	cause, message := "", ""
	types := []string{api.BackupConditionStarted, api.BackupConditionValidated}
	if phase.IsTerminal() {
		types = []string{api.BackupConditionReady}
	}
	for _, conditionType := range types {
		if condition := apiMeta.FindStatusCondition(conditions, conditionType); condition != nil {
			cause, message = condition.Reason, condition.Message
			break
		}
	}
	from := string(previous)
	if from == "" {
		from = "New"
	}
	reason := phaseEventPrefix(r.kind) + string(phase)
	if failedPhase(phase) {
		out.add(r.warning(obj, a.EventActionReconcile, reason, "%s %s: %s -> %s (%s): %s", r.kind, obj.GetName(), from, phase, cause, message))
		return
	}
	out.add(r.info(obj, a.EventActionReconcile, reason, "%s %s: %s -> %s (%s): %s", r.kind, obj.GetName(), from, phase, cause, message))
}

func phaseEventPrefix(kind string) string {
	if kind == api.ClickHouseRestoreCRDResourceKind {
		return "Restore"
	}
	return "Backup"
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

// settingResolver reads Secrets and ConfigMaps from the API server, because the cache does not hold them.
func (r *reconciler) settingResolver(ctx context.Context, namespace string) model.SettingResolver {
	type source struct {
		kind, name string
	}
	read := map[source]map[string]string{}
	data := func(kind, name string) (map[string]string, error) {
		key := source{kind, name}
		if values, found := read[key]; found {
			return values, nil
		}
		values := map[string]string{}
		switch kind {
		case "Secret":
			secret := &core.Secret{}
			err := r.apiReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, secret)
			if client.IgnoreNotFound(err) != nil {
				return nil, err
			}
			for k, v := range secret.Data {
				values[k] = string(v)
			}
		default:
			configMap := &core.ConfigMap{}
			err := r.apiReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, configMap)
			if client.IgnoreNotFound(err) != nil {
				return nil, err
			}
			values = configMap.Data
		}
		read[key] = values
		return values, nil
	}
	return func(env core.EnvVar) (string, bool, error) {
		if env.ValueFrom == nil {
			return env.Value, true, nil
		}
		var (
			values map[string]string
			key    string
			err    error
		)
		switch {
		case env.ValueFrom.SecretKeyRef != nil:
			ref := env.ValueFrom.SecretKeyRef
			values, err = data("Secret", ref.Name)
			key = ref.Key
		case env.ValueFrom.ConfigMapKeyRef != nil:
			ref := env.ValueFrom.ConfigMapKeyRef
			values, err = data("ConfigMap", ref.Name)
			key = ref.Key
		default:
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		value, found := values[key]
		return value, found, nil
	}
}
