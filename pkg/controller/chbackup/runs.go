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

package chbackup

import (
	"context"
	"fmt"
	"sync"
	"time"

	kubeTypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// runKey names a run: the resource it works for, the cluster and the operation.
type runKey struct {
	kind      string
	namespace string
	name      string
	uid       kubeTypes.UID
	cluster   string
	operation string
}

func (k runKey) String() string {
	return fmt.Sprintf("%s:%s/%s cluster %s %s", k.kind, k.namespace, k.name, k.cluster, k.operation)
}

// run is an operation the operator runs on a cluster in a goroutine of its own.
type run struct {
	key  runKey
	chi  string
	orch *orchestrator
	done chan struct{}
	// cancel stops the run, as the deletion of the resource it works for does
	cancel context.CancelFunc
	// outcome is how the run went, once done is closed
	outcome runOutcome
	// interrupted tells a run the operator stopped as it stops itself: it resumes as the operator starts again
	interrupted bool
}

// snapshot returns the status of the run as it is now.
func (r *run) snapshot() *api.BackupRunStatus {
	r.orch.mu.Lock()
	defer r.orch.mu.Unlock()
	return r.orch.status.DeepCopy()
}

// ended reports whether the run ended, however it went.
func (r *run) ended() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// finished returns how the run went, false while it runs.
func (r *run) finished() (runOutcome, bool) {
	select {
	case <-r.done:
		return r.outcome, !r.interrupted
	default:
		return runOutcome{}, false
	}
}

// runRegistry keeps the runs of the operator. The backup controllers are leader-elected, so the runs of one
// installation live in one operator; a run outlives the reconcile that started it, and a reconcile after a restart of
// the operator resumes it from its status.
type runRegistry struct {
	mu   sync.Mutex
	runs map[runKey]*run
	// ctx is the lifetime of the operator, which stops every run
	ctx context.Context
	// notify enqueues a reconcile of the resource a run works for, by kind
	notify map[string]chan event.GenericEvent
	// newSidecars returns how the runs reach the sidecars
	newSidecars func(spec *model.RunSpec) (sidecars, error)
	clock       clock.WithTicker
	poll        time.Duration
}

func newRunRegistry() *runRegistry {
	return &runRegistry{
		runs:        map[runKey]*run{},
		ctx:         context.Background(),
		notify:      map[string]chan event.GenericEvent{},
		newSidecars: func(spec *model.RunSpec) (sidecars, error) { return newSQLSidecars(spec.Endpoint) },
		clock:       clock.RealClock{},
		poll:        pollInterval,
	}
}

// runs holds the runs of the operator, shared by every backup controller.
var runs = newRunRegistry()

// Start sets the lifetime of the runs, until ctx is done: the manager of the backup controllers runs it.
func (r *runRegistry) Start(ctx context.Context) error {
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()
	<-ctx.Done()
	return nil
}

// NeedLeaderElection tells the manager to run the runs only in the leader.
func (r *runRegistry) NeedLeaderElection() bool {
	return true
}

// Runs returns the registry of the runs, which the manager of the backup controllers starts.
func Runs() *runRegistry {
	return runs
}

// Notifications returns the channel a change of the status of a run of a resource of the kind is told on.
func (r *runRegistry) Notifications(kind string) chan event.GenericEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.notify[kind] == nil {
		r.notify[kind] = make(chan event.GenericEvent, 1024)
	}
	return r.notify[kind]
}

func (r *runRegistry) get(key runKey) *run {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runs[key]
}

// forget drops a run whose outcome its resource recorded.
func (r *runRegistry) forget(key runKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if run := r.runs[key]; run != nil {
		select {
		case <-run.done:
			delete(r.runs, key)
		default:
		}
	}
}

// stopOwner stops the runs of the resource of the kind, but its deletion, and reports whether any of them still runs.
func (r *runRegistry) stopOwner(kind string, uid kubeTypes.UID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	running := false
	for key, run := range r.runs {
		if (key.kind != kind) || (key.uid != uid) || (key.operation == operationDelete) {
			continue
		}
		run.cancel()
		running = running || !run.ended()
	}
	return running
}

// forgetOwner drops the runs of the resource of the kind that ended, its deletion included.
func (r *runRegistry) forgetOwner(kind string, uid kubeTypes.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, run := range r.runs {
		if (key.kind == kind) && (key.uid == uid) && run.ended() {
			delete(r.runs, key)
		}
	}
}

// busy reports whether a run of another resource than the one of except works on the cluster of the installation.
func (r *runRegistry) busy(namespace, chiName, cluster string, except runKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, run := range r.runs {
		own := (key.kind == except.kind) && (key.uid == except.uid)
		if own || (key.namespace != namespace) || (run.chi != chiName) || (key.cluster != cluster) {
			continue
		}
		select {
		case <-run.done:
		default:
			return true
		}
	}
	return false
}

// start starts a run of the spec, or resumes one from its status, and returns it; a run of the key already there is
// returned as it is. owner is the resource the run works for, whose Events the run posts.
func (r *runRegistry) start(
	key runKey,
	chiName string,
	owner client.Object,
	spec *model.RunSpec,
	resume *api.BackupRunStatus,
	startedAt time.Time,
	elapsed time.Duration,
	post func(obj client.Object, eventType, reason, message string),
) (*run, error) {
	r.mu.Lock()
	if existing := r.runs[key]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	ctx := r.ctx
	r.mu.Unlock()

	sc, err := r.newSidecars(spec)
	if err != nil {
		return nil, err
	}
	status := resume.DeepCopy()
	if status == nil {
		status = &api.BackupRunStatus{Operation: spec.Operation}
	}
	object := owner.DeepCopyObject().(client.Object)
	notify := r.Notifications(key.kind)
	runCtx, cancel := context.WithCancel(ctx)
	run := &run{key: key, chi: chiName, done: make(chan struct{}), cancel: cancel}
	run.orch = &orchestrator{
		spec:   spec,
		sc:     sc,
		mu:     &sync.Mutex{},
		status: status,
		event: func(eventType, reason, message string) {
			if post != nil {
				post(object, eventType, reason, message)
			}
		},
		changed: func() {
			select {
			case notify <- event.GenericEvent{Object: object}:
			default:
			}
		},
		logPrefix: key.String(),
		clock:     r.clock,
		poll:      r.poll,
		startedAt: startedAt,
		resumed:   resume != nil,
		elapsed:   elapsed,
	}

	r.mu.Lock()
	r.runs[key] = run
	r.mu.Unlock()
	go func() {
		defer close(run.done)
		defer run.orch.changed()
		defer cancel()
		run.outcome = run.orch.run(runCtx)
		run.interrupted = ctx.Err() != nil
	}()
	return run, nil
}
