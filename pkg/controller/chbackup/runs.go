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
	"errors"
	"fmt"
	"sync"
	"time"

	kubeTypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

type runKey struct {
	kind      string
	namespace string
	name      string
	uid       kubeTypes.UID
	cluster   string
	operation api.BackupRunOperation
}

func (k runKey) String() string {
	return fmt.Sprintf("%s:%s/%s cluster %s %s", k.kind, k.namespace, k.name, k.cluster, k.operation)
}

type run struct {
	key         runKey
	chi         string
	orch        *orchestrator
	done        chan struct{}
	cancel      context.CancelFunc
	outcome     runOutcome
	interrupted bool
}

func (r *run) snapshot() *api.BackupRunStatus {
	r.orch.mu.Lock()
	defer r.orch.mu.Unlock()
	return r.orch.status.DeepCopy()
}

func (r *run) ended() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (r *run) finished() (runOutcome, bool) {
	select {
	case <-r.done:
		return r.outcome, !r.interrupted
	default:
		return runOutcome{}, false
	}
}

// Around a leadership change, controllers can reconcile before or after the registry runs, so the reconcile is retried.
var errRunsNotStarted = errors.New("the backup runs of this operator are not started: it does not lead the backup controllers (yet)")

// A run outlives its reconcile. After a restart or a lost lease, a reconcile resumes it from the status.
type runRegistry struct {
	mu   sync.Mutex
	runs map[runKey]*run
	// ctx is nil before the first term. No run starts after it is done.
	ctx         context.Context
	notify      map[string]chan event.GenericEvent
	newSidecars func(spec *model.RunSpec) (sidecars, error)
	clock       clock.WithTicker
	poll        time.Duration
}

func newRunRegistry() *runRegistry {
	return &runRegistry{
		runs:        map[runKey]*run{},
		notify:      map[string]chan event.GenericEvent{},
		newSidecars: func(spec *model.RunSpec) (sidecars, error) { return newSQLSidecars(spec.Endpoint) },
		clock:       clock.RealClock{},
		poll:        pollInterval,
	}
}

var runs = newRunRegistry()

// Start first waits for the runs of the previous term to end, and drops the interrupted ones.
func (r *runRegistry) Start(ctx context.Context) error {
	if err := r.settle(ctx); err != nil {
		return nil
	}
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (r *runRegistry) settle(ctx context.Context) error {
	r.mu.Lock()
	var pending []*run
	for _, run := range r.runs {
		pending = append(pending, run)
	}
	r.mu.Unlock()
	for _, run := range pending {
		select {
		case <-run.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, run := range r.runs {
		if run.ended() && run.interrupted {
			delete(r.runs, key)
		}
	}
	return nil
}

func (r *runRegistry) NeedLeaderElection() bool {
	return true
}

func Runs() *runRegistry {
	return runs
}

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

func (r *runRegistry) forget(key runKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if run := r.runs[key]; (run != nil) && run.ended() {
		delete(r.runs, key)
	}
}

func (r *runRegistry) stopOwner(kind string, uid kubeTypes.UID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	running := false
	for key, run := range r.runs {
		if (key.kind != kind) || (key.uid != uid) || (key.operation == api.BackupRunOperationDelete) {
			continue
		}
		run.cancel()
		running = running || !run.ended()
	}
	return running
}

func (r *runRegistry) forgetOwner(kind string, uid kubeTypes.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, run := range r.runs {
		if (key.kind == kind) && (key.uid == uid) && run.ended() {
			delete(r.runs, key)
		}
	}
}

func (r *runRegistry) busy(namespace, chiName, cluster string, except runKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, run := range r.runs {
		own := (key.kind == except.kind) && (key.uid == except.uid)
		if own || (key.namespace != namespace) || (run.chi != chiName) || (key.cluster != cluster) {
			continue
		}
		if !run.ended() {
			return true
		}
	}
	return false
}

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
	if (ctx == nil) || (ctx.Err() != nil) {
		return nil, errRunsNotStarted
	}

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
		log:       log.V(1).M(key.String()),
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
		// Deferred in reverse order: cancel, close done, then notify, so the next reconcile sees the run as ended.
		defer run.orch.changed()
		defer close(run.done)
		defer cancel()
		run.outcome = run.orch.run(runCtx)
		run.interrupted = ctx.Err() != nil
	}()
	return run, nil
}
