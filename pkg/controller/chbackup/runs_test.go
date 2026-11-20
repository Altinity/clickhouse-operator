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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// newTestRegistry returns a registry whose runs drive the fake sidecars.
func newTestRegistry(sc *fakeSidecars) *runRegistry {
	registry := newRunRegistry()
	registry.newSidecars = func(*model.RunSpec) (sidecars, error) { return sc, nil }
	registry.clock = clock.RealClock{}
	registry.poll = time.Millisecond
	return registry
}

// A run starts only in a term of the registry, which the manager of the backup controllers starts once it leads.
func TestRunRegistryStartsNoRunOutsideATerm(t *testing.T) {
	sc := newFakeSidecars("h0")
	registry := newTestRegistry(sc)
	key := runKey{kind: api.ClickHouseBackupCRDResourceKind, namespace: testNamespace, name: "b1", cluster: "main", operation: api.BackupRunOperationBackup}
	start := func() (*run, error) {
		return registry.start(key, testCHI, newBackupCR("b1"), backupSpec("b1", "h0"), nil, time.Now(), 0, nil)
	}

	_, err := start()
	require.ErrorIs(t, err, errRunsNotStarted, "no term started yet")

	ctx, end := context.WithCancel(context.Background())
	termDone := make(chan struct{})
	go func() {
		defer close(termDone)
		require.NoError(t, registry.Start(ctx))
	}()
	require.Eventually(t, func() bool { _, err := start(); return err == nil }, 5*time.Second, time.Millisecond)

	end()
	<-termDone
	_, err = registry.start(runKey{kind: key.kind, namespace: key.namespace, name: "b2", cluster: "main", operation: key.operation},
		testCHI, newBackupCR("b2"), backupSpec("b2", "h0"), nil, time.Now(), 0, nil)
	require.ErrorIs(t, err, errRunsNotStarted, "the term ended")
}

// A run that the end of a term interrupted is dropped as the next term starts, so that the controllers of that term
// resume it from its status as they do after a restart of the operator; one that completed keeps its outcome.
func TestRunRegistryDropsTheRunsAnEndedTermInterrupted(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h0"].hang = "create_remote"
	registry := newTestRegistry(sc)
	key := func(name string) runKey {
		return runKey{kind: api.ClickHouseBackupCRDResourceKind, namespace: testNamespace, name: name, cluster: "main", operation: api.BackupRunOperationBackup}
	}
	term := func() (context.CancelFunc, <-chan struct{}) {
		ctx, end := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			require.NoError(t, registry.Start(ctx))
		}()
		return end, done
	}
	end, ended := term()
	var hanging, completed *run
	require.Eventually(t, func() (ok bool) {
		var err error
		hanging, err = registry.start(key("hanging"), testCHI, newBackupCR("hanging"), backupSpec("hanging", "h0"), nil, time.Now(), 0, nil)
		return err == nil
	}, 5*time.Second, time.Millisecond)
	completed, err := registry.start(key("completed"), testCHI, newBackupCR("completed"), backupSpec("completed", "h1"), nil, time.Now(), 0, nil)
	require.NoError(t, err)
	<-completed.done
	require.False(t, completed.interrupted)

	end()
	<-ended
	<-hanging.done
	require.True(t, hanging.interrupted)
	_, finished := hanging.finished()
	require.False(t, finished, "an interrupted run is no finished one")

	end, ended = term()
	defer func() { end(); <-ended }()
	require.Eventually(t, func() bool { return registry.get(key("hanging")) == nil }, 5*time.Second, time.Millisecond,
		"the interrupted run is dropped")
	require.Same(t, completed, registry.get(key("completed")), "the completed run keeps its outcome")
	resumed, err := registry.start(key("hanging"), testCHI, newBackupCR("hanging"), backupSpec("hanging", "h0"), hanging.snapshot(), time.Now(), 0, nil)
	require.NoError(t, err)
	require.NotSame(t, hanging, resumed, "the new term resumes the run")
}
