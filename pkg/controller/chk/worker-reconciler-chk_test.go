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

package chk

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
)

type fakeCRStatusWriter struct {
	err     error
	updated api.ICustomResource
	opts    types.UpdateStatusOptions
}

func (f *fakeCRStatusWriter) Get(context.Context, string, string) (api.ICustomResource, error) {
	return nil, nil
}

func (f *fakeCRStatusWriter) StatusUpdate(_ context.Context, cr api.ICustomResource, opts types.UpdateStatusOptions) error {
	f.updated = cr
	f.opts = opts
	return f.err
}

type fakeKubeWithCR struct {
	interfaces.IKube
	cr interfaces.IKubeCR
}

func (f *fakeKubeWithCR) CR() interfaces.IKubeCR {
	return f.cr
}

func newStatusTestWorker(statusWriter interfaces.IKubeCR) *worker {
	controller := &Controller{kube: &fakeKubeWithCR{cr: statusWriter}}
	return &worker{
		c: controller,
		a: a.NewAnnouncer(nil, statusWriter),
	}
}

func TestPersistReconcileCompleted(t *testing.T) {
	target := &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "test", Name: "keeper"},
	}
	cr := &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "test", Name: "keeper"},
		Status: &apiChk.Status{
			TaskID:       "task-1",
			NormalizedCR: target,
		},
	}
	statusWriter := &fakeCRStatusWriter{}
	w := newStatusTestWorker(statusWriter)

	if err := w.persistReconcileCompleted(context.Background(), cr); err != nil {
		t.Fatalf("persistReconcileCompleted() error = %v", err)
	}

	if statusWriter.updated != cr {
		t.Fatal("completion status was not passed to the status writer")
	}
	if got := cr.EnsureStatus().GetStatus(); got != api.StatusCompleted {
		t.Fatalf("status = %q, want %q", got, api.StatusCompleted)
	}
	if cr.GetAncestorT() != target {
		t.Fatal("normalized target was not promoted to normalizedCompleted")
	}
	if cr.GetTarget() != nil {
		t.Fatal("normalized target was not cleared after completion")
	}
	completed := cr.EnsureStatus().GetTaskIDsCompleted()
	if len(completed) != 1 || completed[0] != "task-1" {
		t.Fatalf("taskIDsCompleted = %v, want [task-1]", completed)
	}
	if !statusWriter.opts.CopyStatusOptions.FieldGroupWholeStatus {
		t.Fatal("completion did not request a whole-status update")
	}
}

func TestPersistReconcileCompletedReturnsStatusUpdateError(t *testing.T) {
	wantErr := errors.New("status update rejected")
	statusWriter := &fakeCRStatusWriter{err: wantErr}
	w := newStatusTestWorker(statusWriter)
	cr := &apiChk.ClickHouseKeeperInstallation{Status: &apiChk.Status{TaskID: "task-1"}}

	if err := w.persistReconcileCompleted(context.Background(), cr); !errors.Is(err, wantErr) {
		t.Fatalf("persistReconcileCompleted() error = %v, want %v", err, wantErr)
	}
}

func TestMarkReconcileStartReturnsStatusUpdateError(t *testing.T) {
	wantErr := errors.New("status update rejected")
	statusWriter := &fakeCRStatusWriter{err: wantErr}
	w := newStatusTestWorker(statusWriter)
	cr := &apiChk.ClickHouseKeeperInstallation{Status: &apiChk.Status{TaskID: "task-1"}}
	cr.EnsureRuntime().ActionPlan = api.MakeActionPlan(nil, cr)

	if err := w.markReconcileStart(context.Background(), cr); !errors.Is(err, wantErr) {
		t.Fatalf("markReconcileStart() error = %v, want %v", err, wantErr)
	}
	if got := cr.EnsureStatus().GetStatus(); got != api.StatusInProgress {
		t.Fatalf("status = %q, want %q before persistence attempt", got, api.StatusInProgress)
	}
}

func TestAbortStatusPersistenceIsRequiredBeforeTerminalAcknowledge(t *testing.T) {
	cr := &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: "test", Name: "keeper"},
		Status: &apiChk.Status{
			TaskID: "stalled-joiner",
			Errors: []string{raftStagedJoinerStartupTimeoutStatusMarker + " image unavailable"},
		},
	}
	statusWriter := &fakeCRStatusWriter{err: errors.New("status unavailable")}
	w := newStatusTestWorker(statusWriter)

	err := w.markReconcileCompletedUnsuccessfully(context.Background(), cr,
		errors.Join(errRaftStagedJoinerStartupTimeout, errors.New("startup timeout"), common.ErrCRUDAbort))
	require.ErrorIs(t, err, statusWriter.err,
		"failed status persistence must be returned so controller-runtime can retry writing Aborted")
	require.Equal(t, apiChk.StatusAborted, cr.Status.GetStatus())
	require.Contains(t, cr.Status.GetErrors()[0], raftStagedJoinerStartupTimeoutStatusMarker)

	statusWriter.err = nil
	require.NoError(t, w.markReconcileCompletedUnsuccessfully(context.Background(), cr, common.ErrCRUDAbort))
	require.Equal(t, apiChk.StatusAborted, cr.Status.GetStatus())
	require.Same(t, cr, statusWriter.updated)
}

// Normalization copies the Kubernetes generation into status.normalized,
// which is the durable last-attempt identity used by the terminal-abort guard.
func TestNormalizedCHKPreservesAttemptedGenerationForTerminalAbort(t *testing.T) {
	cr := chkWithHosts(1)
	cr.SetGeneration(17)
	cr.Status = &apiChk.Status{
		Status: apiChk.StatusAborted,
		Errors: []string{raftStagedJoinerStartupTimeoutStatusMarker + " test"},
	}
	w := &worker{normalizer: normalizer.New()}
	normalized := w.createTemplated(cr)
	require.Equal(t, int64(17), normalized.GetGeneration())
	require.NotNil(t, normalized.GetTarget())
	require.Equal(t, int64(17), normalized.GetTarget().GetGeneration())
	require.NotEqual(t, apiChk.StatusAborted, normalized.Status.GetStatus(),
		"a changed spec must not inherit an old terminal abort as a new normalization failure")
}
