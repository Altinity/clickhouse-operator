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
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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

func TestMembershipSettleDelay(t *testing.T) {
	w := &worker{a: a.NewAnnouncer(nil, nil)}

	t.Run("same size does not wait", func(t *testing.T) {
		cr := chkWithHosts(3)
		cr.SetAncestor(chkWithHosts(3))
		if got := w.membershipSettleDelay(cr); got != 0 {
			t.Fatalf("membershipSettleDelay() = %s, want 0", got)
		}
	})

	t.Run("upscale waits for raft membership", func(t *testing.T) {
		cr := chkWithHosts(3)
		cr.SetAncestor(chkWithHosts(1))
		if got := w.membershipSettleDelay(cr); got != 30*time.Second {
			t.Fatalf("membershipSettleDelay() = %s, want 30s", got)
		}
	})

	t.Run("downscale always waits 120s", func(t *testing.T) {
		cr := chkWithHosts(1)
		cr.SetAncestor(chkWithHosts(3))
		if got := w.membershipSettleDelay(cr); got != 120*time.Second {
			t.Fatalf("membershipSettleDelay() = %s, want 120s", got)
		}
	})
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

func TestPrepareStsReconcileOptsWaitSection(t *testing.T) {
	w := &worker{a: a.NewAnnouncer(nil, nil)}

	t.Run("bootstrap skips Ready", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, false)
		if !opts.WaitUntilStarted() || opts.WaitUntilReady() {
			t.Fatal("bootstrap should wait Started only")
		}
	})

	t.Run("rolling waits Ready", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, true)
		if !opts.WaitUntilReady() {
			t.Fatal("rolling should wait Ready")
		}
	})

	t.Run("rolling can opt out of Ready probe", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		host.GetCluster().GetReconcile().Host.Wait.Probes.Readiness = types.NewStringBool(false)
		opts := w.prepareStsReconcileOptsWaitSection(host, statefulset.NewReconcileStatefulSetOptions(), true)
		if opts.WaitUntilReady() {
			t.Fatal("readiness=false should skip Ready wait")
		}
	})

	t.Run("single-host post-restart still waits Ready", func(t *testing.T) {
		w.countReadyEnsembleMembersFn = func(context.Context, api.ICustomResource) int { return 0 }
		host := hostOnCR(chkWithHosts(1))
		snap := w.snapshotHostEnsemble(context.Background(), host)
		if !snap.rolling {
			t.Fatal("single host should be rolling")
		}
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, snap.rolling)
		if !opts.WaitUntilReady() {
			t.Fatal("rolling snapshot must drive Ready wait after force-restart")
		}
	})
}
