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

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgoscheme: %v", err)
	}
	if err := api.AddToScheme(s); err != nil {
		t.Fatalf("api scheme: %v", err)
	}
	return s
}

func completedCHI(name string) *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: api.ChiSpec{Configuration: &api.Configuration{
			Clusters: []*api.Cluster{{Name: "default", Layout: &api.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 2}}},
		}},
		Status: &api.Status{Status: api.StatusCompleted},
	}
}

func reconcileBackup(t *testing.T, objs ...client.Object) (client.Client, ctrl.Result, error) {
	t.Helper()
	s := newScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&api.ClickHouseBackup{}).
		Build()
	r := &BackupController{Client: c, Scheme: s}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "b1"},
	})
	return c, res, err
}

func TestBackupReconcileCreatesJob(t *testing.T) {
	chb := &api.ClickHouseBackup{
		ObjectMeta: meta.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec:       api.ClickHouseBackupSpec{ClickHouseInstallation: "demo"},
	}
	c, _, err := reconcileBackup(t, completedCHI("demo"), chb)
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	job := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "b1-backup"}, job); err != nil {
		t.Fatalf("expected backup job to be created: %v", err)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].Kind != "ClickHouseBackup" {
		t.Errorf("backup job must be owned by the ClickHouseBackup CR, got %+v", job.OwnerReferences)
	}

	got := &api.ClickHouseBackup{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "b1"}, got); err != nil {
		t.Fatalf("get chb: %v", err)
	}
	if got.Status.Phase != api.BackupPhaseRunning {
		t.Errorf("phase = %q, want Running", got.Status.Phase)
	}
	if got.Status.JobName != "b1-backup" {
		t.Errorf("status.jobName = %q, want b1-backup", got.Status.JobName)
	}
}

func TestBackupReconcilePendingWhenCHIMissing(t *testing.T) {
	chb := &api.ClickHouseBackup{
		ObjectMeta: meta.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec:       api.ClickHouseBackupSpec{ClickHouseInstallation: "absent"},
	}
	c, res, err := reconcileBackup(t, chb)
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected requeue while waiting for CHI")
	}
	// No job should be created.
	job := &batchv1.Job{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "b1-backup"}, job)
	if !apierrors.IsNotFound(err) {
		t.Errorf("no job expected when CHI is missing, got err=%v", err)
	}
	got := &api.ClickHouseBackup{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "b1"}, got)
	if got.Status.Phase != api.BackupPhasePending {
		t.Errorf("phase = %q, want Pending", got.Status.Phase)
	}
}

func TestBackupReconcilePendingWhenCHINotCompleted(t *testing.T) {
	chi := completedCHI("demo")
	chi.Status = &api.Status{Status: api.StatusInProgress}
	chb := &api.ClickHouseBackup{
		ObjectMeta: meta.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec:       api.ClickHouseBackupSpec{ClickHouseInstallation: "demo"},
	}
	c, res, err := reconcileBackup(t, chi, chb)
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected requeue while CHI not completed")
	}
	job := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "b1-backup"}, job); !apierrors.IsNotFound(err) {
		t.Errorf("no job expected when CHI not completed, got err=%v", err)
	}
}
