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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func TestBootstrapCreatesRestoreOnce(t *testing.T) {
	s := newScheme(t)
	chi := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{
			Name:        "demo",
			Namespace:   "ns",
			Annotations: map[string]string{AnnotationRecoverFromBackup: "bk"},
		},
		Status: &api.Status{Status: api.StatusCompleted},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(chi).Build()
	r := &BootstrapController{Client: c, Scheme: s}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "demo"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// A one-time restore should exist, referencing the CHI + backup.
	restore := &api.ClickHouseRestore{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "demo-bootstrap"}, restore); err != nil {
		t.Fatalf("expected bootstrap restore: %v", err)
	}
	if restore.Spec.ClickHouseInstallation != "demo" || restore.Spec.BackupName != "bk" {
		t.Errorf("unexpected restore spec: %+v", restore.Spec)
	}

	// The guard annotation must be stamped so it fires once.
	got := &api.ClickHouseInstallation{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "demo"}, got)
	if got.Annotations[AnnotationRecoveredFrom] != "bk" {
		t.Errorf("guard annotation not stamped: %v", got.Annotations)
	}
}

func TestBootstrapSkipsWhenNotAnnotated(t *testing.T) {
	s := newScheme(t)
	chi := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Name: "demo", Namespace: "ns"},
		Status:     &api.Status{Status: api.StatusCompleted},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(chi).Build()
	r := &BootstrapController{Client: c, Scheme: s}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "demo"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	restore := &api.ClickHouseRestore{}
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "demo-bootstrap"}, restore)
	if err == nil {
		t.Errorf("no restore expected for an un-annotated CHI")
	}
}
