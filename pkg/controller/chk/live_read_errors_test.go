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
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// These tests cover a class of defect that only became reachable when the CHK adapter started
// reading live instead of through the manager cache. A cached Get answered, in practice, either
// with the object or with IsNotFound - so code that treated "error" and "absent" as the same thing
// was correct by accident. Against the API server the error space also holds Forbidden
// and a spent retry budget, and conflating those with absence destroys healthy objects.

func forbidden(resource string) error {
	return apiErrors.NewForbidden(schema.GroupResource{Resource: resource}, "obj", errors.New("nope"))
}

type stubPod struct {
	interfaces.IKubePod
	err error
}

func (s *stubPod) Get(context.Context, ...any) (*core.Pod, error) { return nil, s.err }

type stubService struct {
	interfaces.IKubeService
	getReturn *core.Service
	getErr    error
	updateErr error
	deletes   int
	creates   int
	updates   int
}

func (s *stubService) Get(context.Context, ...any) (*core.Service, error) {
	return s.getReturn, s.getErr
}
func (s *stubService) Delete(context.Context, string, string) error { s.deletes++; return nil }
func (s *stubService) Create(_ context.Context, svc *core.Service) (*core.Service, error) {
	s.creates++
	return svc, nil
}

func (s *stubService) Update(_ context.Context, svc *core.Service) (*core.Service, error) {
	s.updates++
	return svc, s.updateErr
}

// stubKube hands back only what a test wired up. Fields are interfaces rather than concrete
// pointers so an unset one is a genuine nil interface: calling it panics on the nil interface,
// not on a nil receiver several frames deeper, which is the difference between "this test needed
// a Service stub" and an unexplained address error mid-assert.
type stubKube struct {
	interfaces.IKube
	pod     interfaces.IKubePod
	service interfaces.IKubeService
}

func (k *stubKube) Pod() interfaces.IKubePod         { return k.pod }
func (k *stubKube) Service() interfaces.IKubeService { return k.service }

// A pod the operator cannot READ is not a pod that has crashed. Conflating them makes
// shouldForceRestartHost fire on an API-server blip and restart a healthy Keeper.
func TestIsPodCrushedDistinguishesUnreadableFromAbsent(t *testing.T) {
	host := &api.Host{}

	tests := []struct {
		what    string
		err     error
		crushed bool
	}{
		{what: "absent pod is crushed - nothing will start it but a recreate", err: apiErrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "p"), crushed: true},
		{what: "forbidden read is not a crash", err: forbidden("pods"), crushed: false},
		{what: "transient read failure is not a crash", err: errors.New("etcdserver: request timed out"), crushed: false},
	}

	for _, tt := range tests {
		t.Run(tt.what, func(t *testing.T) {
			w := &worker{c: &Controller{kube: &stubKube{pod: &stubPod{err: tt.err}}}}
			require.Equal(t, tt.crushed, w.isPodCrushed(context.Background(), host))
		})
	}
}

// reconcileService recreates a Service it believes is gone. "Believes" must mean IsNotFound: on any
// other read error the Service may well be present and serving, and delete+create would drop its
// endpoints - and, for a non-headless Service, change its ClusterIP.
func TestReconcileServiceDoesNotRecreateOnUnreadable(t *testing.T) {
	svc := &core.Service{}
	svc.Namespace, svc.Name = "ns", "svc"

	service := &stubService{getErr: forbidden("services")}
	// A real CR, not nil: the announcer skips event emission entirely when cr is nil, so a nil
	// here would leave the event half of this branch unexercised.
	cr := apiChk.NewClickHouseKeeperInstallation("kpr", "ns")
	w := &worker{
		c: &Controller{kube: &stubKube{service: service}},
		a: a.NewAnnouncer(nil, nil),
	}

	err := w.reconcileService(context.Background(), cr, svc, nil)

	require.Error(t, err, "an unreadable Service must surface, not be silently recreated")
	require.Zero(t, service.creates, "an unreadable Service must not be replaced by a fresh one")
}

// The `curService == nil` conjunct in that guard is load-bearing, not belt-and-braces: a service
// TYPE change is signalled by updateService returning a plain non-NotFound error while curService
// is set, and that case is SUPPOSED to fall through to delete+create. Dropping the conjunct would
// turn a legitimate recreate into a hard reconcile abort, and nothing else would catch it.
func TestReconcileServiceStillRecreatesOnTypeChange(t *testing.T) {
	existing := &core.Service{Spec: core.ServiceSpec{Type: core.ServiceTypeClusterIP}}
	existing.Namespace, existing.Name = "ns", "svc"

	target := &core.Service{Spec: core.ServiceSpec{Type: core.ServiceTypeNodePort}}
	target.Namespace, target.Name = "ns", "svc"

	service := &stubService{getReturn: existing}
	cr := apiChk.NewClickHouseKeeperInstallation("kpr", "ns")
	w := &worker{
		c: &Controller{kube: &stubKube{service: service}},
		a: a.NewAnnouncer(nil, nil),
	}

	_ = w.reconcileService(context.Background(), cr, target, nil)

	require.Equal(t, 1, service.deletes, "a service type change must still recreate")
	require.Equal(t, 1, service.creates, "a service type change must still recreate")
}

// The read succeeding and the UPDATE failing is the other half, and the one a naive guard misses:
// curService is non-nil, so any check keyed on absence lets it through. A Conflict or a 500 from
// the update says nothing about the Service's health, yet the recreate path would delete it -
// costing a non-headless Service its address. Only a type change, which Kubernetes refuses to do
// in place, justifies that, and it is now marked with a sentinel rather than an error string.
func TestReconcileServiceDoesNotRecreateOnTransientUpdateFailure(t *testing.T) {
	existing := &core.Service{Spec: core.ServiceSpec{Type: core.ServiceTypeClusterIP}}
	existing.Namespace, existing.Name = "ns", "svc"

	target := &core.Service{Spec: core.ServiceSpec{Type: core.ServiceTypeClusterIP}}
	target.Namespace, target.Name = "ns", "svc"

	service := &stubService{
		getReturn: existing,
		updateErr: apiErrors.NewConflict(
			schema.GroupResource{Resource: "services"}, "svc", errors.New("object was modified")),
	}
	cr := apiChk.NewClickHouseKeeperInstallation("kpr", "ns")
	w := &worker{
		c: &Controller{kube: &stubKube{service: service}},
		a: a.NewAnnouncer(nil, nil),
	}

	err := w.reconcileService(context.Background(), cr, target, nil)

	require.Error(t, err, "a failed update must surface, not trigger a replacement")
	require.Zero(t, service.deletes, "a Service that updated badly is still a live Service")
	require.Zero(t, service.creates, "replacing it would drop its endpoints and its address")
}
