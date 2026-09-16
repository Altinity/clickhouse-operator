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

package statefulset

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	announcer "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// minimalCR returns a *ClickHouseInstallation populated just enough for
// nil-unsafe call sites in the reconciler (e.g. NamespaceNameString(host.GetCR())
// in recreateStatefulSet's enter/exit log lines). All other CR-driven branches
// (StatusUpdate, IEnsureStatus.HostAdded, etc.) are gated by register=true and
// stay inert in these tests.
func minimalCR(namespace, name string) *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name},
	}
}

// fakeSTS is a minimal IKubeSTS test double recording every call and returning
// injected results so each test scenario can exercise a specific code path.
type fakeSTS struct {
	getCalls    int
	createCalls int
	updateCalls int
	deleteCalls int

	getReturn    *apps.StatefulSet
	getErr       error
	createErr    error
	updateErr    error
	deleteErr    error
	updateReturn *apps.StatefulSet

	lastDeleteNamespace string
	lastDeleteName      string
}

func (f *fakeSTS) Get(ctx context.Context, params ...any) (*apps.StatefulSet, error) {
	f.getCalls++
	return f.getReturn, f.getErr
}

func (f *fakeSTS) Create(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	f.createCalls++
	if f.createErr != nil {
		return nil, f.createErr
	}
	return sts, nil
}

func (f *fakeSTS) Update(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	f.updateCalls++
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	if f.updateReturn != nil {
		return f.updateReturn, nil
	}
	return sts, nil
}

func (f *fakeSTS) Delete(ctx context.Context, namespace, name string) error {
	f.deleteCalls++
	f.lastDeleteNamespace = namespace
	f.lastDeleteName = name
	return f.deleteErr
}

func (f *fakeSTS) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]apps.StatefulSet, error) {
	return nil, nil
}

// fakeNamer returns a fixed name for StatefulSet and Pod lookups.
type fakeNamer struct {
	stsName string
	podName string
}

func (n *fakeNamer) Name(what interfaces.NameType, params ...any) string {
	if what == interfaces.NamePod && n.podName != "" {
		return n.podName
	}
	return n.stsName
}
func (n *fakeNamer) Names(what interfaces.NameType, params ...any) []string {
	return nil
}

// fakePoller is an IHostObjectsPoller test double. The default zero value is a
// successful wait; tests that need a scale-to-0 timeout set waitReadyErr.
type fakePoller struct {
	waitReadyCalls int
	waitReadyErr   error
}

func (p *fakePoller) WaitHostStatefulSetReady(ctx context.Context, host *api.Host) error {
	p.waitReadyCalls++
	return p.waitReadyErr
}
func (p *fakePoller) WaitHostPodStarted(ctx context.Context, host *api.Host) error {
	return nil
}

// fakePod is a minimal IKubePod test double. Only Delete is exercised by the
// scale-to-0 escalate path; the other methods exist to satisfy the interface.
type fakePod struct {
	deleteCalls         int
	deleteErr           error
	lastDeleteNamespace string
	lastDeleteName      string
}

func (f *fakePod) Get(ctx context.Context, params ...any) (*core.Pod, error) {
	return nil, nil
}
func (f *fakePod) GetAll(ctx context.Context, obj any) []*core.Pod { return nil }
func (f *fakePod) Update(ctx context.Context, pod *core.Pod) (*core.Pod, error) {
	return pod, nil
}
func (f *fakePod) Delete(ctx context.Context, namespace, name string) error {
	f.deleteCalls++
	f.lastDeleteNamespace = namespace
	f.lastDeleteName = name
	return f.deleteErr
}

// stsResource is the schema.GroupResource used for constructing typed API
// errors. Any value works — we only care about the typed error kind.
var stsResource = schema.GroupResource{Group: "apps", Resource: "statefulsets"}

// newReconciler builds a Reconciler with the injected IKubeSTS and a fixed
// StatefulSet name on the namer. All other dependencies are no-op stubs.
// The zero-value storage.Reconciler is safe here because the only path that
// reaches r.storage.ReconcilePVCs runs through host.WalkVolumeMounts which
// returns immediately on a host with no desired/cur StatefulSet container
// volumeMounts.
func newReconciler(sts interfaces.IKubeSTS, stsName string) *Reconciler {
	return &Reconciler{
		a:                 announcer.NewAnnouncer(nil, nil),
		hostObjectsPoller: &fakePoller{},
		namer:             &fakeNamer{stsName: stsName, podName: stsName + "-0"},
		storage:           &storage.Reconciler{},
		sts:               sts,
	}
}

// host returns a minimal *api.Host with the namespace populated. Callers may
// further populate Runtime.CurStatefulSet / Runtime.DesiredStatefulSet to drive
// the code paths they care about.
func host(namespace string) *api.Host {
	h := &api.Host{}
	h.Runtime.Address.Namespace = namespace
	return h
}

// hostWithCR returns a host with a backing CR attached so that nil-unsafe
// announcer log sites (NamespaceNameString(host.GetCR())) don't panic.
func hostWithCR(namespace, crName string) *api.Host {
	h := host(namespace)
	h.Runtime.SetCR(minimalCR(namespace, crName))
	return h
}

// stsWithReplicas builds an apps.StatefulSet with the given replica count. A
// nil count is encoded as Spec.Replicas == nil. doDeleteStatefulSet's
// scale-to-zero precondition is `cur.Spec.Replicas == nil || *cur.Spec.Replicas != 0`,
// so this builder is the test fixture for both "scale up to 0" and "already 0" paths.
func stsWithReplicas(replicas *int32) *apps.StatefulSet {
	return &apps.StatefulSet{
		Spec: apps.StatefulSetSpec{
			Replicas: replicas,
			Template: core.PodTemplateSpec{
				Spec: core.PodSpec{
					Containers: []core.Container{{Name: "clickhouse-pod", Image: "x"}},
				},
			},
		},
	}
}

func int32Ptr(v int32) *int32 { return &v }

// TestDoDeleteStatefulSet_GetNotFound — Get returns IsNotFound: doDeleteStatefulSet
// must return nil cleanly with no Update or Delete attempt. This is the
// idempotency invariant on the GET side: a missing STS is already in the
// desired state.
func TestDoDeleteStatefulSet_GetNotFound(t *testing.T) {
	fake := &fakeSTS{
		getErr: apiErrors.NewNotFound(stsResource, "chi-test-cluster-0-0"),
	}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.NoError(t, err)
	assert.Equal(t, 1, fake.getCalls, "Get should be called exactly once")
	assert.Equal(t, 0, fake.updateCalls, "Update must not be called when STS is already gone")
	assert.Equal(t, 0, fake.deleteCalls, "Delete must not be called when STS is already gone")
}

// TestDoDeleteStatefulSet_UpdateConflictFallsThroughToDelete is the regression
// guard for the scale-to-0 fall-through invariant. Previously, a 409 Conflict
// on the scale-to-0 Update caused doDeleteStatefulSet to `return uerr`, leaving
// the StatefulSet undeleted and breaking recreate paths. The current behavior
// is: Update failures are logged at warning level and execution falls through
// to Delete. This test asserts that fall-through.
func TestDoDeleteStatefulSet_UpdateConflictFallsThroughToDelete(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(3))
	fake := &fakeSTS{
		getReturn: cur,
		updateErr: apiErrors.NewConflict(stsResource, "chi-test-cluster-0-0",
			errors.New("the object has been modified; please apply your changes to the latest version and try again")),
	}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.NoError(t, err, "409 on Update must not block Delete; doDeleteStatefulSet should still succeed")
	assert.Equal(t, 1, fake.getCalls)
	assert.Equal(t, 1, fake.updateCalls, "Update should be attempted (scale-to-0 best effort)")
	assert.Equal(t, 1, fake.deleteCalls, "Delete must run even when Update returned 409 Conflict")
	assert.Equal(t, "ns", fake.lastDeleteNamespace)
	assert.Equal(t, "chi-test-cluster-0-0", fake.lastDeleteName)
}

// TestDoDeleteStatefulSet_AlreadyAtZeroSkipsUpdate — when the cur StatefulSet
// already has Replicas=0, the scale-to-0 Update is skipped entirely and
// Delete runs directly. This is the fast-path that avoids a needless write
// against the apiserver during repeated delete attempts.
func TestDoDeleteStatefulSet_AlreadyAtZeroSkipsUpdate(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(0))
	fake := &fakeSTS{getReturn: cur}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.NoError(t, err)
	assert.Equal(t, 1, fake.getCalls)
	assert.Equal(t, 0, fake.updateCalls, "Update must be skipped when Replicas is already 0")
	assert.Equal(t, 1, fake.deleteCalls, "Delete must be invoked directly")
}

// TestDoDeleteStatefulSet_DeleteNotFoundIsIdempotent — Delete returning
// IsNotFound (racey case: STS deleted between our Get and Delete) is treated
// as success. doDeleteStatefulSet must return nil.
func TestDoDeleteStatefulSet_DeleteNotFoundIsIdempotent(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(3))
	fake := &fakeSTS{
		getReturn: cur,
		deleteErr: apiErrors.NewNotFound(stsResource, "chi-test-cluster-0-0"),
	}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.NoError(t, err, "IsNotFound on Delete is benign — idempotent delete")
	assert.Equal(t, 1, fake.deleteCalls)
}

// TestDoDeleteStatefulSet_DeleteRealErrorPropagates — a non-IsNotFound failure
// from Delete (e.g. apiserver outage, permission denial) propagates to the
// caller. The reconciler can't paper over a real failure.
func TestDoDeleteStatefulSet_DeleteRealErrorPropagates(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(3))
	deleteErr := errors.New("internal server error")
	fake := &fakeSTS{
		getReturn: cur,
		deleteErr: deleteErr,
	}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.Error(t, err)
	assert.Equal(t, deleteErr, err, "non-IsNotFound Delete errors must propagate verbatim")
	assert.Equal(t, 1, fake.deleteCalls)
}

// TestRecreateStatefulSet_HappyPath — delete succeeds, create succeeds, the
// whole thing returns nil. We use a desired StatefulSet to feed the create
// path. Register=false so we don't reach the CR.Status update branch.
func TestRecreateStatefulSet_HappyPath(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(0))
	fake := &fakeSTS{getReturn: cur}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	h := hostWithCR("ns", "test-chi")
	h.Runtime.DesiredStatefulSet = stsWithReplicas(int32Ptr(1))

	err := r.recreateStatefulSet(context.Background(), h, false /*register*/, NewReconcileStatefulSetOptions())

	require.NoError(t, err)
	assert.Equal(t, 1, fake.deleteCalls, "delete should be invoked once")
	assert.Equal(t, 1, fake.createCalls, "create should follow a successful delete")
}

// TestRecreateStatefulSet_DeleteFailsSkipsCreate — when the delete leg of
// recreate fails, recreate must NOT attempt to create a new StatefulSet
// (that would leave us in a phantom-create state where the old STS still
// exists). The error must propagate so the caller retries on the next
// reconcile pass.
func TestRecreateStatefulSet_DeleteFailsSkipsCreate(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(3))
	deleteErr := errors.New("apiserver down")
	fake := &fakeSTS{
		getReturn: cur,
		deleteErr: deleteErr,
	}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	h := hostWithCR("ns", "test-chi")
	h.Runtime.DesiredStatefulSet = stsWithReplicas(int32Ptr(1))

	err := r.recreateStatefulSet(context.Background(), h, false /*register*/, NewReconcileStatefulSetOptions())

	require.Error(t, err)
	assert.Equal(t, deleteErr, err, "delete error must propagate")
	assert.Equal(t, 0, fake.createCalls, "create must NOT be invoked when delete fails")
}

// TestCreateStatefulSet_AlreadyExistsPropagatesAsRecreate is the regression
// guard for the ErrCRUDRecreate-propagation invariant. Previously,
// shouldAbortOrContinueCreateStatefulSet swallowed ErrCRUDRecreate as nil, so a
// Create call returning AlreadyExists (stale informer / prior failed delete)
// was reported as a successful reconcile and the caller's failure branch never
// ran. The current behavior is: doCreateStatefulSet maps AlreadyExists to
// ErrCRUDRecreate, and shouldAbortOrContinueCreateStatefulSet propagates that
// sentinel so the next reconcile retries.
func TestCreateStatefulSet_AlreadyExistsPropagatesAsRecreate(t *testing.T) {
	fake := &fakeSTS{
		createErr: apiErrors.NewAlreadyExists(stsResource, "chi-test-cluster-0-0"),
	}
	r := newReconciler(fake, "chi-test-cluster-0-0")

	h := hostWithCR("ns", "test-chi")
	h.Runtime.DesiredStatefulSet = stsWithReplicas(int32Ptr(1))

	err := r.createStatefulSet(context.Background(), h, false /*register*/, NewReconcileStatefulSetOptions())

	require.Error(t, err, "AlreadyExists on Create must NOT be silently swallowed")
	assert.Equal(t, common.ErrCRUDRecreate, err,
		"createStatefulSet must propagate ErrCRUDRecreate so the caller retries on the next reconcile pass")
	assert.Equal(t, 1, fake.createCalls, "Create should be attempted exactly once")
}

// TestDoDeleteStatefulSet_ScaleToZeroTimeoutForceDeletesPod is the #2078
// escalate path: a successful scale-to-0 Update whose wait times out must
// force-delete the host pod and then still run StatefulSet Delete, so
// recreate can create the replacement in this pass.
func TestDoDeleteStatefulSet_ScaleToZeroTimeoutForceDeletesPod(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(1))
	fake := &fakeSTS{getReturn: cur}
	poller := &fakePoller{waitReadyErr: errors.New("wait timeout")}
	pods := &fakePod{}
	r := newReconciler(fake, "chi-test-cluster-0-0")
	r.hostObjectsPoller = poller
	r.pod = pods

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.NoError(t, err)
	assert.Equal(t, 1, poller.waitReadyCalls, "scale-to-0 wait must be honored")
	assert.Equal(t, 1, pods.deleteCalls, "wedged pod must be force-deleted")
	assert.Equal(t, "ns", pods.lastDeleteNamespace)
	assert.Equal(t, "chi-test-cluster-0-0-0", pods.lastDeleteName)
	assert.Equal(t, 1, fake.deleteCalls, "StatefulSet Delete must still run after pod escalate")
}

// TestDoDeleteStatefulSet_ScaleToZeroTimeoutPodNotFoundStillDeletesSTS —
// the pod may already be gone by the time we escalate; IsNotFound is
// success and Delete of the StatefulSet must still proceed.
func TestDoDeleteStatefulSet_ScaleToZeroTimeoutPodNotFoundStillDeletesSTS(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(1))
	fake := &fakeSTS{getReturn: cur}
	pods := &fakePod{
		deleteErr: apiErrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "chi-test-cluster-0-0-0"),
	}
	r := newReconciler(fake, "chi-test-cluster-0-0")
	r.hostObjectsPoller = &fakePoller{waitReadyErr: errors.New("wait timeout")}
	r.pod = pods

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.NoError(t, err, "IsNotFound on pod delete is benign")
	assert.Equal(t, 1, pods.deleteCalls)
	assert.Equal(t, 1, fake.deleteCalls)
}

// TestDoDeleteStatefulSet_ScaleToZeroTimeoutPodDeleteErrorAborts — a real
// pod-delete failure cannot finish Recreate in this pass, so it must
// propagate and skip StatefulSet Delete.
func TestDoDeleteStatefulSet_ScaleToZeroTimeoutPodDeleteErrorAborts(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(1))
	podErr := errors.New("forbidden")
	fake := &fakeSTS{getReturn: cur}
	pods := &fakePod{deleteErr: podErr}
	r := newReconciler(fake, "chi-test-cluster-0-0")
	r.hostObjectsPoller = &fakePoller{waitReadyErr: errors.New("wait timeout")}
	r.pod = pods

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.Error(t, err)
	assert.Equal(t, podErr, err)
	assert.Equal(t, 1, pods.deleteCalls)
	assert.Equal(t, 0, fake.deleteCalls, "StatefulSet Delete must not run when pod escalate fails")
}

// TestDoDeleteStatefulSet_SuccessfulWaitDoesNotDeletePod — when the
// scale-to-0 wait succeeds the pod is already gone, so escalate must not run.
func TestDoDeleteStatefulSet_SuccessfulWaitDoesNotDeletePod(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(1))
	fake := &fakeSTS{getReturn: cur}
	pods := &fakePod{}
	poller := &fakePoller{}
	r := newReconciler(fake, "chi-test-cluster-0-0")
	r.hostObjectsPoller = poller
	r.pod = pods

	err := r.doDeleteStatefulSet(context.Background(), host("ns"))

	require.NoError(t, err)
	assert.Equal(t, 1, poller.waitReadyCalls)
	assert.Equal(t, 0, pods.deleteCalls, "pod delete is only for a timed-out wait")
	assert.Equal(t, 1, fake.deleteCalls)
}

// TestRecreateStatefulSet_ScaleToZeroTimeoutStillCreates — #2078 one-path
// invariant: a wedged pod must not abort Recreate. After force-deleting the
// pod, Delete+Create complete in this pass.
func TestRecreateStatefulSet_ScaleToZeroTimeoutStillCreates(t *testing.T) {
	cur := stsWithReplicas(int32Ptr(1))
	fake := &fakeSTS{getReturn: cur}
	pods := &fakePod{}
	r := newReconciler(fake, "chi-test-cluster-0-0")
	r.hostObjectsPoller = &fakePoller{waitReadyErr: errors.New("wait timeout")}
	r.pod = pods

	h := hostWithCR("ns", "test-chi")
	h.Runtime.DesiredStatefulSet = stsWithReplicas(int32Ptr(1))

	err := r.recreateStatefulSet(context.Background(), h, false /*register*/, NewReconcileStatefulSetOptions())

	require.NoError(t, err)
	assert.Equal(t, 1, pods.deleteCalls, "wedged pod must be force-deleted")
	assert.Equal(t, 1, fake.deleteCalls, "delete should complete after escalate")
	assert.Equal(t, 1, fake.createCalls, "create must run in the same pass")
}
