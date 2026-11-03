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

package kube

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// cachedClient stands in for manager.GetClient(). Its Get panics: the manager cache is narrowed by
// label, so a Get through it can answer IsNotFound for an object that exists, and every reconcile
// path here acts on absence. Panicking makes "this read went through the cache" a test failure
// rather than something that only shows up as a force-recreate on a customer's cluster.
//
// The embedded nil interface means any method this test does not expect also panics, which is the
// point - writes are supposed to keep using it, reads are not.
type cachedClient struct {
	client.Client
	t *testing.T
}

func (c *cachedClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	c.t.Fatalf("by-name Get went through the narrowed cache; it must use apiReader")
	return nil
}

// liveReader stands in for manager.GetAPIReader() and always finds the object.
type liveReader struct {
	client.Reader
	object client.Object
	calls  *int
	// listOpts records what a List asked for, so a test can assert the selector that travelled.
	listOpts *client.ListOptions
}

func (r *liveReader) List(_ context.Context, _ client.ObjectList, opts ...client.ListOption) error {
	if r.listOpts != nil {
		for _, o := range opts {
			o.ApplyToList(r.listOpts)
		}
	}
	return nil
}

func (r *liveReader) Get(_ context.Context, _ client.ObjectKey, out client.Object, _ ...client.GetOption) error {
	*r.calls++
	reflect.ValueOf(out).Elem().Set(reflect.ValueOf(r.object).Elem())
	return nil
}

// TestByNameGetsReadLive pins the half of the narrowing contract that the cache config cannot
// express: narrowing the informers is only safe because no by-name Get consults them.
//
// Reverting any one of these to the cached client reintroduces a silent IsNotFound - for PVCs that
// means ErrPVCIsMissed and a StatefulSet recreate on every pass. Before this test, that revert
// passed the entire suite.
func TestByNameGetsReadLive(t *testing.T) {
	const ns, name = "ns", "obj"

	tests := []struct {
		what   string
		object client.Object
		get    func(t *testing.T, cached client.Client, live client.Reader) error
	}{
		{
			what:   "PVC",
			object: &core.PersistentVolumeClaim{ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name}},
			get: func(t *testing.T, cached client.Client, live client.Reader) error {
				_, err := NewPVC(cached, live).Get(context.Background(), ns, name)
				return err
			},
		},
		{
			what:   "ConfigMap",
			object: &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name}},
			get: func(t *testing.T, cached client.Client, live client.Reader) error {
				_, err := NewConfigMap(cached, live).Get(context.Background(), ns, name)
				return err
			},
		},
		{
			what:   "PDB",
			object: &policy.PodDisruptionBudget{ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name}},
			get: func(t *testing.T, cached client.Client, live client.Reader) error {
				_, err := NewPDB(cached, live).Get(context.Background(), ns, name)
				return err
			},
		},
		{
			what:   "Secret",
			object: &core.Secret{ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name}},
			get: func(t *testing.T, cached client.Client, live client.Reader) error {
				_, err := NewSecret(cached, live, nil).Get(context.Background(), ns, name)
				return err
			},
		},
		{
			what:   "Service",
			object: &core.Service{ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name}},
			get: func(t *testing.T, cached client.Client, live client.Reader) error {
				_, err := NewService(cached, live, nil).Get(context.Background(), ns, name)
				return err
			},
		},
		{
			what:   "Pod",
			object: &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name}},
			get: func(t *testing.T, cached client.Client, live client.Reader) error {
				_, err := NewPod(cached, live, nil).Get(context.Background(), ns, name)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.what, func(t *testing.T) {
			calls := 0
			err := tt.get(t, &cachedClient{t: t}, &liveReader{object: tt.object, calls: &calls})

			require.NoError(t, err)
			require.Equal(t, 1, calls, "%s Get must read through apiReader exactly once", tt.what)
		})
	}
}

// The StatefulSet Get already read live before the cache was narrowed, for a different reason: the
// cache is not write-through, so an Update reaches the API server while the cache waits for the
// watch to deliver it, and a Get in between returns the pre-update object. Pinned here alongside
// the rest so the whole adapter is covered by one rule rather than two overlapping ones.
func TestStatefulSetGetReadsLive(t *testing.T) {
	calls := 0
	sts := &apps.StatefulSet{ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "obj"}}

	_, err := NewSTS(&cachedClient{t: t}, &liveReader{object: sts, calls: &calls}, nil).
		Get(context.Background(), "ns", "obj")

	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

// TestListForHostSelectsKeeperLabels pins which labeler the CHK adapter uses.
//
// ListForHost builds its selector from the labeler returned by labeler(); that function used the
// CHI labeler, so inside the Keeper adapter it asked for clickhouse.altinity.com/* while every
// Keeper object carries clickhouse-keeper.altinity.com/*. The List therefore matched nothing, and
// silently - an empty result is exactly what "no PVCs for this host" looks like. No Keeper path
// calls it yet, so nothing failed when that was reverted; this test is what makes the wrong
// labeler visible before something does.
func TestListForHostSelectsKeeperLabels(t *testing.T) {
	host := &api.Host{}
	host.Runtime.Address.Namespace = "ns"

	captured := &client.ListOptions{}
	_, err := NewPVC(&cachedClient{t: t}, &liveReader{listOpts: captured}).
		ListForHost(context.Background(), host)

	require.NoError(t, err)
	require.NotNil(t, captured.LabelSelector, "ListForHost must constrain by label")
	require.Contains(t, captured.LabelSelector.String(), "clickhouse-keeper.altinity.com/",
		"the Keeper adapter must select Keeper labels; the CHI key matches no Keeper object")
}

// errReader fails every read, so a Get's error path can be asserted.
type errReader struct {
	client.Reader
	err error
}

func (r *errReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return r.err
}

// Every Get in this package returns a nil object alongside its error. Pod was the outlier - it
// returned a zero-valued Pod - which is the shape a caller checking the object rather than the
// error cannot tell apart from a real, empty Pod.
func TestGetsReturnNilObjectOnError(t *testing.T) {
	boom := &errReader{err: apiErrors.NewForbidden(
		schema.GroupResource{Resource: "pods"}, "obj", errors.New("nope"))}
	cached := &cachedClient{t: t}

	pod, err := NewPod(cached, boom, nil).Get(context.Background(), "ns", "obj")
	require.Error(t, err)
	require.Nil(t, pod, "a failed Get must not hand back a zero-valued Pod that looks real")

	pvc, err := NewPVC(cached, boom).Get(context.Background(), "ns", "obj")
	require.Error(t, err)
	require.Nil(t, pvc)

	cm, err := NewConfigMap(cached, boom).Get(context.Background(), "ns", "obj")
	require.Error(t, err)
	require.Nil(t, cm)
}

// TestAdapterWiresLiveReader closes the gap the per-constructor tests leave: they pin each type's
// Get, but NewAdapter is the single place that decides WHICH reader each one gets. Handing it the
// cached client for both arguments reintroduces every cached-read bug at once, and every other
// test in this package still passes - they assert the contract one level below where it breaks.
func TestAdapterWiresLiveReader(t *testing.T) {
	cached := &cachedClient{t: t}
	live := &countingReader{}

	adapter := NewAdapter(cached, live, nil)

	// Each of these panics if the adapter handed the type its cached client.
	reads := map[string]func() error{
		"ConfigMap": func() error { _, err := adapter.ConfigMap().Get(context.Background(), "ns", "o"); return err },
		"PDB":       func() error { _, err := adapter.PDB().Get(context.Background(), "ns", "o"); return err },
		"PVC":       func() error { _, err := adapter.Storage().Get(context.Background(), "ns", "o"); return err },
		"Secret":    func() error { _, err := adapter.Secret().Get(context.Background(), "ns", "o"); return err },
		"Service":   func() error { _, err := adapter.Service().Get(context.Background(), "ns", "o"); return err },
		"Pod":       func() error { _, err := adapter.Pod().Get(context.Background(), "ns", "o"); return err },
		"STS":       func() error { _, err := adapter.STS().Get(context.Background(), "ns", "o"); return err },
	}

	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			before := live.calls
			require.NoError(t, read())
			require.Equal(t, before+1, live.calls,
				"%s must read through the live reader the adapter was given", name)
		})
	}
}

// countingReader succeeds for any type, leaving the caller's object at its zero value: this test
// only cares WHICH reader the adapter consulted, not what came back.
type countingReader struct {
	client.Reader
	calls int
}

func (r *countingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	r.calls++
	return nil
}
