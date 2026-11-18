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
	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// liveReads counts the StatefulSet reads that reach the API server, and can fail the List.
type liveReads struct {
	gets, lists int
	listErr     error
}

// newReadsWorker returns a worker whose apiReader counts into reads, the store behind it, and
// the 3-replica CHK it builds. The cache is a separate, empty client, so a StatefulSet read
// through it finds nothing.
func newReadsWorker(reads *liveReads) (*worker, client.Client, func() *apiChk.ClickHouseKeeperInstallation) {
	store := fake.NewClientBuilder().Build()
	live := interceptor.NewClient(store, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*apps.StatefulSet); ok {
				reads.gets++
			}
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*apps.StatefulSetList); ok {
				reads.lists++
				if reads.listErr != nil {
					return reads.listErr
				}
			}
			return c.List(ctx, list, opts...)
		},
	})
	chk := func() *apiChk.ClickHouseKeeperInstallation {
		cr := &apiChk.ClickHouseKeeperInstallation{ObjectMeta: meta.ObjectMeta{Name: "reads", Namespace: "ns"}}
		cr.Spec.Configuration = &apiChk.Configuration{Clusters: []*apiChk.Cluster{
			{Name: "c", Layout: &apiChk.ChkClusterLayout{ReplicasCount: 3}},
		}}
		return cr
	}
	return NewController(fake.NewClientBuilder().Build(), live, nil, nil).newWorker(), store, chk
}

// buildCRReads runs buildCR and returns the StatefulSet reads it made.
func buildCRReads(w *worker, reads *liveReads, chk *apiChk.ClickHouseKeeperInstallation) (*apiChk.ClickHouseKeeperInstallation, liveReads) {
	*reads = liveReads{listErr: reads.listErr}
	cr := w.buildCR(context.Background(), chk)
	return cr, *reads
}

// createStatefulSets stores the StatefulSets a first reconcile would have created, except the
// one at index skip.
func createStatefulSets(t *testing.T, store client.Client, cr *apiChk.ClickHouseKeeperInstallation, skip int) {
	i := 0
	cr.WalkHosts(func(host *api.Host) error {
		if i != skip {
			require.NoError(t, store.Create(context.Background(), host.Runtime.DesiredStatefulSet.DeepCopy()))
		}
		i++
		return nil
	})
}

func statuses(cr *apiChk.ClickHouseKeeperInstallation) (s []types.ObjectStatus) {
	cr.WalkHosts(func(host *api.Host) error {
		s = append(s, host.GetReconcileAttributes().GetStatus())
		return nil
	})
	return s
}

// https://github.com/Altinity/clickhouse-operator/issues/2063
func TestBuildCRListsStatefulSetsOncePerPass(t *testing.T) {
	reads := &liveReads{}
	w, store, chk := newReadsWorker(reads)
	cr, _ := buildCRReads(w, reads, chk())
	createStatefulSets(t, store, cr, -1)

	cr, got := buildCRReads(w, reads, chk())

	require.Equal(t, 3, cr.HostsCount())
	require.Zero(t, got.gets, "every host is in the List, so none needs its own Get")
	require.Equal(t, 1, got.lists, "one List, through apiReader")
	cr.WalkHosts(func(host *api.Host) error {
		require.Equal(t, host.Runtime.DesiredStatefulSet.Name, host.Runtime.CurStatefulSet.Name)
		return nil
	})
	require.Equal(t, []types.ObjectStatus{types.ObjectStatusSame, types.ObjectStatusSame, types.ObjectStatusSame}, statuses(cr))
}

func TestBuildCRReadsHostMissingFromList(t *testing.T) {
	reads := &liveReads{}
	w, store, chk := newReadsWorker(reads)
	cr, _ := buildCRReads(w, reads, chk())
	createStatefulSets(t, store, cr, 0)

	cr, got := buildCRReads(w, reads, chk())
	require.Equal(t, liveReads{gets: 2, lists: 1}, got, "the missing host is read by name twice, as before")
	require.Equal(t, []types.ObjectStatus{types.ObjectStatusRequested, types.ObjectStatusSame, types.ObjectStatusSame}, statuses(cr))

	// The same CHK once reconciled: a StatefulSet that is gone while its host has an ancestor.
	withAncestor := chk()
	withAncestor.EnsureStatus().NormalizedCRCompleted = chk()
	cr, got = buildCRReads(w, reads, withAncestor)
	require.Equal(t, liveReads{gets: 2, lists: 1}, got)
	require.Equal(t, []types.ObjectStatus{types.ObjectStatusModified, types.ObjectStatusSame, types.ObjectStatusSame}, statuses(cr))
}

func TestBuildCRFallsBackToGetsWhenListFails(t *testing.T) {
	reads := &liveReads{}
	w, store, chk := newReadsWorker(reads)
	cr, _ := buildCRReads(w, reads, chk())
	createStatefulSets(t, store, cr, -1)
	reads.listErr = errors.New("list failed")

	cr, got := buildCRReads(w, reads, chk())

	require.Equal(t, 1, got.lists)
	require.Equal(t, 2*cr.HostsCount(), got.gets, "every host is read by name, as before")
	require.Equal(t, []types.ObjectStatus{types.ObjectStatusSame, types.ObjectStatusSame, types.ObjectStatusSame}, statuses(cr))
}
