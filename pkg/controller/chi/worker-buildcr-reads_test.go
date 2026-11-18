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

package chi

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeFake "k8s.io/client-go/kubernetes/fake"
	k8sTesting "k8s.io/client-go/testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	chopFake "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/fake"
	chiKube "github.com/altinity/clickhouse-operator/pkg/controller/chi/kube"
	ctrlLabeler "github.com/altinity/clickhouse-operator/pkg/controller/chi/labeler"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// newReadsWorker returns a worker on a fake clientset and the 3 shards x 2 replicas CHI it builds.
func newReadsWorker() (*worker, *kubeFake.Clientset, func() *api.ClickHouseInstallation) {
	client := kubeFake.NewSimpleClientset()
	namer := managers.NewNameManager(managers.NameManagerTypeClickHouse)
	kube := chiKube.NewAdapter(client, chopFake.NewSimpleClientset(), namer)
	c := &Controller{kubeClient: client, namer: namer, kube: kube, ctrlLabeler: ctrlLabeler.New(kube)}
	chi := func() *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Name: "reads", Namespace: "ns"}}
		cr.Spec.Configuration = api.NewConfiguration()
		cr.Spec.Configuration.Clusters = append(cr.Spec.Configuration.Clusters, &api.Cluster{
			Name:   "c",
			Layout: &api.ChiClusterLayout{ShardsCount: 3, ReplicasCount: 2},
		})
		return cr
	}
	return c.newWorker(nil, true), client, chi
}

// buildCRReads runs buildCR and counts the StatefulSet requests it makes.
func buildCRReads(w *worker, client *kubeFake.Clientset, chi *api.ClickHouseInstallation) (cr *api.ClickHouseInstallation, gets, lists int) {
	client.ClearActions()
	cr = w.buildCR(context.Background(), chi)
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "statefulsets" {
			continue
		}
		switch action.GetVerb() {
		case "get":
			gets++
		case "list":
			lists++
		}
	}
	return cr, gets, lists
}

// createStatefulSets stores the StatefulSets a first reconcile would have created. edit can change
// one before it is stored, or return nil to leave it out.
func createStatefulSets(t *testing.T, client *kubeFake.Clientset, cr *api.ClickHouseInstallation, edit func(int, *apps.StatefulSet) *apps.StatefulSet) {
	i := 0
	cr.WalkHosts(func(host *api.Host) error {
		if sts := edit(i, host.Runtime.DesiredStatefulSet.DeepCopy()); sts != nil {
			_, err := client.AppsV1().StatefulSets(sts.Namespace).Create(context.Background(), sts, meta.CreateOptions{})
			require.NoError(t, err)
		}
		i++
		return nil
	})
}

func statuses(cr *api.ClickHouseInstallation) (s []types.ObjectStatus) {
	cr.WalkHosts(func(host *api.Host) error {
		s = append(s, host.GetReconcileAttributes().GetStatus())
		return nil
	})
	return s
}

func asIs(_ int, sts *apps.StatefulSet) *apps.StatefulSet { return sts }

// https://github.com/Altinity/clickhouse-operator/issues/2063
func TestBuildCRListsStatefulSetsOncePerPass(t *testing.T) {
	w, client, chi := newReadsWorker()
	cr, _, _ := buildCRReads(w, client, chi())
	createStatefulSets(t, client, cr, asIs)

	cr, gets, lists := buildCRReads(w, client, chi())

	require.Equal(t, 6, cr.HostsCount())
	require.Zero(t, gets, "every host is in the List, so none needs its own Get")
	require.Equal(t, 1, lists)
	cr.WalkHosts(func(host *api.Host) error {
		require.Equal(t, host.Runtime.DesiredStatefulSet.Name, host.Runtime.CurStatefulSet.Name)
		require.Equal(t, types.ObjectStatusSame, host.GetReconcileAttributes().GetStatus())
		return nil
	})
}

// When pods have IPs, as in steady state, buildCR runs the pass twice and each pass lists once.
func TestBuildCRListsStatefulSetsInBothPasses(t *testing.T) {
	w, client, chi := newReadsWorker()
	cr, _, _ := buildCRReads(w, client, chi())
	createStatefulSets(t, client, cr, asIs)
	cr.WalkHosts(func(host *api.Host) error {
		pod := &core.Pod{
			ObjectMeta: meta.ObjectMeta{Name: w.c.namer.Name(interfaces.NamePod, host), Namespace: "ns"},
			Status:     core.PodStatus{PodIP: "10.0.0.1"},
		}
		_, err := client.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, meta.CreateOptions{})
		require.NoError(t, err)
		return nil
	})

	_, gets, lists := buildCRReads(w, client, chi())

	require.Zero(t, gets)
	require.Equal(t, 2, lists, "one List per pass")
}

// A StatefulSet the List does not return may still exist, e.g. with its labels removed. Each such
// host is read by name as before, so only a real NotFound makes it new.
func TestBuildCRReadsHostsMissingFromList(t *testing.T) {
	w, client, chi := newReadsWorker()
	cr, _, _ := buildCRReads(w, client, chi())
	createStatefulSets(t, client, cr, func(i int, sts *apps.StatefulSet) *apps.StatefulSet {
		switch i {
		case 0:
			return nil
		case 1:
			sts.Labels = nil
		}
		return sts
	})

	cr, gets, lists := buildCRReads(w, client, chi())

	require.Equal(t, 1, lists)
	require.Equal(t, 4, gets, "two hosts missing from the List, each read by name twice as before")
	want := []types.ObjectStatus{types.ObjectStatusRequested, types.ObjectStatusUnknown}
	for range cr.HostsCount() - 2 {
		want = append(want, types.ObjectStatusSame)
	}
	require.Equal(t, want, statuses(cr))
}

func TestBuildCRFallsBackToGetsWhenListFails(t *testing.T) {
	w, client, chi := newReadsWorker()
	cr, _, _ := buildCRReads(w, client, chi())
	createStatefulSets(t, client, cr, asIs)
	client.PrependReactor("list", "statefulsets", func(k8sTesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list failed")
	})

	cr, gets, lists := buildCRReads(w, client, chi())

	require.Equal(t, 1, lists)
	require.Equal(t, 2*cr.HostsCount(), gets, "every host is read by name, as before")
	cr.WalkHosts(func(host *api.Host) error {
		require.Equal(t, host.Runtime.DesiredStatefulSet.Name, host.Runtime.CurStatefulSet.Name)
		require.Equal(t, types.ObjectStatusSame, host.GetReconcileAttributes().GetStatus())
		return nil
	})
}
