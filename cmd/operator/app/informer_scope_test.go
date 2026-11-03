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

package app

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeKube "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	ctrlCache "sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/altinity/clickhouse-operator/pkg/controller/chi"
)

// TestKubeInformerFactoryFiltersToCHOPGeneratedObjects pins the CHI wiring, not the helper that
// builds the selector.
//
// A selector that is correct but never handed to the factory is indistinguishable, in any test
// that only inspects the selector, from one that is. This drives the real constructor and asserts
// on what the informer ends up holding, so deleting the WithTweakListOptions line fails here.
func TestKubeInformerFactoryFiltersToCHOPGeneratedObjects(t *testing.T) {
	ours := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      "chi-test-cluster-0-0-0",
			Namespace: "ns",
			Labels:    map[string]string{"clickhouse.altinity.com/app": "chop"},
		},
	}
	foreign := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      "someone-elses-pod",
			Namespace: "ns",
			Labels:    map[string]string{"app": "nginx"},
		},
	}

	factory := chi.NewInformerFactoryForCHOPGeneratedObjects(fakeKube.NewSimpleClientset(ours, foreign), 0)
	informer := factory.Core().V1().Pods().Informer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer factory.Shutdown()
	defer cancel()
	factory.Start(ctx.Done())
	require.True(t, cache.WaitForCacheSync(ctx.Done(), informer.HasSynced), "informer must sync")

	var got []string
	for _, obj := range informer.GetStore().List() {
		got = append(got, obj.(*core.Pod).Name)
	}

	require.Equal(t, []string{"chi-test-cluster-0-0-0"}, got,
		"the kube informer factory must be narrowed to operator-generated objects")
}

// TestKeeperCacheOptionsNarrowsEveryServedType pins the CHK wiring and, just as importantly, what
// is deliberately left out of it.
//
// DefaultLabelSelector must stay unset: this cache also serves the reconciler's read of the
// ClickHouseKeeperInstallation itself, which is user-authored and carries no operator label. A
// cache-wide selector would make the controller unable to read the CR it reconciles, and it would
// surface as a silent IsNotFound rather than an error.
//
// Every type the cache serves reads for must be present. PodDisruptionBudget was the last one
// added: it is read through the cached client, so leaving it out did not avoid a policy/v1
// dependency - it just left an UNFILTERED PDB informer holding every PDB in scope.
func TestKeeperCacheOptionsNarrowsEveryServedType(t *testing.T) {
	// A real sink, not logr.Discard: the effective selector is the one thing support has to go on
	// when this narrowing is wrong, and a zero-value logr would swallow the line without failing.
	var logged []string
	log := funcr.New(func(prefix, args string) { logged = append(logged, args) }, funcr.Options{})

	opts := newKeeperCacheOptions(log, []string{"ns1", "ns2"})

	require.Contains(t, strings.Join(logged, "\n"), "clickhouse-keeper.altinity.com/app=chop",
		"the effective selector must reach the boot log")
	require.Nil(t, opts.DefaultLabelSelector,
		"a cache-wide selector would hide the user-authored CHK CR from the reconciler")
	require.Len(t, opts.DefaultNamespaces, 2, "ByObject must not replace the namespace scoping")

	// ByObject is keyed by a client.Object instance, so index it by type rather than by
	// constructing an equal key.
	narrowed := map[string]ctrlCache.ByObject{}
	for obj, byObject := range opts.ByObject {
		narrowed[fmt.Sprintf("%T", obj)] = byObject
	}

	expected := []string{
		"*v1.StatefulSet",
		"*v1.Pod",
		"*v1.ConfigMap",
		"*v1.Secret",
		"*v1.Service",
		"*v1.PersistentVolumeClaim",
		"*v1.PodDisruptionBudget",
	}
	require.Len(t, narrowed, len(expected), "narrowed set changed - update this test deliberately")

	for _, typeName := range expected {
		byObject, ok := narrowed[typeName]
		require.True(t, ok, "%s must be narrowed - an unfiltered type caches every instance in the cluster", typeName)
		require.NotNil(t, byObject.Label, "%s must carry a label selector", typeName)
		require.Equal(t, "clickhouse-keeper.altinity.com/app=chop", byObject.Label.String(),
			"%s must use the KEEPER key - the CHI key would hide every Keeper object", typeName)
		require.Nil(t, byObject.Namespaces,
			"%s must leave Namespaces nil so it inherits DefaultNamespaces rather than widening "+
				"the cache back to every namespace", typeName)
	}

}
