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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeDiscovery "k8s.io/client-go/discovery/fake"
	k8sTesting "k8s.io/client-go/testing"
	ctrlCache "sigs.k8s.io/controller-runtime/pkg/cache"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// stagedDiscovery serves the backup group version without the restore kind for the first
// missingCalls lookups, failing instead when err is set, and serves it complete afterwards.
// Lookups run on the caller's goroutine, so the counter needs no lock.
type stagedDiscovery struct {
	*fakeDiscovery.FakeDiscovery
	missingCalls int
	err          error
	calls        int
}

func (d *stagedDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*meta.APIResourceList, error) {
	d.calls++
	kinds := backupKinds
	if d.calls <= d.missingCalls {
		if d.err != nil {
			return nil, d.err
		}
		kinds = []string{api.ClickHouseBackupCRDResourceKind, api.ClickHouseBackupScheduleCRDResourceKind}
	}
	list := &meta.APIResourceList{GroupVersion: groupVersion}
	for _, kind := range kinds {
		list.APIResources = append(list.APIResources, meta.APIResource{Name: strings.ToLower(kind) + "s", Kind: kind})
	}
	return list, nil
}

func newStagedDiscovery(missingCalls int, err error) *stagedDiscovery {
	return &stagedDiscovery{
		FakeDiscovery: &fakeDiscovery.FakeDiscovery{Fake: &k8sTesting.Fake{}},
		missingCalls:  missingCalls,
		err:           err,
	}
}

// Helm never upgrades crds/, so an upgraded operator may come up before the backup CRDs exist.
// The backup controllers must then wait, and start on their own once the CRDs are installed.
func TestWaitForBackupCRDsStartsOnceServed(t *testing.T) {
	discovery := newStagedDiscovery(2, nil)

	require.True(t, waitForBackupCRDs(context.Background(), discovery, time.Microsecond))
	require.Equal(t, 3, discovery.calls, "a partially installed set of CRDs must not start the controllers")
}

// A discovery failure says nothing about the CRDs; it is waited out like a missing CRD rather
// than taken as a reason to give up on backup for the lifetime of the process.
func TestWaitForBackupCRDsWaitsOutDiscoveryErrors(t *testing.T) {
	discovery := newStagedDiscovery(2, errors.New("connection refused"))

	require.True(t, waitForBackupCRDs(context.Background(), discovery, time.Microsecond))
	require.Equal(t, 3, discovery.calls)
}

// Shutdown is the only thing that ends the wait for CRDs that are never installed, and it must
// end it, or the WaitGroup in Run would never return.
func TestWaitForBackupCRDsAbandonsOnContextDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.False(t, waitForBackupCRDs(ctx, newStagedDiscovery(1<<30, nil), time.Millisecond))
}

// With no CRD of the API group installed the group version is not served at all, which discovery
// reports as NotFound: every backup kind is missing, and nothing has failed.
func TestMissingBackupKindsWhenGroupVersionIsNotServed(t *testing.T) {
	discovery := &fakeDiscovery.FakeDiscovery{Fake: &k8sTesting.Fake{}}

	missing, err := missingBackupKinds(discovery)
	require.NoError(t, err, "an unserved group version is an answer, not a failure")
	require.Equal(t, backupKinds, missing)
}

// TestBackupCacheOptions pins the backup cache wiring. The custom resources and installations must stay readable: they
// carry no operator label, and a cache-wide selector would turn every read of them into a silent NotFound.
// Installations are trimmed instead, so the cache holds no second full copy of each of them. The backup controllers
// run no Job, CronJob or Pod, and cache none.
func TestBackupCacheOptions(t *testing.T) {
	opts := newBackupCacheOptions(funcr.New(func(prefix, args string) {}, funcr.Options{}), []string{"ns1", "ns2"})

	require.Nil(t, opts.DefaultLabelSelector,
		"a cache-wide selector would hide the user-authored custom resources from the reconcilers")
	require.Len(t, opts.DefaultNamespaces, 2, "ByObject must not replace the namespace scoping")

	narrowed := map[string]ctrlCache.ByObject{}
	for obj, byObject := range opts.ByObject {
		narrowed[fmt.Sprintf("%T", obj)] = byObject
	}
	require.Len(t, narrowed, 1, "narrowed set changed - update this test deliberately")

	installations, ok := narrowed["*v1.ClickHouseInstallation"]
	require.True(t, ok, "installations must be trimmed - the CHI controller caches them in full already")
	require.Nil(t, installations.Label, "installations carry no operator label")
	require.Nil(t, installations.Namespaces)
	require.NotNil(t, installations.Transform)
	chi := &api.ClickHouseInstallation{}
	chi.Name = "demo"
	chi.Spec.Configuration = &api.Configuration{Clusters: []*api.Cluster{{Name: "main"}}}
	chi.EnsureStatus().Status = api.StatusCompleted
	obj, err := installations.Transform(chi)
	require.NoError(t, err)
	trimmed := obj.(*api.ClickHouseInstallation)
	require.Equal(t, "demo", trimmed.Name)
	require.Nil(t, trimmed.Spec.Configuration)
	require.Equal(t, api.StatusCompleted, trimmed.Status.GetStatus())
}
