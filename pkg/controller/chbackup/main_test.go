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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

const (
	testNamespace = "ns"
	testCHI       = "demo"
)

// IsNamespaceWatched, the normalizer and the event emitter resolve configuration through the
// global chop singleton, which nil-panics before chop.New. A watch namespace exported in the
// shell would change what the namespace guard tests assert, so it is cleared first.
func TestMain(m *testing.M) {
	os.Unsetenv("WATCH_NAMESPACE")
	os.Unsetenv("WATCH_NAMESPACES")
	chop.New(nil, nil, "")
	os.Exit(m.Run())
}

// errEmptyName is what the clientset answers a get of no name with, which is no NotFound
var errEmptyName = errors.New("resource name may not be empty")

// fakeCR serves installations together with their normalized completed state, as the operator's
// storage-backed CR reader does.
type fakeCR struct {
	chis map[string]*api.ClickHouseInstallation
}

func newFakeCR(chis ...*api.ClickHouseInstallation) *fakeCR {
	f := &fakeCR{chis: map[string]*api.ClickHouseInstallation{}}
	for _, chi := range chis {
		f.set(chi)
	}
	return f
}

func (f *fakeCR) set(chi *api.ClickHouseInstallation) {
	f.chis[kubeTypes.NamespacedName{Namespace: chi.Namespace, Name: chi.Name}.String()] = chi
}

func (f *fakeCR) Get(_ context.Context, namespace, name string) (api.ICustomResource, error) {
	// As client-go refuses to build the request
	if name == "" {
		return nil, errEmptyName
	}
	chi, ok := f.chis[kubeTypes.NamespacedName{Namespace: namespace, Name: name}.String()]
	if !ok {
		return nil, apiErrors.NewNotFound(api.Resource("clickhouseinstallations"), name)
	}
	return chi.DeepCopy(), nil
}

func (f *fakeCR) StatusUpdate(context.Context, api.ICustomResource, types.UpdateStatusOptions) error {
	return nil
}

const (
	// testStorageSecret is the storage Secret of the backup-enabled clusters of the tests, without
	// which their backup stays off
	testStorageSecret = "s3-credentials"
	// testStorage is the backup storage of the test installation, whose Secret is testStorageSecret
)

func backupCluster(name string, shards int, enabled bool) *api.Cluster {
	return &api.Cluster{
		Name:   name,
		Layout: &api.ChiClusterLayout{ShardsCount: shards, ReplicasCount: 2},
		Backup: &api.ClusterBackup{
			Enabled:         types.NewStringBool(enabled),
			ConfigSecretRef: &core.LocalObjectReference{Name: testStorageSecret},
		},
	}
}

// lackingCluster is a cluster that turns backup on without a storage, which leaves its backup off.
func lackingCluster(name string, shards int) *api.Cluster {
	cluster := backupCluster(name, shards, true)
	cluster.Backup.ConfigSecretRef = nil
	return cluster
}

// installation returns a CHI whose last reconcile completed with the given clusters, with the
// normalized state the controllers resolve hosts from, in the serialized form it is stored in.
func installation(t *testing.T, status string, clusters ...*api.Cluster) *api.ClickHouseInstallation {
	t.Helper()
	return installationFrom(t, installationSpec(clusters...), status)
}

// installationSpec returns the test CHI with the given clusters, as the user created it.
func installationSpec(clusters ...*api.Cluster) *api.ClickHouseInstallation {
	chi := &api.ClickHouseInstallation{}
	chi.Name = testCHI
	chi.Namespace = testNamespace
	chi.UID = "chi-uid"
	chi.Spec.Configuration = &api.Configuration{Clusters: clusters}
	return chi
}

// installationFrom returns chi with the given status and the normalized state of its last completed
// reconcile, in the serialized form it is stored in.
func installationFrom(t *testing.T, chi *api.ClickHouseInstallation, status string) *api.ClickHouseInstallation {
	t.Helper()
	chi = chi.DeepCopy()
	normalized, err := normalizer.New(nil).CreateTemplated(chi.DeepCopy(), commonNormalizer.NewOptions[api.ClickHouseInstallation]())
	require.NoError(t, err)
	data, err := json.Marshal(normalized)
	require.NoError(t, err)
	stored := &api.ClickHouseInstallation{}
	require.NoError(t, json.Unmarshal(data, stored))

	chi.EnsureStatus().Status = status
	chi.SetAncestor(stored)
	return chi
}

// twoClusters returns an installation whose last reconcile completed with two backup-enabled
// clusters, hot with two shards and cold with one, of two replicas each, and plain without backup.
func twoClusters(t *testing.T, status string) *api.ClickHouseInstallation {
	return installation(t, status, backupCluster("hot", 2, true), backupCluster("cold", 1, true), backupCluster("plain", 1, false))
}

func readyInstallation(t *testing.T) *api.ClickHouseInstallation {
	return installation(t, api.StatusCompleted, backupCluster("main", 2, true))
}

// stoppedInstallations returns a ready installation stopped by its spec, and one whose spec no
// longer stops it while the state of its last completed reconcile still does.
func stoppedInstallations(t *testing.T) map[string]*api.ClickHouseInstallation {
	stopped := readyInstallation(t)
	stopped.Spec.Stop = types.NewStringBool(true)
	starting := readyInstallation(t)
	starting.GetAncestorT().Spec.Stop = types.NewStringBool(true)
	return map[string]*api.ClickHouseInstallation{"stopped": stopped, "still stopped": starting}
}

func newScheme(t *testing.T) *apiMachinery.Scheme {
	t.Helper()
	scheme := apiMachinery.NewScheme()
	require.NoError(t, clientGoScheme.AddToScheme(scheme))
	require.NoError(t, api.AddToScheme(scheme))
	return scheme
}

// newClient returns a fake client that, like the API server, gives every created object a UID:
// ownership checks compare UIDs, and the fake leaves them empty.
func newClient(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) client.WithWatch {
	t.Helper()
	if funcs == nil {
		funcs = &interceptor.Funcs{}
	}
	if funcs.Create == nil {
		// The runs post their Events from goroutines of their own, concurrently with a reconcile
		var created atomic.Int64
		funcs.Create = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(kubeTypes.UID(fmt.Sprintf("created-%d", created.Add(1))))
			}
			return c.Create(ctx, obj, opts...)
		}
	}
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&api.ClickHouseBackup{}, &api.ClickHouseBackupSchedule{}, &api.ClickHouseRestore{}).
		WithObjects(objs...).
		WithInterceptorFuncs(*funcs).
		Build()
}

func request(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: kubeTypes.NamespacedName{Namespace: testNamespace, Name: name}}
}

func key(name string) kubeTypes.NamespacedName {
	return kubeTypes.NamespacedName{Namespace: testNamespace, Name: name}
}

func newBackupCR(name string) *api.ClickHouseBackup {
	chb := &api.ClickHouseBackup{}
	chb.Name = name
	chb.Namespace = testNamespace
	chb.UID = kubeTypes.UID(name + "-uid")
	chb.Spec.ClickHouseInstallation = testCHI
	return chb
}

func newRestoreCR(name string) *api.ClickHouseRestore {
	chr := &api.ClickHouseRestore{}
	chr.Name = name
	chr.Namespace = testNamespace
	chr.UID = kubeTypes.UID(name + "-uid")
	chr.Spec.ClickHouseInstallation = testCHI
	chr.Spec.BackupName = "nightly-20260101-000000"
	return chr
}

func newScheduleCR(name string) *api.ClickHouseBackupSchedule {
	chbs := &api.ClickHouseBackupSchedule{}
	chbs.Name = name
	chbs.Namespace = testNamespace
	chbs.UID = kubeTypes.UID(name + "-uid")
	chbs.Spec.ClickHouseInstallation = testCHI
	chbs.Spec.Schedules = []api.BackupScheduleEntry{{Period: api.BackupPeriodDaily, Time: "03:00", Keep: 1}}
	return chbs
}

// eventMessages returns the messages of the events with the given reason.
func eventMessages(t *testing.T, c client.Client, reason string) []string {
	t.Helper()
	return eventMessagesOfType(t, c, reason, "")
}

// eventMessagesOfType returns the messages of the events with the given reason and type, of any
// type when eventType is empty.
func eventMessagesOfType(t *testing.T, c client.Client, reason, eventType string) []string {
	t.Helper()
	events := &core.EventList{}
	require.NoError(t, c.List(context.Background(), events, client.InNamespace(testNamespace)))
	var messages []string
	for _, event := range events.Items {
		if (event.Reason == reason) && ((eventType == "") || (event.Type == eventType)) {
			messages = append(messages, event.Message)
		}
	}
	return messages
}

// outcome is a value a last-outcome gauge of a cluster of an installation is set to.
type outcome struct {
	namespace string
	chi       string
	cluster   string
	at        int64
}

// outcomes collects what the controllers set the last-outcome gauges to.
type outcomes struct {
	successes []outcome
	failures  []outcome
}

// captureOutcomes collects what the controllers set the last-outcome gauges to for the duration of
// the test.
func captureOutcomes(t *testing.T) *outcomes {
	t.Helper()
	captured := &outcomes{}
	savedSuccess, savedFailure := reportSuccess, reportFailure
	reportSuccess = func(_ context.Context, namespace, chi, cluster string, at time.Time) {
		captured.successes = append(captured.successes, outcome{namespace: namespace, chi: chi, cluster: cluster, at: at.Unix()})
	}
	reportFailure = func(_ context.Context, namespace, chi, cluster string, at time.Time) {
		captured.failures = append(captured.failures, outcome{namespace: namespace, chi: chi, cluster: cluster, at: at.Unix()})
	}
	t.Cleanup(func() {
		reportSuccess, reportFailure = savedSuccess, savedFailure
	})
	return captured
}

// eventCount counts the events with the given reason.
func eventCount(t *testing.T, c client.Client, reason string) int {
	t.Helper()
	events := &core.EventList{}
	require.NoError(t, c.List(context.Background(), events, client.InNamespace(testNamespace)))
	count := 0
	for _, event := range events.Items {
		if event.Reason == reason {
			count++
		}
	}
	return count
}

func condition(t *testing.T, conditions []meta.Condition, conditionType string) meta.Condition {
	t.Helper()
	for _, c := range conditions {
		if c.Type == conditionType {
			return c
		}
	}
	require.Failf(t, "missing condition", "%s", conditionType)
	return meta.Condition{}
}

// watchOnly restricts the operator to the given namespace for the duration of the test.
func watchOnly(t *testing.T, namespace string) {
	t.Helper()
	saved := chop.Config().Watch.Namespaces.Include
	chop.Config().Watch.Namespaces.Include = types.NewStrings([]string{namespace})
	t.Cleanup(func() { chop.Config().Watch.Namespaces.Include = saved })
}

// conflictingStatusUpdates fails the status writes of custom resources with a conflict, as a
// concurrent writer would, while *conflicts is positive, counting each failure down.
func conflictingStatusUpdates(conflicts *int) *interceptor.Funcs {
	return &interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if *conflicts > 0 {
				*conflicts--
				return apiErrors.NewConflict(api.Resource("clickhousebackups"), obj.GetName(), nil)
			}
			return c.SubResource(subResource).Update(ctx, obj, opts...)
		},
	}
}

func getRestore(t *testing.T, c client.Client, name string) *api.ClickHouseRestore {
	t.Helper()
	chr := &api.ClickHouseRestore{}
	require.NoError(t, c.Get(context.Background(), key(name), chr))
	return chr
}

func getBackup(t *testing.T, c client.Client, name string) *api.ClickHouseBackup {
	t.Helper()
	chb := &api.ClickHouseBackup{}
	require.NoError(t, c.Get(context.Background(), key(name), chb))
	return chb
}

func getSchedule(t *testing.T, c client.Client, name string) *api.ClickHouseBackupSchedule {
	t.Helper()
	chbs := &api.ClickHouseBackupSchedule{}
	require.NoError(t, c.Get(context.Background(), key(name), chbs))
	return chbs
}
