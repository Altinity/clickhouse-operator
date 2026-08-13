package chk

import (
	"context"
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	commonTypes "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop/choptest"
)

const testShardKey = choptest.ShardLabelKey

func init() {
	// ShouldEnqueue()/Reconcile() → chop.Config(), so the global chop singleton
	// must be initialized before tests run.
	choptest.EnsureInit()
}

var setWatchLabelSelector = choptest.SetWatchLabelSelector

func newCHK(name string, labels map[string]string) *apiChk.ClickHouseKeeperInstallation {
	return &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "clickhouse",
			Name:      name,
			Labels:    labels,
		},
	}
}

func Test_chkShouldEnqueueWithLabelSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		labels   map[string]string
		want     bool
	}{
		{"no selector enqueues everything (backward compat)", "", map[string]string{testShardKey: "logs"}, true},
		{"no selector enqueues unlabeled", "", nil, true},
		{"shard operator enqueues matching CHK", testShardKey + "=stg", map[string]string{testShardKey: "stg"}, true},
		{"shard operator skips other shard's CHK", testShardKey + "=stg", map[string]string{testShardKey: "logs"}, false},
		{"shard operator skips unlabeled CHK", testShardKey + "=stg", nil, false},
		{"legacy operator enqueues unlabeled CHK", "!" + testShardKey, nil, true},
		{"legacy operator skips shard-labeled CHK", "!" + testShardKey, map[string]string{testShardKey: "stg"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.selector != "" {
				setWatchLabelSelector(t, tt.selector)
			}
			if got := ShouldEnqueue(newCHK("test-chk", tt.labels)); got != tt.want {
				t.Errorf("ShouldEnqueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Reconcile requests from owned StatefulSet changes bypass keeperPredicate(), so the
// post-Get label guard in Reconcile must return cleanly, without mutating the CR.
func Test_reconcilePostGetLabelGuard(t *testing.T) {
	scheme := apiMachineryRuntime.NewScheme()
	if err := apiChk.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	otherShardCHK := newCHK("other-shard-chk", map[string]string{testShardKey: "logs"})
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(otherShardCHK).Build()
	c := &Controller{Client: fakeClient}

	setWatchLabelSelector(t, testShardKey+"=stg")

	result, err := c.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: kubeTypes.NamespacedName{Namespace: "clickhouse", Name: "other-shard-chk"},
	})
	if err != nil {
		t.Fatalf("Reconcile() of non-matching CHK returned error: %v, want clean skip", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("Reconcile() of non-matching CHK requested requeue %+v, want none", result)
	}

	after := &apiChk.ClickHouseKeeperInstallation{}
	if err := fakeClient.Get(context.Background(), kubeTypes.NamespacedName{Namespace: "clickhouse", Name: "other-shard-chk"}, after); err != nil {
		t.Fatalf("Get after Reconcile failed: %v", err)
	}
	if len(after.GetFinalizers()) != 0 {
		t.Errorf("non-matching CHK was mutated (finalizers %v), want untouched", after.GetFinalizers())
	}
}

// The informer cache can lag a shard-label flip: cached labels still match this operator
// while the live object already belongs to another shard. Reconcile must skip cleanly on
// the live re-check without mutating the CR.
func Test_reconcileLiveOwnershipGuard(t *testing.T) {
	scheme := apiMachineryRuntime.NewScheme()
	if err := apiChk.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	cachedCHK := newCHK("flipping-chk", map[string]string{testShardKey: "stg"})
	liveCHK := newCHK("flipping-chk", map[string]string{testShardKey: "logs"})
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cachedCHK).Build()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveCHK).Build()
	c := &Controller{Client: cachedClient, APIReader: liveClient}

	setWatchLabelSelector(t, testShardKey+"=stg")

	result, err := c.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: kubeTypes.NamespacedName{Namespace: "clickhouse", Name: "flipping-chk"},
	})
	if err != nil {
		t.Fatalf("Reconcile() with flipped live labels returned error: %v, want clean skip", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("Reconcile() with flipped live labels requested requeue %+v, want none", result)
	}

	after := &apiChk.ClickHouseKeeperInstallation{}
	if err := cachedClient.Get(context.Background(), kubeTypes.NamespacedName{Namespace: "clickhouse", Name: "flipping-chk"}, after); err != nil {
		t.Fatalf("Get after Reconcile failed: %v", err)
	}
	if len(after.GetFinalizers()) != 0 {
		t.Errorf("flipped CHK was mutated (finalizers %v), want untouched", after.GetFinalizers())
	}
}

// ownsLiveCR guards the purge phase: it must confirm ownership on live labels and treat
// lookup failure (deleted CR) as not-owned.
func Test_ownsLiveCR(t *testing.T) {
	scheme := apiMachineryRuntime.NewScheme()
	if err := apiChk.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		newCHK("stg-chk", map[string]string{testShardKey: "stg"}),
		newCHK("logs-chk", map[string]string{testShardKey: "logs"}),
	).Build()
	c := &Controller{APIReader: fakeClient}

	setWatchLabelSelector(t, testShardKey+"=stg")

	tests := []struct {
		name string
		chk  string
		want bool
	}{
		{"owns matching CHK", "stg-chk", true},
		{"does not own other shard's CHK (flipped away)", "logs-chk", false},
		{"does not own missing CHK (deleted)", "gone-chk", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.ownsLiveCR(context.Background(), "clickhouse", tt.chk); got != tt.want {
				t.Errorf("ownsLiveCR(%s) = %v, want %v", tt.chk, got, tt.want)
			}
		})
	}
}

// finalizeCR guards status writes with a CR fetched through the cached client, which can
// lag a shard-label flip: cached labels still match this operator while the live object
// already belongs to another shard. finalizeCR must confirm ownership on live state and
// skip the status mutation entirely — a stale operator writing status would stomp on the
// operator that now owns the CR.
func Test_finalizeCRLiveOwnershipGuard(t *testing.T) {
	scheme := apiMachineryRuntime.NewScheme()
	if err := apiChk.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	setWatchLabelSelector(t, testShardKey+"=stg")

	tests := []struct {
		name       string
		liveLabels map[string]string
		wantWrite  bool
	}{
		{"live labels flipped away: skip status write", map[string]string{testShardKey: "logs"}, false},
		{"live labels still match: status write proceeds", map[string]string{testShardKey: "stg"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Cached view always shows this operator's shard; live view varies per case.
			cachedCHK := newCHK("flipping-chk", map[string]string{testShardKey: "stg"})
			liveCHK := newCHK("flipping-chk", tt.liveLabels)
			cachedClient := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(cachedCHK).WithStatusSubresource(cachedCHK).Build()
			liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveCHK).Build()

			c := NewController(cachedClient, liveClient, scheme, nil)
			w := c.newWorker()

			mutated := false
			err := w.finalizeCR(context.Background(), cachedCHK, commonTypes.UpdateStatusOptions{}, func(chk *apiChk.ClickHouseKeeperInstallation) {
				mutated = true
			})
			if err != nil {
				t.Fatalf("finalizeCR() returned error: %v, want nil", err)
			}
			if mutated != tt.wantWrite {
				t.Errorf("status mutation callback invoked = %v, want %v", mutated, tt.wantWrite)
			}
		})
	}
}

// A reconcile request for a CHK that no longer exists must return cleanly regardless of selector.
func Test_reconcileNotFoundWithSelector(t *testing.T) {
	scheme := apiMachineryRuntime.NewScheme()
	if err := apiChk.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &Controller{Client: fakeClient}

	setWatchLabelSelector(t, testShardKey+"=stg")

	result, err := c.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: kubeTypes.NamespacedName{Namespace: "clickhouse", Name: "gone-chk"},
	})
	if err != nil {
		t.Fatalf("Reconcile() of missing CHK returned error: %v, want clean return", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("Reconcile() of missing CHK requested requeue %+v, want none", result)
	}
}
