package kube

import (
	"context"
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	commonTypes "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop/choptest"
)

const testShardKey = choptest.ShardLabelKey

func init() {
	// statusUpdateProcess() → chop.Config(), so the global chop singleton
	// must be initialized before tests run.
	choptest.EnsureInit()
}

func newCHK(labels map[string]string) *apiChk.ClickHouseKeeperInstallation {
	return &apiChk.ClickHouseKeeperInstallation{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "clickhouse",
			Name:      "test-chk",
			Labels:    labels,
		},
	}
}

// statusUpdateProcess re-reads the CR and publishes status onto whatever it finds,
// adopting the new resourceVersion — so a shard-label flip landing after upstream
// ownership guards (e.g. finalizeCR) would still get status stomped by the stale
// operator. The ownership guard must be evaluated on the same object snapshot whose
// resourceVersion fences the write: a flip visible in the read is skipped here, and a
// flip landing after the read bumps the resourceVersion, so the update conflicts and
// the retry re-runs this check.
func TestStatusUpdateOwnershipGuard(t *testing.T) {
	scheme := apiMachineryRuntime.NewScheme()
	if err := apiChk.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	tests := []struct {
		name         string
		selector     string
		storedLabels map[string]string
		wantWrite    bool
	}{
		{"stored labels flipped away: skip status write", testShardKey + "=stg", map[string]string{testShardKey: "logs"}, false},
		{"stored labels match: status write proceeds", testShardKey + "=stg", map[string]string{testShardKey: "stg"}, true},
		{"unsharded mode: status write proceeds regardless of labels", "", map[string]string{testShardKey: "logs"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.selector != "" {
				choptest.SetWatchLabelSelector(t, tt.selector)
			}

			stored := newCHK(tt.storedLabels)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(stored).WithStatusSubresource(stored).Build()
			c := NewCR(fakeClient)

			// The in-flight reconcile's view of the CR, carrying the status to publish.
			desired := newCHK(map[string]string{testShardKey: "stg"})
			desired.EnsureStatus().CHOpVersion = "test-version"

			err := c.StatusUpdate(context.Background(), desired, commonTypes.UpdateStatusOptions{
				CopyStatusOptions: commonTypes.CopyStatusOptions{
					CopyStatusFieldGroup: commonTypes.CopyStatusFieldGroup{FieldGroupWholeStatus: true},
				},
			})
			if err != nil {
				t.Fatalf("StatusUpdate() returned error: %v, want nil", err)
			}

			after := &apiChk.ClickHouseKeeperInstallation{}
			if err := fakeClient.Get(context.Background(), kubeTypes.NamespacedName{Namespace: "clickhouse", Name: "test-chk"}, after); err != nil {
				t.Fatalf("Get after StatusUpdate failed: %v", err)
			}
			gotWrite := after.Status != nil && after.Status.CHOpVersion == "test-version"
			if gotWrite != tt.wantWrite {
				t.Errorf("status written = %v, want %v", gotWrite, tt.wantWrite)
			}
		})
	}
}

// The key concurrency guarantee of the snapshot-fenced guard: a shard-label flip landing
// AFTER the ownership check's snapshot read must not slip through. The write carries the
// pre-flip resourceVersion, so it conflicts; the retry re-reads the flipped object and
// the guard skips. Without the guard the retry would re-read, copy status onto the
// flipped object and publish it with the fresh resourceVersion — a stale status write.
func TestStatusUpdateConflictRetrySkips(t *testing.T) {
	scheme := apiMachineryRuntime.NewScheme()
	if err := apiChk.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}
	choptest.SetWatchLabelSelector(t, testShardKey+"=stg")

	nn := kubeTypes.NamespacedName{Namespace: "clickhouse", Name: "test-chk"}
	stored := newCHK(map[string]string{testShardKey: "stg"})

	attempts := 0
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(stored).WithStatusSubresource(stored).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if subResourceName == "status" {
					attempts++
					if attempts == 1 {
						// Simulate the race: the shard label flips away between the
						// ownership snapshot read and this status write. The flip bumps
						// the stored resourceVersion, so the delegated update below
						// (carrying the pre-flip resourceVersion) must conflict.
						live := &apiChk.ClickHouseKeeperInstallation{}
						if err := c.Get(ctx, nn, live); err != nil {
							t.Fatalf("interceptor Get failed: %v", err)
						}
						live.Labels = map[string]string{testShardKey: "logs"}
						if err := c.Update(ctx, live); err != nil {
							t.Fatalf("interceptor label flip failed: %v", err)
						}
					}
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).Build()
	c := NewCR(fakeClient)

	desired := newCHK(map[string]string{testShardKey: "stg"})
	desired.EnsureStatus().CHOpVersion = "test-version"

	err := c.StatusUpdate(context.Background(), desired, commonTypes.UpdateStatusOptions{
		CopyStatusOptions: commonTypes.CopyStatusOptions{
			CopyStatusFieldGroup: commonTypes.CopyStatusFieldGroup{FieldGroupWholeStatus: true},
		},
	})
	if err != nil {
		t.Fatalf("StatusUpdate() returned error: %v, want nil (retry must skip cleanly)", err)
	}

	if attempts != 1 {
		t.Errorf("status update attempts = %d, want exactly 1 (first attempt conflicts, retry must skip without a second write)", attempts)
	}

	after := &apiChk.ClickHouseKeeperInstallation{}
	if err := fakeClient.Get(context.Background(), nn, after); err != nil {
		t.Fatalf("Get after StatusUpdate failed: %v", err)
	}
	if after.Status != nil && after.Status.CHOpVersion == "test-version" {
		t.Errorf("stale status was published after the conflict; retry must skip the flipped CR")
	}
}
