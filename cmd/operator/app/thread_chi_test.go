package app

import (
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"

	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
)

// The kube informer factory applies chopGeneratedObjectsLabelSelector() server-side. Handlers still apply
// the client-side Controller.isTrackedObject() check, which requires Labeler.IsCHOPGeneratedObject().
// If the two ever disagree, the operator silently stops seeing objects it is supposed to reconcile, so
// pin down both the literal selector and its equivalence to the client-side predicate.

func Test_chopGeneratedObjectsLabelSelector(t *testing.T) {
	want := "clickhouse.altinity.com/app=chop"
	if got := chopGeneratedObjectsLabelSelector(); got != want {
		t.Errorf("chopGeneratedObjectsLabelSelector() = %q, want %q", got, want)
	}
}

func Test_chopGeneratedObjectsLabelSelectorMatchesIsCHOPGeneratedObject(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			name:   "operator-generated object",
			labels: map[string]string{"clickhouse.altinity.com/app": "chop"},
			want:   true,
		},
		{
			name: "operator-generated object among unrelated labels",
			labels: map[string]string{
				"clickhouse.altinity.com/app":  "chop",
				"clickhouse.altinity.com/chi":  "some-chi",
				"kubernetes.io/service-name":   "some-svc",
				"app.kubernetes.io/managed-by": "Helm",
			},
			want: true,
		},
		{
			name:   "no labels at all",
			labels: nil,
			want:   false,
		},
		{
			name:   "unrelated labels only",
			labels: map[string]string{"app": "clickhouse"},
			want:   false,
		},
		{
			name:   "right key, wrong value",
			labels: map[string]string{"clickhouse.altinity.com/app": "not-chop"},
			want:   false,
		},
		{
			name:   "bare app label is not enough",
			labels: map[string]string{"app": "chop"},
			want:   false,
		},
		{
			// CHK-generated objects carry the clickhouse-keeper API group key, and isTrackedObject()
			// uses the CHI labeler, so they are not tracked by the CHI controller either way.
			name:   "keeper-generated object",
			labels: map[string]string{"clickhouse-keeper.altinity.com/app": "chop"},
			want:   false,
		},
	}

	selector, err := k8sLabels.Parse(chopGeneratedObjectsLabelSelector())
	if err != nil {
		t.Fatalf("unable to parse selector: %v", err)
	}
	labeler := chiLabeler.New(nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Server-side: what the informer factory asks the API server for
			if got := selector.Matches(k8sLabels.Set(tt.labels)); got != tt.want {
				t.Errorf("selector.Matches() = %v, want %v", got, tt.want)
			}
			// Client-side: what the event handlers gate on
			if got := labeler.IsCHOPGeneratedObject(&meta.ObjectMeta{Labels: tt.labels}); got != tt.want {
				t.Errorf("IsCHOPGeneratedObject() = %v, want %v", got, tt.want)
			}
		})
	}
}
