package chi

import (
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"k8s.io/client-go/tools/cache"
)

func init() {
	chop.New(nil, nil, "")
}

func Test_deletedObject(t *testing.T) {
	chi := &api.ClickHouseInstallation{}
	var nilCHI *api.ClickHouseInstallation
	tests := []struct {
		name    string
		obj     interface{}
		want    *api.ClickHouseInstallation
		wantErr bool
	}{
		{
			name: "direct object",
			obj:  chi,
			want: chi,
		},
		{
			name: "tombstone",
			obj:  cache.DeletedFinalStateUnknown{Obj: chi},
			want: chi,
		},
		{
			name:    "unexpected direct object",
			obj:     struct{}{},
			wantErr: true,
		},
		{
			name:    "unexpected tombstone object",
			obj:     cache.DeletedFinalStateUnknown{Obj: struct{}{}},
			wantErr: true,
		},
		{
			name:    "nil tombstone object",
			obj:     cache.DeletedFinalStateUnknown{},
			wantErr: true,
		},
		{
			name:    "typed nil tombstone object",
			obj:     cache.DeletedFinalStateUnknown{Obj: nilCHI},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := deletedObject[api.ClickHouseInstallation](tt.obj)
			if (err != nil) != tt.wantErr {
				t.Fatalf("deletedObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("deletedObject() = %p, want %p", got, tt.want)
			}
		})
	}
}

// deleteHandler is what the informers actually register, so the contract that
// matters is "an undecodable payload must not reach the callback" - a bare
// assertion there panics the whole operator process, which is issue #2050.
func Test_deleteHandler(t *testing.T) {
	chi := &api.ClickHouseInstallation{}
	var nilCHI *api.ClickHouseInstallation
	tests := []struct {
		name       string
		obj        interface{}
		wantCalled bool
	}{
		{
			name:       "direct object reaches the callback",
			obj:        chi,
			wantCalled: true,
		},
		{
			name:       "tombstone is unwrapped and reaches the callback",
			obj:        cache.DeletedFinalStateUnknown{Obj: chi},
			wantCalled: true,
		},
		{
			name: "unexpected direct object is dropped",
			obj:  struct{}{},
		},
		{
			name: "unexpected tombstone object is dropped",
			obj:  cache.DeletedFinalStateUnknown{Obj: struct{}{}},
		},
		{
			name: "nil tombstone object is dropped",
			obj:  cache.DeletedFinalStateUnknown{},
		},
		{
			name: "typed nil tombstone object is dropped",
			obj:  cache.DeletedFinalStateUnknown{Obj: nilCHI},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			var got *api.ClickHouseInstallation
			handler := deleteHandler("test.DeleteFunc", func(deleted *api.ClickHouseInstallation) {
				called = true
				got = deleted
			})

			handler(tt.obj)

			if called != tt.wantCalled {
				t.Fatalf("callback called = %v, want %v", called, tt.wantCalled)
			}
			if tt.wantCalled && (got != chi) {
				t.Errorf("callback received %p, want %p", got, chi)
			}
		})
	}
}

func Test_shouldEnqueue(t *testing.T) {
	// NB: ShouldEnqueue intentionally does NOT pre-filter on Spec.Suspend.
	// The reconciler itself handles suspend (including marking CHI as Aborted when
	// there are pending changes), so the enqueue step must always let suspended
	// CHIs through. See commit 3d2c80334 "dev: generalize enqueue checker" and
	// the "Bug Fix: Suspend sets Aborted when pending changes exist" change.
	// ShouldEnqueue's sole responsibility is the namespace-watched gate.
	tests := []struct {
		name string
		chi  *api.ClickHouseInstallation
		want bool
	}{
		{
			name: "enqueues a non-suspended CHI",
			chi: &api.ClickHouseInstallation{
				Spec: api.ChiSpec{
					Suspend: types.NewStringBool(false),
				},
			},
			want: true,
		},
		{
			name: "enqueues a suspended CHI (reconciler handles suspend, not ShouldEnqueue)",
			chi: &api.ClickHouseInstallation{
				Spec: api.ChiSpec{
					Suspend: types.NewStringBool(true),
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldEnqueue(tt.chi); got != tt.want {
				t.Errorf("ShouldEnqueue() = %v, want %v", got, tt.want)
			}
		})
	}
}
